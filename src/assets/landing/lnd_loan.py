"""
assets/landing/lnd_loan.py
---------------------------
Dagster asset: lnd_loan
Schema: hlx_{ENV}_lnd

Reads raw_loan → clean, type, SCD2 → lnd_loan.
Rejections → lnd_err_loan.
Sequential after raw_payment.
"""

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    COL_LAST_UPDATED_TS, SCD2_OPEN_DATE,
    VALID_PRODUCT_TYPES, VALID_LOAN_STATUSES, VALID_LOAN_CHANNELS,
    TBL_RAW_LOAN, TBL_LND_LOAN, TBL_LND_ERR_LOAN, SCHEMA_LND,
)
from resources.duckdb_resource import DuckDBResource
from utils.cleaners import (
    clean_principal_amount, parse_date,
    normalise_category, clean_string,
)
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

HASH_COLUMNS = [
    "customer_id", "product_type", "principal_amount", "interest_rate",
    "term_months", "origination_date", "origination_channel", "status",
    "borrower_info",
]


@asset(
    group_name="landing",
    deps=["raw_payment"],
    description=f"raw_loan → {TBL_LND_LOAN} (typed, cleaned, SCD2)",
)
def lnd_loan(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="lnd", table=TBL_LND_LOAN,
        message="Starting lnd_loan transformation",
        batch_date=batch_date_str,
    )

    rows_in = rows_inserted = rows_updated = rows_skipped = rows_rejected = 0

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_LND}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_LOAN} (
                loan_id VARCHAR NOT NULL, customer_id VARCHAR,
                product_type VARCHAR, principal_amount DECIMAL(18,2),
                interest_rate DECIMAL(8,4), term_months INTEGER,
                origination_date DATE, origination_channel VARCHAR,
                status VARCHAR, borrower_info VARCHAR,
                row_effective_from TIMESTAMP NOT NULL,
                row_effective_to DATE NOT NULL,
                is_current_flag BOOLEAN NOT NULL,
                _source_file VARCHAR, _last_updated_ts TIMESTAMP,
                _row_hash VARCHAR
            )""")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_ERR_LOAN} (
                loan_id VARCHAR, customer_id VARCHAR, product_type VARCHAR,
                principal_amount VARCHAR, interest_rate VARCHAR,
                term_months VARCHAR, origination_date VARCHAR,
                origination_channel VARCHAR, status VARCHAR,
                borrower_info VARCHAR, _source_file VARCHAR,
                _last_updated_ts TIMESTAMP,
                _rejection_reason VARCHAR, _rejected_at TIMESTAMP
            )""")

        latest_ts = conn.execute(
            f"SELECT MAX({COL_LAST_UPDATED_TS}) FROM {TBL_RAW_LOAN}"
        ).fetchone()[0]

        if latest_ts is None:
            log_event(logger, event="checkpoint", layer="lnd", table=TBL_LND_LOAN,
                      message="raw_loan is empty", batch_date=batch_date_str, level="WARNING")
            return Output(value={"rows_inserted": 0, "rows_rejected": 0})

        raw_rows = conn.execute(
            f"""SELECT loan_id, customer_id, product_type, principal_amount,
                       interest_rate, term_months, origination_date,
                       origination_channel, status, borrower_info,
                       _source_file, _last_updated_ts
                FROM {TBL_RAW_LOAN}
                WHERE {COL_LAST_UPDATED_TS} = ?""",
            [latest_ts],
        ).fetchall()

        rows_in = len(raw_rows)

        # Intra-batch dedup — last occurrence per loan_id wins
        seen = {}
        for row in raw_rows:
            key = row[0] if row[0] else id(row)
            seen[key] = row
        raw_rows = list(seen.values())
        rows_deduped = rows_in - len(raw_rows)

        log_event(
            logger, event="checkpoint", layer="lnd", table=TBL_LND_LOAN,
            message=f"Read {rows_in} rows, {rows_deduped} intra-batch duplicates removed",
            rows_in=rows_in, batch_date=batch_date_str,
        )

        existing = {
            row[0]: row[1]
            for row in conn.execute(
                f"SELECT loan_id, _row_hash FROM {TBL_LND_LOAN} WHERE is_current_flag = TRUE"
            ).fetchall()
        }

        to_insert = []
        to_close  = []

        for raw in raw_rows:
            (loan_id, customer_id, product_type, principal_amount,
             interest_rate, term_months, origination_date,
             origination_channel, status, borrower_info,
             source_file, last_updated_ts) = raw

            try:
                cleaned = _clean_row(
                    loan_id, customer_id, product_type, principal_amount,
                    interest_rate, term_months, origination_date,
                    origination_channel, status, borrower_info,
                )
            except Exception as e:
                rows_rejected += 1
                reason = f"loan_id={loan_id}: {type(e).__name__}: {e}"
                log_event(logger, event="row_rejected", layer="lnd",
                          table=TBL_LND_LOAN, message=reason,
                          batch_date=batch_date_str, level="WARNING")
                conn.execute(
                    f"""INSERT INTO {TBL_LND_ERR_LOAN}
                        SELECT *, ? AS _rejection_reason, ? AS _rejected_at
                        FROM {TBL_RAW_LOAN}
                        WHERE loan_id = ? AND {COL_LAST_UPDATED_TS} = ? LIMIT 1""",
                    [reason, datetime.now(timezone.utc), loan_id, latest_ts],
                )
                continue

            row_hash = _hash(cleaned)

            if loan_id not in existing:
                to_insert.append(_row(cleaned, row_hash, batch_ts, source_file,
                                      last_updated_ts, True))
            elif existing[loan_id] != row_hash:
                to_close.append(loan_id)
                to_insert.append(_row(cleaned, row_hash, batch_ts, source_file,
                                      last_updated_ts, True))
            else:
                rows_skipped += 1

        if to_close:
            placeholders = ",".join(["?" for _ in to_close])
            conn.execute(
                f"""UPDATE {TBL_LND_LOAN}
                    SET is_current_flag = FALSE, row_effective_to = ?
                    WHERE loan_id IN ({placeholders}) AND is_current_flag = TRUE""",
                [batch_date_str] + to_close,
            )
            rows_updated = len(to_close)
            log_event(logger, event="checkpoint", layer="lnd", table=TBL_LND_LOAN,
                      message=f"SCD2: closed {rows_updated} changed rows",
                      batch_date=batch_date_str)

        if to_insert:
            conn.executemany(
                f"INSERT INTO {TBL_LND_LOAN} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute(f"SELECT COUNT(*) FROM {TBL_LND_LOAN}").fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(
        logger, event="load_end", layer="lnd", table=TBL_LND_LOAN,
        message=(f"lnd_loan complete: {rows_inserted} inserted, "
                 f"{rows_updated} SCD2 closes, {rows_skipped} unchanged, "
                 f"{rows_rejected} rejected"),
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, batch_date=batch_date_str,
    )
    context.add_output_metadata({
        "rows_in":          MetadataValue.int(rows_in),
        "rows_inserted":    MetadataValue.int(rows_inserted),
        "rows_scd2_closed": MetadataValue.int(rows_updated),
        "rows_skipped":     MetadataValue.int(rows_skipped),
        "rows_rejected":    MetadataValue.int(rows_rejected),
        "total_in_table":   MetadataValue.int(total),
        "duration_sec":     MetadataValue.float(duration),
        "table":            MetadataValue.text(TBL_LND_LOAN),
    })
    return Output(value={"rows_in": rows_in, "rows_inserted": rows_inserted,
                         "rows_rejected": rows_rejected})


def _clean_row(loan_id, customer_id, product_type, principal_amount,
               interest_rate, term_months, origination_date,
               origination_channel, status, borrower_info) -> dict:
    if not (loan_id and str(loan_id).strip()):
        raise ValueError("loan_id is empty")
    try:
        rate = float(interest_rate) if interest_rate else None
    except ValueError:
        raise ValueError(f"Cannot cast interest_rate: {interest_rate!r}")
    try:
        term = int(term_months) if term_months else None
    except ValueError:
        raise ValueError(f"Cannot cast term_months: {term_months!r}")
    return {
        "loan_id":             clean_string(loan_id),
        "customer_id":         clean_string(customer_id),
        "product_type":        normalise_category(product_type),
        "principal_amount":    clean_principal_amount(principal_amount),
        "interest_rate":       rate,
        "term_months":         term,
        "origination_date":    parse_date(origination_date),
        "origination_channel": normalise_category(origination_channel),
        "status":              normalise_category(status),
        "borrower_info":       clean_string(borrower_info),
    }


def _hash(cleaned: dict) -> str:
    val = "|".join(str(cleaned.get(c)) for c in HASH_COLUMNS)
    return hashlib.md5(val.encode()).hexdigest()


def _row(cleaned, row_hash, batch_ts, source_file, last_updated_ts,
         is_current) -> list:
    return [
        cleaned["loan_id"], cleaned["customer_id"], cleaned["product_type"],
        cleaned["principal_amount"], cleaned["interest_rate"],
        cleaned["term_months"], cleaned["origination_date"],
        cleaned["origination_channel"], cleaned["status"],
        cleaned["borrower_info"],
        batch_ts, SCD2_OPEN_DATE, is_current,
        source_file, last_updated_ts, row_hash,
    ]
