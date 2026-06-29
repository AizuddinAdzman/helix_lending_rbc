"""
assets/landing/lnd_payment.py
------------------------------
Dagster asset: lnd_payment
Schema: hlx_{ENV}_lnd

Reads raw_payment → clean, type, dedup → lnd_payment.
Rejections → lnd_err_payment.
Sequential after lnd_loan.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    COL_LAST_UPDATED_TS,
    TBL_RAW_PAYMENT, TBL_LND_PAYMENT, TBL_LND_ERR_PAYMENT, SCHEMA_LND,
)
from resources.duckdb_resource import DuckDBResource
from utils.cleaners import parse_timestamp_utc, normalise_category, clean_string
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="landing",
    deps=["lnd_loan"],
    description=f"raw_payment → {TBL_LND_PAYMENT} (typed, UTC ts, deduped on payment_id)",
)
def lnd_payment(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="lnd", table=TBL_LND_PAYMENT,
        message="Starting lnd_payment transformation",
        batch_date=batch_date_str,
    )

    rows_in = rows_inserted = rows_skipped = rows_rejected = 0

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_LND}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_PAYMENT} (
                payment_id VARCHAR NOT NULL, loan_id VARCHAR,
                amount DECIMAL(18,2), payment_timestamp TIMESTAMPTZ,
                payment_method_type VARCHAR, payment_method_last_four VARCHAR,
                payment_method_bank VARCHAR, metadata_source VARCHAR,
                metadata_user_agent VARCHAR, _source_file VARCHAR,
                _last_updated_ts TIMESTAMP
            )""")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_ERR_PAYMENT} (
                payment_id VARCHAR, loan_id VARCHAR, amount VARCHAR,
                payment_timestamp VARCHAR, payment_method_type VARCHAR,
                payment_method_last_four VARCHAR, payment_method_bank VARCHAR,
                metadata_source VARCHAR, metadata_user_agent VARCHAR,
                _source_file VARCHAR, _last_updated_ts TIMESTAMP,
                _rejection_reason VARCHAR, _rejected_at TIMESTAMP
            )""")

        latest_ts = conn.execute(
            f"SELECT MAX({COL_LAST_UPDATED_TS}) FROM {TBL_RAW_PAYMENT}"
        ).fetchone()[0]

        if latest_ts is None:
            log_event(logger, event="checkpoint", layer="lnd",
                      table=TBL_LND_PAYMENT,
                      message="raw_payment is empty", batch_date=batch_date_str,
                      level="WARNING")
            return Output(value={"rows_inserted": 0, "rows_rejected": 0})

        raw_rows = conn.execute(
            f"""SELECT payment_id, loan_id, amount, payment_timestamp,
                       payment_method_type, payment_method_last_four,
                       payment_method_bank, metadata_source, metadata_user_agent,
                       _source_file, _last_updated_ts
                FROM {TBL_RAW_PAYMENT}
                WHERE {COL_LAST_UPDATED_TS} = ?""",
            [latest_ts],
        ).fetchall()

        rows_in = len(raw_rows)

        existing_ids = {
            row[0] for row in conn.execute(
                f"SELECT payment_id FROM {TBL_LND_PAYMENT}"
            ).fetchall()
        }

        to_insert = []

        for raw in raw_rows:
            (payment_id, loan_id, amount, payment_timestamp,
             payment_method_type, payment_method_last_four,
             payment_method_bank, metadata_source, metadata_user_agent,
             source_file, last_updated_ts) = raw

            if payment_id and payment_id in existing_ids:
                rows_skipped += 1
                continue

            try:
                if not (payment_id and str(payment_id).strip()):
                    raise ValueError("payment_id is empty")
                amt = float(amount) if amount else None
                if amt is not None and amt < 0:
                    raise ValueError(f"Negative amount: {amount!r}")
                ts = parse_timestamp_utc(payment_timestamp)
            except Exception as e:
                rows_rejected += 1
                reason = f"payment_id={payment_id}: {type(e).__name__}: {e}"
                log_event(logger, event="row_rejected", layer="lnd",
                          table=TBL_LND_PAYMENT, message=reason,
                          batch_date=batch_date_str, level="WARNING")
                conn.execute(
                    f"""INSERT INTO {TBL_LND_ERR_PAYMENT}
                        SELECT *, ? AS _rejection_reason, ? AS _rejected_at
                        FROM {TBL_RAW_PAYMENT}
                        WHERE payment_id = ? AND {COL_LAST_UPDATED_TS} = ? LIMIT 1""",
                    [reason, datetime.now(timezone.utc), payment_id, latest_ts],
                )
                continue

            to_insert.append([
                clean_string(payment_id), clean_string(loan_id),
                amt, ts,
                normalise_category(payment_method_type),
                clean_string(payment_method_last_four),
                clean_string(payment_method_bank),
                clean_string(metadata_source),
                clean_string(metadata_user_agent),
                source_file, last_updated_ts,
            ])
            existing_ids.add(payment_id)

        if to_insert:
            conn.executemany(
                f"INSERT INTO {TBL_LND_PAYMENT} VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_LND_PAYMENT}"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(
        logger, event="load_end", layer="lnd", table=TBL_LND_PAYMENT,
        message=(f"lnd_payment complete: {rows_inserted} inserted, "
                 f"{rows_skipped} skipped (dedup), {rows_rejected} rejected"),
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, batch_date=batch_date_str,
    )
    context.add_output_metadata({
        "rows_in":        MetadataValue.int(rows_in),
        "rows_inserted":  MetadataValue.int(rows_inserted),
        "rows_skipped":   MetadataValue.int(rows_skipped),
        "rows_rejected":  MetadataValue.int(rows_rejected),
        "total_in_table": MetadataValue.int(total),
        "duration_sec":   MetadataValue.float(duration),
        "table":          MetadataValue.text(TBL_LND_PAYMENT),
    })
    return Output(value={"rows_in": rows_in, "rows_inserted": rows_inserted,
                         "rows_rejected": rows_rejected})
