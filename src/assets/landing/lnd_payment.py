"""
assets/landing/lnd_payment.py
------------------------------
Dagster asset: lnd_payment
Schema: hlx_{ENV}_lnd

Reads raw_payment → clean, type, dedup → lnd_payment.

Grain: one row per (payment_id + amount + payment_timestamp) combination.
    - True duplicates (same id + amount + timestamp) → deduped, one kept
    - Same payment_id, different amount → KEPT (split settlement / fee deduction)
    - Same payment_id, different timestamp → KEPT (delayed instalment)

Surrogate key lnd_payment_sk added for downstream joins.
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
    TBL_RAW_LOAN, TBL_RAW_PAYMENT,
    TBL_LND_LOAN, TBL_LND_ERR_LOAN,
    TBL_LND_PAYMENT, TBL_LND_ERR_PAYMENT, SCHEMA_LND,
)
from resources.duckdb_resource import DuckDBResource
from utils.cleaners import parse_timestamp_utc, normalise_category, clean_string
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="landing",
    deps=["lnd_loan"],
    description=(
        f"raw_payment → {TBL_LND_PAYMENT} "
        f"(typed, UTC ts, deduped on payment_id+amount+timestamp)"
    ),
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
    rows_split_settlement = 0  # same payment_id, different amount — kept

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_LND}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_PAYMENT} (
                lnd_payment_sk            BIGINT,
                payment_id                VARCHAR       NOT NULL,
                loan_id                   VARCHAR,
                amount                    DECIMAL(18,2),
                payment_timestamp         TIMESTAMPTZ,
                payment_method_type       VARCHAR,
                payment_method_last_four  VARCHAR,
                payment_method_bank       VARCHAR,
                metadata_source           VARCHAR,
                metadata_user_agent       VARCHAR,
                payment_allocation_status VARCHAR,
                _source_file              VARCHAR,
                _last_updated_ts          TIMESTAMP
            )""")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_ERR_PAYMENT} (
                payment_id                VARCHAR,
                loan_id                   VARCHAR,
                amount                    VARCHAR,
                payment_timestamp         VARCHAR,
                payment_method_type       VARCHAR,
                payment_method_last_four  VARCHAR,
                payment_method_bank       VARCHAR,
                metadata_source           VARCHAR,
                metadata_user_agent       VARCHAR,
                _source_file              VARCHAR,
                _last_updated_ts          TIMESTAMP,
                _rejection_reason         VARCHAR,
                _rejected_at              TIMESTAMP
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

        # ------------------------------------------------------------------
        # Intra-batch dedup on composite key (payment_id + amount + timestamp)
        # Same id + different amount → KEEP BOTH (split settlement)
        # Same id + same amount + same timestamp → KEEP ONE (true duplicate)
        # ------------------------------------------------------------------
        seen_composite = set()   # (payment_id, amount, payment_timestamp)
        seen_payment_ids = set() # track which payment_ids have multiple amounts

        deduplicated = []
        for row in raw_rows:
            pid, _, amt, ts_raw = row[0], row[1], row[2], row[3]
            composite_key = (pid, amt, ts_raw)

            if composite_key in seen_composite:
                rows_skipped += 1  # true duplicate — skip
                continue

            seen_composite.add(composite_key)

            # Track split settlements for logging
            if pid and pid in seen_payment_ids:
                rows_split_settlement += 1
            if pid:
                seen_payment_ids.add(pid)

            deduplicated.append(row)

        log_event(
            logger, event="checkpoint", layer="lnd", table=TBL_LND_PAYMENT,
            message=(
                f"Batch dedup: {rows_in} raw rows → {len(deduplicated)} unique "
                f"({rows_skipped} true duplicates removed, "
                f"{rows_split_settlement} split settlement rows kept)"
            ),
            rows_in=rows_in, batch_date=batch_date_str,
        )

        # ------------------------------------------------------------------
        # Cross-batch dedup — exclude composite keys already in lnd_payment
        # ------------------------------------------------------------------
        existing_composites = set()
        for row in conn.execute(
            f"""SELECT payment_id, CAST(amount AS VARCHAR),
                       CAST(payment_timestamp AS VARCHAR)
                FROM {TBL_LND_PAYMENT}"""
        ).fetchall():
            existing_composites.add((row[0], row[1], row[2]))

        # ------------------------------------------------------------------
        # Get current max surrogate key for sequence
        # ------------------------------------------------------------------
        max_sk = conn.execute(
            f"SELECT COALESCE(MAX(lnd_payment_sk), 0) FROM {TBL_LND_PAYMENT}"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # Clean and insert
        # ------------------------------------------------------------------
        to_insert = []
        sk = max_sk

        for raw in deduplicated:
            (payment_id, loan_id, amount, payment_timestamp,
             payment_method_type, payment_method_last_four,
             payment_method_bank, metadata_source, metadata_user_agent,
             source_file, last_updated_ts) = raw

            if not (payment_id and str(payment_id).strip()):
                rows_rejected += 1
                reason = "payment_id is empty"
                log_event(logger, event="row_rejected", layer="lnd",
                          table=TBL_LND_PAYMENT, message=reason,
                          batch_date=batch_date_str, level="WARNING")
                conn.execute(
                    f"""INSERT INTO {TBL_LND_ERR_PAYMENT}
                        SELECT *, ? AS _rejection_reason, ? AS _rejected_at
                        FROM {TBL_RAW_PAYMENT}
                        WHERE payment_id IS NULL
                          AND {COL_LAST_UPDATED_TS} = ? LIMIT 1""",
                    [reason, datetime.now(timezone.utc), latest_ts],
                )
                continue

            try:
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

            # Cross-batch dedup check on cleaned composite key
            ts_str = str(ts) if ts else None
            amt_str = str(amt) if amt is not None else None
            composite = (payment_id, amt_str, ts_str)

            if composite in existing_composites:
                rows_skipped += 1
                continue

            sk += 1
            # Determine payment allocation status
            alloc_status = _get_allocation_status(
                conn, clean_string(payment_id),
                clean_string(loan_id)
            )

            to_insert.append([
                sk,
                clean_string(payment_id),
                clean_string(loan_id),
                amt, ts,
                normalise_category(payment_method_type),
                clean_string(payment_method_last_four),
                clean_string(payment_method_bank),
                clean_string(metadata_source),
                clean_string(metadata_user_agent),
                alloc_status,
                source_file, last_updated_ts,
            ])
            existing_composites.add(composite)

        if to_insert:
            conn.executemany(
                f"INSERT INTO {TBL_LND_PAYMENT} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_LND_PAYMENT}"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(
        logger, event="load_end", layer="lnd", table=TBL_LND_PAYMENT,
        message=(
            f"lnd_payment complete: {rows_inserted} inserted, "
            f"{rows_skipped} skipped (dedup), {rows_rejected} rejected, "
            f"{rows_split_settlement} split settlement rows kept"
        ),
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, batch_date=batch_date_str,
    )
    context.add_output_metadata({
        "rows_in":              MetadataValue.int(rows_in),
        "rows_inserted":        MetadataValue.int(rows_inserted),
        "rows_skipped":         MetadataValue.int(rows_skipped),
        "rows_rejected":        MetadataValue.int(rows_rejected),
        "split_settlement_rows":MetadataValue.int(rows_split_settlement),
        "total_in_table":       MetadataValue.int(total),
        "duration_sec":         MetadataValue.float(duration),
        "table":                MetadataValue.text(TBL_LND_PAYMENT),
    })
    return Output(value={
        "rows_in": rows_in, "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "split_settlement_rows": rows_split_settlement,
    })


def _get_allocation_status(conn, payment_id: str, loan_id) -> str:
    """
    Determine payment allocation status.

    allocated     : loan_id exists and is clean in lnd_loan
    unallocated   : loan_id not in raw_loan at all (pre-loan, cross-system)
    loan_rejected : loan_id in raw_loan but rejected during cleaning (lnd_err_loan)
    unidentified  : loan_id is NULL — no loan reference in source
    """
    if loan_id is None:
        return "unidentified"

    # Check if loan is clean in lnd_loan
    in_lnd = conn.execute(
        f"SELECT COUNT(*) FROM {TBL_LND_LOAN} WHERE loan_id = ? AND is_current_flag = TRUE",
        [loan_id]
    ).fetchone()[0]
    if in_lnd > 0:
        return "allocated"

    # Check if loan exists in raw_loan (was it ever seen?)
    in_raw = conn.execute(
        f"SELECT COUNT(*) FROM {TBL_RAW_LOAN} WHERE loan_id = ?",
        [loan_id]
    ).fetchone()[0]
    if in_raw == 0:
        return "unallocated"

    # Loan exists in raw but not in lnd — must have been rejected
    return "loan_rejected"
