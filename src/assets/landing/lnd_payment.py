"""
assets/landing/lnd_payment.py
------------------------------
Dagster asset: lnd_payment

Responsibility:
    Read raw_payment → cast, clean, normalise → write lnd_payment.

Transformations applied:
    amount              cast → DECIMAL(18,2)
    payment_timestamp   parse ISO-8601 → TIMESTAMPTZ (UTC)
    payment_method_type lowercase canonical
    all others          clean string

Deduplication:
    Insert only payment_ids not already in lnd_payment.
    payment events are immutable — no SCD2 needed.

Error handling:
    Cast failures → err_payment with _rejection_reason
"""

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import COL_SOURCE_FILE, COL_LAST_UPDATED_TS
from resources.duckdb_resource import DuckDBResource
from utils.cleaners import parse_timestamp_utc, normalise_category, clean_string
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_LND_PAYMENT = """
CREATE TABLE IF NOT EXISTS lnd_payment (
    payment_id                  VARCHAR       NOT NULL,
    loan_id                     VARCHAR,
    amount                      DECIMAL(18,2),
    payment_timestamp           TIMESTAMPTZ,
    payment_method_type         VARCHAR,
    payment_method_last_four    VARCHAR,
    payment_method_bank         VARCHAR,
    metadata_source             VARCHAR,
    metadata_user_agent         VARCHAR,
    _source_file                VARCHAR,
    _last_updated_ts            TIMESTAMP
)
"""


@asset(
    group_name="landing",
    deps=["raw_payment", "lnd_loan"],
    description="raw_payment → lnd_payment (typed, UTC timestamps, deduped on payment_id)",
)
def lnd_payment(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time      = time.time()
    batch_ts        = datetime.now(timezone.utc)
    batch_date_str  = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="lnd", table="lnd_payment",
        message="Starting lnd_payment transformation from raw_payment",
        batch_date=batch_date_str,
    )

    rows_in         = 0
    rows_inserted   = 0
    rows_skipped    = 0   # already in lnd_payment (dedup)
    rows_rejected   = 0

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_LND_PAYMENT)

        # ------------------------------------------------------------------
        # Read latest batch from raw_payment
        # ------------------------------------------------------------------
        latest_ts = conn.execute(
            f"SELECT MAX({COL_LAST_UPDATED_TS}) FROM raw_payment"
        ).fetchone()[0]

        if latest_ts is None:
            log_event(
                logger, event="checkpoint", layer="lnd", table="lnd_payment",
                message="raw_payment is empty — nothing to process",
                batch_date=batch_date_str, level="WARNING",
            )
            return Output(value={"rows_inserted": 0, "rows_rejected": 0})

        raw_rows = conn.execute(
            f"""
            SELECT payment_id, loan_id, amount, payment_timestamp,
                   payment_method_type, payment_method_last_four,
                   payment_method_bank, metadata_source, metadata_user_agent,
                   {COL_SOURCE_FILE}, {COL_LAST_UPDATED_TS}
            FROM raw_payment
            WHERE {COL_LAST_UPDATED_TS} = ?
            """,
            [latest_ts],
        ).fetchall()

        rows_in = len(raw_rows)
        log_event(
            logger, event="checkpoint", layer="lnd", table="lnd_payment",
            message=f"Read {rows_in} rows from raw_payment batch {latest_ts}",
            rows_in=rows_in, batch_date=batch_date_str,
        )

        # ------------------------------------------------------------------
        # Load existing payment_ids for dedup
        # ------------------------------------------------------------------
        existing_ids = set(
            row[0] for row in conn.execute(
                "SELECT payment_id FROM lnd_payment"
            ).fetchall()
        )

        # ------------------------------------------------------------------
        # Process each raw row
        # ------------------------------------------------------------------
        to_insert = []

        for raw in raw_rows:
            (
                payment_id, loan_id, amount, payment_timestamp,
                payment_method_type, payment_method_last_four,
                payment_method_bank, metadata_source, metadata_user_agent,
                source_file, last_updated_ts,
            ) = raw

            # Dedup check
            if payment_id and payment_id in existing_ids:
                rows_skipped += 1
                continue

            try:
                cleaned = _clean_payment_row(
                    payment_id, loan_id, amount, payment_timestamp,
                    payment_method_type, payment_method_last_four,
                    payment_method_bank, metadata_source, metadata_user_agent,
                )
            except Exception as e:
                rows_rejected += 1
                rejection_reason = f"payment_id={payment_id}: {type(e).__name__}: {e}"
                log_event(
                    logger, event="row_rejected", layer="lnd", table="lnd_payment",
                    message=rejection_reason, batch_date=batch_date_str,
                    level="WARNING",
                )
                conn.execute(
                    """
                    INSERT INTO err_payment
                    SELECT *, ? AS _rejection_reason, ? AS _rejected_at
                    FROM raw_payment
                    WHERE payment_id = ?
                      AND _last_updated_ts = ?
                    LIMIT 1
                    """,
                    [rejection_reason, datetime.now(timezone.utc), payment_id, latest_ts],
                )
                continue

            to_insert.append([
                cleaned["payment_id"],
                cleaned["loan_id"],
                cleaned["amount"],
                cleaned["payment_timestamp"],
                cleaned["payment_method_type"],
                cleaned["payment_method_last_four"],
                cleaned["payment_method_bank"],
                cleaned["metadata_source"],
                cleaned["metadata_user_agent"],
                source_file,
                last_updated_ts,
            ])

        # ------------------------------------------------------------------
        # Insert
        # ------------------------------------------------------------------
        if to_insert:
            conn.executemany(
                "INSERT INTO lnd_payment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute("SELECT COUNT(*) FROM lnd_payment").fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="lnd", table="lnd_payment",
        message=(
            f"lnd_payment complete: {rows_inserted} inserted, "
            f"{rows_skipped} skipped (dedup), {rows_rejected} rejected"
        ),
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "rows_in":          MetadataValue.int(rows_in),
        "rows_inserted":    MetadataValue.int(rows_inserted),
        "rows_skipped":     MetadataValue.int(rows_skipped),
        "rows_rejected":    MetadataValue.int(rows_rejected),
        "total_in_table":   MetadataValue.int(total),
        "duration_sec":     MetadataValue.float(duration),
    })

    return Output(value={
        "rows_in":       rows_in,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_payment_row(
    payment_id, loan_id, amount, payment_timestamp,
    payment_method_type, payment_method_last_four,
    payment_method_bank, metadata_source, metadata_user_agent,
) -> dict:
    """
    Cast and clean a single raw payment row.
    Raises ValueError for any field that cannot be cast.
    """
    if not payment_id or not str(payment_id).strip():
        raise ValueError("payment_id is empty")

    try:
        cleaned_amount = float(amount) if amount else None
        if cleaned_amount is not None and cleaned_amount < 0:
            raise ValueError(f"Negative amount: {amount!r}")
    except ValueError as e:
        raise ValueError(f"Cannot cast amount: {amount!r} — {e}")

    cleaned_ts = parse_timestamp_utc(payment_timestamp)

    return {
        "payment_id":               clean_string(payment_id),
        "loan_id":                  clean_string(loan_id),
        "amount":                   cleaned_amount,
        "payment_timestamp":        cleaned_ts,
        "payment_method_type":      normalise_category(payment_method_type),
        "payment_method_last_four": clean_string(payment_method_last_four),
        "payment_method_bank":      clean_string(payment_method_bank),
        "metadata_source":          clean_string(metadata_source),
        "metadata_user_agent":      clean_string(metadata_user_agent),
    }
