"""
assets/landing/lnd_loan.py
---------------------------
Dagster asset: lnd_loan

Responsibility:
    Read raw_loan → cast, clean, normalise → write lnd_loan with SCD2.

Transformations applied:
    principal_amount    strip $, commas → DECIMAL
    origination_date    multi-format parse → DATE
    interest_rate       cast → DECIMAL
    term_months         cast → INTEGER
    product_type        lowercase canonical
    status              lowercase canonical
    origination_channel lowercase canonical
    borrower_info       kept as VARCHAR (flattened at dim_customer)

SCD2 logic:
    - Hash business columns to detect changes
    - New loan_id        → INSERT (row_effective_from=batch_ts, row_effective_to=9999-12-31, is_current=TRUE)
    - Existing, changed  → close old row (row_effective_to=batch_ts, is_current=FALSE)
                           INSERT new row
    - Existing, unchanged → skip

Error handling:
    Cast failures → err_loan with _rejection_reason, pipeline continues
"""

import hashlib
import time
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    COL_SOURCE_FILE, COL_LAST_UPDATED_TS, SCD2_OPEN_DATE,
    VALID_PRODUCT_TYPES, VALID_LOAN_STATUSES, VALID_LOAN_CHANNELS,
)
from resources.duckdb_resource import DuckDBResource
from utils.cleaners import clean_principal_amount, parse_date, normalise_category, clean_string
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_LND_LOAN = f"""
CREATE TABLE IF NOT EXISTS lnd_loan (
    loan_id                 VARCHAR       NOT NULL,
    customer_id             VARCHAR,
    product_type            VARCHAR,
    principal_amount        DECIMAL(18,2),
    interest_rate           DECIMAL(8,4),
    term_months             INTEGER,
    origination_date        DATE,
    origination_channel     VARCHAR,
    status                  VARCHAR,
    borrower_info           VARCHAR,
    row_effective_from      TIMESTAMP     NOT NULL,
    row_effective_to        DATE          NOT NULL,
    is_current_flag         BOOLEAN       NOT NULL,
    _source_file            VARCHAR,
    _last_updated_ts        TIMESTAMP,
    _row_hash               VARCHAR
)
"""

# Business columns included in change hash
HASH_COLUMNS = [
    "customer_id", "product_type", "principal_amount", "interest_rate",
    "term_months", "origination_date", "origination_channel", "status",
    "borrower_info",
]


@asset(
    group_name="landing",
    deps=["raw_loan"],
    description="raw_loan → lnd_loan (typed, cleaned, SCD2)",
)
def lnd_loan(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time      = time.time()
    batch_ts        = datetime.now(timezone.utc)
    batch_date_str  = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="lnd", table="lnd_loan",
        message="Starting lnd_loan transformation from raw_loan",
        batch_date=batch_date_str,
    )

    rows_in         = 0
    rows_inserted   = 0
    rows_updated    = 0   # SCD2 close operations
    rows_skipped    = 0   # unchanged
    rows_rejected   = 0

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_LND_LOAN)

        # ------------------------------------------------------------------
        # Read latest batch from raw_loan
        # ------------------------------------------------------------------
        latest_ts = conn.execute(
            f"SELECT MAX({COL_LAST_UPDATED_TS}) FROM raw_loan"
        ).fetchone()[0]

        if latest_ts is None:
            log_event(
                logger, event="checkpoint", layer="lnd", table="lnd_loan",
                message="raw_loan is empty — nothing to process",
                batch_date=batch_date_str, level="WARNING",
            )
            return Output(value={"rows_inserted": 0, "rows_rejected": 0})

        raw_rows = conn.execute(
            f"""
            SELECT loan_id, customer_id, product_type, principal_amount,
                   interest_rate, term_months, origination_date,
                   origination_channel, status, borrower_info,
                   {COL_SOURCE_FILE}, {COL_LAST_UPDATED_TS}
            FROM raw_loan
            WHERE {COL_LAST_UPDATED_TS} = ?
            """,
            [latest_ts],
        ).fetchall()

        rows_in = len(raw_rows)
        log_event(
            logger, event="checkpoint", layer="lnd", table="lnd_loan",
            message=f"Read {rows_in} rows from raw_loan batch {latest_ts}",
            rows_in=rows_in, batch_date=batch_date_str,
        )

        # ------------------------------------------------------------------
        # Load existing current rows for SCD2 comparison
        # ------------------------------------------------------------------
        existing = {}
        for row in conn.execute(
            "SELECT loan_id, _row_hash FROM lnd_loan WHERE is_current_flag = TRUE"
        ).fetchall():
            existing[row[0]] = row[1]

        # ------------------------------------------------------------------
        # Process each raw row
        # ------------------------------------------------------------------
        to_insert   = []
        to_close    = []

        for raw in raw_rows:
            (
                loan_id, customer_id, product_type, principal_amount,
                interest_rate, term_months, origination_date,
                origination_channel, status, borrower_info,
                source_file, last_updated_ts,
            ) = raw

            try:
                cleaned = _clean_loan_row(
                    loan_id, customer_id, product_type, principal_amount,
                    interest_rate, term_months, origination_date,
                    origination_channel, status, borrower_info,
                )
            except Exception as e:
                rows_rejected += 1
                rejection_reason = f"loan_id={loan_id}: {type(e).__name__}: {e}"
                log_event(
                    logger, event="row_rejected", layer="lnd", table="lnd_loan",
                    message=rejection_reason, batch_date=batch_date_str,
                    level="WARNING",
                )
                conn.execute(
                    """
                    INSERT INTO err_loan
                    SELECT *, ? AS _rejection_reason, ? AS _rejected_at
                    FROM raw_loan
                    WHERE loan_id = ?
                      AND _last_updated_ts = ?
                    LIMIT 1
                    """,
                    [rejection_reason, datetime.now(timezone.utc), loan_id, latest_ts],
                )
                continue

            row_hash = _compute_hash(cleaned)

            if loan_id not in existing:
                # New loan → insert
                to_insert.append(_build_insert_row(
                    cleaned, row_hash, batch_ts, source_file, last_updated_ts,
                    is_current=True,
                ))
            elif existing[loan_id] != row_hash:
                # Changed loan → close old, insert new
                to_close.append(loan_id)
                to_insert.append(_build_insert_row(
                    cleaned, row_hash, batch_ts, source_file, last_updated_ts,
                    is_current=True,
                ))
            else:
                # Unchanged → skip
                rows_skipped += 1

        # ------------------------------------------------------------------
        # Apply SCD2 closes
        # ------------------------------------------------------------------
        if to_close:
            placeholders = ",".join(["?" for _ in to_close])
            conn.execute(
                f"""
                UPDATE lnd_loan
                SET is_current_flag  = FALSE,
                    row_effective_to = ?
                WHERE loan_id IN ({placeholders})
                  AND is_current_flag = TRUE
                """,
                [batch_date_str] + to_close,
            )
            rows_updated = len(to_close)
            log_event(
                logger, event="checkpoint", layer="lnd", table="lnd_loan",
                message=f"SCD2: closed {rows_updated} changed loan rows",
                batch_date=batch_date_str,
            )

        # ------------------------------------------------------------------
        # Insert new / changed rows
        # ------------------------------------------------------------------
        if to_insert:
            conn.executemany(
                """
                INSERT INTO lnd_loan VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                )
                """,
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute("SELECT COUNT(*) FROM lnd_loan").fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="lnd", table="lnd_loan",
        message=(
            f"lnd_loan complete: {rows_inserted} inserted, "
            f"{rows_updated} SCD2 closes, {rows_skipped} unchanged, "
            f"{rows_rejected} rejected"
        ),
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
    })

    return Output(value={
        "rows_in":       rows_in,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_loan_row(
    loan_id, customer_id, product_type, principal_amount,
    interest_rate, term_months, origination_date,
    origination_channel, status, borrower_info,
) -> dict:
    """
    Cast and clean a single raw loan row.
    Raises ValueError for any field that cannot be cast.
    """
    if not loan_id or not loan_id.strip():
        raise ValueError("loan_id is empty")

    cleaned_principal = clean_principal_amount(principal_amount)
    cleaned_date      = parse_date(origination_date)

    try:
        cleaned_rate = float(interest_rate) if interest_rate else None
    except ValueError:
        raise ValueError(f"Cannot cast interest_rate: {interest_rate!r}")

    try:
        cleaned_term = int(term_months) if term_months else None
    except ValueError:
        raise ValueError(f"Cannot cast term_months: {term_months!r}")

    cleaned_product = normalise_category(product_type)
    cleaned_status  = normalise_category(status)
    cleaned_channel = normalise_category(origination_channel)

    return {
        "loan_id":              clean_string(loan_id),
        "customer_id":          clean_string(customer_id),
        "product_type":         cleaned_product,
        "principal_amount":     cleaned_principal,
        "interest_rate":        cleaned_rate,
        "term_months":          cleaned_term,
        "origination_date":     cleaned_date,
        "origination_channel":  cleaned_channel,
        "status":               cleaned_status,
        "borrower_info":        clean_string(borrower_info),
    }


def _compute_hash(cleaned: dict) -> str:
    """
    MD5 hash of business column values for change detection.
    Order is fixed by HASH_COLUMNS constant.
    """
    hash_input = "|".join(
        str(cleaned.get(col)) for col in HASH_COLUMNS
    )
    return hashlib.md5(hash_input.encode()).hexdigest()


def _build_insert_row(
    cleaned: dict,
    row_hash: str,
    batch_ts: datetime,
    source_file: str,
    last_updated_ts,
    is_current: bool,
) -> list:
    """Build a list of values matching lnd_loan column order."""
    return [
        cleaned["loan_id"],
        cleaned["customer_id"],
        cleaned["product_type"],
        cleaned["principal_amount"],
        cleaned["interest_rate"],
        cleaned["term_months"],
        cleaned["origination_date"],
        cleaned["origination_channel"],
        cleaned["status"],
        cleaned["borrower_info"],
        batch_ts,                       # row_effective_from
        SCD2_OPEN_DATE,                 # row_effective_to
        is_current,                     # is_current_flag
        source_file,                    # _source_file
        last_updated_ts,                # _last_updated_ts
        row_hash,                       # _row_hash
    ]
