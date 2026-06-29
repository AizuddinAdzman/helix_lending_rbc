"""
assets/ingestion/raw_payment.py
---------------------------------
Dagster asset: raw_payment

Responsibility:
    Read payment.jsonl line by line into raw_payment table in DuckDB.
    Nested JSON structures are flattened into individual VARCHAR columns.
    All values stored as VARCHAR — no casting, no cleaning.
    Append-only per batch.

Flattening map:
    payment_id                  → payment_id
    loan_id                     → loan_id
    amount                      → amount
    timestamp                   → payment_timestamp  (renamed to avoid SQL keyword)
    payment_method.type         → payment_method_type
    payment_method.details.last_four → payment_method_last_four
    payment_method.details.bank → payment_method_bank
    metadata.source             → metadata_source
    metadata.user_agent         → metadata_user_agent

Missing nested keys → NULL (not an error — metadata is optional per spec)

Audit columns:
    _source_file, _last_updated_ts

Error handling:
    Unparseable JSON lines → err_payment with _rejection_reason
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import PAYMENT_FILE, COL_SOURCE_FILE, COL_LAST_UPDATED_TS
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_RAW_PAYMENT = """
CREATE TABLE IF NOT EXISTS raw_payment (
    payment_id                  VARCHAR,
    loan_id                     VARCHAR,
    amount                      VARCHAR,
    payment_timestamp           VARCHAR,
    payment_method_type         VARCHAR,
    payment_method_last_four    VARCHAR,
    payment_method_bank         VARCHAR,
    metadata_source             VARCHAR,
    metadata_user_agent         VARCHAR,
    _source_file                VARCHAR,
    _last_updated_ts            TIMESTAMP
)
"""

DDL_ERR_PAYMENT = """
CREATE TABLE IF NOT EXISTS err_payment (
    payment_id                  VARCHAR,
    loan_id                     VARCHAR,
    amount                      VARCHAR,
    payment_timestamp           VARCHAR,
    payment_method_type         VARCHAR,
    payment_method_last_four    VARCHAR,
    payment_method_bank         VARCHAR,
    metadata_source             VARCHAR,
    metadata_user_agent         VARCHAR,
    _source_file                VARCHAR,
    _last_updated_ts            TIMESTAMP,
    _rejection_reason           VARCHAR,
    _rejected_at                TIMESTAMP
)
"""


@asset(
    group_name="ingestion",
    deps=["raw_loan"],
    description="Ingest payment.jsonl → raw_payment (flattened, all VARCHAR, append per batch)",
)
def raw_payment(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    """
    Read payment.jsonl line by line, flatten nested JSON,
    store all values as VARCHAR in raw_payment.
    """
    start_time      = time.time()
    source_file     = Path(PAYMENT_FILE).name
    batch_ts        = datetime.now(timezone.utc)
    batch_date_str  = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="raw", table="raw_payment",
        message=f"Starting raw_payment ingestion from {source_file}",
        source_file=source_file, batch_date=batch_date_str,
    )

    rows_in         = 0
    rows_inserted   = 0
    rows_rejected   = 0
    good_rows       = []
    bad_rows        = []

    # ------------------------------------------------------------------
    # Read JSONL
    # ------------------------------------------------------------------
    if not Path(PAYMENT_FILE).exists():
        raise FileNotFoundError(f"Source file not found: {PAYMENT_FILE}")

    with open(PAYMENT_FILE, encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue  # skip blank lines

            rows_in += 1
            try:
                record = json.loads(line)
                flat   = _flatten_payment(record)
                flat[COL_SOURCE_FILE]       = source_file
                flat[COL_LAST_UPDATED_TS]   = batch_ts
                good_rows.append(flat)

            except Exception as e:
                rows_rejected += 1
                rejection_reason = f"Line {line_num}: {type(e).__name__}: {e}"
                log_event(
                    logger, event="row_rejected", layer="raw", table="raw_payment",
                    message=rejection_reason, source_file=source_file,
                    batch_date=batch_date_str, level="WARNING",
                )
                bad_rows.append({
                    "payment_id":               None,
                    "loan_id":                  None,
                    "amount":                   None,
                    "payment_timestamp":        None,
                    "payment_method_type":      None,
                    "payment_method_last_four": None,
                    "payment_method_bank":      None,
                    "metadata_source":          None,
                    "metadata_user_agent":      None,
                    COL_SOURCE_FILE:            source_file,
                    COL_LAST_UPDATED_TS:        batch_ts,
                    "_rejection_reason":        rejection_reason,
                    "_rejected_at":             datetime.now(timezone.utc),
                })

    # ------------------------------------------------------------------
    # Write to DuckDB
    # ------------------------------------------------------------------
    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_RAW_PAYMENT)
        conn.execute(DDL_ERR_PAYMENT)

        if good_rows:
            conn.executemany(
                """
                INSERT INTO raw_payment VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?
                )
                """,
                [[
                    r.get("payment_id"),
                    r.get("loan_id"),
                    r.get("amount"),
                    r.get("payment_timestamp"),
                    r.get("payment_method_type"),
                    r.get("payment_method_last_four"),
                    r.get("payment_method_bank"),
                    r.get("metadata_source"),
                    r.get("metadata_user_agent"),
                    r.get(COL_SOURCE_FILE),
                    r.get(COL_LAST_UPDATED_TS),
                ] for r in good_rows],
            )
            rows_inserted = len(good_rows)

        if bad_rows:
            conn.executemany(
                """
                INSERT INTO err_payment VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                [[
                    r.get("payment_id"),
                    r.get("loan_id"),
                    r.get("amount"),
                    r.get("payment_timestamp"),
                    r.get("payment_method_type"),
                    r.get("payment_method_last_four"),
                    r.get("payment_method_bank"),
                    r.get("metadata_source"),
                    r.get("metadata_user_agent"),
                    r.get(COL_SOURCE_FILE),
                    r.get(COL_LAST_UPDATED_TS),
                    r.get("_rejection_reason"),
                    r.get("_rejected_at"),
                ] for r in bad_rows],
            )

        total_raw = conn.execute(
            "SELECT COUNT(*) FROM raw_payment"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="raw", table="raw_payment",
        message=(
            f"raw_payment complete: {rows_inserted} inserted, "
            f"{rows_rejected} rejected, {total_raw} total rows in table"
        ),
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, source_file=source_file, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "rows_in":          MetadataValue.int(rows_in),
        "rows_inserted":    MetadataValue.int(rows_inserted),
        "rows_rejected":    MetadataValue.int(rows_rejected),
        "total_in_table":   MetadataValue.int(total_raw),
        "duration_sec":     MetadataValue.float(duration),
        "source_file":      MetadataValue.text(source_file),
        "batch_date":       MetadataValue.text(batch_date_str),
    })

    return Output(
        value={
            "rows_in":       rows_in,
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "batch_ts":      batch_ts.isoformat(),
            "source_file":   source_file,
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten_payment(record: dict) -> dict:
    """
    Flatten a payment JSON record into a single-level dict of strings.

    All values are coerced to string or None.
    Missing nested keys return None — not an error.
    """
    pm      = record.get("payment_method") or {}
    details = pm.get("details") or {}
    meta    = record.get("metadata") or {}

    return {
        "payment_id":               _str(record.get("payment_id")),
        "loan_id":                  _str(record.get("loan_id")),
        "amount":                   _str(record.get("amount")),
        "payment_timestamp":        _str(record.get("timestamp")),
        "payment_method_type":      _str(pm.get("type")),
        "payment_method_last_four": _str(details.get("last_four")),
        "payment_method_bank":      _str(details.get("bank")),
        "metadata_source":          _str(meta.get("source")),
        "metadata_user_agent":      _str(meta.get("user_agent")),
    }


def _str(value: Any) -> Optional[str]:
    """Convert any value to string, returning None for None/null."""
    if value is None:
        return None
    return str(value).strip() or None
