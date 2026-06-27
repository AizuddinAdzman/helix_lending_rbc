"""
assets/ingestion/raw_loan.py
------------------------------
Dagster asset: raw_loan

Responsibility:
    Read loan.csv as-is into raw_loan table in DuckDB.
    Every column stored as VARCHAR — no casting, no cleaning.
    Append-only — each run adds a new batch identified by _last_updated_ts.

Audit columns added:
    _source_file        filename with extension (e.g. loan.csv)
    _last_updated_ts    UTC timestamp of this pipeline run

Error handling:
    Rows that cannot be read (encoding issues, malformed CSV structure)
    are written to err_loan with _rejection_reason.
    Pipeline continues after logging errors.

Observability:
    Structured log emitted at: load_start, load_end, row_rejected
"""

import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import LOAN_FILE, COL_SOURCE_FILE, COL_LAST_UPDATED_TS
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_RAW_LOAN = """
CREATE TABLE IF NOT EXISTS raw_loan (
    loan_id                 VARCHAR,
    customer_id             VARCHAR,
    product_type            VARCHAR,
    principal_amount        VARCHAR,
    interest_rate           VARCHAR,
    term_months             VARCHAR,
    origination_date        VARCHAR,
    origination_channel     VARCHAR,
    status                  VARCHAR,
    borrower_info           VARCHAR,
    _source_file            VARCHAR,
    _last_updated_ts        TIMESTAMP
)
"""

DDL_ERR_LOAN = """
CREATE TABLE IF NOT EXISTS err_loan (
    loan_id                 VARCHAR,
    customer_id             VARCHAR,
    product_type            VARCHAR,
    principal_amount        VARCHAR,
    interest_rate           VARCHAR,
    term_months             VARCHAR,
    origination_date        VARCHAR,
    origination_channel     VARCHAR,
    status                  VARCHAR,
    borrower_info           VARCHAR,
    _source_file            VARCHAR,
    _last_updated_ts        TIMESTAMP,
    _rejection_reason       VARCHAR,
    _rejected_at            TIMESTAMP
)
"""

# Expected columns from the CSV header
EXPECTED_COLUMNS = [
    "loan_id", "customer_id", "product_type", "principal_amount",
    "interest_rate", "term_months", "origination_date",
    "origination_channel", "status", "borrower_info",
]


@asset(
    group_name="ingestion",
    description="Ingest loan.csv → raw_loan (all VARCHAR, append per batch)",
)
def raw_loan(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    """
    Read loan.csv row by row into raw_loan.
    All values stored as VARCHAR. No transformation applied.
    """
    start_time      = time.time()
    source_file     = Path(LOAN_FILE).name
    batch_ts        = datetime.now(timezone.utc)
    batch_date_str  = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="raw", table="raw_loan",
        message=f"Starting raw_loan ingestion from {source_file}",
        source_file=source_file, batch_date=batch_date_str,
    )

    rows_in         = 0
    rows_inserted   = 0
    rows_rejected   = 0
    good_rows       = []
    bad_rows        = []

    # ------------------------------------------------------------------
    # Read CSV
    # ------------------------------------------------------------------
    if not Path(LOAN_FILE).exists():
        raise FileNotFoundError(f"Source file not found: {LOAN_FILE}")

    with open(LOAN_FILE, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)

        # Validate header
        actual_cols = set(reader.fieldnames or [])
        missing_cols = set(EXPECTED_COLUMNS) - actual_cols
        if missing_cols:
            raise ValueError(
                f"loan.csv missing expected columns: {missing_cols}"
            )

        for line_num, row in enumerate(reader, start=2):  # start=2 (header=1)
            rows_in += 1
            try:
                good_rows.append({
                    "loan_id":              _safe(row.get("loan_id")),
                    "customer_id":          _safe(row.get("customer_id")),
                    "product_type":         _safe(row.get("product_type")),
                    "principal_amount":     _safe(row.get("principal_amount")),
                    "interest_rate":        _safe(row.get("interest_rate")),
                    "term_months":          _safe(row.get("term_months")),
                    "origination_date":     _safe(row.get("origination_date")),
                    "origination_channel":  _safe(row.get("origination_channel")),
                    "status":               _safe(row.get("status")),
                    "borrower_info":        _safe(row.get("borrower_info")),
                    COL_SOURCE_FILE:        source_file,
                    COL_LAST_UPDATED_TS:    batch_ts,
                })
            except Exception as e:
                rows_rejected += 1
                rejection_reason = f"Line {line_num}: {type(e).__name__}: {e}"
                log_event(
                    logger, event="row_rejected", layer="raw", table="raw_loan",
                    message=rejection_reason, source_file=source_file,
                    batch_date=batch_date_str, level="WARNING",
                )
                bad_rows.append({
                    **{k: _safe(row.get(k)) for k in EXPECTED_COLUMNS},
                    COL_SOURCE_FILE:        source_file,
                    COL_LAST_UPDATED_TS:    batch_ts,
                    "_rejection_reason":    rejection_reason,
                    "_rejected_at":         datetime.now(timezone.utc),
                })

    # ------------------------------------------------------------------
    # Write to DuckDB
    # ------------------------------------------------------------------
    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_RAW_LOAN)
        conn.execute(DDL_ERR_LOAN)

        if good_rows:
            conn.executemany(
                f"""
                INSERT INTO raw_loan VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?
                )
                """,
                [list(r.values()) for r in good_rows],
            )
            rows_inserted = len(good_rows)

        if bad_rows:
            conn.executemany(
                f"""
                INSERT INTO err_loan VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                [list(r.values()) for r in bad_rows],
            )

        total_raw = conn.execute("SELECT COUNT(*) FROM raw_loan").fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="raw", table="raw_loan",
        message=(
            f"raw_loan complete: {rows_inserted} inserted, "
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


def _safe(value: Optional[str]) -> Optional[str]:
    """Return stripped string or None for empty/whitespace-only values."""
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None
