"""
assets/ingestion/raw_loan.py
------------------------------
Dagster asset: raw_loan
Schema: hlx_{ENV}_raw

Reads loan.csv as-is into raw_loan — all VARCHAR, no transformation.
Append-only per batch.
"""

import csv
import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    LOAN_FILE, COL_SOURCE_FILE, COL_LAST_UPDATED_TS,
    TBL_RAW_LOAN, TBL_RAW_AUDIT, SCHEMA_RAW,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

EXPECTED_COLUMNS = [
    "loan_id", "customer_id", "product_type", "principal_amount",
    "interest_rate", "term_months", "origination_date",
    "origination_channel", "status", "borrower_info",
]


@asset(
    group_name="ingestion",
    description=f"Ingest loan.csv → {TBL_RAW_LOAN} (all VARCHAR, append per batch)",
)
def raw_loan(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    source_file    = Path(LOAN_FILE).name
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="raw", table=TBL_RAW_LOAN,
        message=f"Starting raw_loan ingestion from {source_file}",
        source_file=source_file, batch_date=batch_date_str,
    )

    if not Path(LOAN_FILE).exists():
        raise FileNotFoundError(f"Source file not found: {LOAN_FILE}")

    rows_in = rows_inserted = 0
    good_rows = []

    with open(LOAN_FILE, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = set(EXPECTED_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"loan.csv missing columns: {missing}")
        for row in reader:
            rows_in += 1
            good_rows.append([
                _safe(row.get("loan_id")),
                _safe(row.get("customer_id")),
                _safe(row.get("product_type")),
                _safe(row.get("principal_amount")),
                _safe(row.get("interest_rate")),
                _safe(row.get("term_months")),
                _safe(row.get("origination_date")),
                _safe(row.get("origination_channel")),
                _safe(row.get("status")),
                _safe(row.get("borrower_info")),
                source_file,
                batch_ts,
            ])

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_RAW_LOAN} (
                loan_id VARCHAR, customer_id VARCHAR, product_type VARCHAR,
                principal_amount VARCHAR, interest_rate VARCHAR,
                term_months VARCHAR, origination_date VARCHAR,
                origination_channel VARCHAR, status VARCHAR,
                borrower_info VARCHAR, _source_file VARCHAR,
                _last_updated_ts TIMESTAMP
            )""")
        if good_rows:
            conn.executemany(
                f"INSERT INTO {TBL_RAW_LOAN} VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                good_rows,
            )
            rows_inserted = len(good_rows)
        total = conn.execute(f"SELECT COUNT(*) FROM {TBL_RAW_LOAN}").fetchone()[0]

        # ── Write to raw_audit ────────────────────────────────────────────
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_RAW_AUDIT} (
                batch_ts                    TIMESTAMP,
                source_file                 VARCHAR,
                source_table                VARCHAR,
                total_rows_in_file          INTEGER,
                total_rows_inserted         INTEGER,
                distinct_keys               INTEGER,
                duplicate_key_count         INTEGER,
                true_duplicate_count        INTEGER,
                diff_amount_same_id_count   INTEGER,
                _last_updated_ts            TIMESTAMP
            )""")

        # Count duplicates in this batch
        dup_stats = conn.execute(f"""
            SELECT
                COUNT(DISTINCT loan_id)                             AS distinct_keys,
                SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END)           AS duplicate_key_count,
                SUM(CASE WHEN cnt > 1 THEN cnt - 1 ELSE 0 END)     AS true_duplicate_count
            FROM (
                SELECT loan_id, COUNT(*) AS cnt
                FROM {TBL_RAW_LOAN}
                WHERE _last_updated_ts = ?
                GROUP BY loan_id
            )
        """, [batch_ts]).fetchone()

        conn.execute(
            f"INSERT INTO {TBL_RAW_AUDIT} VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                batch_ts, source_file, TBL_RAW_LOAN,
                rows_in, rows_inserted,
                dup_stats[0],   # distinct_keys
                dup_stats[1],   # duplicate_key_count
                dup_stats[2],   # true_duplicate_count
                0,              # diff_amount_same_id_count (N/A for loans)
                batch_ts,
            ]
        )
        log_event(
            logger, event="checkpoint", layer="raw", table=TBL_RAW_LOAN,
            message=(
                f"raw_audit written: distinct_loan_ids={dup_stats[0]}, "
                f"duplicate_key_count={dup_stats[1]}, "
                f"extra_rows_from_duplicates={dup_stats[2]}"
            ),
            source_file=source_file, batch_date=batch_date_str,
        )

    duration = round(time.time() - start_time, 3)
    log_event(
        logger, event="load_end", layer="raw", table=TBL_RAW_LOAN,
        message=f"raw_loan complete: {rows_inserted} inserted, {total} total in table",
        rows_in=rows_in, rows_out=rows_inserted,
        duration_sec=duration, source_file=source_file, batch_date=batch_date_str,
    )
    context.add_output_metadata({
        "rows_in":        MetadataValue.int(rows_in),
        "rows_inserted":  MetadataValue.int(rows_inserted),
        "total_in_table": MetadataValue.int(total),
        "duration_sec":   MetadataValue.float(duration),
        "source_file":    MetadataValue.text(source_file),
        "table":          MetadataValue.text(TBL_RAW_LOAN),
    })
    return Output(value={"rows_in": rows_in, "rows_inserted": rows_inserted})


def _safe(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None
