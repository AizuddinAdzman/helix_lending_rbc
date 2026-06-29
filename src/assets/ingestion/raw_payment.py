"""
assets/ingestion/raw_payment.py
---------------------------------
Dagster asset: raw_payment
Schema: hlx_{ENV}_raw

Reads payment.jsonl line by line, flattens nested JSON into VARCHAR columns.
Append-only per batch. Bad JSON lines → lnd_err_payment.
Sequential after raw_loan — DuckDB single-writer constraint.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    PAYMENT_FILE, COL_SOURCE_FILE, COL_LAST_UPDATED_TS,
    TBL_RAW_PAYMENT, TBL_RAW_AUDIT, TBL_LND_ERR_PAYMENT, SCHEMA_RAW, SCHEMA_LND,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="ingestion",
    deps=["raw_loan"],
    description=f"Ingest payment.jsonl → {TBL_RAW_PAYMENT} (flattened VARCHAR, append per batch)",
)
def raw_payment(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    source_file    = Path(PAYMENT_FILE).name
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(
        logger, event="load_start", layer="raw", table=TBL_RAW_PAYMENT,
        message=f"Starting raw_payment ingestion from {source_file}",
        source_file=source_file, batch_date=batch_date_str,
    )

    if not Path(PAYMENT_FILE).exists():
        raise FileNotFoundError(f"Source file not found: {PAYMENT_FILE}")

    rows_in = rows_inserted = rows_rejected = 0
    good_rows = []
    bad_rows  = []

    with open(PAYMENT_FILE, encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            rows_in += 1
            try:
                r       = json.loads(line)
                pm      = r.get("payment_method") or {}
                details = pm.get("details") or {}
                meta    = r.get("metadata") or {}
                good_rows.append([
                    _str(r.get("payment_id")),
                    _str(r.get("loan_id")),
                    _str(r.get("amount")),
                    _str(r.get("timestamp")),
                    _str(pm.get("type")),
                    _str(details.get("last_four")),
                    _str(details.get("bank")),
                    _str(meta.get("source")),
                    _str(meta.get("user_agent")),
                    source_file, batch_ts,
                ])
            except Exception as e:
                rows_rejected += 1
                reason = f"Line {line_num}: {type(e).__name__}: {e}"
                log_event(
                    logger, event="row_rejected", layer="raw", table=TBL_RAW_PAYMENT,
                    message=reason, source_file=source_file,
                    batch_date=batch_date_str, level="WARNING",
                )
                bad_rows.append([
                    None, None, None, None, None, None, None, None, None,
                    source_file, batch_ts, reason, datetime.now(timezone.utc),
                ])

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_LND}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_RAW_PAYMENT} (
                payment_id VARCHAR, loan_id VARCHAR, amount VARCHAR,
                payment_timestamp VARCHAR, payment_method_type VARCHAR,
                payment_method_last_four VARCHAR, payment_method_bank VARCHAR,
                metadata_source VARCHAR, metadata_user_agent VARCHAR,
                _source_file VARCHAR, _last_updated_ts TIMESTAMP
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
        if good_rows:
            conn.executemany(
                f"INSERT INTO {TBL_RAW_PAYMENT} VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                good_rows,
            )
            rows_inserted = len(good_rows)
        if bad_rows:
            conn.executemany(
                f"INSERT INTO {TBL_LND_ERR_PAYMENT} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                bad_rows,
            )
        total = conn.execute(f"SELECT COUNT(*) FROM {TBL_RAW_PAYMENT}").fetchone()[0]

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

        # Count duplicate patterns in this batch
        dup_stats = conn.execute(f"""
            SELECT
                COUNT(DISTINCT payment_id)                          AS distinct_keys,
                SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END)           AS duplicate_key_count,
                SUM(CASE WHEN true_dup > 0 THEN true_dup ELSE 0 END) AS true_duplicate_count,
                SUM(CASE WHEN diff_amt > 0 THEN diff_amt ELSE 0 END) AS diff_amount_same_id
            FROM (
                SELECT
                    payment_id,
                    COUNT(*) AS cnt,
                    -- true duplicates: same payment_id + amount + timestamp
                    COUNT(*) - COUNT(DISTINCT amount || '|' || payment_timestamp) AS true_dup,
                    -- diff amount same id: same payment_id, different amounts
                    CASE WHEN COUNT(DISTINCT amount) > 1 THEN 1 ELSE 0 END AS diff_amt
                FROM {TBL_RAW_PAYMENT}
                WHERE _last_updated_ts = ?
                GROUP BY payment_id
            )
        """, [batch_ts]).fetchone()

        conn.execute(
            f"INSERT INTO {TBL_RAW_AUDIT} VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                batch_ts, source_file, TBL_RAW_PAYMENT,
                rows_in, rows_inserted,
                dup_stats[0],   # distinct_keys
                dup_stats[1],   # duplicate_key_count
                dup_stats[2],   # true_duplicate_count
                dup_stats[3],   # diff_amount_same_id_count
                batch_ts,
            ]
        )
        log_event(
            logger, event="checkpoint", layer="raw", table=TBL_RAW_PAYMENT,
            message=(
                f"raw_audit written: distinct_payment_ids={dup_stats[0]}, "
                f"duplicate_key_count={dup_stats[1]}, "
                f"true_duplicates={dup_stats[2]}, "
                f"same_id_diff_amount={dup_stats[3]}"
            ),
            source_file=source_file, batch_date=batch_date_str,
        )

    duration = round(time.time() - start_time, 3)
    log_event(
        logger, event="load_end", layer="raw", table=TBL_RAW_PAYMENT,
        message=f"raw_payment complete: {rows_inserted} inserted, {rows_rejected} rejected",
        rows_in=rows_in, rows_out=rows_inserted, rows_rejected=rows_rejected,
        duration_sec=duration, source_file=source_file, batch_date=batch_date_str,
    )
    context.add_output_metadata({
        "rows_in":        MetadataValue.int(rows_in),
        "rows_inserted":  MetadataValue.int(rows_inserted),
        "rows_rejected":  MetadataValue.int(rows_rejected),
        "total_in_table": MetadataValue.int(total),
        "duration_sec":   MetadataValue.float(duration),
        "table":          MetadataValue.text(TBL_RAW_PAYMENT),
    })
    return Output(value={"rows_in": rows_in, "rows_inserted": rows_inserted,
                         "rows_rejected": rows_rejected})


def _str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value).strip() or None
