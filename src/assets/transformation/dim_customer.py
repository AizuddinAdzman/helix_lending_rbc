"""
assets/transformation/dim_customer.py
---------------------------------------
Schema: hlx_{ENV}_dim
Reads from: hlx_{ENV}_lnd
Sequential after stg_loan_payment.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    TBL_LND_LOAN, TBL_DIM_CUSTOMER, SCHEMA_DIM,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="transformation",
    deps=["stg_loan_payment"],
    description=f"Flatten borrower_info JSON → {TBL_DIM_CUSTOMER}",
)
def dim_customer(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(logger, event="load_start", layer="dim", table=TBL_DIM_CUSTOMER,
              message="Building dim_customer", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        raw_rows = conn.execute(f"""
            SELECT DISTINCT ON (customer_id)
                customer_id, borrower_info, loan_id
            FROM {TBL_LND_LOAN}
            WHERE is_current_flag = TRUE AND customer_id IS NOT NULL
            ORDER BY customer_id, origination_date DESC
        """).fetchall()

    rows_in = len(raw_rows)
    to_insert = []
    rows_warned = 0

    for customer_id, borrower_info, loan_id in raw_rows:
        parsed = _parse(borrower_info)
        if parsed.get("_warn"):
            rows_warned += 1
            log_event(logger, event="checkpoint", layer="dim",
                      table=TBL_DIM_CUSTOMER,
                      message=f"Malformed borrower_info for {customer_id}",
                      batch_date=batch_date_str, level="WARNING")
        to_insert.append([
            customer_id,
            parsed.get("credit_score"),
            parsed.get("employment"),
            parsed.get("annual_income"),
            parsed.get("years_employed"),
            loan_id, batch_ts,
        ])

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_DIM}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_DIM_CUSTOMER} (
                customer_id VARCHAR NOT NULL,
                credit_score INTEGER, employment_type VARCHAR,
                annual_income DECIMAL(18,2), years_employed INTEGER,
                _source_loan_id VARCHAR, _last_updated_ts TIMESTAMP
            )""")
        if to_insert:
            conn.executemany(
                f"INSERT INTO {TBL_DIM_CUSTOMER} VALUES (?,?,?,?,?,?,?)",
                to_insert,
            )
        total = conn.execute(f"SELECT COUNT(*) FROM {TBL_DIM_CUSTOMER}").fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="dim", table=TBL_DIM_CUSTOMER,
              message=f"dim_customer complete: {total} rows, {rows_warned} warnings",
              rows_in=rows_in, rows_out=total,
              duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "rows_inserted": MetadataValue.int(total),
        "rows_warned":   MetadataValue.int(rows_warned),
        "duration_sec":  MetadataValue.float(duration),
        "table":         MetadataValue.text(TBL_DIM_CUSTOMER),
    })
    return Output(value={"rows_inserted": total})


def _parse(raw: Optional[str]) -> dict:
    if not raw or not raw.strip():
        return {"_warn": False}
    try:
        d = json.loads(raw)
        return {
            "credit_score":   _int(d.get("credit_score")),
            "employment":     str(d["employment"]).strip().lower()
                              if d.get("employment") else None,
            "annual_income":  _float(d.get("annual_income")),
            "years_employed": _int(d.get("years_employed")),
            "_warn":          False,
        }
    except Exception:
        return {"credit_score": None, "employment": None,
                "annual_income": None, "years_employed": None, "_warn": True}


def _int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def _float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None
