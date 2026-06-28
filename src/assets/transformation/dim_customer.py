"""
assets/transformation/dim_customer.py
---------------------------------------
Dagster asset: dim_customer

Responsibility:
    Flatten borrower_info JSON from lnd_loan into dim_customer.
    One row per customer_id (latest known snapshot).

    borrower_info JSON schema (undocumented — inferred from data):
        credit_score    INTEGER
        employment      VARCHAR  (salaried, self-employed, unemployed)
        annual_income   DECIMAL
        years_employed  INTEGER

Design decision:
    Missing JSON fields → NULL (not an error).
    Malformed JSON → row written with all fields NULL + warning logged.
    Grain: one row per customer_id (deduped on latest origination_date).
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

DDL_DIM_CUSTOMER = """
CREATE OR REPLACE TABLE dim_customer (
    customer_id         VARCHAR       NOT NULL,
    credit_score        INTEGER,
    employment_type     VARCHAR,
    annual_income       DECIMAL(18,2),
    years_employed      INTEGER,
    _source_loan_id     VARCHAR,
    _last_updated_ts    TIMESTAMP
)
"""


@asset(
    group_name="transformation",
    deps=["stg_loan_payment"],
    description="Flatten borrower_info JSON → dim_customer (one row per customer)",
)
def dim_customer(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()
    batch_ts       = datetime.now(timezone.utc)

    log_event(
        logger, event="load_start", layer="dim", table="dim_customer",
        message="Starting dim_customer build from lnd_loan.borrower_info",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_DIM_CUSTOMER)

        # Pull latest snapshot per customer (most recent origination_date)
        raw_rows = conn.execute("""
            SELECT DISTINCT ON (customer_id)
                customer_id,
                borrower_info,
                loan_id
            FROM lnd_loan
            WHERE is_current_flag = TRUE
              AND customer_id IS NOT NULL
            ORDER BY customer_id, origination_date DESC
        """).fetchall()

    rows_in       = len(raw_rows)
    rows_inserted = 0
    rows_warned   = 0
    to_insert     = []

    for customer_id, borrower_info, loan_id in raw_rows:
        parsed = _parse_borrower_info(borrower_info, customer_id)
        if parsed.get("_warn"):
            rows_warned += 1
            log_event(
                logger, event="checkpoint", layer="dim", table="dim_customer",
                message=f"Malformed borrower_info for customer_id={customer_id}",
                batch_date=batch_date_str, level="WARNING",
            )

        to_insert.append([
            customer_id,
            parsed.get("credit_score"),
            parsed.get("employment"),
            parsed.get("annual_income"),
            parsed.get("years_employed"),
            loan_id,
            batch_ts,
        ])

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_DIM_CUSTOMER)
        if to_insert:
            conn.executemany(
                "INSERT INTO dim_customer VALUES (?,?,?,?,?,?,?)",
                to_insert,
            )
            rows_inserted = len(to_insert)

        total = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="dim", table="dim_customer",
        message=f"dim_customer complete: {rows_inserted} rows, {rows_warned} warnings",
        rows_in=rows_in, rows_out=rows_inserted,
        duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "rows_inserted":   MetadataValue.int(rows_inserted),
        "rows_warned":     MetadataValue.int(rows_warned),
        "total_in_table":  MetadataValue.int(total),
        "duration_sec":    MetadataValue.float(duration),
    })

    return Output(value={"rows_inserted": rows_inserted})


def _parse_borrower_info(raw: str | None, customer_id: str) -> dict:
    """
    Parse borrower_info JSON string.
    Returns dict with keys: credit_score, employment, annual_income, years_employed.
    Missing keys → None. Malformed JSON → all None + _warn=True.
    """
    if not raw or not raw.strip():
        return {"_warn": False}
    try:
        data = json.loads(raw)
        return {
            "credit_score":   _safe_int(data.get("credit_score")),
            "employment":     str(data["employment"]).strip().lower()
                              if data.get("employment") else None,
            "annual_income":  _safe_float(data.get("annual_income")),
            "years_employed": _safe_int(data.get("years_employed")),
            "_warn":          False,
        }
    except (json.JSONDecodeError, Exception):
        return {
            "credit_score": None, "employment": None,
            "annual_income": None, "years_employed": None,
            "_warn": True,
        }


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None
