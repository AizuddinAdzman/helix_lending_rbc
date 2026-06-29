"""
assets/transformation/fct_payment.py
--------------------------------------
Schema: hlx_{ENV}_fct
Grain: one row per payment event. Sequential after fct_loan.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    TBL_STG_LOAN_PAYMENT, TBL_DIM_DATE,
    TBL_FCT_PAYMENT, SCHEMA_FCT,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="facts",
    deps=["fct_loan"],
    description=f"Fact table: one row per payment → {TBL_FCT_PAYMENT}",
)
def fct_payment(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(logger, event="load_start", layer="fct", table=TBL_FCT_PAYMENT,
              message="Building fct_payment", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FCT}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_FCT_PAYMENT} AS
            SELECT
                s.payment_id, s.loan_id, s.customer_id, s.product_type,
                s.payment_amount, s.payment_timestamp,
                d.date_id           AS payment_date_id,
                s.payment_method_type, s.payment_method_bank,
                s.metadata_source, s.expected_emi,
                s.is_payment_anomalous, s.days_since_payment,
                s.loan_status, CURRENT_TIMESTAMP AS _last_updated_ts
            FROM {TBL_STG_LOAN_PAYMENT} s
            LEFT JOIN {TBL_DIM_DATE} d
                ON d.full_date = CAST(s.payment_timestamp AS DATE)
            WHERE s.payment_id IS NOT NULL
        """)

        total     = conn.execute(f"SELECT COUNT(*) FROM {TBL_FCT_PAYMENT}").fetchone()[0]
        anomalous = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_FCT_PAYMENT} WHERE is_payment_anomalous = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="fct", table=TBL_FCT_PAYMENT,
              message=f"fct_payment complete: {total} payments, {anomalous} anomalous",
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "total_payments":  MetadataValue.int(total),
        "anomalous_count": MetadataValue.int(anomalous),
        "duration_sec":    MetadataValue.float(duration),
        "table":           MetadataValue.text(TBL_FCT_PAYMENT),
    })
    return Output(value={"total_payments": total})
