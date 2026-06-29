"""
assets/transformation/fct_payment.py
--------------------------------------
Dagster asset: fct_payment

Grain: one row per payment event.
Source: stg_loan_payment (payment rows only) + dim_date.

Excludes loans with no payments (payment_id IS NOT NULL filter).
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="facts",
    deps=["stg_loan_payment", "dim_customer", "dim_date","fct_loan"],
    description="Fact table: one row per payment event with anomaly flag",
)
def fct_payment(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(
        logger, event="load_start", layer="fct", table="fct_payment",
        message="Building fct_payment from stg_loan_payment",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS fct_payment")
        conn.execute("""
            CREATE TABLE fct_payment AS
            SELECT
                s.payment_id,
                s.loan_id,
                s.customer_id,
                s.product_type,
                s.payment_amount,
                s.payment_timestamp,
                d_pay.date_id               AS payment_date_id,
                s.payment_method_type,
                s.payment_method_bank,
                s.metadata_source,
                s.expected_emi,
                s.is_payment_anomalous,
                s.days_since_payment,
                s.loan_status,
                CURRENT_TIMESTAMP           AS _last_updated_ts
            FROM stg_loan_payment s
            LEFT JOIN dim_date d_pay
                ON d_pay.full_date = CAST(s.payment_timestamp AS DATE)
            WHERE s.payment_id IS NOT NULL
        """)

        total     = conn.execute("SELECT COUNT(*) FROM fct_payment").fetchone()[0]
        anomalous = conn.execute(
            "SELECT COUNT(*) FROM fct_payment WHERE is_payment_anomalous = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="fct", table="fct_payment",
        message=f"fct_payment complete: {total} payments, {anomalous} anomalous",
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "total_payments":    MetadataValue.int(total),
        "anomalous_count":   MetadataValue.int(anomalous),
        "duration_sec":      MetadataValue.float(duration),
    })

    return Output(value={"total_payments": total})
