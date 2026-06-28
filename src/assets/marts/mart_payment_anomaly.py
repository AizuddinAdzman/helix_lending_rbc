"""
assets/marts/mart_payment_anomaly.py
--------------------------------------
Dagster asset: mart_payment_anomaly

Business question answered:
    Which customers have payments inconsistent with their loan terms?

Output grain: one row per anomalous payment event.

Columns:
    payment_id, loan_id, customer_id, product_type
    payment_amount, expected_emi, deviation_pct
    payment_method_type, payment_timestamp
    loan_status, anomaly_reason
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import EMI_TOLERANCE_PCT
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["fct_loan", "fct_payment"],
    description="Q2: Payments inconsistent with loan terms (anomaly > 10% EMI deviation)",
)
def mart_payment_anomaly(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(
        logger, event="load_start", layer="mart", table="mart_payment_anomaly",
        message="Building mart_payment_anomaly",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS mart_payment_anomaly")
        conn.execute(f"""
            CREATE TABLE mart_payment_anomaly AS
            SELECT
                p.payment_id,
                p.loan_id,
                p.customer_id,
                p.product_type,
                p.payment_amount,
                p.expected_emi,
                ROUND(
                    ABS(p.payment_amount - p.expected_emi)
                    / NULLIF(p.expected_emi, 0) * 100.0, 2
                )                               AS deviation_pct,
                p.payment_method_type,
                p.payment_timestamp,
                p.loan_status,
                CASE
                    WHEN p.payment_amount < p.expected_emi * (1 - {EMI_TOLERANCE_PCT})
                        THEN 'UNDERPAYMENT'
                    WHEN p.payment_amount > p.expected_emi * (1 + {EMI_TOLERANCE_PCT})
                        THEN 'OVERPAYMENT'
                    ELSE 'ANOMALOUS'
                END                             AS anomaly_reason,
                CURRENT_TIMESTAMP               AS _last_updated_ts
            FROM fct_payment p
            WHERE p.is_payment_anomalous = TRUE
            ORDER BY deviation_pct DESC
        """)

        total       = conn.execute(
            "SELECT COUNT(*) FROM mart_payment_anomaly"
        ).fetchone()[0]
        underpay    = conn.execute(
            "SELECT COUNT(*) FROM mart_payment_anomaly WHERE anomaly_reason = 'UNDERPAYMENT'"
        ).fetchone()[0]
        overpay     = conn.execute(
            "SELECT COUNT(*) FROM mart_payment_anomaly WHERE anomaly_reason = 'OVERPAYMENT'"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="mart", table="mart_payment_anomaly",
        message=(
            f"mart_payment_anomaly complete: {total} anomalies "
            f"({underpay} underpayments, {overpay} overpayments)"
        ),
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "total_anomalies":  MetadataValue.int(total),
        "underpayments":    MetadataValue.int(underpay),
        "overpayments":     MetadataValue.int(overpay),
        "duration_sec":     MetadataValue.float(duration),
    })

    return Output(value={"total_anomalies": total})
