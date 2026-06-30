"""
assets/marts/mart_payment_anomaly.py
--------------------------------------
Schema: hlx_{ENV}_mart
Q2: Payments inconsistent with loan terms. Sequential after mart_delinquency.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import EMI_TOLERANCE_PCT, TBL_FCT_PAYMENT, TBL_MART_ANOMALY, SCHEMA_MART
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["mart_delinquency"],
    description=f"Q2: Anomalous payments (>10% EMI deviation) → {TBL_MART_ANOMALY}",
)
def mart_payment_anomaly(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(logger, event="load_start", layer="mart", table=TBL_MART_ANOMALY,
              message="Building mart_payment_anomaly", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MART}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_MART_ANOMALY} AS
            SELECT
                p.payment_id, p.loan_id, p.customer_id, p.product_type,
                p.payment_amount, p.expected_emi,
                ROUND(ABS(p.payment_amount - p.expected_emi)
                      / NULLIF(p.expected_emi, 0) * 100.0, 2) AS deviation_pct,
                p.payment_method_type, p.payment_timestamp, p.loan_status,
                CASE
                    WHEN p.payment_amount < p.expected_emi * (1 - {EMI_TOLERANCE_PCT})
                        THEN 'UNDERPAYMENT'
                    WHEN p.payment_amount > p.expected_emi * (1 + {EMI_TOLERANCE_PCT})
                        THEN 'OVERPAYMENT'
                    ELSE 'ANOMALOUS'
                END AS anomaly_reason,
                CURRENT_TIMESTAMP AS _last_updated_ts
            FROM {TBL_FCT_PAYMENT} p
            WHERE p.is_payment_anomalous = TRUE
              AND p.loan_status != 'credit_balance'
            -- EMI comparison is undefined for credit balance loans.
            -- Exclude them from anomaly detection to avoid false positives.
            ORDER BY deviation_pct DESC
        """)

        total    = conn.execute(f"SELECT COUNT(*) FROM {TBL_MART_ANOMALY}").fetchone()[0]
        underpay = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_MART_ANOMALY} WHERE anomaly_reason = 'UNDERPAYMENT'"
        ).fetchone()[0]
        overpay  = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_MART_ANOMALY} WHERE anomaly_reason = 'OVERPAYMENT'"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="mart", table=TBL_MART_ANOMALY,
              message=(f"mart_payment_anomaly complete: {total} anomalies "
                       f"({underpay} underpayments, {overpay} overpayments)"),
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "total_anomalies": MetadataValue.int(total),
        "underpayments":   MetadataValue.int(underpay),
        "overpayments":    MetadataValue.int(overpay),
        "duration_sec":    MetadataValue.float(duration),
        "table":           MetadataValue.text(TBL_MART_ANOMALY),
    })
    return Output(value={"total_anomalies": total})
