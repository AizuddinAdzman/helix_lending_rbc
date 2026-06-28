"""
assets/marts/mart_delinquency.py
----------------------------------
Dagster asset: mart_delinquency

Business question answered:
    What is our 30-day delinquency rate by loan product?

Output grain: one row per product_type per run date.

Columns:
    product_type
    run_date
    total_active_loans
    delinquent_loans
    delinquency_rate_pct
    avg_days_since_payment
    avg_principal_delinquent
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["fct_loan", "fct_payment"],
    description="Q1: 30-day delinquency rate by loan product type",
)
def mart_delinquency(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(
        logger, event="load_start", layer="mart", table="mart_delinquency",
        message="Building mart_delinquency",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS mart_delinquency")
        conn.execute(f"""
            CREATE TABLE mart_delinquency AS
            SELECT
                COALESCE(product_type, 'unknown')   AS product_type,
                CURRENT_DATE                        AS run_date,
                COUNT(*)                            AS total_active_loans,
                SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END)
                                                    AS delinquent_loans,
                ROUND(
                    SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                )                                   AS delinquency_rate_pct,
                ROUND(AVG(
                    CASE WHEN is_delinquent
                    THEN days_since_last_payment END
                ), 1)                               AS avg_days_since_payment,
                ROUND(AVG(
                    CASE WHEN is_delinquent
                    THEN principal_amount END
                ), 2)                               AS avg_principal_delinquent,
                CURRENT_TIMESTAMP                   AS _last_updated_ts
            FROM fct_loan
            WHERE loan_status = 'active'
            GROUP BY product_type
            ORDER BY delinquency_rate_pct DESC
        """)

        total = conn.execute(
            "SELECT COUNT(*) FROM mart_delinquency"
        ).fetchone()[0]
        highest = conn.execute("""
            SELECT product_type, delinquency_rate_pct
            FROM mart_delinquency
            ORDER BY delinquency_rate_pct DESC LIMIT 1
        """).fetchone()

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="mart", table="mart_delinquency",
        message=(
            f"mart_delinquency complete: {total} product rows. "
            f"Highest delinquency: {highest[0] if highest else 'N/A'} "
            f"at {highest[1] if highest else 0}%"
        ),
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "product_rows":         MetadataValue.int(total),
        "highest_product":      MetadataValue.text(highest[0] if highest else "N/A"),
        "highest_rate_pct":     MetadataValue.float(float(highest[1]) if highest else 0.0),
        "duration_sec":         MetadataValue.float(duration),
    })

    return Output(value={"product_rows": total})
