"""
assets/marts/mart_delinquency.py
----------------------------------
Schema: hlx_{ENV}_mart
Q1: 30-day delinquency rate by product type. Sequential after fct_payment.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import TBL_FCT_LOAN, TBL_MART_DELINQUENCY, SCHEMA_MART
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["fct_payment"],
    description=f"Q1: 30-day delinquency rate by product → {TBL_MART_DELINQUENCY}",
)
def mart_delinquency(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(logger, event="load_start", layer="mart",
              table=TBL_MART_DELINQUENCY,
              message="Building mart_delinquency", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MART}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_MART_DELINQUENCY} AS
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
                ROUND(AVG(CASE WHEN is_delinquent THEN days_since_last_payment END), 1)
                                                    AS avg_days_since_payment,
                ROUND(AVG(CASE WHEN is_delinquent THEN principal_amount END), 2)
                                                    AS avg_principal_delinquent,
                CURRENT_TIMESTAMP                   AS _last_updated_ts
            FROM {TBL_FCT_LOAN}
            WHERE loan_status = 'active'
            GROUP BY product_type
            ORDER BY delinquency_rate_pct DESC
        """)

        total   = conn.execute(f"SELECT COUNT(*) FROM {TBL_MART_DELINQUENCY}").fetchone()[0]
        highest = conn.execute(
            f"SELECT product_type, delinquency_rate_pct FROM {TBL_MART_DELINQUENCY} "
            f"ORDER BY delinquency_rate_pct DESC LIMIT 1"
        ).fetchone()

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="mart", table=TBL_MART_DELINQUENCY,
              message=(f"mart_delinquency complete: {total} product rows. "
                       f"Highest: {highest[0] if highest else 'N/A'} "
                       f"at {highest[1] if highest else 0}%"),
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "product_rows":     MetadataValue.int(total),
        "highest_product":  MetadataValue.text(highest[0] if highest else "N/A"),
        "highest_rate_pct": MetadataValue.float(float(highest[1]) if highest else 0.0),
        "duration_sec":     MetadataValue.float(duration),
        "table":            MetadataValue.text(TBL_MART_DELINQUENCY),
    })
    return Output(value={"product_rows": total})
