"""
assets/transformation/fct_loan.py
-----------------------------------
Dagster asset: fct_loan

Grain: one row per loan (current snapshot only).
Source: stg_loan_payment (distinct loans) + dim_customer + dim_date.

Fact columns include derived EMI and delinquency status.
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
    deps=["stg_loan_payment", "dim_customer", "dim_date"],
    description="Fact table: one row per loan (current), with EMI and delinquency",
)
def fct_loan(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(
        logger, event="load_start", layer="fct", table="fct_loan",
        message="Building fct_loan from stg_loan_payment",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS fct_loan")
        conn.execute("""
            CREATE TABLE fct_loan AS
            SELECT
                s.loan_id,
                s.customer_id,
                s.product_type,
                s.principal_amount,
                s.interest_rate,
                s.term_months,
                s.origination_date,
                d_orig.date_id                  AS origination_date_id,
                s.origination_channel,
                s.loan_status,
                s.expected_emi,
                s.final_due_date,
                s.is_delinquent,
                -- Most recent payment info per loan
                MAX(s.payment_amount)           AS last_payment_amount,
                MAX(s.payment_timestamp)        AS last_payment_timestamp,
                MIN(s.days_since_payment)       AS days_since_last_payment,
                COUNT(s.payment_id)             AS total_payment_count,
                SUM(s.payment_amount)           AS total_paid_amount,
                -- Customer dim FK
                c.credit_score,
                c.employment_type,
                c.annual_income,
                -- Audit
                CURRENT_TIMESTAMP               AS _last_updated_ts
            FROM stg_loan_payment s
            LEFT JOIN dim_customer c
                ON s.customer_id = c.customer_id
            LEFT JOIN dim_date d_orig
                ON d_orig.full_date = s.origination_date
            GROUP BY
                s.loan_id, s.customer_id, s.product_type,
                s.principal_amount, s.interest_rate, s.term_months,
                s.origination_date, d_orig.date_id,
                s.origination_channel, s.loan_status,
                s.expected_emi, s.final_due_date, s.is_delinquent,
                c.credit_score, c.employment_type, c.annual_income
        """)

        total      = conn.execute("SELECT COUNT(*) FROM fct_loan").fetchone()[0]
        delinquent = conn.execute(
            "SELECT COUNT(*) FROM fct_loan WHERE is_delinquent = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="fct", table="fct_loan",
        message=f"fct_loan complete: {total} loans, {delinquent} delinquent",
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "total_loans":      MetadataValue.int(total),
        "delinquent_loans": MetadataValue.int(delinquent),
        "duration_sec":     MetadataValue.float(duration),
    })

    return Output(value={"total_loans": total})
