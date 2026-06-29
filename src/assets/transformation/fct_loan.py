"""
assets/transformation/fct_loan.py
-----------------------------------
Schema: hlx_{ENV}_fct
Grain: one row per loan (current). Sequential after dim_date.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    TBL_STG_LOAN_PAYMENT, TBL_DIM_CUSTOMER, TBL_DIM_DATE,
    TBL_FCT_LOAN, SCHEMA_FCT,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="facts",
    deps=["dim_date"],
    description=f"Fact table: one row per loan → {TBL_FCT_LOAN}",
)
def fct_loan(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(logger, event="load_start", layer="fct", table=TBL_FCT_LOAN,
              message="Building fct_loan", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FCT}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_FCT_LOAN} AS
            SELECT
                s.loan_id, s.customer_id, s.product_type,
                s.principal_amount, s.interest_rate, s.term_months,
                s.origination_date,
                d.date_id                   AS origination_date_id,
                s.origination_channel, s.loan_status, s.expected_emi,
                s.final_due_date, s.is_delinquent,
                MAX(s.payment_amount)       AS last_payment_amount,
                MAX(s.payment_timestamp)    AS last_payment_timestamp,
                MIN(s.days_since_payment)   AS days_since_last_payment,
                COUNT(s.payment_id)         AS total_payment_count,
                SUM(s.payment_amount)       AS total_paid_amount,
                c.credit_score, c.employment_type, c.annual_income,
                CURRENT_TIMESTAMP           AS _last_updated_ts
            FROM {TBL_STG_LOAN_PAYMENT} s
            LEFT JOIN {TBL_DIM_CUSTOMER} c ON s.customer_id = c.customer_id
            LEFT JOIN {TBL_DIM_DATE} d     ON d.full_date   = s.origination_date
            GROUP BY
                s.loan_id, s.customer_id, s.product_type,
                s.principal_amount, s.interest_rate, s.term_months,
                s.origination_date, d.date_id, s.origination_channel,
                s.loan_status, s.expected_emi, s.final_due_date, s.is_delinquent,
                c.credit_score, c.employment_type, c.annual_income
        """)

        total      = conn.execute(f"SELECT COUNT(*) FROM {TBL_FCT_LOAN}").fetchone()[0]
        delinquent = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_FCT_LOAN} WHERE is_delinquent = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="fct", table=TBL_FCT_LOAN,
              message=f"fct_loan complete: {total} loans, {delinquent} delinquent",
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "total_loans":      MetadataValue.int(total),
        "delinquent_loans": MetadataValue.int(delinquent),
        "duration_sec":     MetadataValue.float(duration),
        "table":            MetadataValue.text(TBL_FCT_LOAN),
    })
    return Output(value={"total_loans": total})
