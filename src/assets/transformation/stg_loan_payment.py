"""
assets/transformation/stg_loan_payment.py
------------------------------------------
Schema: hlx_{ENV}_stg
Reads from: hlx_{ENV}_lnd
Sequential after dq_lnd_payment.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    DELINQUENCY_DAYS, EMI_TOLERANCE_PCT,
    TBL_LND_LOAN, TBL_LND_PAYMENT, TBL_STG_LOAN_PAYMENT, SCHEMA_STG,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="transformation",
    deps=["dq_lnd_payment"],
    description=f"Join lnd_loan + lnd_payment → {TBL_STG_LOAN_PAYMENT} with EMI + flags",
)
def stg_loan_payment(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(logger, event="load_start", layer="stg",
              table=TBL_STG_LOAN_PAYMENT,
              message="Building stg_loan_payment", batch_date=batch_date_str)

    emi_expr = f"""
        CASE
            WHEN l.interest_rate IS NULL OR l.interest_rate = 0
                THEN ROUND(l.principal_amount / NULLIF(l.term_months, 0), 2)
            WHEN l.principal_amount IS NULL OR l.term_months IS NULL THEN NULL
            ELSE ROUND(
                l.principal_amount
                * (l.interest_rate / 12.0 / 100.0)
                * POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months)
                / (POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months) - 1),
            2)
        END"""

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_STG}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_STG_LOAN_PAYMENT} AS
            SELECT
                l.loan_id, l.customer_id, l.product_type,
                l.principal_amount, l.interest_rate, l.term_months,
                l.origination_date, l.origination_channel,
                l.status AS loan_status, l.borrower_info,
                l.row_effective_from,
                p.payment_id,
                p.amount AS payment_amount,
                p.payment_timestamp,
                p.payment_method_type, p.payment_method_last_four,
                p.payment_method_bank, p.metadata_source, p.metadata_user_agent,
                {emi_expr} AS expected_emi,
                CASE
                    WHEN p.payment_timestamp IS NULL THEN NULL
                    ELSE DATEDIFF('day', CAST(p.payment_timestamp AS DATE), CURRENT_DATE)
                END AS days_since_payment,
                l.origination_date + INTERVAL (l.term_months) MONTH AS final_due_date,
                CASE
                    WHEN l.status != 'active' THEN FALSE
                    WHEN p.payment_timestamp IS NULL
                         AND DATEDIFF('day', l.origination_date, CURRENT_DATE)
                             > {DELINQUENCY_DAYS} THEN TRUE
                    WHEN p.payment_timestamp IS NOT NULL
                         AND DATEDIFF('day', CAST(p.payment_timestamp AS DATE), CURRENT_DATE)
                             > {DELINQUENCY_DAYS} THEN TRUE
                    ELSE FALSE
                END AS is_delinquent,
                CASE
                    WHEN p.amount IS NULL THEN FALSE
                    ELSE ABS(p.amount - ({emi_expr}))
                         / NULLIF(({emi_expr}), 0) > {EMI_TOLERANCE_PCT}
                END AS is_payment_anomalous,
                CURRENT_TIMESTAMP AS _last_updated_ts
            FROM {TBL_LND_LOAN} l
            LEFT JOIN {TBL_LND_PAYMENT} p ON l.loan_id = p.loan_id
            WHERE l.is_current_flag = TRUE
        """)

        total      = conn.execute(f"SELECT COUNT(*) FROM {TBL_STG_LOAN_PAYMENT}").fetchone()[0]
        delinquent = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_STG_LOAN_PAYMENT} WHERE is_delinquent = TRUE"
        ).fetchone()[0]
        anomalous  = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_STG_LOAN_PAYMENT} WHERE is_payment_anomalous = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="stg", table=TBL_STG_LOAN_PAYMENT,
              message=f"stg complete: {total} rows, {delinquent} delinquent, {anomalous} anomalous",
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "total_rows":       MetadataValue.int(total),
        "delinquent_count": MetadataValue.int(delinquent),
        "anomalous_count":  MetadataValue.int(anomalous),
        "duration_sec":     MetadataValue.float(duration),
        "table":            MetadataValue.text(TBL_STG_LOAN_PAYMENT),
    })
    return Output(value={"total_rows": total})
