"""
assets/transformation/stg_loan_payment.py
------------------------------------------
Dagster asset: stg_loan_payment

Responsibility:
    Join lnd_loan (current rows) + lnd_payment → enriched staging table.
    Derive EMI, delinquency flag, payment anomaly flag.

Grain: one row per payment event, enriched with loan context.
       Loans with no payments still appear (LEFT JOIN).

Derived columns:
    expected_emi            standard amortisation formula
    days_since_last_payment days between payment_timestamp and today
    is_delinquent           no payment in 30+ days past due date
    is_payment_anomalous    payment amount deviates > 10% from EMI

Dependencies:
    dq_lnd_loan   — must pass before this runs
    dq_lnd_payment — must pass before this runs
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import DELINQUENCY_DAYS, EMI_TOLERANCE_PCT
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

DDL_STG = """
CREATE OR REPLACE TABLE stg_loan_payment AS
SELECT
    -- Loan keys
    l.loan_id,
    l.customer_id,
    l.product_type,
    l.principal_amount,
    l.interest_rate,
    l.term_months,
    l.origination_date,
    l.origination_channel,
    l.status                            AS loan_status,
    l.borrower_info,
    l.row_effective_from,

    -- Payment fields (NULL for loans with no payments)
    p.payment_id,
    p.amount                            AS payment_amount,
    p.payment_timestamp,
    p.payment_method_type,
    p.payment_method_last_four,
    p.payment_method_bank,
    p.metadata_source,
    p.metadata_user_agent,

    -- Derived: expected monthly instalment
    CASE
        WHEN l.interest_rate = 0 OR l.interest_rate IS NULL
            THEN ROUND(l.principal_amount / NULLIF(l.term_months, 0), 2)
        WHEN l.principal_amount IS NULL OR l.term_months IS NULL
            THEN NULL
        ELSE ROUND(
            l.principal_amount
            * (l.interest_rate / 12.0 / 100.0)
            * POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months)
            / (POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months) - 1),
        2)
    END                                 AS expected_emi,

    -- Derived: days since last payment (NULL if no payment)
    CASE
        WHEN p.payment_timestamp IS NULL THEN NULL
        ELSE DATEDIFF('day', CAST(p.payment_timestamp AS DATE), CURRENT_DATE)
    END                                 AS days_since_payment,

    -- Derived: expected due date (origination + 1 month rolling)
    -- Approximated as: origination_date + term_months months for final due
    l.origination_date
        + INTERVAL (l.term_months) MONTH AS final_due_date,

    -- Derived: delinquency flag
    -- Loan is delinquent if:
    --   status = 'active' AND no payment recorded within 30 days
    --   of any expected monthly due date that has passed
    CASE
        WHEN l.status != 'active' THEN FALSE
        WHEN p.payment_timestamp IS NULL
            AND DATEDIFF('day', l.origination_date, CURRENT_DATE) > {delinquency_days}
            THEN TRUE
        WHEN p.payment_timestamp IS NOT NULL
            AND DATEDIFF('day', CAST(p.payment_timestamp AS DATE), CURRENT_DATE)
                > {delinquency_days}
            THEN TRUE
        ELSE FALSE
    END                                 AS is_delinquent,

    -- Derived: payment anomaly flag
    CASE
        WHEN p.amount IS NULL THEN FALSE
        WHEN CASE
                WHEN l.interest_rate = 0 OR l.interest_rate IS NULL
                    THEN ROUND(l.principal_amount / NULLIF(l.term_months, 0), 2)
                WHEN l.principal_amount IS NULL OR l.term_months IS NULL
                    THEN NULL
                ELSE ROUND(
                    l.principal_amount
                    * (l.interest_rate / 12.0 / 100.0)
                    * POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months)
                    / (POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months) - 1),
                2)
             END IS NULL THEN FALSE
        ELSE ABS(p.amount - CASE
                WHEN l.interest_rate = 0 OR l.interest_rate IS NULL
                    THEN ROUND(l.principal_amount / NULLIF(l.term_months, 0), 2)
                ELSE ROUND(
                    l.principal_amount
                    * (l.interest_rate / 12.0 / 100.0)
                    * POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months)
                    / (POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months) - 1),
                2)
             END)
             / NULLIF(CASE
                WHEN l.interest_rate = 0 OR l.interest_rate IS NULL
                    THEN ROUND(l.principal_amount / NULLIF(l.term_months, 0), 2)
                ELSE ROUND(
                    l.principal_amount
                    * (l.interest_rate / 12.0 / 100.0)
                    * POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months)
                    / (POWER(1 + l.interest_rate / 12.0 / 100.0, l.term_months) - 1),
                2)
             END, 0) > {emi_tolerance}
    END                                 AS is_payment_anomalous,

    -- Audit
    CURRENT_TIMESTAMP                   AS _last_updated_ts

FROM lnd_loan l
LEFT JOIN lnd_payment p
    ON l.loan_id = p.loan_id
WHERE l.is_current_flag = TRUE
""".format(
    delinquency_days=DELINQUENCY_DAYS,
    emi_tolerance=EMI_TOLERANCE_PCT,
)


@asset(
    group_name="transformation",
    deps=["dq_lnd_loan", "dq_lnd_payment"],
    description=(
        "Join lnd_loan + lnd_payment → stg_loan_payment. "
        "Derives EMI, delinquency flag, payment anomaly flag. "
        "Blocked until both DQ gates pass."
    ),
)
def stg_loan_payment(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    log_event(
        logger, event="load_start", layer="stg", table="stg_loan_payment",
        message="Starting stg_loan_payment build",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_STG)

        total      = conn.execute("SELECT COUNT(*) FROM stg_loan_payment").fetchone()[0]
        delinquent = conn.execute(
            "SELECT COUNT(*) FROM stg_loan_payment WHERE is_delinquent = TRUE"
        ).fetchone()[0]
        anomalous  = conn.execute(
            "SELECT COUNT(*) FROM stg_loan_payment WHERE is_payment_anomalous = TRUE"
        ).fetchone()[0]
        no_payment = conn.execute(
            "SELECT COUNT(*) FROM stg_loan_payment WHERE payment_id IS NULL"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="stg", table="stg_loan_payment",
        message=(
            f"stg_loan_payment complete: {total} rows, "
            f"{delinquent} delinquent, {anomalous} anomalous payments, "
            f"{no_payment} loans with no payments"
        ),
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "total_rows":          MetadataValue.int(total),
        "delinquent_count":    MetadataValue.int(delinquent),
        "anomalous_count":     MetadataValue.int(anomalous),
        "no_payment_count":    MetadataValue.int(no_payment),
        "duration_sec":        MetadataValue.float(duration),
    })

    return Output(value={"total_rows": total})
