"""
init_db.py
-----------
Database initialisation script for Helix Lending pipeline.

Run this ONCE before the first pipeline execution to:
    1. Create the output/ directory
    2. Create helix_fund.db
    3. Create all tables with correct schemas and constraints
    4. Verify the schema is intact

Usage:
    python src/init_db.py

Safe to re-run — all statements use CREATE TABLE IF NOT EXISTS.
Will not drop or truncate any existing data.
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DB_PATH, OUTPUT_DIR
from utils.logger import get_logger, log_event

import duckdb

logger = get_logger("init_db")


# ---------------------------------------------------------------------------
# DDL — grouped by layer
# ---------------------------------------------------------------------------

RAW_LAYER = [
    (
        "raw_loan",
        """
        CREATE TABLE IF NOT EXISTS raw_loan (
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            principal_amount        VARCHAR,
            interest_rate           VARCHAR,
            term_months             VARCHAR,
            origination_date        VARCHAR,
            origination_channel     VARCHAR,
            status                  VARCHAR,
            borrower_info           VARCHAR,
            _source_file            VARCHAR,
            _last_updated_ts        TIMESTAMP
        )
        """,
    ),
    (
        "raw_payment",
        """
        CREATE TABLE IF NOT EXISTS raw_payment (
            payment_id                  VARCHAR,
            loan_id                     VARCHAR,
            amount                      VARCHAR,
            payment_timestamp           VARCHAR,
            payment_method_type         VARCHAR,
            payment_method_last_four    VARCHAR,
            payment_method_bank         VARCHAR,
            metadata_source             VARCHAR,
            metadata_user_agent         VARCHAR,
            _source_file                VARCHAR,
            _last_updated_ts            TIMESTAMP
        )
        """,
    ),
]

ERROR_TABLES = [
    (
        "err_loan",
        """
        CREATE TABLE IF NOT EXISTS err_loan (
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            principal_amount        VARCHAR,
            interest_rate           VARCHAR,
            term_months             VARCHAR,
            origination_date        VARCHAR,
            origination_channel     VARCHAR,
            status                  VARCHAR,
            borrower_info           VARCHAR,
            _source_file            VARCHAR,
            _last_updated_ts        TIMESTAMP,
            _rejection_reason       VARCHAR,
            _rejected_at            TIMESTAMP
        )
        """,
    ),
    (
        "err_payment",
        """
        CREATE TABLE IF NOT EXISTS err_payment (
            payment_id                  VARCHAR,
            loan_id                     VARCHAR,
            amount                      VARCHAR,
            payment_timestamp           VARCHAR,
            payment_method_type         VARCHAR,
            payment_method_last_four    VARCHAR,
            payment_method_bank         VARCHAR,
            metadata_source             VARCHAR,
            metadata_user_agent         VARCHAR,
            _source_file                VARCHAR,
            _last_updated_ts            TIMESTAMP,
            _rejection_reason           VARCHAR,
            _rejected_at                TIMESTAMP
        )
        """,
    ),
]

LANDING_LAYER = [
    (
        "lnd_loan",
        """
        CREATE TABLE IF NOT EXISTS lnd_loan (
            loan_id                 VARCHAR       NOT NULL,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            principal_amount        DECIMAL(18,2),
            interest_rate           DECIMAL(8,4),
            term_months             INTEGER,
            origination_date        DATE,
            origination_channel     VARCHAR,
            status                  VARCHAR,
            borrower_info           VARCHAR,
            row_effective_from      TIMESTAMP     NOT NULL,
            row_effective_to        DATE          NOT NULL,
            is_current_flag         BOOLEAN       NOT NULL,
            _source_file            VARCHAR,
            _last_updated_ts        TIMESTAMP,
            _row_hash               VARCHAR
        )
        """,
    ),
    (
        "lnd_payment",
        """
        CREATE TABLE IF NOT EXISTS lnd_payment (
            payment_id                  VARCHAR       NOT NULL,
            loan_id                     VARCHAR,
            amount                      DECIMAL(18,2),
            payment_timestamp           TIMESTAMPTZ,
            payment_method_type         VARCHAR,
            payment_method_last_four    VARCHAR,
            payment_method_bank         VARCHAR,
            metadata_source             VARCHAR,
            metadata_user_agent         VARCHAR,
            _source_file                VARCHAR,
            _last_updated_ts            TIMESTAMP
        )
        """,
    ),
]

DQ_TABLES = [
    (
        "dq_results",
        """
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id              VARCHAR,
            table_name          VARCHAR,
            check_name          VARCHAR,
            check_result        VARCHAR,
            metric_value        DOUBLE,
            threshold           DOUBLE,
            breach_flag         BOOLEAN,
            detail              VARCHAR,
            checked_at          TIMESTAMP
        )
        """,
    ),
]

STAGING_LAYER = [
    (
        "stg_loan_payment",
        """
        CREATE TABLE IF NOT EXISTS stg_loan_payment (
            loan_id                     VARCHAR,
            customer_id                 VARCHAR,
            product_type                VARCHAR,
            principal_amount            DECIMAL(18,2),
            interest_rate               DECIMAL(8,4),
            term_months                 INTEGER,
            origination_date            DATE,
            origination_channel         VARCHAR,
            loan_status                 VARCHAR,
            borrower_info               VARCHAR,
            row_effective_from          TIMESTAMP,
            payment_id                  VARCHAR,
            payment_amount              DECIMAL(18,2),
            payment_timestamp           TIMESTAMPTZ,
            payment_method_type         VARCHAR,
            payment_method_last_four    VARCHAR,
            payment_method_bank         VARCHAR,
            metadata_source             VARCHAR,
            metadata_user_agent         VARCHAR,
            expected_emi                DECIMAL(18,2),
            days_since_payment          INTEGER,
            final_due_date              DATE,
            is_delinquent               BOOLEAN,
            is_payment_anomalous        BOOLEAN,
            _last_updated_ts            TIMESTAMP
        )
        """,
    ),
]

DIMENSION_LAYER = [
    (
        "dim_customer",
        """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id         VARCHAR       NOT NULL,
            credit_score        INTEGER,
            employment_type     VARCHAR,
            annual_income       DECIMAL(18,2),
            years_employed      INTEGER,
            _source_loan_id     VARCHAR,
            _last_updated_ts    TIMESTAMP
        )
        """,
    ),
    (
        "dim_date",
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id         INTEGER       NOT NULL PRIMARY KEY,
            full_date       DATE          NOT NULL,
            year            INTEGER,
            quarter         INTEGER,
            month           INTEGER,
            month_name      VARCHAR,
            week_of_year    INTEGER,
            day_of_month    INTEGER,
            day_of_week     INTEGER,
            day_name        VARCHAR,
            is_weekend      BOOLEAN,
            is_month_end    BOOLEAN
        )
        """,
    ),
]

FACT_LAYER = [
    (
        "fct_loan",
        """
        CREATE TABLE IF NOT EXISTS fct_loan (
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            principal_amount        DECIMAL(18,2),
            interest_rate           DECIMAL(8,4),
            term_months             INTEGER,
            origination_date        DATE,
            origination_date_id     INTEGER,
            origination_channel     VARCHAR,
            loan_status             VARCHAR,
            expected_emi            DECIMAL(18,2),
            final_due_date          DATE,
            is_delinquent           BOOLEAN,
            last_payment_amount     DECIMAL(18,2),
            last_payment_timestamp  TIMESTAMPTZ,
            days_since_last_payment INTEGER,
            total_payment_count     BIGINT,
            total_paid_amount       DECIMAL(18,2),
            credit_score            INTEGER,
            employment_type         VARCHAR,
            annual_income           DECIMAL(18,2),
            _last_updated_ts        TIMESTAMP
        )
        """,
    ),
    (
        "fct_payment",
        """
        CREATE TABLE IF NOT EXISTS fct_payment (
            payment_id              VARCHAR,
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            payment_amount          DECIMAL(18,2),
            payment_timestamp       TIMESTAMPTZ,
            payment_date_id         INTEGER,
            payment_method_type     VARCHAR,
            payment_method_bank     VARCHAR,
            metadata_source         VARCHAR,
            expected_emi            DECIMAL(18,2),
            is_payment_anomalous    BOOLEAN,
            days_since_payment      INTEGER,
            loan_status             VARCHAR,
            _last_updated_ts        TIMESTAMP
        )
        """,
    ),
]

MART_LAYER = [
    (
        "mart_delinquency",
        """
        CREATE TABLE IF NOT EXISTS mart_delinquency (
            product_type                VARCHAR,
            run_date                    DATE,
            total_active_loans          INTEGER,
            delinquent_loans            INTEGER,
            delinquency_rate_pct        DECIMAL(6,2),
            avg_days_since_payment      DECIMAL(8,1),
            avg_principal_delinquent    DECIMAL(18,2),
            _last_updated_ts            TIMESTAMP
        )
        """,
    ),
    (
        "mart_payment_anomaly",
        """
        CREATE TABLE IF NOT EXISTS mart_payment_anomaly (
            payment_id              VARCHAR,
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            product_type            VARCHAR,
            payment_amount          DECIMAL(18,2),
            expected_emi            DECIMAL(18,2),
            deviation_pct           DECIMAL(8,2),
            payment_method_type     VARCHAR,
            payment_timestamp       TIMESTAMPTZ,
            loan_status             VARCHAR,
            anomaly_reason          VARCHAR,
            _last_updated_ts        TIMESTAMP
        )
        """,
    ),
    (
        "mart_data_observability",
        """
        CREATE TABLE IF NOT EXISTS mart_data_observability (
            run_date                DATE,
            source_name             VARCHAR,
            raw_rows_in_batch       INTEGER,
            lnd_rows_accepted       INTEGER,
            lnd_rows_rejected       INTEGER,
            acceptance_rate_pct     DECIMAL(6,2),
            dq_checks_run           INTEGER,
            dq_checks_failed        INTEGER,
            dq_breach_flag          BOOLEAN,
            fct_rows                INTEGER,
            latest_batch_ts         TIMESTAMP,
            freshness_hours         DECIMAL(8,2),
            pipeline_status         VARCHAR,
            _last_updated_ts        TIMESTAMP
        )
        """,
    ),
]

ALL_TABLES = (
    RAW_LAYER
    + ERROR_TABLES
    + LANDING_LAYER
    + DQ_TABLES
    + STAGING_LAYER
    + DIMENSION_LAYER
    + FACT_LAYER
    + MART_LAYER
)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def init_db() -> bool:
    """
    Create all tables in helix_fund.db.
    Returns True on success, False on any failure.
    """
    start      = time.time()
    batch_date = datetime.now(timezone.utc).date().isoformat()

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_event(
        logger, event="load_start", layer="init",
        message=f"Initialising database at {DB_PATH}",
        batch_date=batch_date,
    )

    try:
        conn = duckdb.connect(str(DB_PATH))
    except Exception as e:
        log_event(
            logger, event="pipeline_fail", layer="init",
            message=f"Cannot connect to DuckDB at {DB_PATH}: {e}",
            level="ERROR", batch_date=batch_date,
        )
        return False

    created = 0
    failed  = 0

    for table_name, ddl in ALL_TABLES:
        try:
            conn.execute(ddl)
            created += 1
            log_event(
                logger, event="checkpoint", layer="init",
                message=f"✅  {table_name}",
                table=table_name, batch_date=batch_date,
            )
        except Exception as e:
            failed += 1
            log_event(
                logger, event="pipeline_fail", layer="init",
                message=f"❌  {table_name}: {e}",
                table=table_name, level="ERROR", batch_date=batch_date,
            )

    conn.close()
    duration = round(time.time() - start, 3)

    if failed:
        log_event(
            logger, event="pipeline_fail", layer="init",
            message=f"Initialisation FAILED — {failed} table(s) not created",
            duration_sec=duration, batch_date=batch_date, level="ERROR",
        )
        return False

    log_event(
        logger, event="load_end", layer="init",
        message=f"Initialisation complete — {created} tables ready in {DB_PATH}",
        duration_sec=duration, batch_date=batch_date,
    )
    return True


def verify_schema() -> None:
    """Print all tables and their row counts for a quick sanity check."""
    conn = duckdb.connect(str(DB_PATH))
    rows = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()
    conn.close()

    print(f"\n{'─' * 45}")
    print(f"  helix_fund.db — {len(rows)} tables")
    print(f"{'─' * 45}")
    layers = {
        "raw_":   "Raw",
        "err_":   "Error",
        "lnd_":   "Landing",
        "dq_":    "DQ",
        "stg_":   "Staging",
        "dim_":   "Dimension",
        "fct_":   "Fact",
        "mart_":  "Mart",
    }
    for (name,) in rows:
        layer = next(
            (label for prefix, label in layers.items() if name.startswith(prefix)),
            "Other",
        )
        print(f"  [{layer:10s}]  {name}")
    print(f"{'─' * 45}\n")


if __name__ == "__main__":
    print("\n🔧  Helix Lending — Database Initialisation")
    print(f"    Target: {DB_PATH}\n")

    success = init_db()

    if success:
        verify_schema()
        print("✅  Database ready. You can now run the pipeline:\n")
        print("    cd src && dagster dev -f definitions.py\n")
        sys.exit(0)
    else:
        print("\n❌  Initialisation failed. Check logs above.\n")
        sys.exit(1)
