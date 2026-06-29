"""
init_db.py
-----------
Database initialisation for Helix Lending pipeline.

Creates all schemas and tables in helix_{ENV}.db.
Safe to re-run — uses CREATE SCHEMA IF NOT EXISTS and CREATE TABLE IF NOT EXISTS.

Usage:
    python src/init_db.py                  # dev (default)
    HELIX_ENV=prd python src/init_db.py   # production
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    ENV, DB_PATH, OUTPUT_DIR,
    SCHEMA_RAW, SCHEMA_LND, SCHEMA_STG, SCHEMA_DIM, SCHEMA_FCT, SCHEMA_MART,
    TBL_RAW_LOAN, TBL_RAW_PAYMENT, TBL_RAW_AUDIT,
    TBL_LND_LOAN, TBL_LND_PAYMENT, TBL_LND_ERR_LOAN, TBL_LND_ERR_PAYMENT, TBL_LND_DQ_AUDIT,
    TBL_STG_LOAN_PAYMENT,
    TBL_DIM_CUSTOMER, TBL_DIM_DATE,
    TBL_FCT_LOAN, TBL_FCT_PAYMENT,
    TBL_MART_DELINQUENCY, TBL_MART_ANOMALY, TBL_MART_OBSERVABILITY,
)
from utils.logger import get_logger, log_event

import duckdb

logger = get_logger("init_db")

SCHEMAS = [SCHEMA_RAW, SCHEMA_LND, SCHEMA_STG, SCHEMA_DIM, SCHEMA_FCT, SCHEMA_MART]

TABLES = [
    # ── RAW ──────────────────────────────────────────────────────────────────
    (TBL_RAW_LOAN, f"""
        CREATE TABLE IF NOT EXISTS {TBL_RAW_LOAN} (
            loan_id              VARCHAR,
            customer_id          VARCHAR,
            product_type         VARCHAR,
            principal_amount     VARCHAR,
            interest_rate        VARCHAR,
            term_months          VARCHAR,
            origination_date     VARCHAR,
            origination_channel  VARCHAR,
            status               VARCHAR,
            borrower_info        VARCHAR,
            _source_file         VARCHAR,
            _last_updated_ts     TIMESTAMP
        )"""),
    (TBL_RAW_PAYMENT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_RAW_PAYMENT} (
            payment_id                VARCHAR,
            loan_id                   VARCHAR,
            amount                    VARCHAR,
            payment_timestamp         VARCHAR,
            payment_method_type       VARCHAR,
            payment_method_last_four  VARCHAR,
            payment_method_bank       VARCHAR,
            metadata_source           VARCHAR,
            metadata_user_agent       VARCHAR,
            _source_file              VARCHAR,
            _last_updated_ts          TIMESTAMP
        )"""),
    (TBL_RAW_AUDIT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_RAW_AUDIT} (
            batch_ts                    TIMESTAMP     NOT NULL,
            source_file                 VARCHAR       NOT NULL,
            source_table                VARCHAR       NOT NULL,
            total_rows_in_file          INTEGER,
            total_rows_inserted         INTEGER,
            distinct_keys               INTEGER,
            duplicate_key_count         INTEGER,
            true_duplicate_count        INTEGER,
            diff_amount_same_id_count   INTEGER,
            _last_updated_ts            TIMESTAMP
        )"""),
    # ── LANDING ──────────────────────────────────────────────────────────────
    (TBL_LND_LOAN, f"""
        CREATE TABLE IF NOT EXISTS {TBL_LND_LOAN} (
            loan_id              VARCHAR       NOT NULL,
            customer_id          VARCHAR,
            product_type         VARCHAR,
            principal_amount     DECIMAL(18,2),
            interest_rate        DECIMAL(8,4),
            term_months          INTEGER,
            origination_date     DATE,
            origination_channel  VARCHAR,
            status               VARCHAR,
            borrower_info        VARCHAR,
            row_effective_from   TIMESTAMP     NOT NULL,
            row_effective_to     DATE          NOT NULL,
            is_current_flag      BOOLEAN       NOT NULL,
            _source_file         VARCHAR,
            _last_updated_ts     TIMESTAMP,
            _row_hash            VARCHAR
        )"""),
    (TBL_LND_PAYMENT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_LND_PAYMENT} (
            lnd_payment_sk            BIGINT,
            payment_id                VARCHAR       NOT NULL,
            loan_id                   VARCHAR,
            amount                    DECIMAL(18,2),
            payment_timestamp         TIMESTAMPTZ,
            payment_method_type       VARCHAR,
            payment_method_last_four  VARCHAR,
            payment_method_bank       VARCHAR,
            metadata_source           VARCHAR,
            metadata_user_agent       VARCHAR,
            _source_file              VARCHAR,
            _last_updated_ts          TIMESTAMP
        )"""),
    (TBL_LND_ERR_LOAN, f"""
        CREATE TABLE IF NOT EXISTS {TBL_LND_ERR_LOAN} (
            loan_id              VARCHAR,
            customer_id          VARCHAR,
            product_type         VARCHAR,
            principal_amount     VARCHAR,
            interest_rate        VARCHAR,
            term_months          VARCHAR,
            origination_date     VARCHAR,
            origination_channel  VARCHAR,
            status               VARCHAR,
            borrower_info        VARCHAR,
            _source_file         VARCHAR,
            _last_updated_ts     TIMESTAMP,
            _rejection_reason    VARCHAR,
            _rejected_at         TIMESTAMP
        )"""),
    (TBL_LND_ERR_PAYMENT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_LND_ERR_PAYMENT} (
            payment_id                VARCHAR,
            loan_id                   VARCHAR,
            amount                    VARCHAR,
            payment_timestamp         VARCHAR,
            payment_method_type       VARCHAR,
            payment_method_last_four  VARCHAR,
            payment_method_bank       VARCHAR,
            metadata_source           VARCHAR,
            metadata_user_agent       VARCHAR,
            _source_file              VARCHAR,
            _last_updated_ts          TIMESTAMP,
            _rejection_reason         VARCHAR,
            _rejected_at              TIMESTAMP
        )"""),
    (TBL_LND_DQ_AUDIT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_LND_DQ_AUDIT} (
            run_id        VARCHAR,
            table_name    VARCHAR,
            check_name    VARCHAR,
            check_result  VARCHAR,
            metric_value  DOUBLE,
            threshold     DOUBLE,
            breach_flag   BOOLEAN,
            detail        VARCHAR,
            checked_at    TIMESTAMP
        )"""),
    # ── STAGING ──────────────────────────────────────────────────────────────
    (TBL_STG_LOAN_PAYMENT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_STG_LOAN_PAYMENT} (
            loan_id                   VARCHAR,
            customer_id               VARCHAR,
            product_type              VARCHAR,
            principal_amount          DECIMAL(18,2),
            interest_rate             DECIMAL(8,4),
            term_months               INTEGER,
            origination_date          DATE,
            origination_channel       VARCHAR,
            loan_status               VARCHAR,
            borrower_info             VARCHAR,
            row_effective_from        TIMESTAMP,
            payment_id                VARCHAR,
            payment_amount            DECIMAL(18,2),
            payment_timestamp         TIMESTAMPTZ,
            payment_method_type       VARCHAR,
            payment_method_last_four  VARCHAR,
            payment_method_bank       VARCHAR,
            metadata_source           VARCHAR,
            metadata_user_agent       VARCHAR,
            expected_emi              DECIMAL(18,2),
            days_since_payment        INTEGER,
            final_due_date            DATE,
            is_delinquent             BOOLEAN,
            is_payment_anomalous      BOOLEAN,
            _last_updated_ts          TIMESTAMP
        )"""),
    # ── DIMENSIONS ───────────────────────────────────────────────────────────
    (TBL_DIM_CUSTOMER, f"""
        CREATE TABLE IF NOT EXISTS {TBL_DIM_CUSTOMER} (
            customer_id       VARCHAR       NOT NULL,
            credit_score      INTEGER,
            employment_type   VARCHAR,
            annual_income     DECIMAL(18,2),
            years_employed    INTEGER,
            _source_loan_id   VARCHAR,
            _last_updated_ts  TIMESTAMP
        )"""),
    (TBL_DIM_DATE, f"""
        CREATE TABLE IF NOT EXISTS {TBL_DIM_DATE} (
            date_id       INTEGER  NOT NULL PRIMARY KEY,
            full_date     DATE     NOT NULL,
            year          INTEGER,
            quarter       INTEGER,
            month         INTEGER,
            month_name    VARCHAR,
            week_of_year  INTEGER,
            day_of_month  INTEGER,
            day_of_week   INTEGER,
            day_name      VARCHAR,
            is_weekend    BOOLEAN,
            is_month_end  BOOLEAN
        )"""),
    # ── FACTS ────────────────────────────────────────────────────────────────
    (TBL_FCT_LOAN, f"""
        CREATE TABLE IF NOT EXISTS {TBL_FCT_LOAN} (
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
        )"""),
    (TBL_FCT_PAYMENT, f"""
        CREATE TABLE IF NOT EXISTS {TBL_FCT_PAYMENT} (
            lnd_payment_sk        BIGINT,
            payment_id            VARCHAR,
            loan_id               VARCHAR,
            customer_id           VARCHAR,
            product_type          VARCHAR,
            payment_amount        DECIMAL(18,2),
            payment_timestamp     TIMESTAMPTZ,
            payment_date_id       INTEGER,
            payment_method_type   VARCHAR,
            payment_method_bank   VARCHAR,
            metadata_source       VARCHAR,
            expected_emi          DECIMAL(18,2),
            is_payment_anomalous  BOOLEAN,
            days_since_payment    INTEGER,
            loan_status           VARCHAR,
            _last_updated_ts      TIMESTAMP
        )"""),
    # ── MARTS ────────────────────────────────────────────────────────────────
    (TBL_MART_DELINQUENCY, f"""
        CREATE TABLE IF NOT EXISTS {TBL_MART_DELINQUENCY} (
            product_type                VARCHAR,
            run_date                    DATE,
            total_active_loans          INTEGER,
            delinquent_loans            INTEGER,
            delinquency_rate_pct        DECIMAL(6,2),
            avg_days_since_payment      DECIMAL(8,1),
            avg_principal_delinquent    DECIMAL(18,2),
            _last_updated_ts            TIMESTAMP
        )"""),
    (TBL_MART_ANOMALY, f"""
        CREATE TABLE IF NOT EXISTS {TBL_MART_ANOMALY} (
            payment_id            VARCHAR,
            loan_id               VARCHAR,
            customer_id           VARCHAR,
            product_type          VARCHAR,
            payment_amount        DECIMAL(18,2),
            expected_emi          DECIMAL(18,2),
            deviation_pct         DECIMAL(8,2),
            payment_method_type   VARCHAR,
            payment_timestamp     TIMESTAMPTZ,
            loan_status           VARCHAR,
            anomaly_reason        VARCHAR,
            _last_updated_ts      TIMESTAMP
        )"""),
    (TBL_MART_OBSERVABILITY, f"""
        CREATE TABLE IF NOT EXISTS {TBL_MART_OBSERVABILITY} (
            run_date              DATE,
            source_name           VARCHAR,
            raw_rows_in_batch     INTEGER,
            lnd_rows_accepted     INTEGER,
            lnd_rows_rejected     INTEGER,
            acceptance_rate_pct   DECIMAL(6,2),
            dq_checks_run         INTEGER,
            dq_checks_failed      INTEGER,
            dq_breach_flag        BOOLEAN,
            fct_rows              INTEGER,
            latest_batch_ts       TIMESTAMP,
            freshness_hours       DECIMAL(8,2),
            pipeline_status       VARCHAR,
            _last_updated_ts      TIMESTAMP
        )"""),
]


def init_db() -> bool:
    start      = time.time()
    batch_date = datetime.now(timezone.utc).date().isoformat()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_event(
        logger, event="load_start", layer="init",
        message=f"Initialising [{ENV}] database at {DB_PATH}",
        batch_date=batch_date,
    )

    try:
        conn = duckdb.connect(str(DB_PATH))
    except Exception as e:
        log_event(
            logger, event="pipeline_fail", layer="init",
            message=f"Cannot connect to DuckDB: {e}",
            level="ERROR", batch_date=batch_date,
        )
        return False

    # Create schemas
    for schema in SCHEMAS:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        log_event(
            logger, event="checkpoint", layer="init",
            message=f"  schema: {schema}", batch_date=batch_date,
        )

    # Create tables
    created = failed = 0
    for table_name, ddl in TABLES:
        try:
            conn.execute(ddl)
            created += 1
            log_event(
                logger, event="checkpoint", layer="init",
                message=f"  ✅  {table_name}", batch_date=batch_date,
            )
        except Exception as e:
            failed += 1
            log_event(
                logger, event="pipeline_fail", layer="init",
                message=f"  ❌  {table_name}: {e}",
                level="ERROR", batch_date=batch_date,
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
        message=f"Initialisation complete — {len(SCHEMAS)} schemas, {created} tables in {DB_PATH}",
        duration_sec=duration, batch_date=batch_date,
    )
    return True


def verify_schema() -> None:
    conn = duckdb.connect(str(DB_PATH))
    rows = conn.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
        ORDER BY table_schema, table_name
    """).fetchall()
    conn.close()

    print(f"\n{'─' * 55}")
    print(f"  helix_{ENV}.db  [{len(rows)} tables across {len(SCHEMAS)} schemas]")
    print(f"{'─' * 55}")
    current_schema = None
    for schema, name in rows:
        if schema != current_schema:
            print(f"\n  [{schema}]")
            current_schema = schema
        print(f"    {name}")
    print(f"\n{'─' * 55}\n")


if __name__ == "__main__":
    print(f"\n🔧  Helix Lending — Database Initialisation  [ENV={ENV}]")
    print(f"    Target: {DB_PATH}\n")

    success = init_db()
    if success:
        verify_schema()
        print(f"✅  Database ready.\n")
        print(f"    cd src && dagster dev -f definitions.py\n")
        sys.exit(0)
    else:
        print("\n❌  Initialisation failed. Check logs above.\n")
        sys.exit(1)
