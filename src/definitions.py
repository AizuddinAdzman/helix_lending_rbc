"""
definitions.py
---------------
Dagster Definitions — registers all assets, resources, and jobs.

Entry point for `dagster dev` and `dagster asset materialize`.

Asset execution order (enforced by deps):
    PARALLEL:
        raw_loan   → lnd_loan   → dq_lnd_loan   ─┐
        raw_payment → lnd_payment → dq_lnd_payment ─┤
                                                    │
    BARRIER:                                        ▼
        stg_loan_payment ◄──────────────────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
    dim_customer  dim_date   (parallel)
        └─────┬───┘
              ▼
        fct_loan   fct_payment  (parallel)
              │         │
        ┌─────┴─────────┴──────────┐
        ▼           ▼              ▼
    mart_delinquency  mart_payment_anomaly
                                   mart_data_observability (last)
"""

from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DB_PATH
from resources.duckdb_resource import DuckDBResource

# Import asset modules
from assets.ingestion   import raw_loan, raw_payment
from assets.landing     import lnd_loan, lnd_payment
from assets.dq          import dq_lnd_loan, dq_lnd_payment
from assets.transformation import (
    stg_loan_payment, dim_customer, dim_date,
    fct_loan, fct_payment,
)
from assets.marts import (
    mart_delinquency, mart_payment_anomaly, mart_observability,
)

# Load all assets from modules
all_assets = load_assets_from_modules([
    raw_loan, raw_payment,
    lnd_loan, lnd_payment,
    dq_lnd_loan, dq_lnd_payment,
    stg_loan_payment, dim_customer, dim_date,
    fct_loan, fct_payment,
    mart_delinquency, mart_payment_anomaly, mart_observability,
])

# Full pipeline job
helix_pipeline_job = define_asset_job(
    name="helix_full_pipeline",
    selection="*",
    description="Full Helix Lending pipeline: raw → landing → DQ → staging → facts → marts",
)

# Daily schedule (02:00 UTC)
daily_schedule = ScheduleDefinition(
    job=helix_pipeline_job,
    cron_schedule="0 2 * * *",
    name="helix_daily_0200_utc",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_resource": DuckDBResource(db_path=str(DB_PATH)),
    },
    jobs=[helix_pipeline_job],
    schedules=[daily_schedule],
)
