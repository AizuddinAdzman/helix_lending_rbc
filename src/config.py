"""
config.py
---------
Central configuration for the Helix Lending pipeline.

Environment is driven by HELIX_ENV environment variable (default: dev).
All schema names, DB path, and table references are derived from ENV —
nothing is hardcoded.

Usage:
    HELIX_ENV=dev  dagster dev -f definitions.py   # default
    HELIX_ENV=prd  dagster dev -f definitions.py   # production
"""

import os
from pathlib import Path
from datetime import date
from typing import Tuple
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ENV = os.getenv("HELIX_ENV", "dev")

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Source data paths
# ---------------------------------------------------------------------------
DATA_DIR     = PROJECT_ROOT / "data"
LOAN_FILE    = DATA_DIR / "loans.csv"
PAYMENT_FILE = DATA_DIR / "payments.jsonl"

# ---------------------------------------------------------------------------
# Database — ENV-driven filename
# ---------------------------------------------------------------------------
OUTPUT_DIR = PROJECT_ROOT / "output"
DB_PATH    = OUTPUT_DIR / f"helix_{ENV}.db"

# ---------------------------------------------------------------------------
# Schema names — derived from ENV, never hardcoded
# ---------------------------------------------------------------------------
SCHEMA_RAW  = f"hlx_{ENV}_raw"
SCHEMA_LND  = f"hlx_{ENV}_lnd"
SCHEMA_STG  = f"hlx_{ENV}_stg"
SCHEMA_DIM  = f"hlx_{ENV}_dim"
SCHEMA_FCT  = f"hlx_{ENV}_fct"
SCHEMA_MART = f"hlx_{ENV}_mart"

# ---------------------------------------------------------------------------
# Fully qualified table names
# ---------------------------------------------------------------------------

# Raw layer
TBL_RAW_LOAN     = f"{SCHEMA_RAW}.raw_loan"
TBL_RAW_PAYMENT  = f"{SCHEMA_RAW}.raw_payment"

# Landing layer
TBL_LND_LOAN     = f"{SCHEMA_LND}.lnd_loan"
TBL_LND_PAYMENT  = f"{SCHEMA_LND}.lnd_payment"
TBL_LND_ERR_LOAN    = f"{SCHEMA_LND}.lnd_err_loan"
TBL_LND_ERR_PAYMENT = f"{SCHEMA_LND}.lnd_err_payment"
TBL_LND_DQ_AUDIT    = f"{SCHEMA_LND}.lnd_dq_audit"

# Staging layer
TBL_STG_LOAN_PAYMENT = f"{SCHEMA_STG}.stg_loan_payment"

# Dimension layer
TBL_DIM_CUSTOMER = f"{SCHEMA_DIM}.dim_customer"
TBL_DIM_DATE     = f"{SCHEMA_DIM}.dim_date"

# Fact layer
TBL_FCT_LOAN    = f"{SCHEMA_FCT}.fct_loan"
TBL_FCT_PAYMENT = f"{SCHEMA_FCT}.fct_payment"

# Mart layer
TBL_MART_DELINQUENCY   = f"{SCHEMA_MART}.mart_delinquency"
TBL_MART_ANOMALY       = f"{SCHEMA_MART}.mart_payment_anomaly"
TBL_MART_OBSERVABILITY = f"{SCHEMA_MART}.mart_data_observability"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR  = OUTPUT_DIR / "logs"
LOG_FILE = LOG_DIR / f"pipeline_{ENV}.log"

# ---------------------------------------------------------------------------
# DQ thresholds
# ---------------------------------------------------------------------------
DQ_ACCEPTANCE_THRESHOLD = 0.99
DQ_MAX_NULL_RATE        = 0.10

# ---------------------------------------------------------------------------
# dim_date spine — 80 years centred on today()
# ---------------------------------------------------------------------------
def get_dim_date_bounds() -> Tuple[date, date]:
    """Return (lower, upper) bounds for the dim_date spine."""
    today = date.today()
    return today - relativedelta(years=40), today + relativedelta(years=40)

# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------
DELINQUENCY_DAYS  = 30
EMI_TOLERANCE_PCT = 0.10
SCD2_OPEN_DATE    = "9999-12-31"

# ---------------------------------------------------------------------------
# Categorical valid values
# ---------------------------------------------------------------------------
VALID_PRODUCT_TYPES = {"personal", "auto", "mortgage", "student"}
VALID_LOAN_STATUSES = {"active", "closed", "default"}
VALID_LOAN_CHANNELS = {"branch", "online", "partner", "mobile"}

# ---------------------------------------------------------------------------
# Audit column names
# ---------------------------------------------------------------------------
COL_SOURCE_FILE      = "_source_file"
COL_LAST_UPDATED_TS  = "_last_updated_ts"
COL_REJECTION_REASON = "_rejection_reason"
COL_REJECTED_AT      = "_rejected_at"

# ---------------------------------------------------------------------------
# Ensure output directories exist at import time
# ---------------------------------------------------------------------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
