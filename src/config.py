"""
config.py
---------
Central configuration for the Helix Lending pipeline.
All paths, constants, thresholds, and tuneable parameters live here.
Nothing is hardcoded in individual asset files.
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import date, timezone
from dateutil.relativedelta import relativedelta
from typing import Tuple

# ---------------------------------------------------------------------------
# Project root — resolved relative to this file so the project is portable
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Source data paths
# ---------------------------------------------------------------------------
DATA_DIR        = PROJECT_ROOT / "data"
LOAN_FILE       = DATA_DIR / "loans.csv"
PAYMENT_FILE    = DATA_DIR / "payments.jsonl"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
OUTPUT_DIR      = PROJECT_ROOT / "output"
DB_PATH         = OUTPUT_DIR / "helix_fund.db"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR         = PROJECT_ROOT / "output" / "logs"
LOG_FILE        = LOG_DIR / "pipeline.log"

# ---------------------------------------------------------------------------
# DQ thresholds
# ---------------------------------------------------------------------------
DQ_ACCEPTANCE_THRESHOLD = 0.99          # fail if < 99% rows accepted
DQ_MAX_NULL_RATE        = 0.10          # warn if any column null rate > 10%

# ---------------------------------------------------------------------------
# dim_date spine — 80 years centred on today()
# Recomputed at runtime so the spine is always current.
# ---------------------------------------------------------------------------
def get_dim_date_bounds() -> Tuple[date, date]:
    """Return (lower, upper) bounds for the dim_date spine."""
    today = date.today()
    lower = today - relativedelta(years=40)
    upper = today + relativedelta(years=40)
    return lower, upper

# ---------------------------------------------------------------------------
# Delinquency definition
# A loan is considered delinquent if no payment has been recorded
# within 30 days past the expected due date.
# ---------------------------------------------------------------------------
DELINQUENCY_DAYS = 30

# ---------------------------------------------------------------------------
# EMI tolerance — flag a payment as anomalous if it deviates from
# the expected EMI by more than this percentage.
# ---------------------------------------------------------------------------
EMI_TOLERANCE_PCT = 0.10                # 10% tolerance band

# ---------------------------------------------------------------------------
# SCD2 sentinel — open-ended row indicator in lnd_loan
# ---------------------------------------------------------------------------
SCD2_OPEN_DATE = "9999-12-31"

# ---------------------------------------------------------------------------
# Categorical canonical values
# All product_type, status, origination_channel values are lowercased
# at the landing layer. These sets define the known valid values.
# Unknown values are passed through but flagged in DQ.
# ---------------------------------------------------------------------------
VALID_PRODUCT_TYPES     = {"personal", "auto", "mortgage", "student"}
VALID_LOAN_STATUSES     = {"active", "closed", "default"}
VALID_LOAN_CHANNELS     = {"branch", "online", "partner", "mobile"}

# ---------------------------------------------------------------------------
# Audit column names — defined once, used everywhere
# ---------------------------------------------------------------------------
COL_SOURCE_FILE         = "_source_file"
COL_LAST_UPDATED_TS     = "_last_updated_ts"
COL_REJECTION_REASON    = "_rejection_reason"
COL_REJECTED_AT         = "_rejected_at"

# ---------------------------------------------------------------------------
# Ensure output directories exist at import time
# ---------------------------------------------------------------------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
