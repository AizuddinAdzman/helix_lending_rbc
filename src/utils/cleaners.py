"""
utils/cleaners.py
------------------
Pure transformation functions for cleaning raw string values.

Design principles:
  - Every function takes a raw string, returns a cleaned Python native type
    or raises ValueError with a descriptive message.
  - No side effects — safe to unit test in isolation.
  - Used by lnd_ layer assets only. Raw layer never calls these.
"""

import re
import logging
from datetime import date, datetime, timezone
from typing import Optional
from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# principal_amount — handles "$33,517.74", "33517.74", "33,517.74"
# ---------------------------------------------------------------------------

def clean_principal_amount(raw: Optional[str]) -> Optional[float]:
    """
    Strip currency symbols and commas, return float.

    Negative values are accepted — they represent credit balances:
        Overpayments, escrow refunds, lender corrections, etc.
    These are classified as loan_balance_type='credit_balance' at staging.

    Examples:
        "$33,517.74"  → 33517.74   (debit balance)
        "32256.80"    → 32256.80   (debit balance)
        "-5000.00"    → -5000.0    (credit balance — accepted)
        "-$1,250.00"  → -1250.0    (credit balance — accepted)
        None / ""     → None
        "not_a_number" → ValueError
    """
    if raw is None or str(raw).strip() == "":
        return None
    raw_stripped = str(raw).strip()

    # Strip currency symbols and whitespace before parsing
    cleaned = re.sub(r"[\$,\s]", "", raw_stripped)

    if not cleaned or cleaned == "-":
        return None

    try:
        return float(cleaned)
    except ValueError:
        raise ValueError(f"Cannot parse principal_amount: {raw!r}")


# ---------------------------------------------------------------------------
# origination_date — handles mixed format dates
# Supported formats:
#   ISO:        2023-05-03
#   UK-style:   11-Dec-2020
#   US-style:   08/19/2020
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%Y-%m-%d",       # 2023-05-03
    "%d-%b-%Y",       # 11-Dec-2020
    "%m/%d/%Y",       # 08/19/2020
    "%d/%m/%Y",       # 19/08/2020  — less common, try after US format
    "%Y%m%d",         # 20230503
]

def parse_date(raw: Optional[str]) -> Optional[date]:
    """
    Parse a date string in any supported format to a Python date.

    Falls back to dateutil.parser for unlisted formats.
    Returns None if raw is empty/None.
    Raises ValueError if unparseable.
    """
    if raw is None or str(raw).strip() == "":
        return None
    raw_stripped = str(raw).strip()

    # Try known formats first (faster, more predictable)
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw_stripped, fmt).date()
        except ValueError:
            continue

    # Fallback to dateutil for edge cases.
    # dayfirst=False enforces US convention (MM/DD/YYYY) for ambiguous inputs
    # like "01/05/2022". Helix Lending is a US platform — this is intentional.
    # Documented in README under "Undocumented Column Decisions".
    try:
        return dateutil_parser.parse(raw_stripped, dayfirst=False).date()
    except Exception:
        raise ValueError(f"Cannot parse date: {raw!r}")


# ---------------------------------------------------------------------------
# timestamp — normalise to UTC-aware datetime
# Handles:
#   ISO with offset:  2024-06-30T03:58:00-08:00
#   ISO with Z:       2021-05-13T20:44:00Z
#   Naive (no tz):    2022-09-08T12:16:00  → assumed UTC
# ---------------------------------------------------------------------------

def parse_timestamp_utc(raw: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO-8601 timestamp to a UTC-aware datetime.

    Naive timestamps (no timezone info) are assumed to be UTC.
    Returns None if raw is empty/None.
    Raises ValueError if unparseable.
    """
    if raw is None or str(raw).strip() == "":
        return None
    raw_stripped = str(raw).strip()
    try:
        dt = dateutil_parser.isoparse(raw_stripped)
        if dt.tzinfo is None:
            # Naive timestamp — assume UTC, document this decision
            logger.debug(f"Naive timestamp assumed UTC: {raw!r}")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        raise ValueError(f"Cannot parse timestamp: {raw!r}")


# ---------------------------------------------------------------------------
# String normalisation helpers
# ---------------------------------------------------------------------------

def normalise_category(raw: Optional[str]) -> Optional[str]:
    """
    Lowercase and strip a categorical string value.
    Returns None if raw is empty/None.
    """
    if raw is None or str(raw).strip() == "":
        return None
    return str(raw).strip().lower()


def clean_string(raw: Optional[str]) -> Optional[str]:
    """
    Strip whitespace from a string. Returns None if empty.
    """
    if raw is None:
        return None
    cleaned = str(raw).strip()
    return cleaned if cleaned else None
