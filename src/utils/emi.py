"""
utils/emi.py
-------------
Expected Monthly Instalment (EMI) calculation.

Formula (standard amortisation):
    EMI = P × r(1+r)^n / ((1+r)^n − 1)

Where:
    P = principal_amount (USD)
    r = monthly interest rate = annual_rate / 12 / 100
    n = term_months

Edge cases:
    - Zero interest rate → EMI = P / n (flat division, no compounding)
    - Zero term         → undefined, raise ValueError
    - Negative values   → raise ValueError

Design note:
    This is intentionally a pure function with no external dependencies.
    It is unit-tested independently in tests/unit/test_emi_calculation.py.
    The result is used in stg_loan_payment and mart_payment_anomaly.
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_emi(
    principal: float,
    annual_rate_pct: float,
    term_months: int,
) -> Optional[float]:
    """
    Calculate the expected monthly instalment for a loan.

    Args:
        principal       : Loan principal in USD (e.g. 32256.80)
        annual_rate_pct : Annual interest rate in percentage points (e.g. 10.12 = 10.12%)
        term_months     : Loan term in months (e.g. 12, 60, 360)

    Returns:
        Monthly instalment as float, rounded to 2 decimal places.
        Returns None if any input is None or invalid.

    Raises:
        ValueError: If term_months is zero or any input is negative.
    """
    # --- Input validation ---
    if principal is None or annual_rate_pct is None or term_months is None:
        return None

    if principal < 0:
        raise ValueError(f"Principal cannot be negative: {principal}")
    if annual_rate_pct < 0:
        raise ValueError(f"Interest rate cannot be negative: {annual_rate_pct}")
    if term_months <= 0:
        raise ValueError(f"Term months must be positive: {term_months}")

    # --- Zero interest edge case ---
    if annual_rate_pct == 0:
        emi = principal / term_months
        logger.debug(f"Zero-rate EMI: {principal} / {term_months} = {emi:.2f}")
        return round(emi, 2)

    # --- Standard amortisation formula ---
    r = annual_rate_pct / 12 / 100          # monthly rate as decimal
    n = term_months

    numerator   = principal * r * math.pow(1 + r, n)
    denominator = math.pow(1 + r, n) - 1

    if denominator == 0:
        raise ValueError(
            f"EMI denominator is zero — check inputs: "
            f"P={principal}, r={annual_rate_pct}, n={term_months}"
        )

    emi = numerator / denominator
    logger.debug(
        f"EMI calculated: P={principal}, r={annual_rate_pct}%, n={term_months} → {emi:.2f}"
    )
    return round(emi, 2)


def is_payment_anomalous(
    actual_amount: float,
    expected_emi: float,
    tolerance_pct: float = 0.10,
) -> bool:
    """
    Return True if actual_amount deviates from expected_emi
    by more than tolerance_pct.

    Args:
        actual_amount   : Payment amount recorded in lnd_payment
        expected_emi    : Expected monthly instalment from calculate_emi()
        tolerance_pct   : Acceptable deviation (default 10% from config)

    Returns:
        True  → payment is anomalous (flag for mart_payment_anomaly)
        False → payment is within acceptable range

    Design decision:
        10% tolerance accounts for partial payments, rounding differences,
        and early payoff scenarios. Documented in README.
    """
    if actual_amount is None or expected_emi is None or expected_emi == 0:
        return False

    deviation = abs(actual_amount - expected_emi) / expected_emi
    return deviation > tolerance_pct
