"""
tests/unit/test_delinquency_flag.py
-------------------------------------
Unit tests for delinquency detection logic.

Since the delinquency flag will be computed in stg_loan_payment (not yet built),
we test the core logic as a pure function here to lock the business rule.

Business rule:
    A loan is delinquent if the number of days since the last expected
    payment due date exceeds DELINQUENCY_DAYS (30) with no payment recorded.

    expected_due_date = origination_date + (payment_number * 30 days)
    days_overdue = today - expected_due_date
    is_delinquent = days_overdue > DELINQUENCY_DAYS AND last_payment IS NULL
                    OR last_payment_date < expected_due_date - DELINQUENCY_DAYS

We test the pure helper function extracted here for unit testability.
"""

import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from config import DELINQUENCY_DAYS


# ---------------------------------------------------------------------------
# Pure delinquency logic — extracted for unit testing
# This mirrors what stg_loan_payment will implement in SQL
# ---------------------------------------------------------------------------

def is_delinquent(
    last_payment_date: date | None,
    expected_due_date: date,
    as_of_date: date = None,
    delinquency_days: int = DELINQUENCY_DAYS,
) -> bool:
    """
    Determine if a loan is delinquent as of as_of_date.

    Args:
        last_payment_date : Date of most recent payment, or None if no payment
        expected_due_date : Date the payment was due
        as_of_date        : Evaluation date (default: today)
        delinquency_days  : Grace period in days (default: 30 from config)

    Returns:
        True if loan is delinquent, False otherwise
    """
    if as_of_date is None:
        as_of_date = date.today()

    # If due date is in the future, not yet delinquent
    if expected_due_date > as_of_date:
        return False

    # Days past due as of evaluation date
    days_past_due = (as_of_date - expected_due_date).days

    if days_past_due <= delinquency_days:
        return False  # Still within grace period

    # Beyond grace period — check if payment was made
    if last_payment_date is None:
        return True  # No payment at all — delinquent

    # Payment exists — was it made before or after due date?
    if last_payment_date >= expected_due_date:
        return False  # Payment covers this period

    # Payment was made but before this due date — delinquent
    return True


class TestIsDelinquent:

    TODAY = date(2024, 6, 1)   # fixed evaluation date for deterministic tests

    # ------------------------------------------------------------------
    # Not delinquent cases
    # ------------------------------------------------------------------

    def test_payment_on_due_date_not_delinquent(self):
        due = date(2024, 4, 1)
        assert is_delinquent(due, due, self.TODAY) is False

    def test_payment_after_due_date_not_delinquent(self):
        due       = date(2024, 4, 1)
        paid      = date(2024, 4, 15)    # paid 14 days late but within grace
        assert is_delinquent(paid, due, self.TODAY) is False

    def test_within_grace_period_no_payment(self):
        # Due 20 days ago — within 30-day grace period
        due = self.TODAY - timedelta(days=20)
        assert is_delinquent(None, due, self.TODAY) is False

    def test_due_date_in_future_not_delinquent(self):
        due = self.TODAY + timedelta(days=10)
        assert is_delinquent(None, due, self.TODAY) is False

    def test_payment_before_evaluation_covers_due(self):
        due  = date(2024, 3, 1)
        paid = date(2024, 3, 5)   # paid after due date — covers it
        assert is_delinquent(paid, due, self.TODAY) is False

    # ------------------------------------------------------------------
    # Delinquent cases
    # ------------------------------------------------------------------

    def test_no_payment_past_grace_period(self):
        # Due 45 days ago, no payment
        due = self.TODAY - timedelta(days=45)
        assert is_delinquent(None, due, self.TODAY) is True

    def test_payment_before_due_date_then_missed(self):
        # Due April 1, last payment was March 1 (before due date)
        due  = date(2024, 4, 1)
        paid = date(2024, 3, 1)
        assert is_delinquent(paid, due, self.TODAY) is True

    def test_exactly_at_grace_boundary_not_delinquent(self):
        # Due exactly 30 days ago — at boundary, NOT delinquent
        due = self.TODAY - timedelta(days=30)
        assert is_delinquent(None, due, self.TODAY) is False

    def test_one_day_past_grace_delinquent(self):
        # Due 31 days ago — one day past grace period
        due = self.TODAY - timedelta(days=31)
        assert is_delinquent(None, due, self.TODAY) is True

    def test_very_old_unpaid_delinquent(self):
        # 2 years overdue
        due = self.TODAY - timedelta(days=730)
        assert is_delinquent(None, due, self.TODAY) is True

    # ------------------------------------------------------------------
    # Custom delinquency threshold
    # ------------------------------------------------------------------

    def test_custom_threshold_60_days(self):
        # 45 days overdue — not delinquent with 60-day threshold
        due = self.TODAY - timedelta(days=45)
        assert is_delinquent(None, due, self.TODAY, delinquency_days=60) is False

    def test_custom_threshold_60_days_breach(self):
        # 65 days overdue — delinquent with 60-day threshold
        due = self.TODAY - timedelta(days=65)
        assert is_delinquent(None, due, self.TODAY, delinquency_days=60) is True

    def test_zero_grace_period(self):
        # Any day past due = delinquent with 0-day grace
        due = self.TODAY - timedelta(days=1)
        assert is_delinquent(None, due, self.TODAY, delinquency_days=0) is True
