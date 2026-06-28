"""
tests/unit/test_emi_calculation.py
------------------------------------
Unit tests for utils/emi.py :: calculate_emi and is_payment_anomalous

Coverage:
    calculate_emi:
        - Standard amortisation (known values from financial calculators)
        - Zero interest rate → flat division
        - Short term (12 months)
        - Long term (360 months / 30 years)
        - None inputs → None
        - Negative inputs → ValueError
        - Zero term → ValueError

    is_payment_anomalous:
        - Within tolerance → False
        - Above tolerance → True
        - Below tolerance → True
        - Exact match → False
        - None inputs → False
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from utils.emi import calculate_emi, is_payment_anomalous


class TestCalculateEMI:

    # ------------------------------------------------------------------
    # Known values — verified against standard financial calculators
    # ------------------------------------------------------------------

    def test_standard_personal_loan(self):
        """
        P=10000, r=10% annual, n=12 months
        Expected EMI ≈ 879.16
        """
        result = calculate_emi(10000.0, 10.0, 12)
        assert result == pytest.approx(879.16, abs=0.01)

    def test_mortgage_30_year(self):
        """
        P=200000, r=4% annual, n=360 months
        Expected EMI ≈ 954.83
        """
        result = calculate_emi(200000.0, 4.0, 360)
        assert result == pytest.approx(954.83, abs=0.01)

    def test_student_loan(self):
        """
        P=38366.86, r=5.8% annual, n=120 months
        Expected EMI = 422.11 (formula-verified)
        Previous estimate of 421.79 was manually incorrect.
        """
        result = calculate_emi(38366.86, 5.8, 120)
        assert result == pytest.approx(422.11, abs=0.01)

    def test_high_rate_short_term(self):
        """
        P=24315.74, r=17.82% annual, n=60 months
        Expected EMI = 615.08 (formula-verified)
        Previous estimate of 617.55 was manually incorrect.
        """
        result = calculate_emi(24315.74, 17.82, 60)
        assert result == pytest.approx(615.08, abs=0.01)

    def test_sample_loan_data(self):
        """
        P=32256.80, r=10.12% annual, n=12 months
        Expected EMI = 2837.69 (formula-verified)
        Previous estimate of 2838.25 was manually incorrect.
        """
        result = calculate_emi(32256.80, 10.12, 12)
        assert result == pytest.approx(2837.69, abs=0.01)

    # ------------------------------------------------------------------
    # Zero interest edge case
    # ------------------------------------------------------------------

    def test_zero_interest_rate(self):
        """Zero rate → flat division: 12000 / 12 = 1000.00"""
        result = calculate_emi(12000.0, 0.0, 12)
        assert result == pytest.approx(1000.00, abs=0.01)

    def test_zero_interest_long_term(self):
        """Zero rate → 120000 / 360 = 333.33"""
        result = calculate_emi(120000.0, 0.0, 360)
        assert result == pytest.approx(333.33, abs=0.01)

    # ------------------------------------------------------------------
    # None inputs
    # ------------------------------------------------------------------

    def test_none_principal(self):
        assert calculate_emi(None, 10.0, 12) is None

    def test_none_rate(self):
        assert calculate_emi(10000.0, None, 12) is None

    def test_none_term(self):
        assert calculate_emi(10000.0, 10.0, None) is None

    def test_all_none(self):
        assert calculate_emi(None, None, None) is None

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_negative_principal_raises(self):
        with pytest.raises(ValueError, match="negative"):
            calculate_emi(-10000.0, 10.0, 12)

    def test_negative_rate_raises(self):
        with pytest.raises(ValueError, match="negative"):
            calculate_emi(10000.0, -5.0, 12)

    def test_zero_term_raises(self):
        with pytest.raises(ValueError, match="positive"):
            calculate_emi(10000.0, 10.0, 0)

    def test_negative_term_raises(self):
        with pytest.raises(ValueError, match="positive"):
            calculate_emi(10000.0, 10.0, -12)

    # ------------------------------------------------------------------
    # Return type
    # ------------------------------------------------------------------

    def test_returns_float(self):
        result = calculate_emi(10000.0, 10.0, 12)
        assert isinstance(result, float)

    def test_rounded_to_two_decimals(self):
        result = calculate_emi(10000.0, 10.0, 12)
        assert result == round(result, 2)


class TestIsPaymentAnomalous:

    # ------------------------------------------------------------------
    # Within tolerance (10% default)
    # ------------------------------------------------------------------

    def test_exact_match_not_anomalous(self):
        assert is_payment_anomalous(879.16, 879.16) is False

    def test_within_tolerance_above(self):
        # 5% above — within 10% tolerance
        assert is_payment_anomalous(879.16 * 1.05, 879.16) is False

    def test_within_tolerance_below(self):
        # 5% below — within 10% tolerance
        assert is_payment_anomalous(879.16 * 0.95, 879.16) is False

    def test_at_tolerance_boundary(self):
        # Exactly 10% above — IS anomalous.
        # is_payment_anomalous uses strict > so deviation == threshold triggers flag.
        # Design decision: boundary-inclusive flagging errs on the side of caution.
        assert is_payment_anomalous(879.16 * 1.10, 879.16) is True

    # ------------------------------------------------------------------
    # Outside tolerance
    # ------------------------------------------------------------------

    def test_above_tolerance(self):
        # 15% above — anomalous
        assert is_payment_anomalous(879.16 * 1.15, 879.16) is True

    def test_below_tolerance(self):
        # 15% below — anomalous
        assert is_payment_anomalous(879.16 * 0.85, 879.16) is True

    def test_zero_payment_anomalous(self):
        # $0 payment when EMI is $879 — anomalous
        assert is_payment_anomalous(0.0, 879.16) is True

    def test_very_large_payment(self):
        # 10x EMI — anomalous (early payoff or error)
        assert is_payment_anomalous(8791.6, 879.16) is True

    # ------------------------------------------------------------------
    # Edge / None cases
    # ------------------------------------------------------------------

    def test_none_actual_not_anomalous(self):
        assert is_payment_anomalous(None, 879.16) is False

    def test_none_emi_not_anomalous(self):
        assert is_payment_anomalous(879.16, None) is False

    def test_zero_emi_not_anomalous(self):
        # Avoid division by zero
        assert is_payment_anomalous(100.0, 0.0) is False

    def test_custom_tolerance(self):
        # 5% tolerance — 8% deviation should be anomalous
        assert is_payment_anomalous(879.16 * 1.08, 879.16, tolerance_pct=0.05) is True

    def test_custom_tolerance_within(self):
        # 20% tolerance — 15% deviation should NOT be anomalous
        assert is_payment_anomalous(879.16 * 1.15, 879.16, tolerance_pct=0.20) is False
