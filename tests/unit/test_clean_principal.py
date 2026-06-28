"""
tests/unit/test_clean_principal.py
------------------------------------
Unit tests for utils/cleaners.py :: clean_principal_amount

Coverage:
    - Happy path: plain float string
    - Currency symbol: "$33,517.74"
    - Comma-only formatting: "33,517.74"
    - Whitespace padding
    - None / empty string → None
    - Negative value → ValueError
    - Non-numeric garbage → ValueError
"""

import sys
from pathlib import Path

# Make src/ importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from utils.cleaners import clean_principal_amount


class TestCleanPrincipalAmount:

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_plain_float_string(self):
        assert clean_principal_amount("32256.80") == 32256.80

    def test_currency_symbol_with_commas(self):
        assert clean_principal_amount("$33,517.74") == 33517.74

    def test_comma_only_formatting(self):
        assert clean_principal_amount("33,517.74") == 33517.74

    def test_no_decimals(self):
        assert clean_principal_amount("33517") == 33517.0

    def test_comma_no_decimal(self):
        assert clean_principal_amount("33,517") == 33517.0

    def test_whitespace_padding(self):
        assert clean_principal_amount("  32256.80  ") == 32256.80

    def test_dollar_no_cents(self):
        assert clean_principal_amount("$50000") == 50000.0

    def test_large_amount(self):
        assert clean_principal_amount("$223,956.81") == 223956.81

    def test_zero(self):
        assert clean_principal_amount("0.00") == 0.0

    def test_small_amount(self):
        assert clean_principal_amount("17.43") == 17.43

    # ------------------------------------------------------------------
    # Null / empty
    # ------------------------------------------------------------------

    def test_none_returns_none(self):
        assert clean_principal_amount(None) is None

    def test_empty_string_returns_none(self):
        assert clean_principal_amount("") is None

    def test_whitespace_only_returns_none(self):
        assert clean_principal_amount("   ") is None

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_negative_value_raises(self):
        with pytest.raises(ValueError, match="Negative"):
            clean_principal_amount("-100.00")

    def test_negative_with_currency_raises(self):
        with pytest.raises(ValueError, match="Negative"):
            clean_principal_amount("-$100.00")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            clean_principal_amount("abc")

    def test_text_with_dollar_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            clean_principal_amount("$abc")

    def test_multiple_dots_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            clean_principal_amount("1.2.3")
