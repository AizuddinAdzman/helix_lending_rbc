"""
tests/unit/test_scd2_logic.py
-------------------------------
Unit tests for SCD2 hash logic in assets/landing/lnd_loan.py

Coverage:
    _compute_hash:
        - Same inputs produce same hash
        - Different inputs produce different hash
        - None fields handled consistently
        - Column order is fixed (deterministic)

    _clean_loan_row:
        - Happy path full row
        - Empty loan_id raises
        - Bad principal raises
        - Bad date raises
        - Bad interest_rate raises
        - Bad term_months raises
        - Categories lowercased
"""

import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from assets.landing.lnd_loan import _hash as _compute_hash, _clean_row as _clean_loan_row


class TestComputeHash:

    def _make_cleaned(self, **overrides) -> dict:
        base = {
            "loan_id":              "L0001",
            "customer_id":          "C001",
            "product_type":         "personal",
            "principal_amount":     32256.80,
            "interest_rate":        10.12,
            "term_months":          12,
            "origination_date":     date(2023, 5, 3),
            "origination_channel":  "partner",
            "status":               "active",
            "borrower_info":        '{"credit_score": 672}',
        }
        base.update(overrides)
        return base

    def test_same_inputs_same_hash(self):
        a = self._make_cleaned()
        b = self._make_cleaned()
        assert _compute_hash(a) == _compute_hash(b)

    def test_different_status_different_hash(self):
        a = self._make_cleaned(status="active")
        b = self._make_cleaned(status="default")
        assert _compute_hash(a) != _compute_hash(b)

    def test_different_amount_different_hash(self):
        a = self._make_cleaned(principal_amount=10000.0)
        b = self._make_cleaned(principal_amount=20000.0)
        assert _compute_hash(a) != _compute_hash(b)

    def test_none_field_consistent(self):
        a = self._make_cleaned(customer_id=None)
        b = self._make_cleaned(customer_id=None)
        assert _compute_hash(a) == _compute_hash(b)

    def test_none_vs_value_different_hash(self):
        a = self._make_cleaned(customer_id=None)
        b = self._make_cleaned(customer_id="C001")
        assert _compute_hash(a) != _compute_hash(b)

    def test_hash_is_string(self):
        result = _compute_hash(self._make_cleaned())
        assert isinstance(result, str)

    def test_hash_is_32_chars(self):
        """MD5 hash should be 32 hex characters."""
        result = _compute_hash(self._make_cleaned())
        assert len(result) == 32

    def test_order_is_deterministic(self):
        """Hash must not change between calls with same data."""
        cleaned = self._make_cleaned()
        hashes = [_compute_hash(cleaned) for _ in range(10)]
        assert len(set(hashes)) == 1


class TestCleanLoanRow:

    def _call(self, **overrides):
        defaults = dict(
            loan_id="L0001",
            customer_id="C001",
            product_type="PERSONAL",
            principal_amount="32256.80",
            interest_rate="10.12",
            term_months="12",
            origination_date="2023-05-03",
            origination_channel="Partner",
            status="Active",
            borrower_info='{"credit_score": 672}',
        )
        defaults.update(overrides)
        return _clean_loan_row(**defaults)

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_full_clean_row(self):
        result = self._call()
        assert result["loan_id"]             == "L0001"
        assert result["product_type"]        == "personal"       # lowercased
        assert result["principal_amount"]    == 32256.80
        assert result["interest_rate"]       == 10.12
        assert result["term_months"]         == 12
        assert result["origination_date"]    == date(2023, 5, 3)
        assert result["origination_channel"] == "partner"        # lowercased
        assert result["status"]              == "active"         # lowercased

    def test_currency_principal_cleaned(self):
        result = self._call(principal_amount="$33,517.74")
        assert result["principal_amount"] == 33517.74

    def test_uk_date_parsed(self):
        result = self._call(origination_date="11-Dec-2020")
        assert result["origination_date"] == date(2020, 12, 11)

    def test_us_date_parsed(self):
        result = self._call(origination_date="08/19/2020")
        assert result["origination_date"] == date(2020, 8, 19)

    def test_mixed_case_product_type(self):
        result = self._call(product_type="Mortgage")
        assert result["product_type"] == "mortgage"

    def test_uppercase_status(self):
        result = self._call(status="CLOSED")
        assert result["status"] == "closed"

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_empty_loan_id_raises(self):
        with pytest.raises(ValueError, match="loan_id is empty"):
            self._call(loan_id="")

    def test_none_loan_id_raises(self):
        with pytest.raises(ValueError, match="loan_id is empty"):
            self._call(loan_id=None)

    def test_whitespace_loan_id_raises(self):
        with pytest.raises(ValueError, match="loan_id is empty"):
            self._call(loan_id="   ")

    def test_bad_principal_raises(self):
        with pytest.raises(ValueError):
            self._call(principal_amount="not_a_number")

    def test_bad_date_raises(self):
        with pytest.raises(ValueError):
            self._call(origination_date="32-13-2020")

    def test_bad_interest_rate_raises(self):
        with pytest.raises(ValueError, match="Cannot cast interest_rate"):
            self._call(interest_rate="high")

    def test_bad_term_months_raises(self):
        with pytest.raises(ValueError, match="Cannot cast term_months"):
            self._call(term_months="twelve")

    # ------------------------------------------------------------------
    # None / null handling
    # ------------------------------------------------------------------

    def test_none_principal_allowed(self):
        result = self._call(principal_amount=None)
        assert result["principal_amount"] is None

    def test_none_borrower_info_allowed(self):
        result = self._call(borrower_info=None)
        assert result["borrower_info"] is None

    def test_none_customer_id_allowed(self):
        result = self._call(customer_id=None)
        assert result["customer_id"] is None
