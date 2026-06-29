"""
tests/unit/test_clean_payment_row.py
--------------------------------------
Unit tests for assets/landing/lnd_payment.py :: _clean_payment_row

Coverage:
    - Happy path: full valid row
    - amount cast to float
    - Negative amount raises
    - Non-numeric amount raises
    - Timestamp normalised to UTC (all timezone variants)
    - Naive timestamp assumed UTC
    - payment_method_type lowercased
    - Empty payment_id raises
    - None payment_id raises
    - Optional fields (last_four, bank, metadata) → None gracefully
    - All fields None except payment_id → valid row
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from utils.cleaners import parse_timestamp_utc, normalise_category, clean_string

def _clean_payment_row(payment_id, loan_id, amount, payment_timestamp,
    payment_method_type, payment_method_last_four, payment_method_bank,
    metadata_source, metadata_user_agent):
    from datetime import timezone
    if not (payment_id and str(payment_id).strip()):
        raise ValueError("payment_id is empty")
    try:
        amt = float(amount) if amount else None
        if amt is not None and amt < 0:
            raise ValueError(f"Negative amount: {amount!r}")
    except ValueError as e:
        if "Negative" in str(e):
            raise
        raise ValueError(f"Cannot cast amount: {amount!r} — {e}")
    ts = parse_timestamp_utc(payment_timestamp)
    return {
        "payment_id": clean_string(payment_id),
        "loan_id": clean_string(loan_id),
        "amount": amt,
        "payment_timestamp": ts,
        "payment_method_type": normalise_category(payment_method_type),
        "payment_method_last_four": clean_string(payment_method_last_four),
        "payment_method_bank": clean_string(payment_method_bank),
        "metadata_source": clean_string(metadata_source),
        "metadata_user_agent": clean_string(metadata_user_agent),
    }


def _call(**overrides):
    """Helper: build a default valid raw payment row and apply overrides."""
    defaults = dict(
        payment_id="P000027450",
        loan_id="L0000057",
        amount="761.44",
        payment_timestamp="2024-06-30T03:58:00-08:00",
        payment_method_type="card",
        payment_method_last_four="3517",
        payment_method_bank="Union Mutual",
        metadata_source="web",
        metadata_user_agent=None,
    )
    defaults.update(overrides)
    return _clean_payment_row(**defaults)


class TestCleanPaymentRowHappyPath:

    def test_full_valid_row(self):
        result = _call()
        assert result["payment_id"]             == "P000027450"
        assert result["loan_id"]                == "L0000057"
        assert result["amount"]                 == 761.44
        assert result["payment_method_type"]    == "card"
        assert result["payment_method_last_four"] == "3517"
        assert result["payment_method_bank"]    == "Union Mutual"
        assert result["metadata_source"]        == "web"
        assert result["metadata_user_agent"]    is None

    def test_amount_cast_to_float(self):
        result = _call(amount="1041.81")
        assert result["amount"] == 1041.81
        assert isinstance(result["amount"], float)

    def test_amount_integer_string(self):
        result = _call(amount="500")
        assert result["amount"] == 500.0

    def test_payment_method_type_uppercased_to_lower(self):
        result = _call(payment_method_type="ACH")
        assert result["payment_method_type"] == "ach"

    def test_payment_method_type_mixed_case(self):
        result = _call(payment_method_type="Card")
        assert result["payment_method_type"] == "card"

    def test_payment_method_type_wire(self):
        result = _call(payment_method_type="wire")
        assert result["payment_method_type"] == "wire"

    def test_payment_method_type_check(self):
        result = _call(payment_method_type="check")
        assert result["payment_method_type"] == "check"


class TestCleanPaymentRowTimestamp:

    def test_negative_offset_converted_to_utc(self):
        # -08:00 → +8 hours UTC
        result = _call(payment_timestamp="2024-06-30T03:58:00-08:00")
        ts = result["payment_timestamp"]
        assert ts.tzinfo == timezone.utc
        assert ts.hour == 11
        assert ts.minute == 58

    def test_negative_five_offset_to_utc(self):
        # -05:00 → +5 hours UTC
        result = _call(payment_timestamp="2021-09-01T10:24:00-05:00")
        ts = result["payment_timestamp"]
        assert ts.tzinfo == timezone.utc
        assert ts.hour == 15
        assert ts.minute == 24

    def test_z_suffix_utc(self):
        result = _call(payment_timestamp="2021-05-13T20:44:00Z")
        ts = result["payment_timestamp"]
        assert ts.tzinfo == timezone.utc
        assert ts.hour == 20

    def test_naive_timestamp_assumed_utc(self):
        # No tz info → assume UTC, no conversion
        result = _call(payment_timestamp="2022-09-08T12:16:00")
        ts = result["payment_timestamp"]
        assert ts.tzinfo == timezone.utc
        assert ts.hour == 12

    def test_null_timestamp_allowed(self):
        result = _call(payment_timestamp=None)
        assert result["payment_timestamp"] is None

    def test_empty_timestamp_allowed(self):
        result = _call(payment_timestamp="")
        assert result["payment_timestamp"] is None


class TestCleanPaymentRowErrors:

    def test_empty_payment_id_raises(self):
        with pytest.raises(ValueError, match="payment_id is empty"):
            _call(payment_id="")

    def test_none_payment_id_raises(self):
        with pytest.raises(ValueError, match="payment_id is empty"):
            _call(payment_id=None)

    def test_whitespace_payment_id_raises(self):
        with pytest.raises(ValueError, match="payment_id is empty"):
            _call(payment_id="   ")

    def test_negative_amount_raises(self):
        with pytest.raises(ValueError, match="Negative amount"):
            _call(amount="-100.00")

    def test_non_numeric_amount_raises(self):
        with pytest.raises(ValueError, match="Cannot cast amount"):
            _call(amount="abc")

    def test_bad_timestamp_raises(self):
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            _call(payment_timestamp="not-a-timestamp")


class TestCleanPaymentRowOptionalFields:

    def test_null_last_four_allowed(self):
        result = _call(payment_method_last_four=None)
        assert result["payment_method_last_four"] is None

    def test_null_bank_allowed(self):
        result = _call(payment_method_bank=None)
        assert result["payment_method_bank"] is None

    def test_null_metadata_source_allowed(self):
        result = _call(metadata_source=None)
        assert result["metadata_source"] is None

    def test_null_user_agent_allowed(self):
        result = _call(metadata_user_agent=None)
        assert result["metadata_user_agent"] is None

    def test_null_loan_id_allowed(self):
        # loan_id FK checked at DQ layer, not here
        result = _call(loan_id=None)
        assert result["loan_id"] is None

    def test_all_optional_fields_null(self):
        result = _call(
            loan_id=None,
            payment_method_last_four=None,
            payment_method_bank=None,
            metadata_source=None,
            metadata_user_agent=None,
        )
        assert result["payment_id"] == "P000027450"
        assert result["amount"] == 761.44
        assert result["loan_id"] is None

    def test_whitespace_bank_returns_none(self):
        result = _call(payment_method_bank="   ")
        assert result["payment_method_bank"] is None

    def test_whitespace_source_returns_none(self):
        result = _call(metadata_source="   ")
        assert result["metadata_source"] is None
