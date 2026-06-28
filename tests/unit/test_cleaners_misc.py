"""
tests/unit/test_cleaners_misc.py
----------------------------------
Unit tests for utils/cleaners.py :: normalise_category and clean_string

Coverage:
    normalise_category:
        - Uppercase → lowercase
        - Mixed case → lowercase
        - Whitespace stripped
        - None / empty → None
        - Already lowercase passthrough
        - Source data specific cases (PERSONAL, Mortgage, ACH, etc.)

    clean_string:
        - Normal string stripped
        - Internal spaces preserved
        - JSON blob preserved
        - None → None
        - Empty / whitespace only → None
        - Tab / newline stripped at boundaries
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from utils.cleaners import normalise_category, clean_string


class TestNormaliseCategory:

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_uppercase_to_lowercase(self):
        assert normalise_category("PERSONAL") == "personal"

    def test_mixed_case_to_lowercase(self):
        assert normalise_category("Mortgage") == "mortgage"

    def test_already_lowercase_passthrough(self):
        assert normalise_category("student") == "student"

    def test_whitespace_stripped(self):
        assert normalise_category("  ACTIVE  ") == "active"

    def test_leading_whitespace(self):
        assert normalise_category("  branch") == "branch"

    def test_trailing_whitespace(self):
        assert normalise_category("online  ") == "online"

    def test_mixed_case_with_spaces(self):
        assert normalise_category("  Partner  ") == "partner"

    def test_single_char(self):
        assert normalise_category("A") == "a"

    def test_numeric_string(self):
        assert normalise_category("123") == "123"

    def test_hyphenated_value(self):
        assert normalise_category("Self-Employed") == "self-employed"

    def test_underscore_value(self):
        assert normalise_category("MOBILE_APP") == "mobile_app"

    # ------------------------------------------------------------------
    # Source data specific
    # ------------------------------------------------------------------

    def test_product_type_personal_upper(self):
        assert normalise_category("PERSONAL") == "personal"

    def test_product_type_mortgage_mixed(self):
        assert normalise_category("Mortgage") == "mortgage"

    def test_status_active_upper(self):
        assert normalise_category("ACTIVE") == "active"

    def test_channel_partner_mixed(self):
        assert normalise_category("Partner") == "partner"

    def test_payment_method_ach_upper(self):
        assert normalise_category("ACH") == "ach"

    def test_payment_method_card_lower(self):
        assert normalise_category("card") == "card"

    # ------------------------------------------------------------------
    # Null / empty
    # ------------------------------------------------------------------

    def test_none_returns_none(self):
        assert normalise_category(None) is None

    def test_empty_string_returns_none(self):
        assert normalise_category("") is None

    def test_whitespace_only_returns_none(self):
        assert normalise_category("   ") is None

    def test_tab_only_returns_none(self):
        assert normalise_category("\t") is None


class TestCleanString:

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_normal_string_passthrough(self):
        assert clean_string("L0009053") == "L0009053"

    def test_leading_whitespace_stripped(self):
        assert clean_string("  L0009053") == "L0009053"

    def test_trailing_whitespace_stripped(self):
        assert clean_string("L0009053  ") == "L0009053"

    def test_both_sides_stripped(self):
        assert clean_string("  L0009053  ") == "L0009053"

    def test_internal_spaces_preserved(self):
        # bank names like "Union Mutual" must keep internal spaces
        assert clean_string("Union Mutual") == "Union Mutual"

    def test_internal_spaces_with_padding(self):
        assert clean_string("  Sentinel Trust  ") == "Sentinel Trust"

    def test_numeric_string(self):
        assert clean_string("12345") == "12345"

    def test_json_string_preserved(self):
        # borrower_info is a JSON blob — internal content must not change
        json_val = '{"credit_score": 672, "employment": "salaried"}'
        assert clean_string(json_val) == json_val

    def test_json_with_outer_whitespace_stripped(self):
        json_val = '{"credit_score": 672}'
        assert clean_string(f"  {json_val}  ") == json_val

    def test_payment_id_format(self):
        assert clean_string("P000027450") == "P000027450"

    def test_last_four_digits(self):
        assert clean_string("3517") == "3517"

    def test_user_agent_string(self):
        assert clean_string("HelixApp/3.2.1") == "HelixApp/3.2.1"

    def test_tab_stripped_at_boundary(self):
        assert clean_string("\tL0009053\t") == "L0009053"

    def test_newline_stripped_at_boundary(self):
        assert clean_string("\nL0009053\n") == "L0009053"

    # ------------------------------------------------------------------
    # Null / empty
    # ------------------------------------------------------------------

    def test_none_returns_none(self):
        assert clean_string(None) is None

    def test_empty_string_returns_none(self):
        assert clean_string("") is None

    def test_whitespace_only_returns_none(self):
        assert clean_string("   ") is None

    def test_tab_only_returns_none(self):
        assert clean_string("\t") is None

    def test_newline_only_returns_none(self):
        assert clean_string("\n") is None
