"""
tests/unit/test_parse_dates.py
--------------------------------
Unit tests for utils/cleaners.py :: parse_date and parse_timestamp_utc

Coverage:
    parse_date:
        - ISO format: 2023-05-03
        - UK-style:   11-Dec-2020
        - US-style:   08/19/2020
        - Compact:    20230503
        - None / empty → None
        - Garbage → ValueError

    parse_timestamp_utc:
        - ISO with negative offset: 2024-06-30T03:58:00-08:00
        - ISO with positive offset: 2021-09-01T10:24:00-05:00
        - ISO with Z:               2021-05-13T20:44:00Z
        - Naive (no tz):            2022-09-08T12:16:00 → assumed UTC
        - None / empty → None
        - Garbage → ValueError
"""

import sys
from pathlib import Path
from datetime import date, datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from utils.cleaners import parse_date, parse_timestamp_utc


class TestParseDate:

    # ------------------------------------------------------------------
    # Supported formats
    # ------------------------------------------------------------------

    def test_iso_format(self):
        assert parse_date("2023-05-03") == date(2023, 5, 3)

    def test_uk_style_dec(self):
        assert parse_date("11-Dec-2020") == date(2020, 12, 11)

    def test_uk_style_jan(self):
        assert parse_date("01-Jan-2021") == date(2021, 1, 1)

    def test_uk_style_lowercase(self):
        assert parse_date("11-dec-2020") == date(2020, 12, 11)

    def test_us_style(self):
        assert parse_date("08/19/2020") == date(2020, 8, 19)

    def test_us_style_leading_zero(self):
        assert parse_date("01/05/2022") == date(2022, 1, 5)

    def test_compact_format(self):
        assert parse_date("20230503") == date(2023, 5, 3)

    def test_whitespace_stripped(self):
        assert parse_date("  2023-05-03  ") == date(2023, 5, 3)

    # ------------------------------------------------------------------
    # Edge dates
    # ------------------------------------------------------------------

    def test_end_of_year(self):
        assert parse_date("2020-12-31") == date(2020, 12, 31)

    def test_leap_year(self):
        assert parse_date("2020-02-29") == date(2020, 2, 29)

    # ------------------------------------------------------------------
    # Null / empty
    # ------------------------------------------------------------------

    def test_none_returns_none(self):
        assert parse_date(None) is None

    def test_empty_string_returns_none(self):
        assert parse_date("") is None

    def test_whitespace_only_returns_none(self):
        assert parse_date("   ") is None

    # ------------------------------------------------------------------
    # Ambiguous date format — US convention locked explicitly
    # ------------------------------------------------------------------

    def test_ambiguous_slash_treated_as_us_format(self):
        """
        01/05/2022 is ambiguous — Jan 5 (US) or May 1 (EU).
        Helix Lending is a US platform. dayfirst=False enforces MM/DD/YYYY.
        Expected: January 5, 2022.
        Documented in README under Undocumented Column Decisions.
        """
        assert parse_date("01/05/2022") == date(2022, 1, 5)

    def test_ambiguous_slash_month_not_day(self):
        """12/11/2021 → December 11, not November 12."""
        assert parse_date("12/11/2021") == date(2021, 12, 11)

    def test_unambiguous_us_day_gt_12(self):
        """08/19/2020 — day=19 > 12, only valid as US format."""
        assert parse_date("08/19/2020") == date(2020, 8, 19)

    # ------------------------------------------------------------------
    # Normalised output format — always YYYY-MM-DD
    # ------------------------------------------------------------------

    def test_output_is_always_iso_format(self):
        """
        Regardless of input format, output must be YYYY-MM-DD.
        This is what DuckDB DATE column stores and returns.
        """
        cases = [
            ("2023-05-03",  date(2023, 5, 3)),
            ("11-Dec-2020", date(2020, 12, 11)),
            ("08/19/2020",  date(2020, 8, 19)),
            ("20230503",    date(2023, 5, 3)),
        ]
        for raw, expected in cases:
            result = parse_date(raw)
            assert result == expected, f"{raw} → {result}, expected {expected}"
            assert result.isoformat() == expected.isoformat(), \
                f"isoformat mismatch for {raw}"

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_garbage_raises(self):
        with pytest.raises(ValueError, match="Cannot parse date"):
            parse_date("not-a-date")

    def test_invalid_month_raises(self):
        with pytest.raises(ValueError, match="Cannot parse date"):
            parse_date("2023-13-01")

    def test_invalid_day_raises(self):
        with pytest.raises(ValueError, match="Cannot parse date"):
            parse_date("2023-02-30")


class TestParseTimestampUTC:

    # ------------------------------------------------------------------
    # Timezone-aware inputs
    # ------------------------------------------------------------------

    def test_negative_offset(self):
        result = parse_timestamp_utc("2024-06-30T03:58:00-08:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        # -08:00 means UTC = 03:58 + 8h = 11:58
        assert result.hour == 11
        assert result.minute == 58

    def test_negative_offset_five(self):
        result = parse_timestamp_utc("2021-09-01T10:24:00-05:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        # -05:00 means UTC = 10:24 + 5h = 15:24
        assert result.hour == 15
        assert result.minute == 24

    def test_utc_z_suffix(self):
        result = parse_timestamp_utc("2021-05-13T20:44:00Z")
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 20
        assert result.minute == 44

    def test_utc_zero_offset(self):
        result = parse_timestamp_utc("2021-05-13T20:44:00+00:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 20

    # ------------------------------------------------------------------
    # Naive timestamp — assumed UTC
    # ------------------------------------------------------------------

    def test_naive_timestamp_assumed_utc(self):
        result = parse_timestamp_utc("2022-09-08T12:16:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 12   # no conversion — assumed UTC already
        assert result.minute == 16

    # ------------------------------------------------------------------
    # Null / empty
    # ------------------------------------------------------------------

    def test_none_returns_none(self):
        assert parse_timestamp_utc(None) is None

    def test_empty_string_returns_none(self):
        assert parse_timestamp_utc("") is None

    def test_whitespace_only_returns_none(self):
        assert parse_timestamp_utc("   ") is None

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_garbage_raises(self):
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp_utc("not-a-timestamp")

    def test_date_only_no_time(self):
        # dateutil can parse this — should not raise
        # just verify it returns something sensible
        result = parse_timestamp_utc("2023-05-03")
        assert result is not None
        assert result.tzinfo == timezone.utc
