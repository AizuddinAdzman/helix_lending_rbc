"""
tests/unit/test_logging.py
----------------------------
Tests that the logging system emits structured JSON records correctly
at every meaningful checkpoint.

Coverage:
    - log_event emits valid JSON
    - All required fields present
    - Level routing works (INFO, WARNING, ERROR)
    - Missing optional fields handled gracefully
    - Duration rounds to 3 decimal places
    - Timestamp is UTC ISO-8601
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from io import StringIO

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
from utils.logger import get_logger, log_event, _JsonFormatter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _capture_logger(name: str):
    """Return a logger that writes to a StringIO buffer for inspection."""
    logger = logging.getLogger(f"test_{name}")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    buffer = StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(_JsonFormatter())
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    return logger, buffer


def _last_record(buffer: StringIO) -> dict:
    """Parse the last JSON record from the buffer."""
    buffer.seek(0)
    lines = [l.strip() for l in buffer.read().splitlines() if l.strip()]
    assert lines, "No log records emitted"
    return json.loads(lines[-1])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestJsonFormatter:

    def test_emits_valid_json(self):
        logger, buf = _capture_logger("json_valid")
        log_event(logger, event="checkpoint", message="test message")
        record = _last_record(buf)
        assert isinstance(record, dict)

    def test_timestamp_field_present(self):
        logger, buf = _capture_logger("ts_field")
        log_event(logger, event="checkpoint", message="test")
        record = _last_record(buf)
        assert "timestamp" in record
        # Should be parseable as ISO-8601
        dt = datetime.fromisoformat(record["timestamp"])
        assert dt.tzinfo is not None

    def test_level_field_present(self):
        logger, buf = _capture_logger("level_field")
        log_event(logger, event="checkpoint", message="test", level="INFO")
        record = _last_record(buf)
        assert record["level"] == "INFO"

    def test_message_field_present(self):
        logger, buf = _capture_logger("msg_field")
        log_event(logger, event="checkpoint", message="hello pipeline")
        record = _last_record(buf)
        assert record["message"] == "hello pipeline"

    def test_event_field_present(self):
        logger, buf = _capture_logger("event_field")
        log_event(logger, event="load_start", message="starting")
        record = _last_record(buf)
        assert record["event"] == "load_start"


class TestLogEventFields:

    def test_layer_field_set(self):
        logger, buf = _capture_logger("layer")
        log_event(logger, event="load_start", message="x", layer="raw")
        record = _last_record(buf)
        assert record["layer"] == "raw"

    def test_table_field_set(self):
        logger, buf = _capture_logger("table")
        log_event(logger, event="load_end", message="x", table="raw_loan")
        record = _last_record(buf)
        assert record["table"] == "raw_loan"

    def test_rows_in_field_set(self):
        logger, buf = _capture_logger("rows_in")
        log_event(logger, event="load_end", message="x", rows_in=1000)
        record = _last_record(buf)
        assert record["rows_in"] == 1000

    def test_rows_out_field_set(self):
        logger, buf = _capture_logger("rows_out")
        log_event(logger, event="load_end", message="x", rows_out=995)
        record = _last_record(buf)
        assert record["rows_out"] == 995

    def test_rows_rejected_field_set(self):
        logger, buf = _capture_logger("rows_rej")
        log_event(logger, event="load_end", message="x", rows_rejected=5)
        record = _last_record(buf)
        assert record["rows_rejected"] == 5

    def test_duration_rounded_to_3_places(self):
        logger, buf = _capture_logger("duration")
        log_event(logger, event="load_end", message="x", duration_sec=1.23456789)
        record = _last_record(buf)
        assert record["duration_sec"] == 1.235

    def test_source_file_field_set(self):
        logger, buf = _capture_logger("src_file")
        log_event(logger, event="load_start", message="x",
                  source_file="loan.csv")
        record = _last_record(buf)
        assert record["source_file"] == "loan.csv"

    def test_batch_date_field_set(self):
        logger, buf = _capture_logger("batch_date")
        log_event(logger, event="load_start", message="x",
                  batch_date="2024-01-15")
        record = _last_record(buf)
        assert record["batch_date"] == "2024-01-15"

    def test_optional_fields_omitted_when_none(self):
        """None fields must not appear in the JSON output."""
        logger, buf = _capture_logger("omit_none")
        log_event(logger, event="checkpoint", message="x")
        record = _last_record(buf)
        assert "rows_in" not in record
        assert "rows_out" not in record
        assert "duration_sec" not in record
        assert "source_file" not in record


class TestLogLevels:

    def test_info_level(self):
        logger, buf = _capture_logger("level_info")
        log_event(logger, event="load_end", message="done", level="INFO")
        record = _last_record(buf)
        assert record["level"] == "INFO"

    def test_warning_level(self):
        logger, buf = _capture_logger("level_warn")
        log_event(logger, event="row_rejected", message="bad row",
                  level="WARNING")
        record = _last_record(buf)
        assert record["level"] == "WARNING"

    def test_error_level(self):
        logger, buf = _capture_logger("level_err")
        log_event(logger, event="dq_fail", message="breach detected",
                  level="ERROR")
        record = _last_record(buf)
        assert record["level"] == "ERROR"

    def test_debug_level(self):
        logger, buf = _capture_logger("level_debug")
        log_event(logger, event="checkpoint", message="debug info",
                  level="DEBUG")
        record = _last_record(buf)
        assert record["level"] == "DEBUG"


class TestCheckpointEvents:
    """
    Verify the canonical set of checkpoint event names are valid strings.
    These are the events that must appear in every asset's logging.
    """

    VALID_EVENTS = {
        "load_start", "load_end", "row_rejected",
        "dq_pass", "dq_fail", "checkpoint", "pipeline_fail",
    }

    def test_load_start_event(self):
        logger, buf = _capture_logger("evt_start")
        log_event(logger, event="load_start", message="starting raw_loan",
                  layer="raw", table="raw_loan", source_file="loan.csv",
                  batch_date="2024-01-15")
        record = _last_record(buf)
        assert record["event"] in self.VALID_EVENTS
        assert record["layer"] == "raw"
        assert record["table"] == "raw_loan"

    def test_load_end_event_with_metrics(self):
        logger, buf = _capture_logger("evt_end")
        log_event(logger, event="load_end", message="raw_loan done",
                  layer="raw", table="raw_loan",
                  rows_in=1000, rows_out=997, rows_rejected=3,
                  duration_sec=2.541, source_file="loan.csv",
                  batch_date="2024-01-15")
        record = _last_record(buf)
        assert record["event"] == "load_end"
        assert record["rows_in"] == 1000
        assert record["rows_out"] == 997
        assert record["rows_rejected"] == 3
        assert record["duration_sec"] == 2.541

    def test_dq_fail_event(self):
        logger, buf = _capture_logger("evt_dq_fail")
        log_event(logger, event="dq_fail",
                  message="acceptance rate 0.88 < threshold 0.99",
                  layer="dq", table="lnd_loan",
                  batch_date="2024-01-15", level="ERROR")
        record = _last_record(buf)
        assert record["event"] == "dq_fail"
        assert record["level"] == "ERROR"
        assert "0.88" in record["message"]

    def test_row_rejected_event(self):
        logger, buf = _capture_logger("evt_reject")
        log_event(logger, event="row_rejected",
                  message="loan_id=L001: Cannot cast principal: 'abc'",
                  layer="lnd", table="lnd_loan",
                  batch_date="2024-01-15", level="WARNING")
        record = _last_record(buf)
        assert record["event"] == "row_rejected"
        assert record["level"] == "WARNING"
        assert "L001" in record["message"]
