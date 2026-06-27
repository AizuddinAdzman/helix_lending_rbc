"""
utils/logger.py
----------------
Structured JSON logging for the Helix Lending pipeline.

Every meaningful checkpoint emits a structured log record with:
    timestamp       UTC ISO-8601
    layer           raw | lnd | dq | stg | dim | fct | mart
    table           target table name
    event           load_start | load_end | dq_pass | dq_fail |
                    row_rejected | pipeline_fail | checkpoint
    rows_in         rows read from source
    rows_out        rows written to target
    rows_rejected   rows sent to err_ table
    duration_sec    wall-clock seconds for the operation
    source_file     filename being processed
    batch_date      pipeline run date (YYYY-MM-DD)
    message         human-readable description

Usage:
    from utils.logger import get_logger, log_event

    logger = get_logger(__name__)
    log_event(logger, layer="raw", table="raw_loan", event="load_start", ...)
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Import config carefully — avoid circular imports
# ---------------------------------------------------------------------------
try:
    from config import LOG_FILE
except ImportError:
    # Fallback for test context where src/ is not on path
    LOG_FILE = Path("output/logs/pipeline.log")


class _JsonFormatter(logging.Formatter):
    """
    Format every log record as a single-line JSON object.
    Standard fields are always present; extras come from the `extra` dict.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Merge any structured fields passed via extra={}
        for key in (
            "layer", "table", "event",
            "rows_in", "rows_out", "rows_rejected",
            "duration_sec", "source_file", "batch_date",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger that writes structured JSON to both:
      - stdout          (for Dagster UI capture)
      - output/logs/pipeline.log (persistent file)

    Calling get_logger() multiple times with the same name is safe —
    Python's logging module deduplicates handlers.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.DEBUG)

    formatter = _JsonFormatter()

    # --- stdout handler (Dagster captures this) ---
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)

    # --- file handler (persistent across runs) ---
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def log_event(
    logger:         logging.Logger,
    event:          str,
    message:        str,
    layer:          Optional[str] = None,
    table:          Optional[str] = None,
    rows_in:        Optional[int] = None,
    rows_out:       Optional[int] = None,
    rows_rejected:  Optional[int] = None,
    duration_sec:   Optional[float] = None,
    source_file:    Optional[str] = None,
    batch_date:     Optional[str] = None,
    level:          str = "INFO",
) -> None:
    """
    Emit a structured pipeline checkpoint log.

    Args:
        logger        : Logger instance from get_logger()
        event         : One of: load_start, load_end, dq_pass, dq_fail,
                        row_rejected, pipeline_fail, checkpoint
        message       : Human-readable description
        layer         : Pipeline layer (raw, lnd, dq, stg, dim, fct, mart)
        table         : Target DuckDB table name
        rows_in       : Rows read from source
        rows_out      : Rows successfully written
        rows_rejected : Rows sent to err_ table
        duration_sec  : Wall-clock duration of the operation
        source_file   : Source filename (with extension)
        batch_date    : Pipeline run date as YYYY-MM-DD string
        level         : Logging level (INFO, WARNING, ERROR, DEBUG)
    """
    extra = {
        k: v for k, v in {
            "layer":         layer,
            "table":         table,
            "event":         event,
            "rows_in":       rows_in,
            "rows_out":      rows_out,
            "rows_rejected": rows_rejected,
            "duration_sec":  round(duration_sec, 3) if duration_sec else None,
            "source_file":   source_file,
            "batch_date":    batch_date,
        }.items() if v is not None
    }

    log_fn = getattr(logger, level.lower(), logger.info)
    log_fn(message, extra=extra)
