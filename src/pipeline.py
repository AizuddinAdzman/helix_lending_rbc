"""
pipeline.py
------------
CLI entry point for running the Helix Lending pipeline locally
without the Dagster UI.

Usage:
    python src/pipeline.py                    # run with today's date (UTC)
    python src/pipeline.py --date 20240115    # run for specific date

What it does:
    1. Validates source files exist
    2. Runs each layer in dependency order
    3. Logs structured JSON at every checkpoint
    4. Prints a run summary on completion or failure

Exit codes:
    0  — all layers completed successfully
    1  — one or more layers failed
"""

import argparse
import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import LOAN_FILE, PAYMENT_FILE, DB_PATH
from utils.logger import get_logger, log_event

logger = get_logger("pipeline")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Helix Lending data pipeline"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Batch date as YYYYMMDD (default: today UTC)",
    )
    return parser.parse_args()


def validate_sources() -> bool:
    """Check source files exist before starting pipeline."""
    ok = True
    for f in [LOAN_FILE, PAYMENT_FILE]:
        if not Path(f).exists():
            log_event(
                logger, event="pipeline_fail", layer="pipeline",
                message=f"Source file not found: {f}",
                level="ERROR",
            )
            ok = False
    return ok


def run_pipeline(batch_date: str) -> bool:
    """
    Run all pipeline layers in order.
    Returns True if all succeed, False if any fail.
    """
    import duckdb
    from assets.ingestion.raw_loan     import ingest_raw_loan_direct
    from assets.ingestion.raw_payment  import ingest_raw_payment_direct
    from assets.landing.lnd_loan       import transform_lnd_loan_direct
    from assets.landing.lnd_payment    import transform_lnd_payment_direct
    from assets.dq.dq_lnd_loan        import run_dq_lnd_loan
    from assets.dq.dq_lnd_payment     import run_dq_lnd_payment

    batch_ts  = datetime.now(timezone.utc)
    start     = time.time()
    failures  = []

    log_event(
        logger, event="load_start", layer="pipeline",
        message=f"Helix pipeline starting — batch_date={batch_date}",
        batch_date=batch_date,
    )

    conn = duckdb.connect(str(DB_PATH))

    steps = [
        ("raw_loan",        lambda: ingest_raw_loan_direct(conn, batch_ts, batch_date)),
        ("raw_payment",     lambda: ingest_raw_payment_direct(conn, batch_ts, batch_date)),
        ("lnd_loan",        lambda: transform_lnd_loan_direct(conn, batch_date)),
        ("lnd_payment",     lambda: transform_lnd_payment_direct(conn, batch_date)),
        ("dq_lnd_loan",     lambda: run_dq_lnd_loan(conn, batch_date)),
        ("dq_lnd_payment",  lambda: run_dq_lnd_payment(conn, batch_date)),
    ]

    for step_name, step_fn in steps:
        try:
            log_event(
                logger, event="load_start", layer="pipeline",
                message=f"Step: {step_name}", batch_date=batch_date,
            )
            step_start = time.time()
            result = step_fn()
            step_dur = round(time.time() - step_start, 3)
            log_event(
                logger, event="load_end", layer="pipeline",
                message=f"Step {step_name} completed",
                duration_sec=step_dur, batch_date=batch_date,
            )
        except Exception as e:
            failures.append(step_name)
            log_event(
                logger, event="pipeline_fail", layer="pipeline",
                message=f"Step {step_name} FAILED: {type(e).__name__}: {e}",
                batch_date=batch_date, level="ERROR",
            )

    conn.close()
    total_dur = round(time.time() - start, 3)

    if failures:
        log_event(
            logger, event="pipeline_fail", layer="pipeline",
            message=f"Pipeline FAILED. Failed steps: {failures}",
            duration_sec=total_dur, batch_date=batch_date, level="ERROR",
        )
        return False

    log_event(
        logger, event="load_end", layer="pipeline",
        message=f"Pipeline completed successfully in {total_dur}s",
        duration_sec=total_dur, batch_date=batch_date,
    )
    return True


def main():
    args = parse_args()

    if args.date:
        try:
            batch_date = datetime.strptime(args.date, "%Y%m%d").date().isoformat()
        except ValueError:
            print(f"ERROR: Invalid date format '{args.date}'. Use YYYYMMDD.")
            sys.exit(1)
    else:
        batch_date = date.today().isoformat()

    if not validate_sources():
        sys.exit(1)

    success = run_pipeline(batch_date)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
