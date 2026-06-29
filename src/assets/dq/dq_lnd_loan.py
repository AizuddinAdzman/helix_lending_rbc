"""
assets/dq/dq_lnd_loan.py
--------------------------
Dagster asset: dq_lnd_loan

Responsibility:
    Run data quality checks on lnd_loan after each landing load.
    Acts as a gate — downstream staging is blocked until this passes.

DQ checks performed:
    1. Volume        — rows_inserted / rows_in_raw >= 99% threshold
    2. Uniqueness    — no duplicate loan_id where is_current_flag = TRUE
    3. Completeness  — null rates per critical column <= 10%
    4. Validity      — product_type, status, channel in known value sets
    5. Referential   — N/A at this layer (RI checked at staging)

On breach:
    - Raises Exception → Dagster marks asset as FAILED
    - Staging asset will not run
    - Results logged to dq_results table for mart_data_observability

On pass:
    - Results logged to dq_results table
    - Returns Output with full DQ report
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    DQ_ACCEPTANCE_THRESHOLD, DQ_MAX_NULL_RATE,
    VALID_PRODUCT_TYPES, VALID_LOAN_STATUSES, VALID_LOAN_CHANNELS,
    COL_LAST_UPDATED_TS,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

DDL_DQ_RESULTS = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id              VARCHAR,
    table_name          VARCHAR,
    check_name          VARCHAR,
    check_result        VARCHAR,
    metric_value        DOUBLE,
    threshold           DOUBLE,
    breach_flag         BOOLEAN,
    detail              VARCHAR,
    checked_at          TIMESTAMP
)
"""

CRITICAL_COLUMNS = [
    "loan_id", "customer_id", "product_type",
    "principal_amount", "interest_rate", "term_months",
    "origination_date", "status",
]


@asset(
    group_name="dq",
    deps=["lnd_loan","lnd_payment"],
    description="DQ gate for lnd_loan — blocks staging if checks fail",
)
def dq_lnd_loan(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time      = time.time()
    batch_date_str  = datetime.now(timezone.utc).date().isoformat()
    run_id          = f"dq_lnd_loan_{batch_date_str}"
    breaches        = []
    dq_records      = []

    log_event(
        logger, event="load_start", layer="dq", table="lnd_loan",
        message="Starting DQ checks on lnd_loan",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute(DDL_DQ_RESULTS)

        total_lnd   = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE is_current_flag = TRUE"
        ).fetchone()[0]

        total_raw   = conn.execute(
            "SELECT COUNT(*) FROM raw_loan WHERE _last_updated_ts = "
            "(SELECT MAX(_last_updated_ts) FROM raw_loan)"
        ).fetchone()[0]

        total_err   = conn.execute(
            "SELECT COUNT(*) FROM err_loan WHERE _last_updated_ts = "
            "(SELECT MAX(_last_updated_ts) FROM err_loan)"
        ).fetchone()[0] if _table_exists(conn, "err_loan") else 0

        # ------------------------------------------------------------------
        # CHECK 1: Volume — acceptance rate
        # ------------------------------------------------------------------
        accepted    = total_raw - total_err
        accept_rate = (accepted / total_raw) if total_raw > 0 else 0.0
        breach      = accept_rate < DQ_ACCEPTANCE_THRESHOLD

        dq_records.append(_dq_record(
            run_id, "lnd_loan", "volume_acceptance_rate",
            "FAIL" if breach else "PASS",
            accept_rate, DQ_ACCEPTANCE_THRESHOLD, breach,
            f"{accepted}/{total_raw} rows accepted",
        ))
        if breach:
            breaches.append(
                f"Volume: acceptance rate {accept_rate:.2%} < "
                f"threshold {DQ_ACCEPTANCE_THRESHOLD:.2%}"
            )
            log_event(
                logger, event="dq_fail", layer="dq", table="lnd_loan",
                message=breaches[-1], batch_date=batch_date_str, level="ERROR",
            )

        # ------------------------------------------------------------------
        # CHECK 2: Uniqueness — loan_id in current rows
        # ------------------------------------------------------------------
        dup_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT loan_id, COUNT(*) AS cnt
                FROM lnd_loan
                WHERE is_current_flag = TRUE
                GROUP BY loan_id
                HAVING cnt > 1
            )
            """
        ).fetchone()[0]

        dup_breach = dup_count > 0
        dq_records.append(_dq_record(
            run_id, "lnd_loan", "uniqueness_loan_id",
            "FAIL" if dup_breach else "PASS",
            float(dup_count), 0.0, dup_breach,
            f"{dup_count} duplicate loan_ids found in current rows",
        ))
        if dup_breach:
            breaches.append(f"Uniqueness: {dup_count} duplicate loan_ids")
            log_event(
                logger, event="dq_fail", layer="dq", table="lnd_loan",
                message=breaches[-1], batch_date=batch_date_str, level="ERROR",
            )

        # ------------------------------------------------------------------
        # CHECK 3: Completeness — null rates per critical column
        # ------------------------------------------------------------------
        if total_lnd > 0:
            for col in CRITICAL_COLUMNS:
                null_count = conn.execute(
                    f"""
                    SELECT COUNT(*) FROM lnd_loan
                    WHERE is_current_flag = TRUE
                      AND {col} IS NULL
                    """
                ).fetchone()[0]
                null_rate   = null_count / total_lnd
                col_breach  = null_rate > DQ_MAX_NULL_RATE
                dq_records.append(_dq_record(
                    run_id, "lnd_loan", f"completeness_{col}",
                    "FAIL" if col_breach else "PASS",
                    null_rate, DQ_MAX_NULL_RATE, col_breach,
                    f"{null_count}/{total_lnd} nulls in {col}",
                ))
                if col_breach:
                    breaches.append(
                        f"Completeness: {col} null rate {null_rate:.2%} > "
                        f"threshold {DQ_MAX_NULL_RATE:.2%}"
                    )
                    log_event(
                        logger, event="dq_fail", layer="dq", table="lnd_loan",
                        message=breaches[-1], batch_date=batch_date_str, level="WARNING",
                    )

        # ------------------------------------------------------------------
        # CHECK 4: Validity — categorical values
        # ------------------------------------------------------------------
        for col, valid_set in [
            ("product_type", VALID_PRODUCT_TYPES),
            ("status",       VALID_LOAN_STATUSES),
            ("origination_channel", VALID_LOAN_CHANNELS),
        ]:
            in_clause = ",".join([f"'{v}'" for v in valid_set])
            invalid_count = conn.execute(
                f"""
                SELECT COUNT(*) FROM lnd_loan
                WHERE is_current_flag = TRUE
                  AND {col} IS NOT NULL
                  AND {col} NOT IN ({in_clause})
                """
            ).fetchone()[0]
            val_breach = invalid_count > 0
            dq_records.append(_dq_record(
                run_id, "lnd_loan", f"validity_{col}",
                "WARN" if val_breach else "PASS",
                float(invalid_count), 0.0, False,  # warning only, not a hard breach
                f"{invalid_count} rows with unrecognised {col} values",
            ))
            if val_breach:
                log_event(
                    logger, event="checkpoint", layer="dq", table="lnd_loan",
                    message=f"Validity warning: {invalid_count} unrecognised {col} values",
                    batch_date=batch_date_str, level="WARNING",
                )

        # ------------------------------------------------------------------
        # Persist DQ results
        # ------------------------------------------------------------------
        conn.executemany(
            """
            INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            dq_records,
        )

    duration = round(time.time() - start_time, 3)

    if breaches:
        log_event(
            logger, event="dq_fail", layer="dq", table="lnd_loan",
            message=f"DQ FAILED — {len(breaches)} breach(es): {'; '.join(breaches)}",
            duration_sec=duration, batch_date=batch_date_str, level="ERROR",
        )
        raise Exception(
            f"DQ gate dq_lnd_loan FAILED with {len(breaches)} breach(es):\n"
            + "\n".join(f"  • {b}" for b in breaches)
        )

    log_event(
        logger, event="dq_pass", layer="dq", table="lnd_loan",
        message=f"DQ PASSED — {len(dq_records)} checks, 0 breaches",
        duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "checks_run":       MetadataValue.int(len(dq_records)),
        "breaches":         MetadataValue.int(0),
        "total_lnd_rows":   MetadataValue.int(total_lnd),
        "acceptance_rate":  MetadataValue.float(accept_rate),
        "duration_sec":     MetadataValue.float(duration),
    })

    return Output(value={
        "checks_run": len(dq_records),
        "breaches":   0,
        "passed":     True,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dq_record(
    run_id, table, check, result, metric, threshold, breach, detail
) -> list:
    return [
        run_id, table, check, result,
        float(metric), float(threshold), breach,
        detail, datetime.now(timezone.utc),
    ]


def _table_exists(conn, table_name: str) -> bool:
    result = conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{table_name}'"
    ).fetchone()[0]
    return result > 0
