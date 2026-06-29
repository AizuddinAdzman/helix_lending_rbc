"""
assets/dq/dq_lnd_payment.py
-----------------------------
Dagster asset: dq_lnd_payment

Responsibility:
    Run data quality checks on lnd_payment after each landing load.
    Acts as an independent gate — runs in parallel with dq_lnd_loan.
    Staging is blocked until BOTH gates pass.

DQ checks performed:
    1. Volume        — rows_inserted / rows_in_raw >= 99% threshold
    2. Uniqueness    — no duplicate payment_id in lnd_payment
    3. Completeness  — null rates per critical column <= 10%
    4. Validity      — payment_method_type in known value set
    5. Referential   — loan_id in lnd_payment must exist in lnd_loan
                       (cross-source RI check — key for lending accuracy)

On breach:
    Raises Exception → Dagster marks asset FAILED → staging blocked

On pass:
    Results logged to dq_results table
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import DQ_ACCEPTANCE_THRESHOLD, DQ_MAX_NULL_RATE
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

VALID_PAYMENT_METHODS   = {"ach", "card", "check", "wire", "cash"}
VALID_METADATA_SOURCES  = {"web", "mobile_app", "branch", "api"}

CRITICAL_COLUMNS = [
    "payment_id", "loan_id", "amount", "payment_timestamp",
]


@asset(
    group_name="dq",
    deps=["lnd_payment", "dq_lnd_loan"],
    description="DQ gate for lnd_payment — runs independently of dq_lnd_loan",
)
def dq_lnd_payment(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time      = time.time()
    batch_date_str  = datetime.now(timezone.utc).date().isoformat()
    run_id          = f"dq_lnd_payment_{batch_date_str}"
    breaches        = []
    dq_records      = []

    log_event(
        logger, event="load_start", layer="dq", table="lnd_payment",
        message="Starting DQ checks on lnd_payment",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        # dq_results table already created by dq_lnd_loan
        # but create if running independently
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id          VARCHAR,
                table_name      VARCHAR,
                check_name      VARCHAR,
                check_result    VARCHAR,
                metric_value    DOUBLE,
                threshold       DOUBLE,
                breach_flag     BOOLEAN,
                detail          VARCHAR,
                checked_at      TIMESTAMP
            )
        """)

        total_lnd = conn.execute(
            "SELECT COUNT(*) FROM lnd_payment"
        ).fetchone()[0]

        total_raw = conn.execute(
            "SELECT COUNT(*) FROM raw_payment WHERE _last_updated_ts = "
            "(SELECT MAX(_last_updated_ts) FROM raw_payment)"
        ).fetchone()[0]

        total_err = conn.execute(
            "SELECT COUNT(*) FROM err_payment WHERE _last_updated_ts = "
            "(SELECT MAX(_last_updated_ts) FROM err_payment)"
        ).fetchone()[0] if _table_exists(conn, "err_payment") else 0

        # ------------------------------------------------------------------
        # CHECK 1: Volume — acceptance rate
        # ------------------------------------------------------------------
        accepted    = total_raw - total_err
        accept_rate = (accepted / total_raw) if total_raw > 0 else 0.0
        breach      = accept_rate < DQ_ACCEPTANCE_THRESHOLD

        dq_records.append(_dq_record(
            run_id, "lnd_payment", "volume_acceptance_rate",
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
                logger, event="dq_fail", layer="dq", table="lnd_payment",
                message=breaches[-1], batch_date=batch_date_str, level="ERROR",
            )

        # ------------------------------------------------------------------
        # CHECK 2: Uniqueness — payment_id
        # ------------------------------------------------------------------
        dup_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT payment_id, COUNT(*) AS cnt
                FROM lnd_payment
                GROUP BY payment_id
                HAVING cnt > 1
            )
            """
        ).fetchone()[0]

        dup_breach = dup_count > 0
        dq_records.append(_dq_record(
            run_id, "lnd_payment", "uniqueness_payment_id",
            "FAIL" if dup_breach else "PASS",
            float(dup_count), 0.0, dup_breach,
            f"{dup_count} duplicate payment_ids found",
        ))
        if dup_breach:
            breaches.append(f"Uniqueness: {dup_count} duplicate payment_ids")
            log_event(
                logger, event="dq_fail", layer="dq", table="lnd_payment",
                message=breaches[-1], batch_date=batch_date_str, level="ERROR",
            )

        # ------------------------------------------------------------------
        # CHECK 3: Completeness — null rates per critical column
        # ------------------------------------------------------------------
        if total_lnd > 0:
            for col in CRITICAL_COLUMNS:
                null_count  = conn.execute(
                    f"SELECT COUNT(*) FROM lnd_payment WHERE {col} IS NULL"
                ).fetchone()[0]
                null_rate   = null_count / total_lnd
                col_breach  = null_rate > DQ_MAX_NULL_RATE
                dq_records.append(_dq_record(
                    run_id, "lnd_payment", f"completeness_{col}",
                    "FAIL" if col_breach else "PASS",
                    null_rate, DQ_MAX_NULL_RATE, col_breach,
                    f"{null_count}/{total_lnd} nulls in {col}",
                ))
                if col_breach:
                    breaches.append(
                        f"Completeness: {col} null rate {null_rate:.2%}"
                    )
                    log_event(
                        logger, event="dq_fail", layer="dq", table="lnd_payment",
                        message=breaches[-1], batch_date=batch_date_str, level="WARNING",
                    )

        # ------------------------------------------------------------------
        # CHECK 4: Validity — payment_method_type
        # ------------------------------------------------------------------
        in_clause = ",".join([f"'{v}'" for v in VALID_PAYMENT_METHODS])
        invalid_method_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM lnd_payment
            WHERE payment_method_type IS NOT NULL
              AND LOWER(payment_method_type) NOT IN ({in_clause})
            """
        ).fetchone()[0]

        val_breach = invalid_method_count > 0
        dq_records.append(_dq_record(
            run_id, "lnd_payment", "validity_payment_method_type",
            "WARN" if val_breach else "PASS",
            float(invalid_method_count), 0.0, False,
            f"{invalid_method_count} unrecognised payment_method_type values",
        ))
        if val_breach:
            log_event(
                logger, event="checkpoint", layer="dq", table="lnd_payment",
                message=f"Validity warning: {invalid_method_count} unknown payment methods",
                batch_date=batch_date_str, level="WARNING",
            )

        # ------------------------------------------------------------------
        # CHECK 5: Referential integrity — loan_id must exist in lnd_loan
        # ------------------------------------------------------------------
        orphan_count = conn.execute(
            """
            SELECT COUNT(*) FROM lnd_payment p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM lnd_loan l
                  WHERE l.loan_id = p.loan_id
              )
            """
        ).fetchone()[0]

        ri_breach = orphan_count > 0
        dq_records.append(_dq_record(
            run_id, "lnd_payment", "referential_integrity_loan_id",
            "FAIL" if ri_breach else "PASS",
            float(orphan_count), 0.0, ri_breach,
            f"{orphan_count} payment loan_ids not found in lnd_loan",
        ))
        if ri_breach:
            breaches.append(
                f"Referential integrity: {orphan_count} payments "
                f"reference loan_ids not in lnd_loan"
            )
            log_event(
                logger, event="dq_fail", layer="dq", table="lnd_payment",
                message=breaches[-1], batch_date=batch_date_str, level="ERROR",
            )

        # ------------------------------------------------------------------
        # Persist DQ results
        # ------------------------------------------------------------------
        conn.executemany(
            "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            dq_records,
        )

    duration = round(time.time() - start_time, 3)

    if breaches:
        log_event(
            logger, event="dq_fail", layer="dq", table="lnd_payment",
            message=f"DQ FAILED — {len(breaches)} breach(es): {'; '.join(breaches)}",
            duration_sec=duration, batch_date=batch_date_str, level="ERROR",
        )
        raise Exception(
            f"DQ gate dq_lnd_payment FAILED with {len(breaches)} breach(es):\n"
            + "\n".join(f"  • {b}" for b in breaches)
        )

    log_event(
        logger, event="dq_pass", layer="dq", table="lnd_payment",
        message=f"DQ PASSED — {len(dq_records)} checks, 0 breaches",
        duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "checks_run":       MetadataValue.int(len(dq_records)),
        "breaches":         MetadataValue.int(0),
        "total_lnd_rows":   MetadataValue.int(total_lnd),
        "acceptance_rate":  MetadataValue.float(accept_rate),
        "ri_orphans":       MetadataValue.int(orphan_count),
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
    return conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{table_name}'"
    ).fetchone()[0] > 0
