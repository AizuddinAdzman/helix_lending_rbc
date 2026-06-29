"""
assets/dq/dq_lnd_payment.py
-----------------------------
Dagster asset: dq_lnd_payment
Writes results to: hlx_{ENV}_lnd.lnd_dq_audit

DQ gate for lnd_payment. Blocks staging if any hard check fails.
Sequential after dq_lnd_loan.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    DQ_ACCEPTANCE_THRESHOLD, DQ_MAX_NULL_RATE,
    TBL_RAW_PAYMENT, TBL_LND_LOAN, TBL_LND_PAYMENT,
    TBL_LND_ERR_PAYMENT, TBL_LND_DQ_AUDIT, SCHEMA_LND,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

CRITICAL_COLUMNS = ["payment_id", "loan_id", "amount", "payment_timestamp"]
VALID_PAYMENT_METHODS = {"ach", "card", "check", "wire", "cash"}


@asset(
    group_name="dq",
    deps=["dq_lnd_loan"],
    description=f"DQ gate for {TBL_LND_PAYMENT} — results written to {TBL_LND_DQ_AUDIT}",
)
def dq_lnd_payment(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()
    run_id         = f"dq_lnd_payment_{batch_date_str}"
    breaches       = []
    dq_records     = []

    log_event(logger, event="load_start", layer="dq", table=TBL_LND_PAYMENT,
              message="Starting DQ checks on lnd_payment", batch_date=batch_date_str)

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_LND}")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TBL_LND_DQ_AUDIT} (
                run_id VARCHAR, table_name VARCHAR, check_name VARCHAR,
                check_result VARCHAR, metric_value DOUBLE, threshold DOUBLE,
                breach_flag BOOLEAN, detail VARCHAR, checked_at TIMESTAMP
            )""")

        total_lnd = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_LND_PAYMENT}"
        ).fetchone()[0]
        total_raw = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_RAW_PAYMENT}"
        ).fetchone()[0]
        total_err = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_LND_ERR_PAYMENT}"
        ).fetchone()[0] if _exists(conn, TBL_LND_ERR_PAYMENT) else 0

        # CHECK 1: Volume
        accepted    = total_raw - total_err
        accept_rate = accepted / total_raw if total_raw > 0 else 0.0
        breach      = accept_rate < DQ_ACCEPTANCE_THRESHOLD
        dq_records.append(_rec(run_id, TBL_LND_PAYMENT, "volume_acceptance_rate",
            "FAIL" if breach else "PASS", accept_rate,
            DQ_ACCEPTANCE_THRESHOLD, breach, f"{accepted}/{total_raw} rows accepted"))
        if breach:
            breaches.append(f"Volume: acceptance {accept_rate:.2%} < {DQ_ACCEPTANCE_THRESHOLD:.2%}")
            log_event(logger, event="dq_fail", layer="dq", table=TBL_LND_PAYMENT,
                      message=breaches[-1], batch_date=batch_date_str, level="ERROR")

        # CHECK 2: Uniqueness
        dups = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT payment_id, COUNT(*) c FROM {TBL_LND_PAYMENT}
                GROUP BY payment_id HAVING c > 1
            )""").fetchone()[0]
        dup_breach = dups > 0
        dq_records.append(_rec(run_id, TBL_LND_PAYMENT, "uniqueness_payment_id",
            "FAIL" if dup_breach else "PASS", float(dups), 0.0,
            dup_breach, f"{dups} duplicate payment_ids"))
        if dup_breach:
            breaches.append(f"Uniqueness: {dups} duplicate payment_ids")
            log_event(logger, event="dq_fail", layer="dq", table=TBL_LND_PAYMENT,
                      message=breaches[-1], batch_date=batch_date_str, level="ERROR")

        # CHECK 3: Completeness
        if total_lnd > 0:
            for col in CRITICAL_COLUMNS:
                null_count = conn.execute(
                    f"SELECT COUNT(*) FROM {TBL_LND_PAYMENT} WHERE {col} IS NULL"
                ).fetchone()[0]
                null_rate  = null_count / total_lnd
                col_breach = null_rate > DQ_MAX_NULL_RATE
                dq_records.append(_rec(run_id, TBL_LND_PAYMENT, f"completeness_{col}",
                    "FAIL" if col_breach else "PASS", null_rate,
                    DQ_MAX_NULL_RATE, col_breach,
                    f"{null_count}/{total_lnd} nulls in {col}"))
                if col_breach:
                    breaches.append(f"Completeness: {col} null rate {null_rate:.2%}")
                    log_event(logger, event="dq_fail", layer="dq", table=TBL_LND_PAYMENT,
                              message=breaches[-1], batch_date=batch_date_str, level="WARNING")

        # CHECK 4: Validity (warning only)
        in_clause = ",".join([f"'{v}'" for v in VALID_PAYMENT_METHODS])
        invalid = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_LND_PAYMENT} "
            f"WHERE payment_method_type IS NOT NULL "
            f"AND LOWER(payment_method_type) NOT IN ({in_clause})"
        ).fetchone()[0]
        dq_records.append(_rec(run_id, TBL_LND_PAYMENT, "validity_payment_method_type",
            "WARN" if invalid > 0 else "PASS", float(invalid), 0.0,
            False, f"{invalid} unrecognised payment methods"))

        # CHECK 5: Referential integrity
        orphans = conn.execute(f"""
            SELECT COUNT(*) FROM {TBL_LND_PAYMENT} p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {TBL_LND_LOAN} l WHERE l.loan_id = p.loan_id
              )""").fetchone()[0]
        ri_breach = orphans > 0
        dq_records.append(_rec(run_id, TBL_LND_PAYMENT, "referential_integrity_loan_id",
            "FAIL" if ri_breach else "PASS", float(orphans), 0.0,
            ri_breach, f"{orphans} orphan loan_ids"))
        if ri_breach:
            breaches.append(f"Referential integrity: {orphans} orphan loan_ids")
            log_event(logger, event="dq_fail", layer="dq", table=TBL_LND_PAYMENT,
                      message=breaches[-1], batch_date=batch_date_str, level="ERROR")

        conn.executemany(
            f"INSERT INTO {TBL_LND_DQ_AUDIT} VALUES (?,?,?,?,?,?,?,?,?)",
            dq_records,
        )

    duration = round(time.time() - start_time, 3)

    if breaches:
        log_event(logger, event="dq_fail", layer="dq", table=TBL_LND_PAYMENT,
                  message=f"DQ FAILED — {len(breaches)} breach(es)",
                  duration_sec=duration, batch_date=batch_date_str, level="ERROR")
        raise Exception(
            f"DQ gate dq_lnd_payment FAILED with {len(breaches)} breach(es):\n"
            + "\n".join(f"  • {b}" for b in breaches)
        )

    log_event(logger, event="dq_pass", layer="dq", table=TBL_LND_PAYMENT,
              message=f"DQ PASSED — {len(dq_records)} checks, 0 breaches",
              duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "checks_run":      MetadataValue.int(len(dq_records)),
        "breaches":        MetadataValue.int(0),
        "ri_orphans":      MetadataValue.int(orphans),
        "acceptance_rate": MetadataValue.float(accept_rate),
        "duration_sec":    MetadataValue.float(duration),
        "audit_table":     MetadataValue.text(TBL_LND_DQ_AUDIT),
    })
    return Output(value={"checks_run": len(dq_records), "breaches": 0})


def _rec(run_id, table, check, result, metric, threshold, breach, detail):
    return [run_id, table, check, result, float(metric), float(threshold),
            breach, detail, datetime.now(timezone.utc)]

def _exists(conn, table: str) -> bool:
    schema, tbl = table.split(".")
    return conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='{schema}' AND table_name='{tbl}'"
    ).fetchone()[0] > 0
