"""
assets/marts/mart_observability.py
------------------------------------
Schema: hlx_{ENV}_mart
Q3: Data freshness and pipeline health. Last asset to run.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import (
    TBL_RAW_LOAN, TBL_RAW_PAYMENT, TBL_RAW_AUDIT,
    TBL_LND_PAYMENT,
    TBL_LND_ERR_LOAN, TBL_LND_ERR_PAYMENT, TBL_LND_DQ_AUDIT,
    TBL_LND_LOAN, TBL_LND_PAYMENT,
    TBL_FCT_LOAN, TBL_FCT_PAYMENT,
    TBL_MART_OBSERVABILITY, SCHEMA_MART,
)
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["mart_payment_anomaly"],
    description=f"Q3: Pipeline health summary → {TBL_MART_OBSERVABILITY}",
)
def mart_data_observability(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_ts       = datetime.now(timezone.utc)
    batch_date_str = batch_ts.date().isoformat()

    log_event(logger, event="load_start", layer="mart",
              table=TBL_MART_OBSERVABILITY,
              message="Building mart_data_observability", batch_date=batch_date_str)

    sources = [
        {
            "name":      "loan",
            "raw":       TBL_RAW_LOAN,
            "lnd":       TBL_LND_LOAN,
            "err":       TBL_LND_ERR_LOAN,
            "fct":       TBL_FCT_LOAN,
        },
        {
            "name":      "payment",
            "raw":       TBL_RAW_PAYMENT,
            "lnd":       TBL_LND_PAYMENT,
            "err":       TBL_LND_ERR_PAYMENT,
            "fct":       TBL_FCT_PAYMENT,
        },
    ]

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MART}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TBL_MART_OBSERVABILITY} (
                run_date                    DATE,
                source_name                 VARCHAR,
                raw_rows_in_batch           INTEGER,
                raw_distinct_keys           INTEGER,
                raw_duplicate_key_count     INTEGER,
                raw_true_duplicate_count    INTEGER,
                raw_diff_amount_same_id     INTEGER,
                lnd_rows_accepted           INTEGER,
                lnd_rows_rejected           INTEGER,
                acceptance_rate_pct         DECIMAL(6,2),
                dq_checks_run               INTEGER,
                dq_checks_failed            INTEGER,
                dq_breach_flag              BOOLEAN,
                fct_rows                    INTEGER,
                latest_batch_ts             TIMESTAMP,
                freshness_hours             DECIMAL(8,2),
                payments_allocated          INTEGER,
                payments_unallocated        INTEGER,
                payments_loan_rejected      INTEGER,
                payments_unidentified       INTEGER,
                pipeline_status             VARCHAR,
                _last_updated_ts            TIMESTAMP
            )""")

        for src in sources:
            raw_n = conn.execute(
                f"SELECT COUNT(*) FROM {src['raw']}"
            ).fetchone()[0]
            err_n = conn.execute(
                f"SELECT COUNT(*) FROM {src['err']}"
            ).fetchone()[0] if _exists(conn, src["err"]) else 0
            acc   = raw_n - err_n
            rate  = round(acc * 100.0 / raw_n, 2) if raw_n > 0 else 0.0

            dq_t = conn.execute(
                f"SELECT COUNT(*) FROM {TBL_LND_DQ_AUDIT} "
                f"WHERE table_name = '{src['lnd']}'"
            ).fetchone()[0] if _exists(conn, TBL_LND_DQ_AUDIT) else 0
            dq_f = conn.execute(
                f"SELECT COUNT(*) FROM {TBL_LND_DQ_AUDIT} "
                f"WHERE table_name = '{src['lnd']}' AND check_result = 'FAIL'"
            ).fetchone()[0] if _exists(conn, TBL_LND_DQ_AUDIT) else 0

            fct_n = conn.execute(
                f"SELECT COUNT(*) FROM {src['fct']}"
            ).fetchone()[0] if _exists(conn, src["fct"]) else 0

            lt = conn.execute(
                f"SELECT MAX(_last_updated_ts) FROM {src['raw']}"
            ).fetchone()[0]
            fh = None
            if lt:
                ltu = lt.replace(tzinfo=timezone.utc) if lt.tzinfo is None else lt
                fh  = round((batch_ts - ltu).total_seconds() / 3600, 2)

            status = "FAIL" if dq_f > 0 else "PASS"

            # Pull raw_audit stats for this source
            audit_row = conn.execute(f"""
                SELECT distinct_keys, duplicate_key_count,
                       true_duplicate_count, diff_amount_same_id_count
                FROM {TBL_RAW_AUDIT}
                WHERE source_table = '{src["raw"]}'
                ORDER BY batch_ts DESC LIMIT 1
            """).fetchone() if _exists(conn, TBL_RAW_AUDIT) else None

            distinct_keys   = audit_row[0] if audit_row else None
            dup_key_count   = audit_row[1] if audit_row else None
            true_dup_count  = audit_row[2] if audit_row else None
            diff_amt_count  = audit_row[3] if audit_row else None

            # Allocation breakdown for payment source
            alloc_allocated = alloc_unallocated = alloc_loan_rejected = alloc_unidentified = None
            if src["name"] == "payment" and _exists(conn, TBL_LND_PAYMENT):
                rows = conn.execute(
                    f"SELECT payment_allocation_status, COUNT(*) cnt "
                    f"FROM {TBL_LND_PAYMENT} "
                    f"GROUP BY payment_allocation_status"
                ).fetchall()
                alloc_map = {r[0]: r[1] for r in rows}
                alloc_allocated      = alloc_map.get("allocated",     0)
                alloc_unallocated    = alloc_map.get("unallocated",   0)
                alloc_loan_rejected  = alloc_map.get("loan_rejected", 0)
                alloc_unidentified   = alloc_map.get("unidentified",  0)

            conn.execute(
                """INSERT INTO """ + TBL_MART_OBSERVABILITY + """ VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [batch_date_str, src["name"],
                 raw_n, distinct_keys, dup_key_count, true_dup_count, diff_amt_count,
                 acc, err_n, rate,
                 dq_t, dq_f, dq_f > 0, fct_n, lt, fh,
                 alloc_allocated, alloc_unallocated, alloc_loan_rejected, alloc_unidentified,
                 status, batch_ts],
            )
            log_event(
                logger, event="checkpoint", layer="mart",
                table=TBL_MART_OBSERVABILITY,
                message=(f"source={src['name']}: raw={raw_n}, "
                         f"distinct_keys={distinct_keys}, "
                         f"true_dups={true_dup_count}, diff_amt_same_id={diff_amt_count}, "
                         f"accepted={acc}, dq_checks={dq_t}, dq_failed={dq_f}, "
                         f"fct_rows={fct_n}, freshness={fh}h, status={status}"),
                batch_date=batch_date_str,
            )

        total    = conn.execute(f"SELECT COUNT(*) FROM {TBL_MART_OBSERVABILITY}").fetchone()[0]
        breaches = conn.execute(
            f"SELECT COUNT(*) FROM {TBL_MART_OBSERVABILITY} WHERE dq_breach_flag = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="mart", table=TBL_MART_OBSERVABILITY,
              message=f"mart_data_observability complete: {total} rows, {breaches} breaches",
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "source_rows":  MetadataValue.int(total),
        "dq_breaches":  MetadataValue.int(breaches),
        "duration_sec": MetadataValue.float(duration),
        "table":        MetadataValue.text(TBL_MART_OBSERVABILITY),
    })
    return Output(value={"source_rows": total, "dq_breaches": breaches})


def _exists(conn, table: str) -> bool:
    schema, tbl = table.split(".")
    return conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='{schema}' AND table_name='{tbl}'"
    ).fetchone()[0] > 0