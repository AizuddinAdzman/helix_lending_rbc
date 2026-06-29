"""
assets/marts/mart_observability.py
------------------------------------
Dagster asset: mart_data_observability

Business question answered:
    What is the data freshness and completeness for each source?

Output grain: one row per source table per pipeline run.

Reads from:
    dq_results          — DQ check outcomes per layer
    raw_loan            — row counts per batch
    raw_payment         — row counts per batch
    fct_loan            — final output row count
    fct_payment         — final output row count

Dependencies: dq_lnd_loan, dq_lnd_payment, fct_loan, fct_payment
"""

import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import asset, Output, MetadataValue

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)


@asset(
    group_name="marts",
    deps=["dq_lnd_loan", "dq_lnd_payment", "fct_loan", "fct_payment", "mart_payment_anomaly"],
    description="Q3: Data freshness, completeness, and pipeline health per run",
)
def mart_data_observability(
    context,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()
    batch_ts       = datetime.now(timezone.utc)

    log_event(
        logger, event="load_start", layer="mart", table="mart_data_observability",
        message="Building mart_data_observability",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS mart_data_observability")
        conn.execute("""
            CREATE TABLE mart_data_observability (
                run_date                DATE,
                source_name             VARCHAR,
                raw_rows_in_batch       INTEGER,
                lnd_rows_accepted       INTEGER,
                lnd_rows_rejected       INTEGER,
                acceptance_rate_pct     DECIMAL(6,2),
                dq_checks_run           INTEGER,
                dq_checks_failed        INTEGER,
                dq_breach_flag          BOOLEAN,
                fct_rows                INTEGER,
                latest_batch_ts         TIMESTAMP,
                freshness_hours         DECIMAL(8,2),
                pipeline_status         VARCHAR,
                _last_updated_ts        TIMESTAMP
            )
        """)

        for source in ("loan", "payment"):
            raw_table  = f"raw_{source}"
            lnd_table  = f"lnd_{source}"
            err_table  = f"err_{source}"
            fct_table  = "fct_loan" if source == "loan" else "fct_payment"

            # Raw counts
            raw_rows = conn.execute(
                f"SELECT COUNT(*) FROM {raw_table} WHERE _last_updated_ts = "
                f"(SELECT MAX(_last_updated_ts) FROM {raw_table})"
            ).fetchone()[0]

            latest_ts_row = conn.execute(
                f"SELECT MAX(_last_updated_ts) FROM {raw_table}"
            ).fetchone()[0]

            # Error counts
            err_rows = conn.execute(
                f"SELECT COUNT(*) FROM {err_table} WHERE _last_updated_ts = "
                f"(SELECT MAX(_last_updated_ts) FROM {err_table})"
            ).fetchone()[0] if _table_exists(conn, err_table) else 0

            accepted     = raw_rows - err_rows
            accept_pct   = round(accepted * 100.0 / raw_rows, 2) if raw_rows > 0 else 0.0

            # DQ results
            dq_total = conn.execute(
                f"SELECT COUNT(*) FROM dq_results WHERE table_name = '{lnd_table}'"
            ).fetchone()[0] if _table_exists(conn, "dq_results") else 0

            dq_failed = conn.execute(
                f"""SELECT COUNT(*) FROM dq_results
                    WHERE table_name = '{lnd_table}'
                      AND check_result = 'FAIL'"""
            ).fetchone()[0] if _table_exists(conn, "dq_results") else 0

            dq_breach = dq_failed > 0

            # Fact counts
            fct_rows = conn.execute(
                f"SELECT COUNT(*) FROM {fct_table}"
            ).fetchone()[0] if _table_exists(conn, fct_table) else 0

            # Freshness
            freshness_hours = None
            if latest_ts_row:
                lt = latest_ts_row
                if hasattr(lt, 'tzinfo') and lt.tzinfo is None:
                    lt = lt.replace(tzinfo=timezone.utc)
                freshness_hours = round(
                    (batch_ts - lt).total_seconds() / 3600, 2
                )

            pipeline_status = "FAIL" if dq_breach else "PASS"

            conn.execute("""
                INSERT INTO mart_data_observability VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                batch_date_str, source,
                raw_rows, accepted, err_rows, accept_pct,
                dq_total, dq_failed, dq_breach,
                fct_rows,
                latest_ts_row, freshness_hours,
                pipeline_status, batch_ts,
            ])

            log_event(
                logger, event="checkpoint", layer="mart",
                table="mart_data_observability",
                message=(
                    f"source={source}: raw={raw_rows}, accepted={accepted}, "
                    f"dq_checks={dq_total}, dq_failed={dq_failed}, "
                    f"fct_rows={fct_rows}, freshness={freshness_hours}h, "
                    f"status={pipeline_status}"
                ),
                batch_date=batch_date_str,
            )

        total    = conn.execute(
            "SELECT COUNT(*) FROM mart_data_observability"
        ).fetchone()[0]
        breaches = conn.execute(
            "SELECT COUNT(*) FROM mart_data_observability WHERE dq_breach_flag = TRUE"
        ).fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="mart", table="mart_data_observability",
        message=(
            f"mart_data_observability complete: {total} source rows, "
            f"{breaches} DQ breach(es)"
        ),
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "source_rows":   MetadataValue.int(total),
        "dq_breaches":   MetadataValue.int(breaches),
        "duration_sec":  MetadataValue.float(duration),
    })

    return Output(value={"source_rows": total, "dq_breaches": breaches})


def _table_exists(conn, name: str) -> bool:
    return conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{name}'"
    ).fetchone()[0] > 0
