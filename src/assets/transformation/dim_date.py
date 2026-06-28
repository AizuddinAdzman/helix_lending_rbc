"""
assets/transformation/dim_date.py
-----------------------------------
Dagster asset: dim_date

Responsibility:
    Build a date spine covering today - 40 years to today + 40 years (80 years total).
    ~29,200 rows. Self-updating — bounds computed from today() at runtime.

Columns:
    date_id         INTEGER     YYYYMMDD surrogate key
    full_date       DATE
    year            INTEGER
    quarter         INTEGER     1-4
    month           INTEGER     1-12
    month_name      VARCHAR     January .. December
    week_of_year    INTEGER
    day_of_month    INTEGER
    day_of_week     INTEGER     1=Monday .. 7=Sunday
    day_name        VARCHAR     Monday .. Sunday
    is_weekend      BOOLEAN
    is_month_end    BOOLEAN
"""

import time
from datetime import datetime, timezone, date, timedelta

from dagster import asset, AssetExecutionContext, Output, MetadataValue

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import get_dim_date_bounds
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
]


@asset(
    group_name="transformation",
    deps=["stg_loan_payment"],
    description="Date spine: today − 40y to today + 40y (80 years, ~29,200 rows)",
)
def dim_date(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()

    lower, upper   = get_dim_date_bounds()

    log_event(
        logger, event="load_start", layer="dim", table="dim_date",
        message=f"Building dim_date spine: {lower} → {upper}",
        batch_date=batch_date_str,
    )

    rows = []
    current = lower
    while current <= upper:
        iso_cal  = current.isocalendar()
        dom      = current.day
        # Last day of month check
        next_day = current + timedelta(days=1)
        is_month_end = (next_day.month != current.month)

        rows.append([
            int(current.strftime("%Y%m%d")),   # date_id
            current,                            # full_date
            current.year,
            (current.month - 1) // 3 + 1,      # quarter
            current.month,
            MONTH_NAMES[current.month],
            iso_cal[1],                         # week_of_year (ISO)
            dom,
            iso_cal[2],                         # day_of_week 1=Mon 7=Sun
            DAY_NAMES[iso_cal[2] - 1],
            iso_cal[2] >= 6,                    # is_weekend (Sat=6, Sun=7)
            is_month_end,
        ])
        current += timedelta(days=1)

    total_rows = len(rows)

    log_event(
        logger, event="checkpoint", layer="dim", table="dim_date",
        message=f"Generated {total_rows} date rows, writing to DuckDB",
        batch_date=batch_date_str,
    )

    with duckdb_resource.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS dim_date")
        conn.execute("""
            CREATE TABLE dim_date (
                date_id         INTEGER     NOT NULL PRIMARY KEY,
                full_date       DATE        NOT NULL,
                year            INTEGER,
                quarter         INTEGER,
                month           INTEGER,
                month_name      VARCHAR,
                week_of_year    INTEGER,
                day_of_month    INTEGER,
                day_of_week     INTEGER,
                day_name        VARCHAR,
                is_weekend      BOOLEAN,
                is_month_end    BOOLEAN
            )
        """)
        conn.executemany(
            "INSERT INTO dim_date VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        total = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]

    duration = round(time.time() - start_time, 3)

    log_event(
        logger, event="load_end", layer="dim", table="dim_date",
        message=f"dim_date complete: {total} rows ({lower} → {upper})",
        rows_out=total, duration_sec=duration, batch_date=batch_date_str,
    )

    context.add_output_metadata({
        "rows_inserted":  MetadataValue.int(total),
        "lower_bound":    MetadataValue.text(str(lower)),
        "upper_bound":    MetadataValue.text(str(upper)),
        "duration_sec":   MetadataValue.float(duration),
    })

    return Output(value={"rows_inserted": total})
