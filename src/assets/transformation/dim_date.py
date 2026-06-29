"""
assets/transformation/dim_date.py
-----------------------------------
Schema: hlx_{ENV}_dim
80-year date spine centred on today(). Sequential after dim_customer.
"""

import time
from datetime import datetime, timezone, timedelta

from dagster import asset, Output, MetadataValue

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import get_dim_date_bounds, TBL_DIM_DATE, SCHEMA_DIM
from resources.duckdb_resource import DuckDBResource
from utils.logger import get_logger, log_event

logger = get_logger(__name__)

MONTH_NAMES = ["","January","February","March","April","May","June",
               "July","August","September","October","November","December"]
DAY_NAMES   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


@asset(
    group_name="transformation",
    deps=["dim_customer"],
    description=f"Date spine today−40y to today+40y → {TBL_DIM_DATE}",
)
def dim_date(context, duckdb_resource: DuckDBResource) -> Output:
    start_time     = time.time()
    batch_date_str = datetime.now(timezone.utc).date().isoformat()
    lower, upper   = get_dim_date_bounds()

    log_event(logger, event="load_start", layer="dim", table=TBL_DIM_DATE,
              message=f"Building dim_date spine {lower} → {upper}",
              batch_date=batch_date_str)

    rows = []
    cur  = lower
    while cur <= upper:
        iso = cur.isocalendar()
        nxt = cur + timedelta(days=1)
        rows.append([
            int(cur.strftime("%Y%m%d")), cur, cur.year,
            (cur.month - 1) // 3 + 1, cur.month, MONTH_NAMES[cur.month],
            iso[1], cur.day, iso[2], DAY_NAMES[iso[2] - 1],
            iso[2] >= 6, nxt.month != cur.month,
        ])
        cur = nxt

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_DIM}")
        conn.execute(f"DROP TABLE IF EXISTS {TBL_DIM_DATE}")
        conn.execute(f"""
            CREATE TABLE {TBL_DIM_DATE} (
                date_id INTEGER NOT NULL PRIMARY KEY, full_date DATE NOT NULL,
                year INTEGER, quarter INTEGER, month INTEGER,
                month_name VARCHAR, week_of_year INTEGER,
                day_of_month INTEGER, day_of_week INTEGER,
                day_name VARCHAR, is_weekend BOOLEAN, is_month_end BOOLEAN
            )""")
        conn.executemany(
            f"INSERT INTO {TBL_DIM_DATE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        total = conn.execute(f"SELECT COUNT(*) FROM {TBL_DIM_DATE}").fetchone()[0]

    duration = round(time.time() - start_time, 3)
    log_event(logger, event="load_end", layer="dim", table=TBL_DIM_DATE,
              message=f"dim_date complete: {total} rows",
              rows_out=total, duration_sec=duration, batch_date=batch_date_str)
    context.add_output_metadata({
        "rows_inserted": MetadataValue.int(total),
        "lower_bound":   MetadataValue.text(str(lower)),
        "upper_bound":   MetadataValue.text(str(upper)),
        "duration_sec":  MetadataValue.float(duration),
        "table":         MetadataValue.text(TBL_DIM_DATE),
    })
    return Output(value={"rows_inserted": total})
