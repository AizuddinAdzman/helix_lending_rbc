"""
tests/unit/test_dq_checks.py
------------------------------
Unit tests for DQ gate logic — both lnd_loan and lnd_payment.

Strategy:
    We test the DQ assertions as pure SQL against in-memory DuckDB.
    This validates the check logic independently of the Dagster asset wrapper.

Coverage:
    Volume / acceptance rate:
        - Pass when rows_accepted / rows_in >= 0.99
        - Fail when below threshold
        - Edge: all rows rejected
        - Edge: empty table

    Uniqueness:
        - Pass: no duplicates
        - Fail: duplicate loan_id / payment_id

    Completeness (null rates):
        - Pass: nulls within threshold
        - Fail: nulls exceed threshold on critical column

    Referential integrity (payment → loan):
        - Pass: all loan_ids exist in lnd_loan
        - Fail: orphan loan_id

    Freshness:
        - Pass: latest batch within expected window
        - Fail: stale data (no recent batch)
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import duckdb
import pytest

from config import DQ_ACCEPTANCE_THRESHOLD, DQ_MAX_NULL_RATE


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _setup_lnd_loan(conn, rows: list[dict]):
    """Insert rows into a minimal lnd_loan table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lnd_loan (
            loan_id         VARCHAR,
            customer_id     VARCHAR,
            product_type    VARCHAR,
            principal_amount DECIMAL(18,2),
            interest_rate   DECIMAL(8,4),
            term_months     INTEGER,
            origination_date DATE,
            status          VARCHAR,
            is_current_flag BOOLEAN,
            _last_updated_ts TIMESTAMP
        )
    """)
    for r in rows:
        conn.execute("""
            INSERT INTO lnd_loan VALUES (?,?,?,?,?,?,?,?,?,?)
        """, [
            r.get("loan_id"), r.get("customer_id"), r.get("product_type"),
            r.get("principal_amount"), r.get("interest_rate"),
            r.get("term_months"), r.get("origination_date"),
            r.get("status"), r.get("is_current_flag", True),
            r.get("ts", datetime.now(timezone.utc)),
        ])


def _setup_lnd_payment(conn, rows: list[dict]):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lnd_payment (
            payment_id      VARCHAR,
            loan_id         VARCHAR,
            amount          DECIMAL(18,2),
            payment_timestamp TIMESTAMPTZ,
            _last_updated_ts TIMESTAMP
        )
    """)
    for r in rows:
        conn.execute(
            "INSERT INTO lnd_payment VALUES (?,?,?,?,?)",
            [r.get("payment_id"), r.get("loan_id"), r.get("amount"),
             r.get("ts", datetime.now(timezone.utc)),
             r.get("ts", datetime.now(timezone.utc))]
        )


def _setup_raw_counts(conn, raw_in: int, err_count: int, table: str):
    """Set up raw_ and err_ counts for acceptance rate checks."""
    ts = datetime.now(timezone.utc)
    raw_table = f"raw_{table}"
    err_table = f"err_{table}"

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {raw_table} (
            loan_id VARCHAR, _last_updated_ts TIMESTAMP
        )
    """) if table == "loan" else conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {raw_table} (
            payment_id VARCHAR, _last_updated_ts TIMESTAMP
        )
    """)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {err_table} (
            loan_id VARCHAR, _last_updated_ts TIMESTAMP,
            _rejection_reason VARCHAR
        )
    """) if table == "loan" else conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {err_table} (
            payment_id VARCHAR, _last_updated_ts TIMESTAMP,
            _rejection_reason VARCHAR
        )
    """)

    id_col = "loan_id" if table == "loan" else "payment_id"
    for i in range(raw_in):
        conn.execute(
            f"INSERT INTO {raw_table} VALUES (?, ?)", [f"ID{i:05d}", ts]
        )
    for i in range(err_count):
        conn.execute(
            f"INSERT INTO {err_table} VALUES (?, ?, ?)",
            [f"ID{i:05d}", ts, "test rejection"]
        )


# ---------------------------------------------------------------------------
# CHECK 1: Volume / Acceptance Rate
# ---------------------------------------------------------------------------

class TestVolumeAcceptanceRate:

    def _rate(self, raw_in, err_count):
        return (raw_in - err_count) / raw_in if raw_in > 0 else 0.0

    def test_100pct_acceptance_passes(self):
        rate = self._rate(1000, 0)
        assert rate >= DQ_ACCEPTANCE_THRESHOLD

    def test_99pct_acceptance_passes(self):
        rate = self._rate(1000, 10)
        assert rate >= DQ_ACCEPTANCE_THRESHOLD

    def test_98pct_acceptance_fails(self):
        rate = self._rate(1000, 20)
        assert rate < DQ_ACCEPTANCE_THRESHOLD

    def test_exact_threshold_passes(self):
        # Exactly 990/1000 = 0.990 — meets threshold
        rate = self._rate(1000, 10)
        assert rate >= DQ_ACCEPTANCE_THRESHOLD

    def test_one_below_threshold_fails(self):
        # 989/1000 = 0.989 — just below
        rate = self._rate(1000, 11)
        assert rate < DQ_ACCEPTANCE_THRESHOLD

    def test_all_rejected_fails(self):
        rate = self._rate(100, 100)
        assert rate < DQ_ACCEPTANCE_THRESHOLD

    def test_empty_source_zero_rate(self):
        rate = self._rate(0, 0)
        assert rate == 0.0

    def test_fixture_loan_rate(self):
        # fixture: 10 rows, 0 raw rejections (bad rows still insert at raw)
        rate = self._rate(10, 0)
        assert rate >= DQ_ACCEPTANCE_THRESHOLD

    def test_fixture_payment_rate(self):
        # fixture: 10 lines, 1 bad JSON → err_payment
        rate = self._rate(10, 1)
        # 9/10 = 90% — BELOW threshold. This fixture intentionally breaches DQ.
        # The bad JSON line makes acceptance rate 90%, triggering DQ gate failure.
        assert rate < DQ_ACCEPTANCE_THRESHOLD


class TestVolumeSQL:

    def test_acceptance_rate_sql(self, conn):
        """Verify the SQL-based acceptance rate calculation."""
        _setup_raw_counts(conn, raw_in=100, err_count=2, table="loan")
        rate = conn.execute("""
            SELECT (COUNT(*) - (SELECT COUNT(*) FROM err_loan)) * 1.0
                   / NULLIF(COUNT(*), 0)
            FROM raw_loan
        """).fetchone()[0]
        assert abs(rate - 0.98) < 0.001


# ---------------------------------------------------------------------------
# CHECK 2: Uniqueness
# ---------------------------------------------------------------------------

class TestUniqueness:

    def test_no_duplicates_passes(self, conn):
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "is_current_flag": True},
            {"loan_id": "L002", "is_current_flag": True},
            {"loan_id": "L003", "is_current_flag": True},
        ])
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT loan_id, COUNT(*) AS cnt
                FROM lnd_loan WHERE is_current_flag = TRUE
                GROUP BY loan_id HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 0

    def test_duplicate_loan_id_detected(self, conn):
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "is_current_flag": True},
            {"loan_id": "L001", "is_current_flag": True},  # duplicate
            {"loan_id": "L002", "is_current_flag": True},
        ])
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT loan_id, COUNT(*) AS cnt
                FROM lnd_loan WHERE is_current_flag = TRUE
                GROUP BY loan_id HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 1

    def test_inactive_rows_excluded_from_uniqueness(self, conn):
        """SCD2 history rows (is_current=FALSE) don't trigger uniqueness fail."""
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "is_current_flag": False},  # old version
            {"loan_id": "L001", "is_current_flag": True},   # current version
        ])
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT loan_id, COUNT(*) AS cnt
                FROM lnd_loan WHERE is_current_flag = TRUE
                GROUP BY loan_id HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 0

    def test_duplicate_payment_id_detected(self, conn):
        _setup_lnd_payment(conn, [
            {"payment_id": "P001", "loan_id": "L001", "amount": 100},
            {"payment_id": "P001", "loan_id": "L001", "amount": 100},
            {"payment_id": "P002", "loan_id": "L001", "amount": 200},
        ])
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT payment_id, COUNT(*) AS cnt
                FROM lnd_payment
                GROUP BY payment_id HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 1

    def test_unique_payment_ids_pass(self, conn):
        _setup_lnd_payment(conn, [
            {"payment_id": "P001", "loan_id": "L001", "amount": 100},
            {"payment_id": "P002", "loan_id": "L001", "amount": 200},
            {"payment_id": "P003", "loan_id": "L002", "amount": 300},
        ])
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT payment_id, COUNT(*) AS cnt
                FROM lnd_payment
                GROUP BY payment_id HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 0


# ---------------------------------------------------------------------------
# CHECK 3: Completeness (null rates)
# ---------------------------------------------------------------------------

class TestCompleteness:

    def test_no_nulls_passes(self, conn):
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "status": "active", "product_type": "personal",
             "principal_amount": 10000, "interest_rate": 5.0, "term_months": 12,
             "origination_date": date(2023, 1, 1), "is_current_flag": True},
        ])
        null_count = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE loan_id IS NULL"
        ).fetchone()[0]
        null_rate = null_count / 1
        assert null_rate <= DQ_MAX_NULL_RATE

    def test_null_rate_within_threshold_passes(self, conn):
        # 5 rows, 0 nulls on loan_id = 0% null rate — passes
        rows = [{"loan_id": f"L00{i}", "is_current_flag": True} for i in range(5)]
        _setup_lnd_loan(conn, rows)
        total = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE is_current_flag = TRUE"
        ).fetchone()[0]
        null_count = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE loan_id IS NULL AND is_current_flag = TRUE"
        ).fetchone()[0]
        null_rate = null_count / total
        assert null_rate <= DQ_MAX_NULL_RATE

    def test_high_null_rate_fails(self, conn):
        # 10 rows, 5 with null status = 50% null rate — exceeds 10% threshold
        rows = (
            [{"loan_id": f"L00{i}", "status": "active", "is_current_flag": True}
             for i in range(5)]
            + [{"loan_id": f"L01{i}", "status": None, "is_current_flag": True}
               for i in range(5)]
        )
        _setup_lnd_loan(conn, rows)
        total = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE is_current_flag = TRUE"
        ).fetchone()[0]
        null_count = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE status IS NULL AND is_current_flag = TRUE"
        ).fetchone()[0]
        null_rate = null_count / total
        assert null_rate > DQ_MAX_NULL_RATE

    def test_exactly_at_null_threshold_passes(self, conn):
        # 100 rows, 10 nulls = exactly 10% — passes (threshold is <=)
        rows = (
            [{"loan_id": f"L{i:03d}", "status": "active", "is_current_flag": True}
             for i in range(90)]
            + [{"loan_id": f"N{i:03d}", "status": None, "is_current_flag": True}
               for i in range(10)]
        )
        _setup_lnd_loan(conn, rows)
        total = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE is_current_flag = TRUE"
        ).fetchone()[0]
        null_count = conn.execute(
            "SELECT COUNT(*) FROM lnd_loan WHERE status IS NULL AND is_current_flag = TRUE"
        ).fetchone()[0]
        null_rate = null_count / total
        assert null_rate <= DQ_MAX_NULL_RATE


# ---------------------------------------------------------------------------
# CHECK 4: Referential Integrity
# ---------------------------------------------------------------------------

class TestReferentialIntegrity:

    def _setup_both(self, conn, loan_ids, payment_loan_ids):
        _setup_lnd_loan(conn, [
            {"loan_id": lid, "is_current_flag": True} for lid in loan_ids
        ])
        _setup_lnd_payment(conn, [
            {"payment_id": f"P{i:03d}", "loan_id": lid, "amount": 100}
            for i, lid in enumerate(payment_loan_ids)
        ])

    def test_all_loan_ids_exist_passes(self, conn):
        self._setup_both(conn, ["L001", "L002"], ["L001", "L001", "L002"])
        orphans = conn.execute("""
            SELECT COUNT(*) FROM lnd_payment p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM lnd_loan l WHERE l.loan_id = p.loan_id
              )
        """).fetchone()[0]
        assert orphans == 0

    def test_orphan_loan_id_detected(self, conn):
        self._setup_both(conn, ["L001"], ["L001", "L9999"])  # L9999 is orphan
        orphans = conn.execute("""
            SELECT COUNT(*) FROM lnd_payment p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM lnd_loan l WHERE l.loan_id = p.loan_id
              )
        """).fetchone()[0]
        assert orphans == 1

    def test_null_loan_id_not_counted_as_orphan(self, conn):
        """Payments with null loan_id are not RI violations — they're completeness issues."""
        _setup_lnd_loan(conn, [{"loan_id": "L001", "is_current_flag": True}])
        _setup_lnd_payment(conn, [
            {"payment_id": "P001", "loan_id": None, "amount": 100},
            {"payment_id": "P002", "loan_id": "L001", "amount": 200},
        ])
        orphans = conn.execute("""
            SELECT COUNT(*) FROM lnd_payment p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM lnd_loan l WHERE l.loan_id = p.loan_id
              )
        """).fetchone()[0]
        assert orphans == 0

    def test_multiple_orphans_counted(self, conn):
        self._setup_both(
            conn,
            ["L001"],
            ["L001", "L9998", "L9999"],  # 2 orphans
        )
        orphans = conn.execute("""
            SELECT COUNT(*) FROM lnd_payment p
            WHERE p.loan_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM lnd_loan l WHERE l.loan_id = p.loan_id
              )
        """).fetchone()[0]
        assert orphans == 2


# ---------------------------------------------------------------------------
# CHECK 5: Freshness
# ---------------------------------------------------------------------------

class TestFreshness:

    def test_fresh_data_within_window(self, conn):
        """Batch loaded today → fresh."""
        now = datetime.now(timezone.utc)
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "is_current_flag": True, "ts": now}
        ])
        latest = conn.execute(
            "SELECT MAX(_last_updated_ts) FROM lnd_loan"
        ).fetchone()[0]
        # Should be within last 24 hours
        age_hours = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        assert age_hours < 24

    def test_stale_data_detected(self, conn):
        """Batch loaded 3 days ago → stale for a daily pipeline."""
        stale_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        _setup_lnd_loan(conn, [
            {"loan_id": "L001", "is_current_flag": True, "ts": stale_ts}
        ])
        latest = conn.execute(
            "SELECT MAX(_last_updated_ts) FROM lnd_loan"
        ).fetchone()[0]
        now = datetime.now(timezone.utc)
        age_hours = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        assert age_hours > 24  # stale by any reasonable measure

    def test_empty_table_has_no_freshness(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lnd_loan (
                loan_id VARCHAR, _last_updated_ts TIMESTAMP,
                is_current_flag BOOLEAN
            )
        """)
        latest = conn.execute(
            "SELECT MAX(_last_updated_ts) FROM lnd_loan"
        ).fetchone()[0]
        assert latest is None
