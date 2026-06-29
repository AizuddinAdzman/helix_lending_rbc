"""
tests/unit/test_raw_ingestion.py
----------------------------------
Integration tests for raw ingestion assets using fixture files.

Tests raw_loan and raw_payment against known fixture data.
Uses an in-memory DuckDB instance — no output/helix_fund.db touched.

Fixture summary (loan_fixture.csv):
    10 rows total
    Row 6:  L0000001 duplicate     → still inserted (raw keeps everything)
    Row 7:  L0000006 bad amount    → still inserted (raw = no casting)
    Row 8:  empty loan_id          → still inserted (raw = no validation)
    8 clean rows + 2 problematic = 10 rows total in raw_loan

Fixture summary (payment_fixture.jsonl):
    10 lines total
    Line 6: P000000001 duplicate   → still inserted (raw keeps everything)
    Line 7: P000000006 orphan FK   → still inserted (raw = no RI check)
    Line 8: bad JSON               → goes to err_payment (cannot parse)
    9 parseable + 1 bad = 9 rows raw_payment, 1 row err_payment
"""

import sys
import json
import csv
import duckdb
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"
LOAN_FIXTURE    = FIXTURE_DIR / "loan_fixture.csv"
PAYMENT_FIXTURE = FIXTURE_DIR / "payment_fixture.jsonl"


# ---------------------------------------------------------------------------
# Helpers — replicate raw ingestion logic without Dagster context
# ---------------------------------------------------------------------------

def ingest_raw_loan(conn, source_path: Path, batch_ts: datetime) -> dict:
    """Replicate raw_loan.py ingestion logic against an in-memory DuckDB."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hlx_dev_raw.raw_loan (
            loan_id VARCHAR, customer_id VARCHAR, product_type VARCHAR,
            principal_amount VARCHAR, interest_rate VARCHAR, term_months VARCHAR,
            origination_date VARCHAR, origination_channel VARCHAR,
            status VARCHAR, borrower_info VARCHAR,
            _source_file VARCHAR, _last_updated_ts TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hlx_dev_lnd.lnd_err_loan (
            loan_id VARCHAR, customer_id VARCHAR, product_type VARCHAR,
            principal_amount VARCHAR, interest_rate VARCHAR, term_months VARCHAR,
            origination_date VARCHAR, origination_channel VARCHAR,
            status VARCHAR, borrower_info VARCHAR,
            _source_file VARCHAR, _last_updated_ts TIMESTAMP,
            _rejection_reason VARCHAR, _rejected_at TIMESTAMP
        )
    """)

    source_file = source_path.name
    rows_in = rows_inserted = rows_rejected = 0
    good_rows = []

    with open(source_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows_in += 1
            good_rows.append([
                row.get("loan_id") or None,
                row.get("customer_id") or None,
                row.get("product_type") or None,
                row.get("principal_amount") or None,
                row.get("interest_rate") or None,
                row.get("term_months") or None,
                row.get("origination_date") or None,
                row.get("origination_channel") or None,
                row.get("status") or None,
                row.get("borrower_info") or None,
                source_file,
                batch_ts,
            ])

    if good_rows:
        conn.executemany(
            "INSERT INTO hlx_dev_raw.raw_loan VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            good_rows,
        )
        rows_inserted = len(good_rows)

    return {"rows_in": rows_in, "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected}


def ingest_raw_payment(conn, source_path: Path, batch_ts: datetime) -> dict:
    """Replicate raw_payment.py ingestion logic against an in-memory DuckDB."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hlx_dev_raw.raw_payment (
            payment_id VARCHAR, loan_id VARCHAR, amount VARCHAR,
            payment_timestamp VARCHAR, payment_method_type VARCHAR,
            payment_method_last_four VARCHAR, payment_method_bank VARCHAR,
            metadata_source VARCHAR, metadata_user_agent VARCHAR,
            _source_file VARCHAR, _last_updated_ts TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hlx_dev_lnd.lnd_err_payment (
            payment_id VARCHAR, loan_id VARCHAR, amount VARCHAR,
            payment_timestamp VARCHAR, payment_method_type VARCHAR,
            payment_method_last_four VARCHAR, payment_method_bank VARCHAR,
            metadata_source VARCHAR, metadata_user_agent VARCHAR,
            _source_file VARCHAR, _last_updated_ts TIMESTAMP,
            _rejection_reason VARCHAR, _rejected_at TIMESTAMP
        )
    """)

    source_file = source_path.name
    rows_in = rows_inserted = rows_rejected = 0
    good_rows = []
    bad_rows = []

    with open(source_path, encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            rows_in += 1
            try:
                r       = json.loads(line)
                pm      = r.get("payment_method") or {}
                details = pm.get("details") or {}
                meta    = r.get("metadata") or {}
                good_rows.append([
                    str(r.get("payment_id")) if r.get("payment_id") else None,
                    str(r.get("loan_id")) if r.get("loan_id") else None,
                    str(r.get("amount")) if r.get("amount") is not None else None,
                    str(r.get("timestamp")) if r.get("timestamp") else None,
                    str(pm.get("type")) if pm.get("type") else None,
                    str(details.get("last_four")) if details.get("last_four") else None,
                    str(details.get("bank")) if details.get("bank") else None,
                    str(meta.get("source")) if meta.get("source") else None,
                    str(meta.get("user_agent")) if meta.get("user_agent") else None,
                    source_file, batch_ts,
                ])
            except Exception as e:
                rows_rejected += 1
                bad_rows.append([
                    None, None, None, None, None, None, None, None, None,
                    source_file, batch_ts,
                    f"Line {line_num}: {e}",
                    datetime.now(timezone.utc),
                ])

    if good_rows:
        conn.executemany(
            "INSERT INTO hlx_dev_raw.raw_payment VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            good_rows,
        )
        rows_inserted = len(good_rows)

    if bad_rows:
        conn.executemany(
            "INSERT INTO hlx_dev_lnd.lnd_err_payment VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            bad_rows,
        )

    return {"rows_in": rows_in, "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """Fresh in-memory DuckDB per test."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA IF NOT EXISTS hlx_dev_raw")
    c.execute("CREATE SCHEMA IF NOT EXISTS hlx_dev_lnd")
    yield c
    c.close()


@pytest.fixture
def batch_ts():
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# raw_loan tests
# ---------------------------------------------------------------------------

class TestRawLoanIngestion:

    def test_all_rows_inserted_including_dirty(self, conn, batch_ts):
        """Raw layer inserts everything — no validation, no rejection."""
        result = ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        total = conn.execute("SELECT COUNT(*) FROM hlx_dev_raw.raw_loan").fetchone()[0]
        assert total == 10        # all 10 rows including duplicate + bad amount

    def test_rows_in_matches_csv_line_count(self, conn, batch_ts):
        result = ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        assert result["rows_in"] == 10

    def test_zero_rejections_at_raw_layer(self, conn, batch_ts):
        """Raw layer never rejects — even bad rows go in as VARCHAR."""
        result = ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        assert result["rows_rejected"] == 0

    def test_duplicate_row_preserved(self, conn, batch_ts):
        """Duplicate loan_id L0000001 appears twice in raw — both kept."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        count = conn.execute(
            "SELECT COUNT(*) FROM hlx_dev_raw.raw_loan WHERE loan_id = 'L0000001'"
        ).fetchone()[0]
        assert count == 2

    def test_bad_amount_preserved_as_varchar(self, conn, batch_ts):
        """not_a_number is stored as-is — raw never casts."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        row = conn.execute(
            "SELECT principal_amount FROM hlx_dev_raw.raw_loan WHERE loan_id = 'L0000006'"
        ).fetchone()
        assert row is not None
        assert row[0] == "not_a_number"

    def test_empty_loan_id_preserved(self, conn, batch_ts):
        """Row with empty loan_id is stored with NULL loan_id in raw."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        count = conn.execute(
            "SELECT COUNT(*) FROM hlx_dev_raw.raw_loan WHERE loan_id IS NULL"
        ).fetchone()[0]
        assert count == 1

    def test_currency_amount_preserved_as_varchar(self, conn, batch_ts):
        """$8,500.00 stays as string at raw layer."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        row = conn.execute(
            "SELECT principal_amount FROM hlx_dev_raw.raw_loan WHERE loan_id = 'L0000004'"
        ).fetchone()
        assert row[0] == "$8,500.00"

    def test_mixed_case_product_type_preserved(self, conn, batch_ts):
        """MORTGAGE, Student, AUTO all stored as-is at raw layer."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        types = set(r[0] for r in conn.execute(
            "SELECT DISTINCT product_type FROM hlx_dev_raw.raw_loan"
        ).fetchall())
        assert "MORTGAGE" in types
        assert "Student" in types
        assert "AUTO" in types

    def test_audit_columns_populated(self, conn, batch_ts):
        """Every row has _source_file and _last_updated_ts."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        nulls = conn.execute(
            """SELECT COUNT(*) FROM hlx_dev_raw.raw_loan
               WHERE _source_file IS NULL OR _last_updated_ts IS NULL"""
        ).fetchone()[0]
        assert nulls == 0

    def test_source_file_is_filename_with_extension(self, conn, batch_ts):
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        fname = conn.execute(
            "SELECT DISTINCT _source_file FROM hlx_dev_raw.raw_loan"
        ).fetchone()[0]
        assert fname == "loan_fixture.csv"

    def test_batch_ts_stored_correctly(self, conn, batch_ts):
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        stored = conn.execute(
            "SELECT DISTINCT _last_updated_ts FROM hlx_dev_raw.raw_loan"
        ).fetchone()[0]
        # DuckDB returns datetime — compare date portion
        assert stored.year == 2024
        assert stored.month == 1
        assert stored.day == 15

    def test_append_second_batch_accumulates(self, conn, batch_ts):
        """Second run appends — does not truncate."""
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts)
        batch_ts2 = datetime(2024, 1, 16, 0, 0, 0, tzinfo=timezone.utc)
        ingest_raw_loan(conn, LOAN_FIXTURE, batch_ts2)
        total = conn.execute("SELECT COUNT(*) FROM hlx_dev_raw.raw_loan").fetchone()[0]
        assert total == 20   # 10 + 10


# ---------------------------------------------------------------------------
# raw_payment tests
# ---------------------------------------------------------------------------

class TestRawPaymentIngestion:

    def test_good_rows_inserted(self, conn, batch_ts):
        """9 parseable lines inserted, 1 bad JSON line rejected."""
        result = ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        total = conn.execute("SELECT COUNT(*) FROM hlx_dev_raw.raw_payment").fetchone()[0]
        assert total == 9

    def test_bad_json_goes_to_err_payment(self, conn, batch_ts):
        """Unparseable JSON line → err_payment."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        err_count = conn.execute(
            "SELECT COUNT(*) FROM hlx_dev_lnd.lnd_err_payment"
        ).fetchone()[0]
        assert err_count == 1

    def test_bad_json_rejection_reason_populated(self, conn, batch_ts):
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        reason = conn.execute(
            "SELECT _rejection_reason FROM hlx_dev_lnd.lnd_err_payment LIMIT 1"
        ).fetchone()[0]
        assert reason is not None
        assert len(reason) > 0

    def test_duplicate_payment_preserved_at_raw(self, conn, batch_ts):
        """P000000001 appears twice in fixture — both kept at raw layer."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        count = conn.execute(
            "SELECT COUNT(*) FROM hlx_dev_raw.raw_payment WHERE payment_id = 'P000000001'"
        ).fetchone()[0]
        assert count == 2

    def test_orphan_fk_preserved_at_raw(self, conn, batch_ts):
        """P000000006 references L9999999 which doesn't exist — raw keeps it."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        row = conn.execute(
            "SELECT loan_id FROM hlx_dev_raw.raw_payment WHERE payment_id = 'P000000006'"
        ).fetchone()
        assert row is not None
        assert row[0] == "L9999999"

    def test_missing_metadata_stored_as_null(self, conn, batch_ts):
        """P000000004 has no metadata block — metadata fields → NULL."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        row = conn.execute(
            """SELECT metadata_source, metadata_user_agent
               FROM hlx_dev_raw.raw_payment WHERE payment_id = 'P000000004'"""
        ).fetchone()
        assert row is not None
        assert row[0] is None
        assert row[1] is None

    def test_amount_stored_as_varchar(self, conn, batch_ts):
        """Amount is stored as string at raw layer — no casting."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        row = conn.execute(
            "SELECT amount FROM hlx_dev_raw.raw_payment WHERE payment_id = 'P000000001'"
            " LIMIT 1"
        ).fetchone()
        assert isinstance(row[0], str)
        assert row[0] == "856.07"

    def test_timestamp_stored_as_raw_string(self, conn, batch_ts):
        """Timestamps kept as original strings — no UTC conversion at raw."""
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        row = conn.execute(
            """SELECT payment_timestamp FROM hlx_dev_raw.raw_payment
               WHERE payment_id = 'P000000001' LIMIT 1"""
        ).fetchone()
        assert "Z" in row[0] or "+" in row[0] or "-" in row[0]

    def test_audit_columns_populated(self, conn, batch_ts):
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        nulls = conn.execute(
            """SELECT COUNT(*) FROM hlx_dev_raw.raw_payment
               WHERE _source_file IS NULL OR _last_updated_ts IS NULL"""
        ).fetchone()[0]
        assert nulls == 0

    def test_source_file_is_filename_with_extension(self, conn, batch_ts):
        ingest_raw_payment(conn, PAYMENT_FIXTURE, batch_ts)
        fname = conn.execute(
            "SELECT DISTINCT _source_file FROM hlx_dev_raw.raw_payment"
        ).fetchone()[0]
        assert fname == "payment_fixture.jsonl"
