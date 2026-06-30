# Helix Lending — Data Pipeline

A production-quality data pipeline ingesting loan origination and payment event data into a modelled DuckDB star schema, with full DQ validation, structured observability, and Dagster orchestration.

---

## How to Run

### Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate          # macOS/Linux
.venv\Scripts\Activate.ps1         # Windows PowerShell

pip install -r requirements.txt
```

### Initialise the database

```bash
python src/init_db.py
```

Creates 6 schemas and 16 tables in `output/helix_dev.db`. Safe to re-run — uses `CREATE SCHEMA IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`.

> **Note on schema changes during development:** `CREATE TABLE IF NOT EXISTS` only creates a table if it's missing — it never alters an existing table. If you pull updated code that changes a table's columns, delete `output/helix_dev.db` and re-run `init_db.py` before materialising in Dagster, or you will hit `BinderException` column-count errors.

### Place source files

```
data/loan.csv
data/payment.jsonl
```

### Run via Dagster UI (recommended)

```bash
cd src
dagster dev -f definitions.py
# Open http://localhost:3000
# Click "Materialize All" to run the full pipeline
```

### Run via CLI

```bash
python src/pipeline.py                  # today's date (UTC)
python src/pipeline.py --date 20240115  # specific date backfill
```

### Switching environments

Schema names and the database filename are both driven by `HELIX_ENV` (default `dev`):

```bash
HELIX_ENV=prd python src/init_db.py
HELIX_ENV=prd dagster dev -f definitions.py
```

This builds `helix_prd.db` with `hlx_prd_*` schemas — same code, different environment, zero hardcoding.

### Run tests

```bash
pytest tests/                  # full suite — 295 tests
pytest tests/unit/             # unit tests only
pytest tests/e2e/              # end-to-end test only
pytest tests/ -v --tb=short
```

---

## Architecture

### Schema Design — One Schema Per Layer, Per Environment

```
helix_{env}.db
├── hlx_{env}_raw    raw_loan, raw_payment, raw_audit
├── hlx_{env}_lnd    lnd_loan, lnd_payment, lnd_err_loan, lnd_err_payment, lnd_dq_audit
├── hlx_{env}_stg    stg_loan_payment
├── hlx_{env}_dim    dim_customer, dim_date
├── hlx_{env}_fct    fct_loan, fct_payment
└── hlx_{env}_mart   mart_delinquency, mart_payment_anomaly, mart_data_observability
```

The environment is embedded directly in the schema name (not just the database filename) so that a query against `hlx_prd_raw.raw_loan` is unambiguous even if someone is connected to the wrong database file — a deliberate safety net against cross-environment mistakes.

### Layer Responsibilities

```
loan.csv / payment.jsonl
        │
        ▼
   raw_loan / raw_payment          Exact file replica, all VARCHAR, append per batch
   raw_audit                       Per-batch duplicate/quality stats (see below)
        │
        ▼
   lnd_loan / lnd_payment          Typed, cleaned, SCD2 (loans), deduped (payments)
   lnd_err_loan / lnd_err_payment  Rows that failed cleaning, with rejection reason
   lnd_dq_audit                    Every DQ check outcome, every run
        │
        ▼ (DQ gates — both must pass before staging proceeds)
   dq_lnd_loan / dq_lnd_payment
        │
        ▼
   stg_loan_payment                Joined, EMI derived, delinquency + anomaly flags,
                                    loan_balance_type, payment_allocation_status
        │
   ┌────┴────┐
   ▼         ▼
dim_customer  dim_date             Borrower JSON flattened; 80-year self-updating date spine
   └────┬────┘
        ▼
   fct_loan / fct_payment          Grain: one row per loan / one per payment event
        │
   ┌────┴────────────┐
   ▼                 ▼             ▼
mart_delinquency  mart_payment_anomaly  mart_data_observability
```

### Execution Order — Fully Sequential

```
raw_loan → raw_payment → lnd_loan → lnd_payment
→ dq_lnd_loan → dq_lnd_payment → stg_loan_payment
→ dim_customer → dim_date → fct_loan → fct_payment
→ mart_delinquency → mart_payment_anomaly → mart_data_observability
```

This was a deliberate departure from the original parallel design. DuckDB is single-writer — two assets attempting to open the database simultaneously throws `IOException: Could not set lock on file`. Rather than work around this with retries or connection pooling, every asset declares an explicit `deps=[...]` on the previous asset, making the constraint visible in the DAG itself instead of hidden behind error-handling code.

### Why Star Schema Over One-Big-Table

Star schema was chosen over OBT for three reasons:

1. **Grain conflict.** Loans (one row per origination) and payments (many rows per loan) have fundamentally different grains. Collapsing them into OBT requires either fan-out (duplicate loan rows per payment) or aggregation loss. Star schema keeps each grain clean.
2. **Query performance.** `mart_delinquency` only needs `fct_loan`. `mart_payment_anomaly` only needs `fct_payment`. Neither scans a combined wide table.
3. **Extensibility.** New fact tables can be added without restructuring existing ones.

### Why Tables, Not Views, for Marts

All mart tables are materialised tables, not views, for five reasons:

1. **Query performance** — pre-aggregated once per run rather than recomputed on every read
2. **Point-in-time snapshots** — `run_date` is preserved, enabling historical trend queries
3. **Decoupling from upstream rebuilds** — a stable snapshot survives mid-pipeline fact table rebuilds
4. **Observability** — row counts and durations are concrete, loggable events
5. **DuckDB's single-writer constraint** — a view querying live fact tables while the pipeline writes to them would block or error

### Why Dagster Over Airflow

Dagster was chosen because this pipeline is fundamentally asset-producing — each layer materialises a named data asset with known lineage. Dagster's asset model maps directly to this design: DQ checks are first-class assets (`dq_lnd_loan`, `dq_lnd_payment`) acting as explicit gates, built-in run metadata reduces custom instrumentation, and asset lineage is visible in the UI without additional tooling.

Airflow remains the safer enterprise choice for teams already running it — noted as a known portability limitation.

### Why a Custom DQ Framework Over Great Expectations

A custom SQL-native DQ layer was chosen for this scope: DuckDB SQL expresses all required DQ dimensions natively, with no additional runtime dependency. Every check writes a row to `lnd_dq_audit` — queryable, persistable, auditable. Great Expectations would add real value at scale (HTML reports, suite management) — documented below as a future improvement.

---

## Key Design Decisions

### Negative Principal Amount → Credit Balance, Not an Error

**Original design:** any negative `principal_amount` was rejected to `lnd_err_loan`.

**Revised design:** negative principal is accepted as valid. In lending, a negative balance represents legitimate business events — overpayment credits, escrow surplus refunds, lender corrections, or accounting timing differences between a final payment clearing and final interest posting. Rejecting these as errors would silently discard real data.

`stg_loan_payment` derives `loan_balance_type`:

| Condition | `loan_balance_type` |
|---|---|
| `principal_amount < 0` | `credit_balance` |
| `principal_amount = 0` | `zero_balance` |
| `principal_amount > 0` | `debit_balance` |

Downstream impact:
- `mart_delinquency` filters to `debit_balance` only — a credit balance loan cannot be delinquent by definition (the lender owes the customer, not the reverse)
- `mart_payment_anomaly` excludes `credit_balance` loans — EMI comparison is undefined for negative principal
- Only genuinely unparseable values (`not_a_number`, `N/A`, `TBD`, malformed decimals) are still rejected to `lnd_err_loan`

### Payment Allocation Status — Not Every Unmatched Payment Is a Referential Integrity Violation

**Original design:** any `lnd_payment.loan_id` not found in `lnd_loan` was treated as a hard DQ breach, halting the pipeline.

**Investigation finding:** 100 payments (0.13% of 74,756) referenced `loan_id`s numerically outside the entire loan universe (`loans.csv` contains `L0000001`–`L0010000`; the orphan payments referenced IDs like `L0100604`–`L0991391`). These are not data corruption or join bugs — they are payments for loans that were never originated in this dataset, a realistic scenario for pre-loan deposits, cross-system originations, or pipeline timing lag.

**Revised design:** `lnd_payment` derives `payment_allocation_status`:

| Status | Meaning |
|---|---|
| `allocated` | Loan exists and is clean in `lnd_loan` — fully reportable |
| `unallocated` | `loan_id` not found anywhere in `raw_loan` — loan was never originated in this dataset |
| `loan_rejected` | `loan_id` exists in `raw_loan` but failed `lnd_loan` cleaning — investigate `lnd_err_loan` |
| `unidentified` | No `loan_id` present in the source record at all |

The DQ gate (`dq_lnd_payment`) no longer halts the pipeline on this condition. It writes an `allocation_status_summary` INFO record to `lnd_dq_audit` with the full breakdown, and the counts are surfaced per-batch in `mart_data_observability`. Unallocated and rejected-loan payments are naturally excluded from `stg_loan_payment` via the `LEFT JOIN` from `lnd_loan` — they never silently appear in delinquency or anomaly reporting, but the pipeline does not halt processing the other 99.87% of valid records over them.

### Payment Uniqueness — Composite Key, Not `payment_id` Alone

**Investigation finding:** the same `payment_id` can legitimately appear more than once with a different `amount` — split settlements (multiple payment methods for one transaction), fee deductions (gross vs net amount), or partial refunds. Deduplicating on `payment_id` alone would silently discard one leg of a real transaction.

**Revised design:** uniqueness is defined as the composite key `(payment_id, amount, payment_timestamp)`. `lnd_payment` carries a surrogate key `lnd_payment_sk` for downstream joins, since `payment_id` is no longer guaranteed unique. The DQ check `uniqueness_payment_id_amount_timestamp` only flags true duplicates (identical id, amount, and timestamp); a separate INFO check (`info_split_settlement_payment_ids`) counts — but does not flag — payment_ids with multiple distinct amounts, for visibility without false alarms.

### `raw_audit` — Per-Batch Source Quality Metrics

A dedicated table in `hlx_{env}_raw` captures, per batch, per source file: total rows read, rows inserted, distinct business keys, duplicate key count, true duplicate count, and (for payments) the count of `payment_id`s with differing amounts. This gives a permanent, queryable record of source data quality at the file level, independent of any downstream cleaning decisions.

---

## Data Quality Checks

| Dimension | Check | Threshold | Tables |
|---|---|---|---|
| Volume | Acceptance rate (rows inserted / rows in file) | ≥ 99% | `lnd_loan`, `lnd_payment` |
| Completeness | Null rate per critical column | ≤ 10% | `lnd_loan`, `lnd_payment` |
| Uniqueness | Duplicate `loan_id` (current rows) | 0 duplicates | `lnd_loan` |
| Uniqueness | Duplicate `(payment_id, amount, payment_timestamp)` | 0 duplicates | `lnd_payment` |
| Validity | Categorical values in known sets | Warning only | `product_type`, `status`, `payment_method_type` |
| Allocation | Payment-to-loan traceability breakdown | Informational | `lnd_payment` |
| Freshness | Hours since latest batch | Surfaced in observability | `raw_loan`, `raw_payment` |

All results are written to `hlx_{env}_lnd.lnd_dq_audit` and summarised in `hlx_{env}_mart.mart_data_observability`.

---

## Undocumented Column Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Expected monthly payment | Standard amortisation EMI: `P × r(1+r)^n / ((1+r)^n − 1)` | Industry standard for fixed-rate consumer loans |
| Delinquency definition | No payment recorded within 30 days past expected due date | Matches standard 30-day DPD convention |
| Negative principal | Accepted as `credit_balance`, not rejected | See Key Design Decisions above |
| Payment-to-loan mismatch | Categorised via `payment_allocation_status`, not a hard breach | See Key Design Decisions above |
| Payment uniqueness | Composite key `(payment_id, amount, payment_timestamp)` | Same `payment_id` can have multiple legitimate amounts |
| Null handling in `borrower_info` | Missing JSON fields → NULL in `dim_customer` | Preserves row, surfaces gap in DQ completeness check |
| Duplicate loan resolution | Latest `_last_updated_ts` wins for `is_current` (SCD2) | Most recent snapshot is authoritative |
| Categorical casing | Lowercase canonical throughout (`personal`, `active`, `ach`) | Prevents case-sensitive grouping errors in marts |
| Ambiguous slash dates (`01/05/2022`) | US format assumed: MM/DD/YYYY | Helix Lending is a US platform; documented limitation if source is European |
| Payment anomaly tolerance | 10% deviation from EMI | Accounts for partial payments, rounding, early payoff |
| `dim_date` spine range | today − 40 years to today + 40 years (80 years, self-updating) | Covers all historical originations and max loan horizon (360 months = 30 years); requires no maintenance |

---

## Observability

Every pipeline run emits structured JSON logs to stdout (captured by Dagster UI) and `output/logs/pipeline_{env}.log`.

```json
{
  "timestamp": "2024-01-15T02:00:04.123Z",
  "level": "INFO",
  "event": "load_end",
  "layer": "raw",
  "table": "hlx_dev_raw.raw_loan",
  "rows_in": 10000,
  "rows_out": 9987,
  "rows_rejected": 13,
  "duration_sec": 4.231,
  "source_file": "loan.csv",
  "batch_date": "2024-01-15"
}
```

Checkpoint events: `load_start`, `load_end`, `row_rejected`, `dq_pass`, `dq_fail`, `checkpoint`, `pipeline_fail`.

Run metrics are also written to `mart_data_observability` for SQL-queryable run history, including the per-batch payment allocation breakdown and raw-layer duplicate statistics.

---

## Data Lineage

```
loan.csv     → raw_loan → lnd_loan → stg_loan_payment → fct_loan → mart_delinquency
                                   ↘                  ↗
payment.jsonl → raw_payment → lnd_payment → stg_loan_payment → fct_payment → mart_payment_anomaly
                                                                           ↘
                                                          lnd_dq_audit → mart_data_observability
                                                                       ↗
                                              lnd_loan (borrower_info) → dim_customer
                                              config.get_dim_date_bounds() → dim_date
```

---

## Known Limitations

1. **Static source files.** The pipeline reads from fixed paths (`data/loan.csv`, `data/payment.jsonl`). Date-suffixed file routing (`loan_YYYYMMDD.csv`) would require a file resolver in `config.py`.
2. **Date ambiguity.** Slash-formatted dates (`01/05/2022`) are parsed as US MM/DD/YYYY. European-format sources would silently produce wrong dates for day-of-month ≤ 12.
3. **No Dagster partitions.** The daily schedule uses a single job rather than partitioned assets. Backfilling multiple dates requires manual `--date` invocations.
4. **DuckDB single-writer.** All asset execution is sequential as a result. A multi-writer database (Postgres) would be required for true parallelism.
5. **SCD2 for payments not implemented.** Payment events are treated as immutable except for the composite-key dedup described above. A genuine correction to a historical payment would require a new record, not an update.
6. **No automated re-initialisation on schema change.** `CREATE TABLE IF NOT EXISTS` does not alter existing tables. Developers must manually delete and re-run `init_db.py` after a DDL change — documented above, but not automated.
7. **No email/alerting on DQ breach.** Failures are visible in the Dagster UI and `lnd_dq_audit` but do not push external notifications.

---

## What I Would Do With More Time

1. **Dagster partitioned assets** for date-based incremental runs and clean backfill UX
2. **Automated DDL migration** — a lightweight schema-diff tool to alter existing tables instead of requiring manual DB deletion during development
3. **Great Expectations suite** for richer DQ reporting
4. **dbt for the transformation layer** — SQL-native, version-controlled, auto-documented lineage
5. **`mart_unallocated_payments`** — a dedicated view for the finance team to investigate and manually reconcile `unallocated` and `loan_rejected` payments
6. **CI/CD** — GitHub Actions running `pytest` on every push, plus a DDL-consistency check between `init_db.py` and asset files (the recurring class of bug we hit during development)
7. **Alerting** — Dagster sensor triggering a notification on DQ breach or pipeline failure

---

## Project Structure

```
helix_lending_rbc/
├── data/
│   ├── loan.csv
│   └── payment.jsonl
├── src/
│   ├── config.py                ENV-driven schema/table constants
│   ├── definitions.py           Dagster entry point, sequential deps
│   ├── init_db.py               Schema + table creation, idempotent
│   ├── pipeline.py               CLI runner
│   ├── assets/
│   │   ├── ingestion/           raw_loan, raw_payment
│   │   ├── landing/              lnd_loan, lnd_payment
│   │   ├── dq/                   dq_lnd_loan, dq_lnd_payment
│   │   ├── transformation/       stg, dim_customer, dim_date, fct_loan, fct_payment
│   │   └── marts/                mart_delinquency, mart_payment_anomaly, mart_observability
│   ├── resources/
│   │   └── duckdb_resource.py
│   └── utils/
│       ├── cleaners.py
│       ├── emi.py
│       └── logger.py
├── tests/
│   ├── fixtures/
│   │   ├── loan_fixture.csv
│   │   └── payment_fixture.jsonl
│   ├── unit/                     10 files, covering all utils and asset layers
│   └── e2e/                      Full pipeline E2E test on fixtures
├── output/
│   ├── helix_dev.db
│   └── logs/pipeline_dev.log
├── requirements.txt
├── README.md
└── AI_LOG.md
