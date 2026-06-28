# Helix Lending — Data Pipeline

A production-quality data pipeline ingesting loan origination and payment event data into a modelled DuckDB star schema, with full DQ validation, structured observability, and Dagster orchestration.

---

## How to Run

### Prerequisites

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

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

### Run tests

```bash
pytest tests/                           # full suite
pytest tests/unit/                      # unit tests only
pytest tests/e2e/                       # end-to-end test only
pytest tests/ -v --tb=short            # verbose with short tracebacks
```

---

## Architecture

### Layer Design

```
loan.csv / payment.jsonl
        │
        ▼
   raw_loan / raw_payment          All VARCHAR, append per batch, full forensic trail
        │
        ▼
   lnd_loan / lnd_payment          Typed, cleaned, SCD2 (loans), deduped (payments)
        │
        ▼ (DQ gates — both must pass)
   dq_lnd_loan / dq_lnd_payment   Uniqueness, completeness, RI, volume, freshness
        │
        ▼
   stg_loan_payment                Joined, EMI derived, delinquency + anomaly flags
        │
   ┌────┴────┐
   ▼         ▼
dim_customer  dim_date             Borrower JSON flattened; 80-year date spine
   └────┬────┘
        ▼
   fct_loan / fct_payment          Grain: one row per loan / one per payment event
        │
   ┌────┴────────────┐
   ▼                 ▼             ▼
mart_delinquency  mart_payment_anomaly  mart_data_observability
```

### Why Star Schema

Star schema was chosen over one-big-table (OBT) for three reasons:

1. **Grain conflict.** Loans (one per origination) and payments (many per loan) have fundamentally different grains. Collapsing them into OBT requires either fan-out (duplicate loan rows per payment) or aggregation loss (losing payment-level detail). Star schema keeps each grain clean.

2. **Query performance.** `mart_delinquency` only needs `fct_loan`. `mart_payment_anomaly` only needs `fct_payment`. Neither needs to scan a combined wide table.

3. **Extensibility.** New fact tables (e.g. `fct_collections`, `fct_disbursement`) can be added without restructuring existing tables. OBT would require schema changes.

### Why Dagster over Airflow

Dagster was chosen because this pipeline is fundamentally asset-producing — each layer materialises a named data asset with known lineage. Dagster's asset model maps directly to this design:

- Asset-level DQ checks are first-class (`dq_lnd_loan`, `dq_lnd_payment` as explicit gate assets)
- Built-in run metadata (rows in/out, duration) reduces custom instrumentation
- Asset lineage graph is visible in the UI without additional tooling
- `define_asset_job` + `ScheduleDefinition` gives daily scheduling in ~5 lines

Airflow remains the safer enterprise choice for teams already running it. This is noted as a known portability limitation.

### Why Custom DQ Framework over Great Expectations

A custom SQL-native DQ layer was chosen over Great Expectations for this scope:

- DuckDB SQL expresses all four DQ dimensions (freshness, completeness, uniqueness, RI) natively
- No additional runtime dependency or configuration overhead
- Results written to `dq_results` table — queryable, persistable, auditable
- Each check is a single SQL assertion with a documented threshold
- Great Expectations adds significant value at scale (HTML reports, suite management) — documented as a "with more time" improvement

---

## Data Quality Checks

| Dimension | Check | Threshold | Tables |
|---|---|---|---|
| Volume | Acceptance rate (rows inserted / rows in file) | ≥ 99% | `lnd_loan`, `lnd_payment` |
| Completeness | Null rate per critical column | ≤ 10% | `lnd_loan`, `lnd_payment` |
| Uniqueness | Duplicate primary keys | 0 duplicates | `loan_id`, `payment_id` |
| Referential integrity | Orphan `loan_id` in payments | 0 orphans | `lnd_payment → lnd_loan` |
| Freshness | Hours since latest batch | < 24h (daily pipeline) | `raw_loan`, `raw_payment` |
| Validity | Categorical values in known sets | Warning only | `product_type`, `status`, `payment_method_type` |

DQ results are written to `dq_results` table and summarised in `mart_data_observability`.

---

## Undocumented Column Decisions

These decisions were made where the source data dictionary was silent:

| Decision | Choice | Rationale |
|---|---|---|
| Expected monthly payment | Standard amortisation EMI: `P × r(1+r)^n / ((1+r)^n − 1)` | Industry standard for fixed-rate consumer loans |
| Delinquency definition | No payment recorded within 30 days past expected due date | Matches standard 30-day DPD definition used in consumer lending |
| Null handling in `borrower_info` | Missing JSON fields → NULL in `dim_customer` | Preserves row, surfaces gap in DQ completeness check |
| Duplicate loan resolution | Latest `_last_updated_ts` wins for `is_current` (SCD2) | Most recent snapshot is the authoritative state |
| Nullable columns | All non-key columns are nullable at landing; documented per table in `dq_results` | Conservative — reject less, surface issues in DQ |
| Categorical casing | Lowercase canonical throughout (`personal`, `active`, `ach`) | Prevents case-sensitive grouping errors in mart aggregations |
| Ambiguous slash dates (`01/05/2022`) | US format assumed: MM/DD/YYYY (`dayfirst=False`) | Helix Lending is a US platform; documented limitation if source is European |
| Payment anomaly tolerance | 10% deviation from EMI | Accounts for partial payments, rounding differences, early payoff scenarios |
| `dim_date` spine range | today − 40 years to today + 40 years (80 years, self-updating) | Covers all historical originations and max loan horizon (360 months = 30 years) |

---

## Observability

Every pipeline run emits structured JSON logs to:
- **stdout** — captured by Dagster UI
- **`output/logs/pipeline.log`** — persistent file log

Every log record includes:

```json
{
  "timestamp": "2024-01-15T02:00:04.123Z",
  "level": "INFO",
  "event": "load_end",
  "layer": "raw",
  "table": "raw_loan",
  "rows_in": 10000,
  "rows_out": 9987,
  "rows_rejected": 13,
  "duration_sec": 4.231,
  "source_file": "loan_20240115.csv",
  "batch_date": "2024-01-15"
}
```

Checkpoint events: `load_start`, `load_end`, `row_rejected`, `dq_pass`, `dq_fail`, `checkpoint`, `pipeline_fail`.

Run metrics are also written to `mart_data_observability` for SQL-queryable run history.

---

## Data Lineage

```
loan.csv          → raw_loan → lnd_loan → stg_loan_payment → fct_loan → mart_delinquency
                                        ↘                  ↗
payment.jsonl → raw_payment → lnd_payment → stg_loan_payment → fct_payment → mart_payment_anomaly
                                                                           ↘
                                                          dq_results → mart_data_observability
                                                                     ↗
                                              lnd_loan (borrower_info) → dim_customer
                                              config.get_dim_date_bounds() → dim_date
```

---

## Known Limitations

1. **Static source files.** The pipeline is designed for daily-file delivery but currently reads from fixed paths (`data/loan.csv`, `data/payment.jsonl`). Date-suffixed file routing (`loan_YYYYMMDD.csv`) would require a file resolver in `config.py`.

2. **Date ambiguity.** Slash-formatted dates (`01/05/2022`) are parsed as US MM/DD/YYYY. If the source system uses European DD/MM/YYYY, dates would be silently wrong for values where day ≤ 12.

3. **No Dagster partitions.** The daily schedule uses a single job rather than Dagster partitioned assets. Backfilling multiple dates requires manual `--date` invocations rather than a single partition backfill command.

4. **DuckDB single-writer.** DuckDB supports only one write connection at a time. Dagster's parallel asset execution is sequential in practice for this pipeline. True parallelism would require a multi-writer database.

5. **SCD2 for payments not implemented.** Payment events are treated as immutable. If a payment record is ever corrected at source, the correction would not be reflected without a full reload.

6. **No email/alerting on DQ breach.** DQ failures raise exceptions in Dagster (visible in UI) but do not send external notifications. Adding a Slack or email alert on `dq_fail` events is straightforward with Dagster sensors.

---

## What I Would Do With More Time

1. **Dagster partitioned assets** for date-based incremental runs and clean backfill UX
2. **Great Expectations suite** for richer DQ reporting (HTML reports, expectation suites per table)
3. **dbt for transformation layer** — SQL-native, version-controlled, auto-documented lineage
4. **Parquet export** of mart tables to `output/` for downstream consumption
5. **Docker Compose** setup for reproducible local environment
6. **CI/CD** — GitHub Actions running `pytest` on every push
7. **Alerting** — Dagster sensor triggering Slack notification on DQ breach or pipeline failure

---

## Project Structure

```
helix_lending_rbc/
├── data/
│   ├── loan.csv
│   └── payment.jsonl
├── src/
│   ├── config.py
│   ├── definitions.py          Dagster entry point
│   ├── pipeline.py             CLI runner
│   ├── assets/
│   │   ├── ingestion/          raw_loan, raw_payment
│   │   ├── landing/            lnd_loan, lnd_payment
│   │   ├── dq/                 dq_lnd_loan, dq_lnd_payment
│   │   ├── transformation/     stg, dim_customer, dim_date, fct_loan, fct_payment
│   │   └── marts/              mart_delinquency, mart_payment_anomaly, mart_observability
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
│   ├── unit/                   115+ unit tests across all utils and layers
│   └── e2e/                    Full pipeline E2E test on fixtures
├── output/
│   ├── helix_fund.db
│   └── logs/pipeline.log
├── requirements.txt
├── README.md
└── AI_LOG.md
```
