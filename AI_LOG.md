# AI Collaboration Log — Helix Lending Pipeline

This log documents the collaboration between Aizuddin (engineer) and Claude (AI assistant) in designing and building the Helix Lending data pipeline.

---

## Collaboration Summary

**Total design sessions:** 1 extended session  
**Total codegen sessions:** 1 extended session  
**Approach:** Design-first, zero-code until architecture was fully locked and agreed

---

## Phase 1 — Design (Human-led, AI-assisted)

### What the human drove

- Initial requirements: ingest CSV + JSONL, store in DuckDB, answer 3 business questions
- Proposed adding a **raw layer** before landing (AI had proposed landing-first)
- Proposed **flattening JSONL to per-column VARCHAR** at raw layer (AI had proposed single raw_record column)
- Renamed `loaded_at` → `_last_updated_ts` (UTC) for clarity
- Chose `helix_fund.db` as the database name
- Chose **Dagster** over Airflow for local dev (accepted AI's pros/cons analysis)
- Proposed **parallel execution** of raw/landing streams and **mart independence**
- Proposed **daily snapshot** cadence assumption
- Specified **80-year dim_date spine centred on today** (rejected 9999-12-31, 100-year, and 2060 options in sequence)
- Required **error handling Option A** (skip and log, never halt on single row failure)
- Set **99% acceptance threshold** (accepted AI's recommendation for lending context)
- Required **Option C** for date parameter (arg OR auto today)
- Insisted on design review rounds before any code was generated

### What the AI proposed (accepted by human)

- Two-layer ingestion pattern (raw + landing) — human then proposed this independently
- SCD2 for `lnd_loan`, insert-new-only for `lnd_payment`
- `dq_gate` as an explicit Dagster asset (human refined to per-table gates)
- Star schema justification over OBT
- Dagster pros/cons analysis leading to Dagster selection
- `dim_date` self-updating bounds using `today()` (human then chose 80-year range)
- `mart_data_observability` depending on both DQ gates AND fact tables
- Error tables (`err_loan`, `err_payment`) with `_rejection_reason`
- Structured JSON logging with `log_event()` helper
- Full delivery requirements gap audit before codegen

### What the AI proposed (modified by human)

- AI proposed single `dq_gate` asset → human changed to per-table `dq_lnd_loan` + `dq_lnd_payment`
- AI proposed `mart_data_observability` deps after staging → human correctly identified it should run after facts
- AI proposed `9999-12-31` for dim_date → human rejected, eventually settled on 80-year centred spine
- AI proposed monthly simulation → human pushed to daily cadence assumption

### What the AI proposed (rejected by human)

- Single `raw_record` VARCHAR column for JSONL → human required per-column flattening
- `CREATE OR REPLACE` at raw layer → human required append semantics to preserve history

---

## Phase 2 — Codegen (AI-led, human-reviewed)

### Build order

1. `config.py` — paths, constants, thresholds
2. `duckdb_resource.py` — shared Dagster resource
3. `utils/cleaners.py` — pure transformation functions
4. `utils/emi.py` — EMI formula + anomaly detection
5. `utils/logger.py` — structured JSON logging
6. `assets/ingestion/raw_loan.py` + `raw_payment.py`
7. `assets/landing/lnd_loan.py` + `lnd_payment.py`
8. `assets/dq/dq_lnd_loan.py` + `dq_lnd_payment.py`
9. Unit tests (steps 1–4 above) — 115 tests
10. Fixture files + integration tests
11. DQ unit tests + logging unit tests (252 total unit tests)
12. `assets/transformation/stg_loan_payment.py`
13. `assets/transformation/dim_customer.py` + `dim_date.py`
14. `assets/transformation/fct_loan.py` + `fct_payment.py`
15. `assets/marts/mart_delinquency.py` + `mart_payment_anomaly.py` + `mart_observability.py`
16. `src/definitions.py` + `src/pipeline.py`
17. E2E test (38 tests, 290 total)
18. `README.md` + `AI_LOG.md`

---

## Bugs Found by Testing

| # | Bug | Root Cause | Fix |
|---|---|---|---|
| 1 | `clean_principal_amount` didn't catch `-$100.00` | Negativity check ran after `float()` cast which succeeded | Check for `-` sign before casting |
| 2 | 3 EMI test expected values were wrong | Values were hand-estimated, not formula-derived | Recomputed from formula |
| 3 | Tolerance boundary behaviour was ambiguous | `>` vs `>=` undefined for exactly 10% deviation | Decided: boundary-inclusive flagging (safer for lending) |
| 4 | Date format assumption was implicit | `dateutil` defaulted to `dayfirst=False` silently | Made explicit with comment + test |
| 5 | Payment fixture DQ acceptance rate was 90% | 1 bad JSON line out of 10 = below 99% threshold | Test corrected to assert breach (correct behaviour) |
| 6 | E2E: `err_loan` count wrong in raw layer test | `err_loan` is populated by `lnd_` transform, not raw | Moved assertion to correct layer |
| 7 | E2E: `amount` type was `Decimal` not `float` | DuckDB returns `Decimal` for `DECIMAL(18,2)` columns | Assert `isinstance(x, (float, Decimal))` |
| 8 | E2E: `fct_payment` had 7 rows vs `lnd_payment` 8 | Orphan payment (L9999999) excluded from stg LEFT JOIN | Correct behaviour — test updated to account for orphans |

---

## Key Design Decisions Log

| Decision | Made By | Rationale |
|---|---|---|
| Raw layer before landing | Human (proposed) | Forensic trail — re-derive landing without re-reading source |
| All VARCHAR at raw | Human (confirmed) | Raw = exact replica, no interpretation |
| Append at raw, never truncate | Human (confirmed) | History preservation for SCD2 change detection |
| SCD2 for loans | AI (proposed), Human (confirmed) | Loans mutate (status changes) — history matters for delinquency |
| Insert-only for payments | AI (proposed), Human (confirmed) | Payment events are immutable facts |
| Per-table DQ gates | Human (refined AI's shared gate) | Precise failure isolation — one gate failing shouldn't block the other |
| 99% acceptance threshold | AI (recommended), Human (confirmed) | Financial data = near-zero tolerance; 1% allows for known source noise |
| Star schema over OBT | AI (proposed), Human (confirmed) | Grain conflict between loans and payments makes OBT lossy |
| Dagster over Airflow | AI (recommended), Human (confirmed) | Asset-native model maps directly to layer design |
| 80-year dim_date spine | Human (decided after rejecting 3 AI proposals) | Self-updating, no maintenance, covers all realistic loan horizons |
| Custom DQ framework | AI (proposed), Human (confirmed) | No Great Expectations overhead for this scope; DuckDB SQL sufficient |
| US date format assumption | AI (identified gap), documented in README | Helix Lending is US platform; noted as known limitation |

---

## Human Decisions That Improved the Design

1. **Raw layer proposal** — the original design would have lost forensic traceability. The raw layer is now the foundation for SCD2 change detection.
2. **Per-column JSONL flattening** — a single `raw_record` column would have made the raw layer unqueryable without JSON parsing. Per-column is immediately useful.
3. **Per-table DQ gates** — the shared gate would have hidden which source was failing. Independent gates give precise failure attribution in Dagster's UI.
4. **80-year self-updating date spine** — anchoring to `today()` means the pipeline never needs maintenance for the date dimension.
5. **Insisting on design review rounds** — prevented premature codegen on an underspecified design. The 5-year simulation exercise caught the full-dump duplication problem before a single line of code was written.

---

## AI Limitations Encountered

1. **EMI expected values** — AI generated test expected values manually rather than deriving them from the formula. Three tests were wrong. Fix: always derive expected values programmatically.
2. **Boundary behaviour ambiguity** — AI didn't make the `>` vs `>=` tolerance boundary decision explicit until tests exposed it.
3. **E2E test layer timing** — AI's initial test assertion assumed `err_loan` was empty at the raw layer, not accounting for the fact that the full pipeline had already run by the time the assertion was checked.
4. **DuckDB type return** — AI assumed DuckDB returns `float` for `DECIMAL(18,2)` columns. It returns `Decimal`. Required a test fix.
