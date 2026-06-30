# AI Collaboration Log — Helix Lending Pipeline

This log documents the collaboration between Aizuddin (engineer) and Claude (AI assistant) in designing, building, debugging, and finalising the Helix Lending data pipeline.

---

## Collaboration Summary

**Approach:** Design-first, zero-code until architecture was fully locked and agreed. Followed by full codegen, then a debugging cycle as the pipeline was run for real on the engineer's local machine, then a substantive design revision driven by actual data investigation, then final documentation.

**Final state:** 295 passing tests, 16 tables across 6 schemas, fully sequential Dagster DAG, running cleanly end-to-end on the engineer's macOS/Windows environment.

---

## Phase 1 — Design (Human-led, AI-assisted)

### What the human drove

- Initial requirements: ingest CSV + JSONL, store in DuckDB, answer 3 business questions
- Proposed adding a **raw layer** before landing (AI had proposed landing-first)
- Proposed **flattening JSONL to per-column VARCHAR** at raw layer (AI had proposed single raw_record column)
- Renamed `loaded_at` → `_last_updated_ts` (UTC) for clarity
- Chose **Dagster** over Airflow for local dev (accepted AI's pros/cons analysis)
- Proposed **parallel execution** of raw/landing streams (later reversed — see Phase 3)
- Proposed **daily snapshot** cadence assumption
- Specified **80-year dim_date spine centred on today** after rejecting `9999-12-31`, 100-year, and 2060 options in sequence
- Required **error handling Option A** (skip and log, never halt on single row failure)
- Set **99% acceptance threshold**
- Required **Option C** for date parameter (arg OR auto today)
- Insisted on design review rounds before any code was generated
- Drove the entire **schema-per-layer redesign** (Phase 4) including the `ENV` prefix decision
- Identified and drove the **negative principal** and **payment allocation** redesigns (Phase 5) — the most consequential changes in the project

### What the AI proposed (accepted by human)

- Two-layer ingestion pattern (raw + landing) — human then proposed this independently
- SCD2 for `lnd_loan`, insert-new-only for `lnd_payment`
- Star schema justification over OBT
- Dagster pros/cons analysis leading to Dagster selection
- `dim_date` self-updating bounds using `today()`
- Error tables (`err_loan`, `err_payment`) with `_rejection_reason`
- Structured JSON logging with `log_event()` helper
- Full delivery requirements gap audit before codegen

### What the AI proposed (rejected or modified by human)

- AI proposed single `dq_gate` asset → human required per-table gates (`dq_lnd_loan`, `dq_lnd_payment`) for failure isolation
- Single `raw_record` VARCHAR column for JSONL → human required per-column flattening
- `CREATE OR REPLACE` at raw layer → human required append semantics to preserve history

---

## Phase 2 — Codegen (AI-led, human-reviewed)

Built layer by layer: config → utils → ingestion → landing → DQ → staging → dimensions → facts → marts → Dagster definitions → tests → fixtures → E2E test. 290 tests passing at end of this phase.

### Bugs Found by Testing (8 total, all in tests/test-expectations, not production logic)

| # | Bug | Root Cause | Fix |
|---|---|---|---|
| 1 | `clean_principal_amount` didn't catch `-$100.00` | Negativity check ran after `float()` cast which succeeded | Check for `-` sign before casting |
| 2 | 3 EMI test expected values were wrong | Hand-estimated, not formula-derived | Recomputed from formula |
| 3 | Tolerance boundary behaviour was ambiguous | `>` vs `>=` undefined for exactly 10% deviation | Decided: boundary-inclusive flagging |
| 4 | Date format assumption was implicit | `dateutil` defaulted to `dayfirst=False` silently | Made explicit with comment + test |
| 5 | Payment fixture DQ acceptance rate was 90% | 1 bad JSON line out of 10 = below 99% threshold | Test corrected to assert breach (correct behaviour) |
| 6 | E2E: `err_loan` count wrong in raw layer test | `err_loan` populated by `lnd_` transform, not raw | Moved assertion to correct layer |
| 7 | E2E: `amount` type was `Decimal` not `float` | DuckDB returns `Decimal` for `DECIMAL(18,2)` columns | Assert `isinstance(x, (float, Decimal))` |
| 8 | E2E: `fct_payment` had fewer rows than `lnd_payment` | Orphan payment excluded from stg LEFT JOIN | Correct behaviour — test updated |

---

## Phase 3 — Local Deployment Debugging (Human ran it, AI diagnosed remotely)

The engineer ran the pipeline on their own machine (macOS, then Windows) via `dagster dev`. This surfaced a long sequence of environment and runtime issues the sandboxed test suite could not have caught.

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `source: command not found` | `source` is bash-only; engineer was on different shells (macOS zsh path syntax, then Windows PowerShell) | Provided OS-specific activation commands |
| 2 | `ModuleNotFoundError: dateutil` | `pip install -r requirements.txt` run before venv was activated | Re-run install inside activated venv |
| 3 | `TypeError: unsupported operand type(s) for \|: 'type' and 'NoneType'` | Engineer's Python was 3.9; code used 3.10+ `str \| None` union syntax | Replaced with `Optional[str]` from `typing` across all affected files |
| 4 | `DagsterInvalidDefinitionError: Cannot annotate context parameter with AssetExecutionContext` | Installed Dagster version didn't recognise `AssetExecutionContext` as a valid context type in this combination with `from __future__ import annotations` | Removed the type annotation from `context` entirely (Dagster identifies it by parameter name, not type) |
| 5 | `FileNotFoundError: loan.csv` | Source files not yet placed in `data/` | Directed engineer to copy real files into place |
| 6 | Filename mismatch (`loans.csv` vs expected `loan.csv`) | Engineer's actual file was pluralised | Renamed file to match config |
| 7 | `IOException: Could not set lock on file` | DuckDB single-writer constraint; `raw_loan` and `raw_payment` ran in parallel | **Major design change**: converted entire DAG from parallel to fully sequential via explicit `deps=[]` chains |
| 8 | `NameError: name 'context' is not defined` | Removing the type annotation in fix #4 accidentally dropped the parameter itself in one file | Restored `context` as untyped first parameter |
| 9 | `Exception: DQ gate dq_lnd_loan FAILED — 14 duplicate loan_ids` | Source CSV had genuine duplicate `loan_id`s within a single batch; SCD2 logic processed them sequentially and created two `is_current=TRUE` rows | Added intra-batch dedup (last occurrence wins) before SCD2 comparison |
| 10 | `Exception: DQ gate dq_lnd_payment FAILED — 130 orphan loan_ids` | Initial investigation suspected RI bug | **Human-led investigation** (see Phase 5) revealed this needed a design change, not a code fix |
| 11 | `BinderException: table X has N columns but M values supplied` (recurring, 4+ times) | Sed-based DDL edits during iterative design changes updated one of two near-identical DDL blocks (e.g. `raw_payment` vs `lnd_payment`) but not both, or updated the asset file but not `init_db.py` | Each time: diagnosed via column-count diff, fixed the specific DDL block, **eventually ran a full structural cross-check script** comparing every table's DDL between `init_db.py` and its owning asset file to catch all remaining drift at once |

### Pattern recognised from Phase 3

The recurring `BinderException` column-mismatch bugs were not isolated mistakes — they were a structural risk of maintaining the same table schema in two places (`init_db.py` for explicit initialisation, and each asset's own `CREATE TABLE IF NOT EXISTS` for safety). Documented in the README's "What I Would Do With More Time" as a candidate for an automated DDL-consistency check.

---

## Phase 4 — Schema Redesign (Human-led)

The engineer proposed moving from a single flat `main` schema to schema-per-layer, with environment embedded in the schema name itself.

| Decision | Made By | Rationale |
|---|---|---|
| Schema-per-layer (`hlx_{env}_raw`, `_lnd`, `_stg`, `_dim`, `_fct`, `_mart`) | Human (proposed) | Mirrors production data platform conventions; clear ownership boundaries |
| Environment in schema name, not just DB filename | Human (chose Option C explicitly) | "Skips the part we sometimes mislook in which environment we are checking" — a safety net against cross-environment query mistakes |
| `err_loan`/`err_payment` → `lnd_err_loan`/`lnd_err_payment` | Human (renamed) | Errors are produced by landing cleanup, not raw ingestion — naming should reflect ownership |
| `dq_results` → `lnd_dq_audit` | Human (renamed) | More meaningful name — explicitly an audit trail, not generic "results" |
| `ENV` driven by environment variable, never hardcoded | Human (required) | Enables `HELIX_ENV=prd` switch with zero code changes |
| Fully sequential Dagster DAG (formalised) | Human (confirmed, given DuckDB constraint) | "Since we are having duckdb file lock situation, shall we do a sequential for this case?" — correctly identified this as the proper fix, not a workaround |

This phase required updating 20 files (config, init_db, all 14 assets, definitions, 3 test files) in a single coordinated pass, verified by a full test suite run (293 passing after this phase, plus new tests for the schema-aware fixtures).

---

## Phase 5 — Data-Driven Design Revision (Human-led investigation, most significant phase)

This phase began when the DQ gate correctly caught a referential integrity issue, and the human refused to accept either a quick fix or the AI's first proposed categorisation — insisting on investigating the actual data first.

### Negative Principal Amount

**Trigger:** AI surfaced that `clean_principal_amount` was rejecting negative values as errors.

**Human's response:** Provided detailed real-world context on why negative principal is a legitimate lending scenario (overpayment, escrow refund, lender correction, accounting timing) rather than accepting the existing reject-on-negative behaviour.

**Resolution:** Negative principal accepted; `loan_balance_type` derived column added (`credit_balance` / `zero_balance` / `debit_balance`); `mart_delinquency` and `mart_payment_anomaly` updated to exclude credit balance loans, since delinquency and EMI comparison are undefined for them.

### Payment Allocation Status

**Trigger:** DQ gate failed with "130 orphan loan_ids" as a hard breach.

**Human's response:** Explicitly paused codegen and requested investigation before any decision: *"lets first go through our investigation before deciding anything."* This led to a multi-step SQL investigation (numeric range check, gap analysis, format comparison, sample payment inspection) that revealed the orphan `loan_id`s were numerically outside the entire loan universe (`L0100604`+ vs `loans.csv`'s `L0000001`–`L0010000`) — not a join bug, not a duplicate issue, but payments for loans that were never originated in this dataset.

**Human's framing (verbatim insight):** *"those orphaned payments are not due to loss of referential [integrity], but rather, the loan_id itself is not available from loan.csv... we can categorize it as unallocated or pre-loan payments... could be many reasons the payment record is there without the loan record."* This reframing — from "RI violation" to "business categorisation problem" — directly shaped the final design.

**Resolution:** Hard RI breach replaced with `payment_allocation_status` (`allocated` / `unallocated` / `loan_rejected` / `unidentified`), surfaced as an INFO-level audit entry rather than a pipeline-halting exception.

### Payment Uniqueness — Composite Key

**Trigger:** AI initially proposed rejecting both records when the same `payment_id` appeared with different amounts.

**Human's response:** Provided detailed context on legitimate causes of same-ID-different-amount (split settlements across payment methods, merchant fee deductions, partial refunds, batch consolidation) — directly contradicting the AI's "reject both" proposal.

**Human's specific catch:** *"i think uniq must be payment_id+amount+payment_timestamp because it is possible payment_id+amount but different payment_timestamp = split payment to same payment_id"* — identified a gap in the AI's two-field proposal (payment_id + amount) before it was implemented, preventing a real bug.

**Resolution:** Composite uniqueness key `(payment_id, amount, payment_timestamp)`; surrogate key `lnd_payment_sk` added since `payment_id` is no longer unique; true duplicates still flagged, split settlements explicitly preserved with an informational (non-breach) count.

### Why Tables Not Views

**Trigger:** Human asked a direct architecture question mid-build: *"marts are currently tables rather than view; why not view and why table?"*

**Resolution:** AI provided the full trade-off (query performance, point-in-time snapshots, decoupling from upstream rebuilds, observability, DuckDB single-writer contention) — documented in README rather than just answered conversationally, since it's a defensible architectural choice worth justifying to an assessor.

---

## Key Design Decisions Log (Final)

| Decision | Made By | Rationale |
|---|---|---|
| Raw layer before landing | Human (proposed) | Forensic trail — re-derive landing without re-reading source |
| Schema-per-layer with ENV in schema name | Human (proposed, Option C) | Prevents cross-environment query mistakes |
| Fully sequential Dagster DAG | Human (confirmed after hitting DuckDB lock error) | Honest design for single-writer DB, not a workaround |
| Negative principal accepted as credit balance | Human (challenged AI's reject-on-negative default) | Real lending scenarios, not all errors |
| Payment allocation status over hard RI breach | Human (drove investigation before deciding) | 0.13% of payments reference loans outside the dataset's loan universe — categorise, don't halt |
| Composite uniqueness key for payments | Human (caught AI's incomplete two-field proposal) | Split settlements need three fields to distinguish from true duplicates |
| Tables not views for marts | Human (asked directly) | Performance, snapshots, DuckDB write contention |
| 80-year dim_date spine, self-updating | Human (rejected 3 AI proposals first) | Zero maintenance, covers all realistic loan horizons |

---

## AI Limitations Encountered

1. **EMI expected test values were hand-estimated**, not formula-derived — caused 3 false test failures early on.
2. **Boundary behaviour ambiguity** (`>` vs `>=`) wasn't made explicit until tests exposed it.
3. **DuckDB's `Decimal` return type** for `DECIMAL` columns wasn't anticipated — assumed `float`.
4. **Initial proposal for same-payment_id-different-amount was to reject both records** — would have silently discarded legitimate split settlements had the human not pushed back with real-world payment processing context.
5. **Initial RI check treated all unmatched loan_ids identically** — conflated "loan never existed" with "loan existed but failed cleaning," which the human's investigation separated into distinct, correctly-handled categories.
6. **Recurring DDL drift bugs** — sed-based multi-file edits during iterative design changes repeatedly updated one of two near-identical DDL definitions (asset file vs `init_db.py`) without updating the other, causing 4+ rounds of `BinderException` column-count errors during the engineer's local runs. Root cause was not caught until a full structural comparison script was run across all 16 tables — should have been done after the first such bug, not the fourth.