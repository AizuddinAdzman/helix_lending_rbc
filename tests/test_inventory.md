# Test Inventory — Helix Lending Pipeline

**Total: 295 tests across 11 files**

---

## Unit Tests

### `test_clean_principal.py` — 18 tests
Tests `clean_principal_amount()` in `utils/cleaners.py`

| Test | What It Checks |
|---|---|
| `test_plain_float_string` | `"32256.80"` → `32256.80` |
| `test_currency_symbol_with_commas` | `"$33,517.74"` → `33517.74` |
| `test_comma_only_formatting` | `"33,517.74"` → `33517.74` |
| `test_no_decimals` | `"33517"` → `33517.0` |
| `test_comma_no_decimal` | `"33,517"` → `33517.0` |
| `test_whitespace_padding` | `"  32256.80  "` → `32256.80` |
| `test_dollar_no_cents` | `"$50000"` → `50000.0` |
| `test_large_amount` | `"$223,956.81"` → `223956.81` |
| `test_zero` | `"0.00"` → `0.0` |
| `test_small_amount` | `"17.43"` → `17.43` |
| `test_none_returns_none` | `None` → `None` |
| `test_empty_string_returns_none` | `""` → `None` |
| `test_whitespace_only_returns_none` | `"   "` → `None` |
| `test_negative_value_raises` | `"-100.00"` → `ValueError` |
| `test_negative_with_currency_raises` | `"-$100.00"` → `ValueError` |
| `test_non_numeric_raises` | `"abc"` → `ValueError` |
| `test_text_with_dollar_raises` | `"$abc"` → `ValueError` |
| `test_multiple_dots_raises` | `"1.2.3"` → `ValueError` |

---

### `test_parse_dates.py` — 30 tests
Tests `parse_date()` and `parse_timestamp_utc()` in `utils/cleaners.py`

**`TestParseDate` (20 tests)**

| Test | What It Checks |
|---|---|
| `test_iso_format` | `"2023-05-03"` → `date(2023,5,3)` |
| `test_uk_style_dec` | `"11-Dec-2020"` → `date(2020,12,11)` |
| `test_uk_style_jan` | `"01-Jan-2021"` → `date(2021,1,1)` |
| `test_uk_style_lowercase` | `"11-dec-2020"` → `date(2020,12,11)` |
| `test_us_style` | `"08/19/2020"` → `date(2020,8,19)` |
| `test_us_style_leading_zero` | `"01/05/2022"` → `date(2022,1,5)` |
| `test_compact_format` | `"20230503"` → `date(2023,5,3)` |
| `test_whitespace_stripped` | `"  2023-05-03  "` → `date(2023,5,3)` |
| `test_end_of_year` | `"2020-12-31"` → `date(2020,12,31)` |
| `test_leap_year` | `"2020-02-29"` → `date(2020,2,29)` |
| `test_none_returns_none` | `None` → `None` |
| `test_empty_string_returns_none` | `""` → `None` |
| `test_whitespace_only_returns_none` | `"   "` → `None` |
| `test_ambiguous_slash_treated_as_us_format` | `"01/05/2022"` → Jan 5 not May 1 (US platform assumption) |
| `test_ambiguous_slash_month_not_day` | `"12/11/2021"` → Dec 11 not Nov 12 |
| `test_unambiguous_us_day_gt_12` | `"08/19/2020"` → Aug 19 (day > 12, unambiguous) |
| `test_output_is_always_iso_format` | All source formats normalise to `YYYY-MM-DD` |
| `test_garbage_raises` | `"not-a-date"` → `ValueError` |
| `test_invalid_month_raises` | `"2023-13-01"` → `ValueError` |
| `test_invalid_day_raises` | `"2023-02-30"` → `ValueError` |

**`TestParseTimestampUTC` (10 tests)**

| Test | What It Checks |
|---|---|
| `test_negative_offset` | `-08:00` offset converted to UTC correctly |
| `test_negative_offset_five` | `-05:00` offset converted to UTC correctly |
| `test_utc_z_suffix` | `Z` suffix recognised as UTC |
| `test_utc_zero_offset` | `+00:00` treated as UTC |
| `test_naive_timestamp_assumed_utc` | No timezone → assumed UTC, no conversion |
| `test_none_returns_none` | `None` → `None` |
| `test_empty_string_returns_none` | `""` → `None` |
| `test_whitespace_only_returns_none` | `"   "` → `None` |
| `test_garbage_raises` | `"not-a-timestamp"` → `ValueError` |
| `test_date_only_no_time` | Date-only string returns UTC datetime |

---

### `test_cleaners_misc.py` — 40 tests
Tests `normalise_category()` and `clean_string()` in `utils/cleaners.py`

**`TestNormaliseCategory` (21 tests)** — verifies uppercased/mixed-case categoricals (product types, statuses, channels, payment methods) are lowercased and whitespace-stripped. Confirms `None`, `""`, and whitespace-only inputs return `None`.

**`TestCleanString` (19 tests)** — verifies strings are boundary-stripped, internal spaces preserved, JSON blobs untouched, and `None`/empty/whitespace-only return `None`. Covers IDs, bank names, user-agent strings, and embedded JSON from `borrower_info`.

---

### `test_emi_calculation.py` — 30 tests
Tests `calculate_emi()` and `is_payment_anomalous()` in `utils/emi.py`

**`TestCalculateEMI` (17 tests)**

| Test | What It Checks |
|---|---|
| `test_standard_personal_loan` | P=10,000, r=10%, n=12 → EMI=$879.16 |
| `test_mortgage_30_year` | P=200,000, r=4%, n=360 → EMI=$954.83 |
| `test_student_loan` | P=38,366.86, r=5.8%, n=120 → EMI=$422.11 |
| `test_high_rate_short_term` | P=24,315.74, r=17.82%, n=60 → EMI=$615.08 |
| `test_sample_loan_data` | P=32,256.80, r=10.12%, n=12 → EMI=$2,837.69 |
| `test_zero_interest_rate` | r=0 falls back to flat P/n division |
| `test_zero_interest_long_term` | Same edge case over 360 months |
| `test_none_*` (3 tests) | Any None input returns None |
| `test_negative_*_raises` (2 tests) | Negative P or r raises ValueError |
| `test_zero_term_raises` | n=0 raises ValueError |
| `test_negative_term_raises` | n<0 raises ValueError |
| `test_returns_float` | Return type is float |
| `test_rounded_to_two_decimals` | Result is rounded to 2dp |

**`TestIsPaymentAnomalous` (13 tests)** — verifies the 10% EMI tolerance band: exact match passes, within 5% passes, at boundary flags, over 15% flags, zero payment flags, large overpayment flags. Also tests None inputs, zero EMI, and custom tolerance values.

---

### `test_delinquency_flag.py` — 13 tests
Tests the 30-day delinquency business rule as a pure function

| Test | What It Checks |
|---|---|
| `test_payment_on_due_date_not_delinquent` | Paid on due date → not delinquent |
| `test_payment_after_due_date_not_delinquent` | Paid late but within grace period → not delinquent |
| `test_within_grace_period_no_payment` | 20 days overdue → still in grace period |
| `test_due_date_in_future_not_delinquent` | Due date hasn't arrived yet → not delinquent |
| `test_payment_before_evaluation_covers_due` | Payment after due date covers that period |
| `test_no_payment_past_grace_period` | 45 days, no payment → delinquent |
| `test_payment_before_due_date_then_missed` | Last payment was before this due date → delinquent |
| `test_exactly_at_grace_boundary_not_delinquent` | Exactly 30 days → at boundary, not delinquent |
| `test_one_day_past_grace_delinquent` | 31 days → one day past grace, delinquent |
| `test_very_old_unpaid_delinquent` | 2 years overdue → delinquent |
| `test_custom_threshold_60_days` | 45 days with 60-day threshold → not delinquent |
| `test_custom_threshold_60_days_breach` | 65 days with 60-day threshold → delinquent |
| `test_zero_grace_period` | Any day past due with 0-day grace → delinquent |

---

### `test_scd2_logic.py` — 24 tests
Tests SCD2 hash and loan row cleaning in `assets/landing/lnd_loan.py`

**`TestComputeHash` (8 tests)** — verifies MD5 hash is deterministic, changes when any business column changes, handles None fields consistently, and always produces a 32-character hex string.

**`TestCleanLoanRow` (16 tests)** — verifies `_clean_row()` correctly casts principal, parses multi-format dates, lowercases categoricals, raises on empty `loan_id`, raises on bad principal/date/rate/term, and accepts None for optional fields.

---

### `test_clean_payment_row.py` — 27 tests
Tests `_clean_payment_row()` in `assets/landing/lnd_payment.py`

**`TestCleanPaymentRowHappyPath` (7 tests)** — full valid row, amount cast to float, integer string amounts, payment method type normalised to lowercase.

**`TestCleanPaymentRowTimestamp` (6 tests)** — negative UTC offsets converted correctly, Z suffix, naive timestamp assumed UTC, None/empty timestamp returns None.

**`TestCleanPaymentRowErrors` (6 tests)** — empty/None/whitespace `payment_id` raises, negative amount raises, non-numeric amount raises, bad timestamp raises.

**`TestCleanPaymentRowOptionalFields` (8 tests)** — `last_four`, `bank`, `metadata_source`, `user_agent`, `loan_id` all accept None. All optional fields None still produces a valid row. Whitespace-only optional fields return None.

---

### `test_dq_checks.py` — 27 tests
Tests DQ check SQL logic against in-memory DuckDB

**`TestVolumeAcceptanceRate` (9 tests)** — threshold boundary at 99%, above passes, below fails, exactly at threshold passes, one below fails, all rejected fails, empty source returns 0%.

**`TestVolumeSQL` (1 test)** — verifies the acceptance rate SQL calculation produces the correct ratio.

**`TestUniqueness` (5 tests)** — no duplicates passes, duplicate `loan_id` in current rows detected, SCD2 historical rows (is_current=FALSE) excluded from uniqueness check, duplicate `payment_id` detected, unique records pass.

**`TestCompleteness` (4 tests)** — no nulls passes, null rate within 10% passes, 50% null rate fails, exactly at 10% threshold passes.

**`TestReferentialIntegrity` (4 tests)** — all `loan_id`s in payments exist in loans passes, orphan `loan_id` detected, NULL `loan_id` in payments is not an RI violation (completeness issue), multiple orphans counted correctly.

**`TestFreshness` (3 tests)** — batch loaded today is fresh, batch from 2020 is stale (> 24h), empty table has no freshness timestamp.

---

### `test_logging.py` — 22 tests
Tests the structured JSON logging system in `utils/logger.py`

**`TestJsonFormatter` (5 tests)** — emits valid JSON, timestamp field present and parseable as ISO-8601, level/message/event fields present.

**`TestLogEventFields` (9 tests)** — each structured field (`layer`, `table`, `rows_in`, `rows_out`, `rows_rejected`, `duration_sec`, `source_file`, `batch_date`) correctly written. None values correctly omitted from output.

**`TestLogLevels` (4 tests)** — INFO, WARNING, ERROR, DEBUG all route correctly.

**`TestCheckpointEvents` (4 tests)** — `load_start`, `load_end`, `dq_fail`, and `row_rejected` events each produce correctly structured records with all required fields.

---

### `test_raw_ingestion.py` — 32 tests
Integration tests for raw ingestion assets using fixture files against in-memory DuckDB

**`TestRawLoanIngestion` (14 tests)**

| Test | What It Checks |
|---|---|
| `test_all_rows_inserted_including_dirty` | All 10 fixture rows inserted including duplicates and bad values |
| `test_rows_in_matches_csv_line_count` | `rows_in` equals actual CSV line count |
| `test_zero_rejections_at_raw_layer` | Raw layer never rejects — even bad rows go in as VARCHAR |
| `test_duplicate_row_preserved` | `L0000001` appears twice in fixture — both rows kept |
| `test_bad_amount_preserved_as_varchar` | `"not_a_number"` stored as-is, no casting attempted |
| `test_empty_loan_id_preserved` | Empty `loan_id` stored as NULL, not rejected |
| `test_currency_amount_preserved_as_varchar` | `"$8,500.00"` preserved as string |
| `test_mixed_case_product_type_preserved` | `MORTGAGE`, `Student`, `AUTO` stored exactly as received |
| `test_audit_columns_populated` | Every row has `_source_file` and `_last_updated_ts` |
| `test_source_file_is_filename_with_extension` | `_source_file` = `"loan_fixture.csv"` (with extension) |
| `test_batch_ts_stored_correctly` | `_last_updated_ts` matches the batch timestamp |
| `test_append_second_batch_accumulates` | Second run appends 10 more rows — does not truncate |
| `test_raw_audit_written_after_loan_load` | `raw_audit` gets one row per batch |
| `test_raw_audit_duplicate_count_correct` | `duplicate_key_count` ≥ 1 (L0000001 is duplicated) |

**`TestRawPaymentIngestion` (10 tests)**

| Test | What It Checks |
|---|---|
| `test_good_rows_inserted` | 9 parseable lines inserted (1 bad JSON rejected) |
| `test_bad_json_goes_to_err_payment` | Unparseable line → `lnd_err_payment` |
| `test_bad_json_rejection_reason_populated` | `_rejection_reason` is non-empty |
| `test_duplicate_payment_preserved_at_raw` | `P000000001` appears twice — both kept |
| `test_orphan_fk_preserved_at_raw` | `L9999999` (orphan) preserved — raw doesn't check RI |
| `test_missing_metadata_stored_as_null` | Record without `metadata` block → NULL fields |
| `test_amount_stored_as_varchar` | Amount stored as string, not cast to number |
| `test_timestamp_stored_as_raw_string` | Timestamps kept with original timezone info |
| `test_audit_columns_populated` | `_source_file` and `_last_updated_ts` on every row |
| `test_source_file_is_filename_with_extension` | `_source_file` = `"payment_fixture.jsonl"` |

---

## End-to-End Test

### `test_pipeline_e2e.py` — 38 tests
Full pipeline run on fixture files against in-memory DuckDB. Asserts correct row counts, data types, and business logic across all 15 tables.

**`TestRawLayer` (4 tests)** — raw_loan gets all 10 rows including dirty ones; raw_payment gets 9 parseable rows; bad JSON captured in err_payment; raw layer makes no rejections itself.

**`TestLandingLayer` (7 tests)** — lnd_loan has 7 current rows (deduped + cleaned); 2 bad rows in lnd_err_loan; product_type lowercased; origination_date is Python `date` type; lnd_payment has 8 rows (1 deduped); amount is numeric; timestamps are UTC-aware.

**`TestDQLayer` (4 tests)** — DQ results written to lnd_dq_audit; RI orphan from L9999999 detected; uniqueness checks for loan_id and payment_id both pass.

**`TestStagingLayer` (4 tests)** — stg_loan_payment has rows; expected_emi derived; delinquency flag exists; LEFT JOIN from lnd_loan preserves loans with no payments.

**`TestDimensions` (7 tests)** — dim_customer populated; credit_score parsed from JSON; one row per customer; dim_date includes today; dim_date includes lower bound (40 years ago); dim_date has no gaps; Saturday correctly flagged as weekend.

**`TestFacts` (5 tests)** — fct_loan grain is one row per loan; no duplicate loan_ids; fct_payment excludes orphan payments (correct — unallocated payments have no matching loan for the JOIN); anomaly flag set for P000000004 (massive overpayment); fct_loan enriched with customer credit score from dim_customer.

**`TestMarts` (7 tests)** — mart_delinquency has rows by product; delinquency rate between 0–100; mart_payment_anomaly populated; every anomaly row has a reason; mart_data_observability has exactly 2 rows (one per source); freshness_hours populated on all rows; pipeline_status is either PASS or FAIL.

---

## Test Fixture Contents

### `loan_fixture.csv` — 10 rows
| Row | loan_id | Characteristic | Layer Outcome |
|---|---|---|---|
| 1 | L0000001 | Clean | lnd_loan current row |
| 2 | L0000002 | Clean (MORTGAGE uppercase) | lnd_loan, product_type normalised |
| 3 | L0000003 | UK date format | lnd_loan, date parsed |
| 4 | L0000004 | `$8,500.00` currency amount | lnd_loan, principal cleaned |
| 5 | L0000005 | Clean | lnd_loan current row |
| 6 | L0000001 | **Duplicate** of row 1 | Raw: kept. Landing: deduped (last wins) |
| 7 | L0000006 | `principal_amount = "not_a_number"` | lnd_err_loan (cast failure) |
| 8 | *(empty)* | Empty `loan_id` | lnd_err_loan (empty key) |
| 9 | L0000008 | Clean | lnd_loan current row |
| 10 | L0000009 | Clean | lnd_loan current row |

### `payment_fixture.jsonl` — 10 lines
| Line | payment_id | Characteristic | Layer Outcome |
|---|---|---|---|
| 1 | P000000001 | Clean | lnd_payment |
| 2 | P000000002 | Clean, timezone offset | lnd_payment, UTC normalised |
| 3 | P000000003 | Clean, negative UTC offset | lnd_payment |
| 4 | P000000004 | `amount=9999.99` (overpayment) | lnd_payment, is_payment_anomalous=TRUE |
| 5 | P000000005 | Naive timestamp (no tz) | lnd_payment, assumed UTC |
| 6 | P000000001 | **Duplicate** of line 1 | Raw: kept. Landing: deduped |
| 7 | P000000006 | `loan_id=L9999999` (orphan) | lnd_payment, allocation_status='unallocated' |
| 8 | *(bad JSON)* | `"not valid json line"` | lnd_err_payment (parse failure) |
| 9 | P000000008 | Clean | lnd_payment |
| 10 | P000000009 | Positive timezone offset | lnd_payment, UTC normalised |