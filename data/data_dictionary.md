# Data Dictionary — Helix Lending

This document describes the source data schemas. It is **incomplete** — some
columns are undocumented intentionally. Use your judgment.

---

## `loans.csv`

A flat CSV of loan origination records.

| Column                | Type   | Description                                                       |
|-----------------------|--------|-------------------------------------------------------------------|
| `loan_id`             | string | Loan identifier. Expected to be unique.                           |
| `customer_id`         | string | FK to a customer master (not provided in this assessment).        |
| `product_type`        | string | One of: personal, auto, mortgage, student.                        |
| `principal_amount`    | string | Original loan amount in USD.                                      |
| `interest_rate`       | number | Annual interest rate, percentage points (e.g. `7.25` = 7.25%).   |
| `term_months`         | int    | Loan term in months.                                              |
| `origination_date`    | string | Date the loan was originated.                                     |
| `origination_channel` | string | Channel that originated the loan.                                 |
| `status`              | string | Current loan status.                                              |
| `borrower_info`       | string | JSON-encoded snapshot of borrower attributes at origination.     |

Notes:
- `borrower_info` is a JSON object embedded in the CSV column. You will need
  to parse it. Schema of the JSON is not formally documented here.
- A small fraction of source records may have malformed values in any column.
  Decide your handling strategy and document it.

---

## `payments.jsonl`

Newline-delimited JSON. Each line is one payment event.

```json
{
  "payment_id": "P000000123",
  "loan_id":    "L0001234",
  "amount":     452.18,
  "timestamp":  "2024-03-15T14:22:00-05:00",
  "payment_method": {
    "type":    "ACH",
    "details": { "last_four": "1234", "bank": "Helix First" }
  },
  "metadata": { "source": "mobile_app", "user_agent": "HelixApp/3.2.1" }
}
```

| Field            | Notes                                                          |
|------------------|----------------------------------------------------------------|
| `payment_id`     | Payment event ID. Expected to be unique per logical payment.   |
| `loan_id`        | FK to `loans.csv`.                                             |
| `amount`         | Payment amount in USD.                                         |
| `timestamp`      | ISO-8601 timestamp. Timezone handling is your responsibility.  |
| `payment_method` | Nested structure. Type and details vary by method.             |
| `metadata`       | Optional. Not all events carry it.                             |

---

## Things this dictionary does not tell you

- Which columns are nullable
- Acceptable value ranges
- How to resolve conflicts between duplicate records
- The canonical casing for categorical fields
- How to interpret missing fields in nested structures

These are decisions you will need to make. Document your choices in your README.
