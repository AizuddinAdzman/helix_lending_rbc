import duckdb
conn = duckdb.connect("output/helix_dev.db")

print("--- 6. Numeric range check ---")
orphan_nums = conn.execute("""
    SELECT
        MIN(CAST(REPLACE(loan_id, 'L', '') AS INTEGER)) AS min_num,
        MAX(CAST(REPLACE(loan_id, 'L', '') AS INTEGER)) AS max_num
    FROM hlx_dev_lnd.lnd_payment
    WHERE loan_id IS NOT NULL
      AND loan_id LIKE 'L%'
      AND NOT EXISTS (
          SELECT 1 FROM hlx_dev_raw.raw_loan r WHERE r.loan_id = loan_id
      )
""").fetchone()

raw_nums = conn.execute("""
    SELECT
        MIN(CAST(REPLACE(loan_id, 'L', '') AS INTEGER)) AS min_num,
        MAX(CAST(REPLACE(loan_id, 'L', '') AS INTEGER)) AS max_num
    FROM hlx_dev_raw.raw_loan
    WHERE loan_id LIKE 'L%'
""").fetchone()

print(f"  Orphan loan_id numeric range : {orphan_nums[0]} → {orphan_nums[1]}")
print(f"  raw_loan numeric range       : {raw_nums[0]} → {raw_nums[1]}")

print("\n--- 7. Payment details for orphans ---")
orphan_details = conn.execute("""
    SELECT p.loan_id, p.amount, p.payment_method_type,
           CAST(p.payment_timestamp AS DATE) AS pay_date
    FROM hlx_dev_lnd.lnd_payment p
    WHERE p.loan_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM hlx_dev_raw.raw_loan r WHERE r.loan_id = p.loan_id
      )
    ORDER BY p.loan_id
    LIMIT 10
""").fetchall()
for r in orphan_details:
    print(f"  loan_id={r[0]}  amount={r[1]}  method={r[2]}  date={r[3]}")

print("\n--- 8. Are these loan_ids sequential gaps in raw_loan? ---")
# Check if orphan IDs fall in gaps between existing raw_loan IDs
gap_check = conn.execute("""
    WITH orphan_ids AS (
        SELECT DISTINCT loan_id,
               CAST(REPLACE(loan_id, 'L0', '') AS INTEGER) AS num
        FROM hlx_dev_lnd.lnd_payment
        WHERE loan_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM hlx_dev_raw.raw_loan r WHERE r.loan_id = loan_id
          )
    ),
    raw_ids AS (
        SELECT DISTINCT loan_id,
               CAST(REPLACE(loan_id, 'L0', '') AS INTEGER) AS num
        FROM hlx_dev_raw.raw_loan
        WHERE loan_id IS NOT NULL
    )
    SELECT
        o.loan_id AS orphan_id,
        o.num AS orphan_num,
        MAX(r.num) FILTER (WHERE r.num < o.num) AS closest_below,
        MIN(r.num) FILTER (WHERE r.num > o.num) AS closest_above
    FROM orphan_ids o
    CROSS JOIN raw_ids r
    GROUP BY o.loan_id, o.num
    ORDER BY o.num
    LIMIT 10
""").fetchall()
print("  Sample orphans vs closest raw_loan neighbours:")
print(f"  {'orphan_id':<12} {'num':<10} {'closest_below':<15} {'closest_above'}")
for r in gap_check:
    print(f"  {r[0]:<12} {r[1]:<10} {r[2]:<15} {r[3]}")

print("\n--- 9. How many loans in raw_loan? ---")
raw_count = conn.execute(
    "SELECT COUNT(DISTINCT loan_id) FROM hlx_dev_raw.raw_loan"
).fetchone()[0]
print(f"  Distinct loan_ids in raw_loan: {raw_count}")

print("\n--- 10. Max loan_id number in raw_loan ---")
max_raw = conn.execute("""
    SELECT MAX(CAST(REPLACE(loan_id, 'L0', '') AS INTEGER))
    FROM hlx_dev_raw.raw_loan WHERE loan_id LIKE 'L0%'
""").fetchone()[0]
print(f"  Max loan_id number in raw_loan: {max_raw}")
print(f"  If loans go L0000001–L0{max_raw:07d}, expected ~{max_raw} loans")
print(f"  Actual distinct loans: {raw_count}")
print(f"  Gap: {max_raw - raw_count} missing loan_ids in raw_loan")

conn.close()