#!/usr/bin/env python3
"""Verify that the voltage_level patch propagates to existing ELECTRICITY categories.

Run after uploading the Electricity Excel via the API so staging is populated,
OR run this script directly to simulate the DO UPDATE with :db_sector_name = 'ELECTRICITY'
by directly executing the enriched CTE query against the current staging table.
"""
import psycopg2

conn = psycopg2.connect(
    host="10.1.8.144", port=5432, dbname="auth_migration_v2",
    user="appuser", password="ewura@123"
)
conn.autocommit = False
cur = conn.cursor()

stage = "public.stage_license_category_fees_raw"
db_sector_name = "ELECTRICITY"

# Show what the enriched CTE would produce for voltage_level
cur.execute(f"""
    SELECT DISTINCT ON (lower(btrim(s.categoryorclass)))
        btrim(s.categoryorclass) AS display_name,
        CASE
            WHEN upper(btrim(COALESCE(s.voltage_level, ''))) IN ('HV', 'MV', 'LV_1', 'LV_3')
                THEN upper(btrim(s.voltage_level))
            WHEN %(db_sector_name)s = 'ELECTRICITY' THEN 'HV'
            ELSE NULL
        END AS computed_voltage_level
    FROM {stage} s
    WHERE s.categoryorclass IS NOT NULL AND btrim(s.categoryorclass) <> ''
    ORDER BY lower(btrim(s.categoryorclass))
""", {"db_sector_name": db_sector_name})

rows = cur.fetchall()
print(f"Enriched CTE voltage_level output ({len(rows)} categories):")
for name, vl in rows:
    print(f"  {name!r:55s} -> voltage_level={vl!r}")

# Check current state in categories table for these names
print("\nCurrent voltage_level in categories table for these names:")
for name, vl in rows:
    cur.execute("""
        SELECT c.name, c.voltage_level
        FROM public.categories c
        WHERE lower(btrim(c.name)) = lower(btrim(%s)) AND c.deleted_at IS NULL
    """, (name,))
    res = cur.fetchone()
    if res:
        print(f"  {res[0]!r:55s} -> DB voltage_level={res[1]!r}  (would update to {vl!r})")
    else:
        print(f"  {name!r:55s} -> NOT IN DB yet (would insert with voltage_level={vl!r})")

conn.rollback()
cur.close()
conn.close()
print("\nDone (read-only, no changes made).")
