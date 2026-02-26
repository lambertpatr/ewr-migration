#!/usr/bin/env python3
"""Debug categories / staging table state."""
import psycopg2

conn = psycopg2.connect(
    host="10.1.8.144", port=5432, dbname="auth_migration_v2",
    user="appuser", password="ewura@123"
)
cur = conn.cursor()

# 1. All ELECTRICITY categories
cur.execute("""
    SELECT c.name, c.code, c.voltage_level, c.category_type
    FROM public.categories c
    JOIN public.sectors sec ON sec.id = c.sector_id
    WHERE sec.name = 'ELECTRICITY' AND c.deleted_at IS NULL
    ORDER BY c.name
""")
print("=== ELECTRICITY CATEGORIES ===")
for row in cur.fetchall():
    print(f"  name={row[0]}  code={row[1]}  voltage_level={row[2]}  type={row[3]}")

# 2. Distinct licencetype + categoryorclass in stage
cur.execute("""
    SELECT DISTINCT licencetype, categoryorclass
    FROM public.stage_license_category_fees_raw
    ORDER BY licencetype, categoryorclass
""")
print("\n=== STAGE licencetype x categoryorclass ===")
for row in cur.fetchall():
    print(f"  licencetype={row[0]}  cat={row[1]}")

# 3. How many total rows + distinct names in stage
cur.execute("""
    SELECT COUNT(*), COUNT(DISTINCT lower(btrim(categoryorclass)))
    FROM public.stage_license_category_fees_raw
    WHERE categoryorclass IS NOT NULL AND btrim(categoryorclass) <> ''
""")
row = cur.fetchone()
print(f"\nStage: {row[0]} rows, {row[1]} distinct category names")

# 4. Sectors in DB
cur.execute("SELECT name FROM public.sectors ORDER BY name")
print("\n=== SECTORS IN DB ===")
for row in cur.fetchall():
    print(f"  {row[0]}")

cur.close()
conn.close()
print("\nDone.")
