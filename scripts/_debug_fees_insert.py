#!/usr/bin/env python3
"""Diagnose why fees are not inserting for ELECTRICITY FEES-UPDATED.xlsx"""
import sys
sys.path.insert(0, '/Users/lambert/Desktop/fast-api/ewura-migration')

import pandas as pd
import psycopg2

# --- 1. Read the Excel file the same way the service does ---
xlsx_path = '/Users/lambert/Desktop/fast-api/ewura-migration/app/data/ELECTRICITY FEES-UPDATED.xlsx'
df = pd.read_excel(xlsx_path, sheet_name=0)
df.columns = [str(c).strip().lower() for c in df.columns]
print(f"Excel rows: {len(df)},  columns: {list(df.columns)}")
print()

# --- 2. Connect to DB ---
conn = psycopg2.connect(
    host='10.1.8.144', port=5432, dbname='auth_migration_v2',
    user='appuser', password='ewura@123'
)
cur = conn.cursor()

# --- 3. Show current staging table content ---
cur.execute("""
    SELECT DISTINCT
        lower(btrim(categoryorclass)) AS cat,
        licencetype,
        acapacityfrom, acapacityto,
        prefix, licenseprefix, licenseperiod_y
    FROM public.stage_license_category_fees_raw
    ORDER BY cat, licencetype
""")
stage_rows = cur.fetchall()
print(f"=== STAGING TABLE ({len(stage_rows)} distinct rows) ===")
for r in stage_rows:
    print(f"  cat={r[0]!r:45s} lt={r[1]!r:12s} from={r[2]} to={r[3]} pfx={r[4]} lpfx={r[5]} mo={r[6]}")

# --- 4. Check what category_ids map to these names ---
cur.execute("""
    SELECT lower(btrim(c.name)) AS key_name, c.id, c.name
    FROM public.categories c
    WHERE c.deleted_at IS NULL
      AND lower(btrim(c.name)) IN (
          SELECT DISTINCT lower(btrim(categoryorclass))
          FROM public.stage_license_category_fees_raw
          WHERE categoryorclass IS NOT NULL
      )
    ORDER BY key_name
""")
cats = cur.fetchall()
print(f"\n=== MATCHING CATEGORIES ({len(cats)}) ===")
cat_ids = {}
for r in cats:
    cat_ids[r[0]] = r[1]
    print(f"  {r[0]!r:45s} -> {r[1]}")

# --- 5. Show existing fees for these categories ---
if cat_ids:
    ids_list = list(cat_ids.values())
    placeholders = ','.join(['%s'] * len(ids_list))
    cur.execute(f"""
        SELECT c.name, f.application_type, f.capacity_from, f.capacity_to,
               f.application_prefix, f.license_prefix, f.months_eligible, f.sector_type
        FROM public.application_category_fees f
        JOIN public.categories c ON c.id = f.category_id
        WHERE f.category_id IN ({placeholders})
          AND f.deleted_at IS NULL
        ORDER BY c.name, f.application_type
    """, ids_list)
    fees = cur.fetchall()
    print(f"\n=== EXISTING FEES ({len(fees)}) for these categories ===")
    for r in fees:
        print(f"  cat={r[0]!r:40s} app_type={r[1]!r:15s} from={r[2]} to={r[3]} pfx={r[4]} lpfx={r[5]} mo={r[6]} sector={r[7]}")

# --- 6. Simulate what the typed CTE produces and compare with existing ---
db_sector_name = 'ELECTRICITY'
cur.execute(f"""
    WITH typed AS (
        SELECT
            ids.category_id,
            upper(COALESCE(NULLIF(btrim(s.licencetype), ''), 'NEW')) AS application_type,
            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN btrim(s.acapacityfrom)::numeric ELSE 1 END AS capacity_from,
            CASE WHEN btrim(s.acapacityto)   ~ '^-?\\d+(\\.\\d+)?$' THEN btrim(s.acapacityto)::numeric   ELSE 10 END AS capacity_to,
            COALESCE(NULLIF(btrim(s.prefix), ''), 'AECBL') AS application_prefix,
            COALESCE(NULLIF(btrim(s.licenseprefix), ''), 'ECBL') AS license_prefix,
            CASE WHEN btrim(s.licenseperiod_y) ~ '^-?\\d+(\\.\\d+)?$' THEN btrim(s.licenseperiod_y)::bigint ELSE 36 END AS months_eligible,
            NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
            NULLIF(btrim(s.nocustomerto), '') AS no_customer_to
        FROM public.stage_license_category_fees_raw s
        JOIN (
            SELECT lower(btrim(c.name)) AS key_name, c.id AS category_id
            FROM public.categories c WHERE c.deleted_at IS NULL
        ) ids ON ids.key_name = lower(btrim(s.categoryorclass))
    ),
    deduped AS (
        SELECT DISTINCT ON (
            category_id, lower(btrim(application_type)),
            COALESCE(no_customer_from,''), COALESCE(no_customer_to,''),
            capacity_from, capacity_to, application_prefix, license_prefix, months_eligible
        ) * FROM typed
        ORDER BY category_id, lower(btrim(application_type)),
            COALESCE(no_customer_from,''), COALESCE(no_customer_to,''),
            capacity_from, capacity_to, application_prefix, license_prefix, months_eligible
    )
    SELECT
        d.*,
        EXISTS (
            SELECT 1 FROM public.application_category_fees f
            WHERE f.category_id = d.category_id
              AND lower(btrim(f.application_type)) = lower(btrim(d.application_type))
              AND COALESCE(f.capacity_from, -1) = COALESCE(d.capacity_from, -1)
              AND COALESCE(f.capacity_to, -1) = COALESCE(d.capacity_to, -1)
              AND COALESCE(f.no_customer_from, '') = COALESCE(d.no_customer_from, '')
              AND COALESCE(f.no_customer_to, '') = COALESCE(d.no_customer_to, '')
              AND COALESCE(f.application_prefix, '') = COALESCE(d.application_prefix, '')
              AND COALESCE(f.license_prefix, '') = COALESCE(d.license_prefix, '')
              AND COALESCE(f.months_eligible, 0) = COALESCE(d.months_eligible, 0)
              AND f.deleted_at IS NULL
        ) AS already_exists
    FROM deduped d
    ORDER BY d.category_id, d.application_type
""")
deduped_rows = cur.fetchall()
print(f"\n=== DEDUPED STAGING vs EXISTING ({len(deduped_rows)} rows) ===")
new_count = 0
for r in deduped_rows:
    cat_id, app_type, cap_from, cap_to, app_pfx, lic_pfx, months, nc_from, nc_to, exists = r
    # reverse-lookup name
    name = next((k for k, v in cat_ids.items() if str(v) == str(cat_id)), str(cat_id)[:8])
    flag = "ALREADY EXISTS" if exists else "*** WOULD INSERT ***"
    if not exists:
        new_count += 1
    print(f"  [{flag}] cat={name!r:40s} app={app_type!r:12s} from={cap_from} to={cap_to} pfx={app_pfx} lpfx={lic_pfx} mo={months}")

print(f"\nWould insert: {new_count}, Already exist: {len(deduped_rows) - new_count}")

cur.close()
conn.close()
