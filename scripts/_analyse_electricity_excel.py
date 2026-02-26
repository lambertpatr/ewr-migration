#!/usr/bin/env python3
"""Analyse ELECTRICITY FEES-UPDATED.xlsx to understand why only 1 fee row inserted."""
import sys
sys.path.insert(0, '/Users/lambert/Desktop/fast-api/ewura-migration')

import pandas as pd
import psycopg2

xlsx_path = '/Users/lambert/Desktop/fast-api/ewura-migration/app/data/ELECTRICITY FEES-UPDATED.xlsx'
df = pd.read_excel(xlsx_path, sheet_name=0)
df.columns = [str(c).strip().lower() for c in df.columns]

print(f"Excel: {len(df)} rows")
print(f"Columns: {list(df.columns)}\n")
print("First 5 rows:")
print(df.head(5).to_string())
print()

# Find months col
months_col = "licenseperiod_x" if "licenseperiod_x" in df.columns else ("licenseperiod" if "licenseperiod" in df.columns else None)
print(f"months_col detected: {months_col}")

# Show distinct combos that would form unique fee slots
df2 = df.copy()
df2['_cat'] = df2['categoryorclass'].str.strip().str.lower()
df2['_lt']  = df2.get('licencetype', pd.Series([''] * len(df2))).fillna('').str.strip().str.upper()
cap_from_col = 'acapacityfrom' if 'acapacityfrom' in df2.columns else ('capacityfrom' if 'capacityfrom' in df2.columns else None)
cap_to_col   = 'acapacityto'   if 'acapacityto'   in df2.columns else ('capacityto'   if 'capacityto'   in df2.columns else None)
df2['_cfr'] = pd.to_numeric(df2.get(cap_from_col, pd.Series([None]*len(df2))), errors='coerce').fillna(1)
df2['_cto'] = pd.to_numeric(df2.get(cap_to_col,   pd.Series([None]*len(df2))), errors='coerce').fillna(10)
df2['_pfx'] = df2.get('prefix',       pd.Series(['']*len(df2))).fillna('').str.strip().str.upper()
df2['_lpfx']= df2.get('licenseprefix',pd.Series(['']*len(df2))).fillna('').str.strip().str.upper()
df2['_mo']  = pd.to_numeric(df2.get(months_col, pd.Series([None]*len(df2))), errors='coerce').fillna(36).astype(int)

df2['_at']  = df2.get('applicationtype', pd.Series(['NEW']*len(df2))).fillna('NEW').str.strip().str.upper()

key_cols = ['_cat','_lt','_at','_cfr','_cto','_pfx','_lpfx','_mo']
unique_slots = df2.drop_duplicates(subset=key_cols)
print(f"\nUnique fee slots in Excel: {len(unique_slots)}")
display_cols = ['categoryorclass','licencetype']
if cap_from_col: display_cols.append(cap_from_col)
if cap_to_col:   display_cols.append(cap_to_col)
if 'applicationtype' in df.columns: display_cols.append('applicationtype')
display_cols += ['prefix','licenseprefix', months_col or 'licenseperiod_y']
display_cols = [c for c in display_cols if c in unique_slots.columns]
print(unique_slots[display_cols].to_string())

# Compare against what's in the DB
conn = psycopg2.connect(host='10.1.8.144', port=5432, dbname='auth_migration_v2', user='appuser', password='ewura@123')
cur = conn.cursor()

cur.execute("""
    SELECT c.name, f.application_type, f.license_type, f.capacity_from, f.capacity_to,
           f.application_prefix, f.license_prefix, f.months_eligible
    FROM public.application_category_fees f
    JOIN public.categories c ON c.id = f.category_id
    WHERE f.deleted_at IS NULL
      AND lower(btrim(c.name)) IN (
          SELECT DISTINCT lower(btrim(categoryorclass))
          FROM public.stage_license_category_fees_raw
          WHERE categoryorclass IS NOT NULL
      )
    ORDER BY c.name, f.license_type, f.application_type
""")
existing = cur.fetchall()
print(f"\nExisting fees for these categories in DB: {len(existing)}")
for r in existing:
    print(f"  cat={r[0]!r:45s} app={r[1]!r:8s} lt={r[2]!r:15s} from={r[3]} to={r[4]} pfx={r[5]} lpfx={r[6]} mo={r[7]}")

cur.close()
conn.close()
