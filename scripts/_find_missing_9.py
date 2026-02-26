#!/usr/bin/env python3
"""Find which 9 Excel rows did not get inserted into the DB fees table."""
import sys, os
# Prevent uvicorn/app imports from running
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

import pandas as pd
import psycopg2

xlsx = '/Users/lambert/Desktop/fast-api/ewura-migration/app/data/ELECTRICITY FEES-UPDATED.xlsx'
df = pd.read_excel(xlsx, sheet_name=0)
df.columns = [str(c).strip().lower() for c in df.columns]

cap_from = 'capacityfrom' if 'capacityfrom' in df.columns else 'acapacityfrom'
cap_to   = 'capacityto'   if 'capacityto'   in df.columns else 'acapacityto'

conn = psycopg2.connect(host='10.1.8.144', port=5432, dbname='auth_migration_v2', user='appuser', password='ewura@123')
cur = conn.cursor()

cur.execute("""
    SELECT c.name, f.application_type, f.license_type,
           f.capacity_from, f.capacity_to,
           f.application_prefix, f.license_prefix, f.months_eligible
    FROM public.application_category_fees f
    JOIN public.categories c ON c.id = f.category_id
    JOIN public.sectors sec ON sec.id = c.sector_id
    WHERE f.deleted_at IS NULL AND upper(sec.name) = 'ELECTRICITY'
    ORDER BY c.name, f.license_type, f.application_type
""")
db_fees = cur.fetchall()
print(f"Total electricity fees in DB now: {len(db_fees)}")

# For each Excel row, check if it found a DB match
missing = []
for idx, row in df.iterrows():
    cat = str(row.get('categoryorclass', '')).strip().lower()
    lt  = str(row.get('licencetype', '')).strip().upper()
    at  = str(row.get('applicationtype', '')).strip().upper()
    pfx  = str(row.get('prefix', '')).strip().upper()
    lpfx = str(row.get('licenseprefix', '')).strip().upper()

    try:
        cfr = float(str(row.get(cap_from, 1)).strip().replace(',', '').replace(' ', ''))
    except:
        cfr = 1.0

    cto_raw = str(row.get(cap_to, '10')).strip()
    is_above = cto_raw.strip().upper() == 'ABOVE'
    try:
        cto_num = float(cto_raw.replace(',', '').replace(' ', '')) if not is_above else None
    except:
        cto_num = None

    matched = False
    for dbr in db_fees:
        db_cat  = str(dbr[0] or '').strip().lower()
        db_at   = str(dbr[1] or '').strip().upper()
        db_lt   = str(dbr[2] or '').strip().upper()
        db_cfr  = float(dbr[3]) if dbr[3] is not None else 1.0
        db_cto  = dbr[4]  # numeric or None
        db_pfx  = str(dbr[5] or '').strip().upper()
        db_lpfx = str(dbr[6] or '').strip().upper()

        if (db_cat == cat and db_at == at and db_lt == lt
                and db_pfx == pfx and db_lpfx == lpfx
                and abs(db_cfr - cfr) < 0.01):
            matched = True
            break

    if not matched:
        missing.append({
            'row': idx, 'cat': cat, 'lt': lt, 'at': at,
            'cfr': cfr, 'cto': cto_raw, 'pfx': pfx, 'lpfx': lpfx
        })

print(f"\nExcel rows NOT found in DB: {len(missing)}")
for m in missing:
    print(f"  row={m['row']:3d}  cat={m['cat']!r:52s}  lt={m['lt']!r:13s}  at={m['at']!r:10s}  pfx={m['pfx']:7s}  cfr={m['cfr']}  cto={m['cto']}")

# Also show what deduplication might have caused — check for duplicate Excel keys
print("\n--- Checking for duplicate keys within Excel (dedup would collapse these) ---")
df2 = df.copy()
df2['_key'] = (
    df2['categoryorclass'].str.strip().str.lower() + '|' +
    df2.get('licencetype', pd.Series([''] * len(df2))).fillna('').str.strip().str.upper() + '|' +
    df2.get('applicationtype', pd.Series(['NEW'] * len(df2))).fillna('NEW').str.strip().str.upper() + '|' +
    df2.get(cap_from, pd.Series([1] * len(df2))).astype(str).str.strip() + '|' +
    df2.get(cap_to,   pd.Series([10] * len(df2))).astype(str).str.strip() + '|' +
    df2.get('prefix', pd.Series([''] * len(df2))).fillna('').str.strip().str.upper() + '|' +
    df2.get('licenseprefix', pd.Series([''] * len(df2))).fillna('').str.strip().str.upper()
)
dupes = df2[df2.duplicated('_key', keep=False)].sort_values('_key')
print(f"Rows with duplicate keys (same slot, different fees?): {len(dupes)}")
if len(dupes):
    for _, r in dupes.iterrows():
        print(f"  key={r['_key']!r}")

cur.close()
conn.close()
