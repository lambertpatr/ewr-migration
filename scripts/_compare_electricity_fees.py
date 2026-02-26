#!/usr/bin/env python3
"""
Compare ELECTRICITY FEES-UPDATED.xlsx against what's now in the DB.
Shows exactly which 9 Excel rows were NOT inserted and why.
"""
import sys
sys.path.insert(0, '/Users/lambert/Desktop/fast-api/ewura-migration')

import pandas as pd
import psycopg2

xlsx_path = '/Users/lambert/Desktop/fast-api/ewura-migration/app/data/ELECTRICITY FEES-UPDATED.xlsx'
df = pd.read_excel(xlsx_path, sheet_name=0)
df.columns = [str(c).strip().lower() for c in df.columns]

# Detect column aliases
cap_from_col = next((c for c in ['acapacityfrom', 'capacityfrom', 'capacity_from'] if c in df.columns), None)
cap_to_col   = next((c for c in ['acapacityto',   'capacityto',   'capacity_to']   if c in df.columns), None)
months_col   = next((c for c in ['licenseperiod_x', 'licenseperiod', 'licenseperiod_y'] if c in df.columns), None)

print(f"Excel rows  : {len(df)}")
print(f"cap_from_col: {cap_from_col}")
print(f"cap_to_col  : {cap_to_col}")
print(f"months_col  : {months_col}")
print()

# --------------------------------------------------------------------------
# Build the same key the service uses:
#   (category_name, license_type UPPER, application_type UPPER,
#    capacity_from numeric, capacity_to numeric,
#    application_prefix UPPER, license_prefix UPPER, months int)
# --------------------------------------------------------------------------
def clean_num(val, default):
    try:
        s = str(val).replace(',', '').replace(' ', '').strip()
        if s in ('', 'nan', 'None', 'Above', 'above'):
            return default
        return float(s)
    except Exception:
        return default

rows = []
for _, r in df.iterrows():
    cat   = str(r.get('categoryorclass', '') or '').strip().lower()
    lt    = str(r.get('licencetype',     '') or '').strip().upper()
    at    = str(r.get('applicationtype', '') or '').strip().upper() or 'NEW'
    cfr   = clean_num(r.get(cap_from_col) if cap_from_col else None, 1)
    cto   = clean_num(r.get(cap_to_col)   if cap_to_col   else None, 10)
    pfx   = str(r.get('prefix',        '') or '').strip().upper()
    lpfx  = str(r.get('licenseprefix', '') or '').strip().upper()
    mo    = int(clean_num(r.get(months_col) if months_col else None, 36))

    # Strip non-alphanum from license_type (mirrors the SQL regexp_replace)
    import re
    lt_clean = re.sub(r'_+$|^_+', '', re.sub(r'[^A-Z0-9]+', '_', lt)).strip('_') or None
    at_clean = at

    rows.append({
        'cat': cat,
        'lt': lt_clean,
        'at': at_clean,
        'cfr': cfr,
        'cto': cto,
        'pfx': pfx,
        'lpfx': lpfx,
        'mo': mo,
        '_raw_cat': str(r.get('categoryorclass', '') or '').strip(),
        '_raw_lt':  str(r.get('licencetype',     '') or '').strip(),
        '_raw_at':  str(r.get('applicationtype', '') or '').strip(),
        '_raw_cfr': str(r.get(cap_from_col) if cap_from_col else '') ,
        '_raw_cto': str(r.get(cap_to_col)   if cap_to_col   else '') ,
    })

excel_df = pd.DataFrame(rows)

# Deduplicate the same way SQL DISTINCT ON does
key_cols = ['cat','lt','at','cfr','cto','pfx','lpfx','mo']
excel_deduped = excel_df.drop_duplicates(subset=key_cols)
print(f"Excel unique fee slots (after dedup): {len(excel_deduped)}")
print()

# --------------------------------------------------------------------------
# Pull what's actually in DB for electricity categories
# --------------------------------------------------------------------------
conn = psycopg2.connect(
    host='10.1.8.144', port=5432, dbname='auth_migration_v2',
    user='appuser', password='ewura@123'
)
cur = conn.cursor()

cur.execute("""
    SELECT
        lower(btrim(c.name))                               AS cat,
        lower(btrim(COALESCE(f.license_type, '')))         AS lt,
        lower(btrim(f.application_type))                   AS at,
        COALESCE(f.capacity_from, -1)::float               AS cfr,
        COALESCE(f.capacity_to,   -1)::float               AS cto,
        upper(btrim(COALESCE(f.application_prefix, '')))   AS pfx,
        upper(btrim(COALESCE(f.license_prefix, '')))       AS lpfx,
        COALESCE(f.months_eligible, 0)                     AS mo,
        f.id,
        f.application_type,
        f.license_type,
        f.capacity_from,
        f.capacity_to
    FROM public.application_category_fees f
    JOIN public.categories c ON c.id = f.category_id
    JOIN public.sectors s ON s.id = c.sector_id
    WHERE f.deleted_at IS NULL
      AND lower(btrim(s.name)) = 'electricity'
    ORDER BY c.name, f.license_type, f.application_type
""")
db_rows = cur.fetchall()
print(f"DB electricity fees (total, all app_types): {len(db_rows)}")

db_set = set()
for r in db_rows:
    # build same key — DB already has UPPER for app_type/lt in stored value
    # but we compare lower(btrim(...)) as the service does
    db_set.add((
        r[0],                         # cat lower
        r[1],                         # lt  lower
        r[2],                         # at  lower
        float(r[3]),                  # cfr
        float(r[4]),                  # cto
        r[5],                         # pfx upper
        r[6],                         # lpfx upper
        int(r[7]),                    # mo
    ))

# --------------------------------------------------------------------------
# Find missing: Excel slots NOT in DB
# --------------------------------------------------------------------------
missing = []
for _, row in excel_deduped.iterrows():
    key = (
        row['cat'],
        (row['lt'] or '').lower(),
        row['at'].lower(),
        float(row['cfr']),
        float(row['cto']),
        row['pfx'],
        row['lpfx'],
        int(row['mo']),
    )
    if key not in db_set:
        missing.append(row)

print(f"\n{'='*80}")
print(f"MISSING FROM DB ({len(missing)} rows):")
print(f"{'='*80}")
if not missing:
    print("None — all Excel rows are present in DB!")
else:
    for r in missing:
        print(
            f"  cat={r['_raw_cat']!r:50s} lt={r['_raw_lt']!r:14s} "
            f"at={r['_raw_at']!r:10s} cfr={r['cfr']} cto={r['cto']} "
            f"pfx={r['pfx']} lpfx={r['lpfx']} mo={r['mo']}"
        )

# --------------------------------------------------------------------------
# Also show what IS in DB that has NO match in Excel (extra/stale rows)
# --------------------------------------------------------------------------
excel_set = set()
for _, row in excel_deduped.iterrows():
    excel_set.add((
        row['cat'],
        (row['lt'] or '').lower(),
        row['at'].lower(),
        float(row['cfr']),
        float(row['cto']),
        row['pfx'],
        row['lpfx'],
        int(row['mo']),
    ))

extras = [r for r in db_rows if (r[0], r[1], r[2], float(r[3]), float(r[4]), r[5], r[6], int(r[7])) not in excel_set]
print(f"\n{'='*80}")
print(f"DB rows with NO match in Excel ({len(extras)} rows) — these were inserted from a prior import or different sector:")
print(f"{'='*80}")
for r in extras[:30]:
    print(f"  cat={r[0]!r:50s} lt={r[2]!r:10s} at={r[1]!r:10s} cfr={r[3]} cto={r[4]} pfx={r[5]} lpfx={r[6]} mo={r[7]}")
if len(extras) > 30:
    print(f"  ... and {len(extras)-30} more")

cur.close()
conn.close()
