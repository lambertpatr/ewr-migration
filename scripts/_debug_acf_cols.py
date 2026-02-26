#!/usr/bin/env python3
import psycopg2
conn = psycopg2.connect(host='10.1.8.144', port=5432, dbname='auth_migration_v2', user='appuser', password='ewura@123')
cur = conn.cursor()
cur.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='application_category_fees'
    ORDER BY ordinal_position
""")
print("=== application_category_fees columns ===")
for r in cur.fetchall():
    print(f"  {r[0]:35s} {r[1]:20s} nullable={r[2]}  default={r[3]}")
cur.close()
conn.close()
