#!/usr/bin/env python3
import psycopg2
conn = psycopg2.connect(host='10.1.8.144', port=5432, dbname='auth_migration_v2', user='appuser', password='ewura@123')
cur = conn.cursor()
cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename='categories' AND schemaname='public' ORDER BY indexname")
for r in cur.fetchall():
    print(r[0], '->', r[1][:140])
cur.close(); conn.close()
