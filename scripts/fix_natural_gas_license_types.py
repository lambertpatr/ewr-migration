#!/usr/bin/env python3
"""
Update license_type from CONSTRUCTION_NATURAL_GAS to LICENSE_NATURAL_GAS
for Natural Gas operational licenses (CNGFSL, LCNGF, NGCL).

Database target: 10.1.8.166 (eservice_applications)
"""

import sys
import psycopg2

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

approval_nos = [
    'CNGFSL-2021-001',
    'CNGFSL-2025-001',
    'CNGFSL-2023-001',
    'LCNGF-2024-001',
    'NGCL-2024-001',
    'NGCL-2025-003',
    'NGCL-2025-006',
    'LCNGF-2026-001'
]

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("UPDATING NATURAL GAS LICENSE TYPES ON 10.1.8.166")
    print("=" * 80)

    # 1. Update applications
    cur.execute("""
        UPDATE applications
        SET license_type = 'LICENSE_NATURAL_GAS',
            updated_at = now()
        WHERE approval_no = ANY(%s) OR category_id IN (
            SELECT id FROM categories WHERE name = 'Compressed Natural Gas Filling Station Operations'
        );
    """, (approval_nos,))
    apps_updated = cur.rowcount
    print(f"Updated {apps_updated} applications to LICENSE_NATURAL_GAS.")

    # 2. Update certificates
    cur.execute("""
        UPDATE certificates
        SET license_type = 'LICENSE_NATURAL_GAS',
            sector = 'NATURAL_GAS',
            updated_at = now()
        WHERE approval_no = ANY(%s) OR application_id IN (
            SELECT id FROM applications WHERE category_id IN (
                SELECT id FROM categories WHERE name = 'Compressed Natural Gas Filling Station Operations'
            )
        );
    """, (approval_nos,))
    certs_updated = cur.rowcount
    print(f"Updated {certs_updated} certificates to LICENSE_NATURAL_GAS.")

    conn.commit()

    print("\nRefreshing Materialized Views...")
    cur.execute("REFRESH MATERIALIZED VIEW public.website_api_mv;")
    print("✓ Refreshed website_api_mv")

    try:
        cur.execute("REFRESH MATERIALIZED VIEW public.npgis_api_mv;")
        print("✓ Refreshed npgis_api_mv")
    except Exception as e:
        conn.rollback()
        print("Note on npgis_api_mv:", e)

    try:
        cur.execute("REFRESH MATERIALIZED VIEW public.tanesco_api_mv;")
        print("✓ Refreshed tanesco_api_mv")
    except Exception as e:
        conn.rollback()
        print("Note on tanesco_api_mv:", e)

    conn.commit()
    cur.close()
    conn.close()
    print("\nDatabase update and view refresh complete!")

if __name__ == "__main__":
    main()
