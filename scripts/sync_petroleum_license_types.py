#!/usr/bin/env python3
"""
Sync and fix Petroleum sector applications and certificates where license_type
was misclassified as LICENSE_ELECTRICITY_SUPPLY or sector was misclassified as ELECTRICITY.

Usage:
    python3 scripts/sync_petroleum_license_types.py [--apply]
"""

import sys
import argparse
import psycopg2

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

def main():
    parser = argparse.ArgumentParser(description="Sync Petroleum license_type and sector fields.")
    parser.add_argument("--apply", action="store_true", help="Apply fixes to database (default is dry-run)")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("AUDITING PETROLEUM LICENSE_TYPE MISMATCHES ON 10.1.8.166")
    print("Mode:", "APPLY FIXES" if args.apply else "DRY-RUN (Pass --apply to execute)")
    print("=" * 80)

    query = """
    SELECT 
        a.id as app_id,
        a.application_number,
        a.approval_no,
        a.license_type as app_license_type,
        a.category_id,
        cat.name as category_name,
        cat.category_type,
        sec.name as sector_name,
        CASE 
            WHEN cat.category_type = 'Construction' THEN 'CONSTRUCTION_PETROLEUM'
            ELSE 'LICENSE_PETROLEUM'
        END as target_license_type,
        c.id as cert_id,
        c.license_type as cert_license_type,
        c.sector as cert_sector
    FROM applications a
    LEFT JOIN categories cat ON cat.id = a.category_id
    LEFT JOIN sectors sec ON sec.id = cat.sector_id
    LEFT JOIN certificates c ON c.application_id = a.id
    WHERE 
        (sec.name = 'PETROLEUM' OR a.approval_no ~* '^(PWL|PRL|PRLR|PSBL|PCIOL|VPS|LPG)')
        AND (
            a.license_type LIKE '%%ELECTRICITY%%' 
            OR c.license_type LIKE '%%ELECTRICITY%%' 
            OR c.sector = 'ELECTRICITY'
        )
    ORDER BY a.approval_no, a.created_at DESC;
    """

    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    print(f"Found {len(rows)} Petroleum records with misclassified license_type or sector:")

    for r in rows:
        print(f"\nApp ID: {r['app_id']} | App Number: {r['application_number']} | Approval No: {r['approval_no']}")
        print(f"  Category: {r['category_name']} (Sector: {r['sector_name']}, Type: {r['category_type']})")
        print(f"  Current App LicenseType: {r['app_license_type']}  -> Target: {r['target_license_type']}")
        print(f"  Current Cert LicenseType: {r['cert_license_type']}, Sector: {r['cert_sector']} -> Target Sector: PETROLEUM")

    if args.apply and rows:
        print("\nApplying fixes to database...")
        for r in rows:
            target_lt = r['target_license_type']

            # 1. Update applications table
            cur.execute("""
                UPDATE applications
                SET license_type = %s,
                    updated_at = now()
                WHERE id = %s;
            """, (target_lt, r['app_id']))

            # 2. Update certificates table if present
            if r['cert_id']:
                cur.execute("""
                    UPDATE certificates
                    SET license_type = %s,
                        sector = 'PETROLEUM',
                        updated_at = now()
                    WHERE id = %s;
                """, (target_lt, r['cert_id']))

        conn.commit()
        print(f"Successfully updated {len(rows)} records in applications & certificates.")

        print("\nRefreshing materialized views...")
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
        print("\nMaterialized view refresh complete!")
    elif not args.apply:
        print("\n[DRY RUN COMPLETE] Run with --apply to perform database updates.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
