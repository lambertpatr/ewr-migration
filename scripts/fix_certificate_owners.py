#!/usr/bin/env python3
"""
Backfill and sync certificate_owner in certificates table:
1. For Electrical Installation (LICENSE_ELECTRICITY_INSTALLATION):
   certificate_owner = application_electrical_installation.applicant_name
2. For all other sectors (Petroleum, Natural Gas, Electricity Supply, Water Supply):
   certificate_owner = application_sector_details.facility_name
   (formatted with po_box if present: facility_name || ', ' || po_box)
   falling back to company_name / tin_name.

Database target: 10.1.8.166 (eservice_applications)
"""

import sys
import psycopg2

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("BACKFILLING & SYNCING CERTIFICATE_OWNER ON 10.1.8.166")
    print("=" * 80)

    # 1. Electrical Installation Sector: applicant_name
    cur.execute("""
        UPDATE certificates c
        SET certificate_owner = TRIM(aei.applicant_name),
            updated_at = now()
        FROM applications a
        JOIN application_electrical_installation aei ON aei.application_id = a.id
        WHERE c.application_id = a.id
          AND (c.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION')
          AND NULLIF(TRIM(aei.applicant_name), '') IS NOT NULL
          AND (c.certificate_owner IS NULL OR TRIM(c.certificate_owner) = '' OR c.certificate_owner IS DISTINCT FROM TRIM(aei.applicant_name));
    """)
    elec_updated = cur.rowcount
    print(f"Updated {elec_updated} Electrical Installation certificate owners.")

    # 2. Other Sectors (Petroleum, Natural Gas, Electricity Supply, Water Supply): facility_name (+ po_box) / company_name
    cur.execute("""
        UPDATE certificates c
        SET certificate_owner = TRIM(
                COALESCE(NULLIF(TRIM(asd.facility_name), ''), NULLIF(TRIM(asd.company_name), ''), NULLIF(TRIM(asd.tin_name), '')) ||
                CASE 
                    WHEN NULLIF(TRIM(asd.po_box), '') IS NOT NULL THEN ', ' || TRIM(asd.po_box)
                    ELSE ''
                END
            ),
            updated_at = now()
        FROM applications a
        JOIN application_sector_details asd ON asd.application_id = a.id
        WHERE c.application_id = a.id
          AND (c.license_type IS NULL OR c.license_type != 'LICENSE_ELECTRICITY_INSTALLATION')
          AND (NULLIF(TRIM(asd.facility_name), '') IS NOT NULL OR NULLIF(TRIM(asd.company_name), '') IS NOT NULL)
          AND (c.certificate_owner IS NULL OR TRIM(c.certificate_owner) = '');
    """)
    other_updated = cur.rowcount
    print(f"Updated {other_updated} other sector certificate owners.")

    conn.commit()

    print("\n" + "=" * 80)
    print("REFRESHING MATERIALIZED VIEWS...")
    print("=" * 80)

    cur.execute("REFRESH MATERIALIZED VIEW public.website_api_mv;")
    print("✓ Refreshed website_api_mv")

    try:
        cur.execute("REFRESH MATERIALIZED VIEW public.tanesco_api_mv;")
        print("✓ Refreshed tanesco_api_mv")
    except Exception as e:
        conn.rollback()
        print("Note on tanesco_api_mv:", e)

    conn.commit()
    cur.close()
    conn.close()

    print("\nCertificate owners backfill and view refresh complete!")

if __name__ == "__main__":
    main()
