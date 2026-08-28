#!/usr/bin/env python3
"""
Generate an Excel report of unrenewed licenses focusing on November 2025 onwards (expire_date >= '2025-11-01').

Includes:
1. EXPIRED: Licenses expired between Nov 1, 2025 and the month-end snapshot date and never renewed.
2. ELIGIBLE_TO_RENEW: Licenses in 180-day window (standard) or 90-day window (Electrical Installation) on the snapshot date.

Snapshot dates:
- June 30, 2026
- July 31, 2026

Database target: 10.1.8.166 (eservice_applications)
"""

import sys
import os
import json
from datetime import date, datetime

def main():
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 not found in current environment.")
        sys.exit(1)

    DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    def fetch_nov2025_report(snapshot_date_str):
        query = """
        SELECT DISTINCT ON (a.id)
            a.application_number,
            a.approval_no,
            COALESCE(asd.company_name, aei.applicant_name, asd.facility_name, c.certificate_owner, 'N/A') AS company_or_facility_name,
            COALESCE(c.sector, sec.name, 'PETROLEUM') AS sector,
            a.license_type,
            COALESCE(approved_cat.name, cat.name, 'N/A') AS category_name,
            COALESCE(cat.category_type, 'License') AS category_type,
            COALESCE(asd.region, a.region, 'N/A') AS region,
            COALESCE(asd.district, a.district, 'N/A') AS district,
            COALESCE(asd.ward, a.ward, 'N/A') AS ward,
            COALESCE(a.zone_name, z.name, 'N/A') AS zone_name,
            COALESCE(c.effective_date, c.approval_date, a.approval_date, a.effective_date) AS effective_date,
            COALESCE(c.expire_date, a.expire_date) AS expire_date,
            CASE 
                WHEN COALESCE(c.expire_date, a.expire_date) < %s::date THEN 'EXPIRED'
                ELSE 'ELIGIBLE_TO_RENEW'
            END AS renewal_status,
            CASE 
                WHEN a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR cat.name ~* 'Electrical Installation'
                    THEN 90
                ELSE 180
            END AS reminder_window_days,
            (
                CASE 
                    WHEN a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR cat.name ~* 'Electrical Installation'
                        THEN COALESCE(c.expire_date, a.expire_date) - INTERVAL '90 days'
                    ELSE COALESCE(c.expire_date, a.expire_date) - INTERVAL '180 days'
                END
            )::date AS intimate_date,
            CASE WHEN a.is_from_lois IS TRUE THEN 'LOIS Legacy' ELSE 'eService New' END AS origin
        FROM applications a
        LEFT JOIN categories cat ON cat.id = a.category_id
        LEFT JOIN sectors sec ON sec.id = cat.sector_id
        LEFT JOIN certificates c ON c.application_id = a.id
        LEFT JOIN zones z ON z.id = a.zone_id
        LEFT JOIN application_sector_details asd ON asd.application_id = a.id
        LEFT JOIN application_electrical_installation aei ON aei.application_id = a.id
        LEFT JOIN categories approved_cat ON approved_cat.id = aei.approved_class_id
        WHERE a.status = 'APPROVED'
          AND a.approval_no IS NOT NULL
          AND COALESCE(c.expire_date, a.expire_date) >= '2025-11-01'
          AND (
              CASE 
                  WHEN a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR cat.name ~* 'Electrical Installation'
                      THEN COALESCE(c.expire_date, a.expire_date) - INTERVAL '90 days'
                  ELSE COALESCE(c.expire_date, a.expire_date) - INTERVAL '180 days'
              END
          )::date <= %s::date
          AND NOT EXISTS (
              SELECT 1 FROM applications r
              WHERE r.approval_no = a.approval_no
                AND r.id != a.id
                AND r.status IN ('APPROVED', 'SUBMITTED', 'IN_PROGRESS', 'PAYMENT_PENDING')
                AND r.expire_date > COALESCE(c.expire_date, a.expire_date)
          )
        ORDER BY a.id, COALESCE(c.expire_date, a.expire_date) ASC;
        """
        cur.execute(query, (snapshot_date_str, snapshot_date_str))
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        
        for r in rows:
            for k, v in r.items():
                if isinstance(v, (date, datetime)):
                    r[k] = v.strftime("%Y-%m-%d")
        return rows

    june_rows = fetch_nov2025_report('2026-06-30')
    july_rows = fetch_nov2025_report('2026-07-31')

    cur.close()
    conn.close()

    data = {
        "june": june_rows,
        "july": july_rows
    }
    
    out_json = "/Users/lambert/Desktop/fast-api/ewura-migration/expired_data.json"
    with open(out_json, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Data dumped to {out_json}. June 30 total (Nov 2025+): {len(june_rows)}, July 31 total (Nov 2025+): {len(july_rows)}")

if __name__ == "__main__":
    main()
