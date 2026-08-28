#!/usr/bin/env python3
"""
Audit and fix invalid/expired application dates and refresh website materialized views.

Usage:
    python3 scripts/audit_and_fix_expiration_dates.py [--apply]
"""

import sys
import argparse
import psycopg2
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

def get_duration_years(license_type, months_eligible=None):
    if months_eligible and int(months_eligible) > 0:
        return int(months_eligible) // 12
    
    if not license_type:
        return 5
    
    lt = str(license_type).upper()
    if 'CONSTRUCTION' in lt:
        return 2
    elif 'PETROLEUM' in lt:
        return 5
    elif 'ELECTRICITY' in lt:
        return 5
    elif 'WATER' in lt:
        return 5
    return 5

def main():
    parser = argparse.ArgumentParser(description="Audit and repair application expiration dates.")
    parser.add_argument("--apply", action="store_true", help="Apply fixes to database (default is dry-run)")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("AUDITING APPLICATIONS ON 10.1.8.166")
    print("Mode:", "APPLY FIXES" if args.apply else "DRY-RUN (Pass --apply to execute)")
    print("=" * 80)

    # 1. PWL-2016-006 & similar renewed applications where expire_date is <= CURRENT_DATE
    query = """
    SELECT 
        a.id,
        a.application_number,
        a.approval_no,
        a.application_type,
        a.license_type,
        a.status,
        a.is_from_lois,
        a.approval_date,
        a.effective_date,
        a.expire_date,
        a.months_eligible,
        a.created_at,
        c.id as cert_id,
        c.expire_date as cert_expire_date
    FROM applications a
    LEFT JOIN certificates c ON c.application_id = a.id
    WHERE a.status = 'APPROVED'
      AND (
          (a.is_from_lois = false AND a.expire_date < CURRENT_DATE)
          OR (a.application_type = 'RENEW' AND a.approval_date >= '2024-01-01' AND a.expire_date < CURRENT_DATE)
          OR (a.approval_date IS NOT NULL AND a.expire_date <= a.approval_date)
      )
    ORDER BY a.approval_no, a.created_at DESC;
    """

    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    print(f"Found {len(rows)} candidate applications for fix:")
    
    fixes = []

    for r in rows:
        app_id = r['id']
        app_num = r['application_number']
        apprv_no = r['approval_no']
        license_type = r['license_type']
        approval_date = r['approval_date']
        curr_eff = r['effective_date']
        curr_exp = r['expire_date']
        months = r['months_eligible']

        duration_years = get_duration_years(license_type, months)

        # Base effective date logic
        new_eff = None
        if approval_date:
            new_eff = approval_date
        elif curr_eff:
            # If current effective date was set to e.g. 2021-04-30 and expired 2026-04-29
            # and today is 2026-07-29, move forward by 5 years to current cycle 2026-04-30
            if isinstance(curr_eff, (datetime, date)) and isinstance(curr_exp, (datetime, date)):
                if curr_exp < date.today():
                    new_eff = curr_exp + timedelta(days=1)
                else:
                    new_eff = curr_eff
        
        if not new_eff:
            new_eff = date.today()

        new_exp = new_eff + relativedelta(years=duration_years) - timedelta(days=1)

        print(f"\nApp ID: {app_id} | App Number: {app_num} | Approval No: {apprv_no}")
        print(f"  Current: Effective={curr_eff}, Expire={curr_exp}, ApprovalDate={approval_date}")
        print(f"  Proposed Fix: Effective={new_eff}, Expire={new_exp} ({duration_years} years)")

        fixes.append({
            'app_id': app_id,
            'app_num': app_num,
            'apprv_no': apprv_no,
            'cert_id': r['cert_id'],
            'new_eff': new_eff,
            'new_exp': new_exp
        })

    if args.apply and fixes:
        print("\nApplying fixes to database...")
        for f in fixes:
            cur.execute("""
                UPDATE applications
                SET effective_date = %s,
                    expire_date = %s,
                    updated_at = now()
                WHERE id = %s;
            """, (f['new_eff'], f['new_exp'], f['app_id']))

            if f['cert_id']:
                cur.execute("""
                    UPDATE certificates
                    SET effective_date = %s,
                        expire_date = %s,
                        updated_at = now()
                    WHERE id = %s;
                """, (f['new_eff'], f['new_exp'], f['cert_id']))

        conn.commit()
        print(f"Successfully updated {len(fixes)} records in applications & certificates.")

        print("Refreshing materialized views...")
        cur.execute("REFRESH MATERIALIZED VIEW public.website_api_mv;")
        try:
            cur.execute("REFRESH MATERIALIZED VIEW public.npgis_api_mv;")
        except Exception as e:
            conn.rollback()
            print("Note on npgis_api_mv refresh:", e)
        else:
            conn.commit()
        print("Materialized views refreshed successfully!")
    elif not args.apply:
        print("\n[DRY RUN COMPLETE] Run with --apply to perform database updates.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
