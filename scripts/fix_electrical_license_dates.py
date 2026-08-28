#!/usr/bin/env python3
"""
Audit and fix Electrical Installation and Electricity sector application expiration dates.
Ensures Electrical licenses use standard 5-year duration from approval/effective date.

Usage:
    python3 scripts/fix_electrical_license_dates.py [--apply]
"""

import sys
import argparse
import psycopg2
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

def main():
    parser = argparse.ArgumentParser(description="Audit and repair Electrical license expiration dates.")
    parser.add_argument("--apply", action="store_true", help="Apply fixes to database (default is dry-run)")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("AUDITING ELECTRICAL LICENSES ON 10.1.8.166")
    print("Mode:", "APPLY FIXES" if args.apply else "DRY-RUN (Pass --apply to execute)")
    print("=" * 80)

    # Fetch all approved Electrical applications where duration is < 4.9 years or expired
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
        c.id as cert_id,
        c.expire_date as cert_expire_date,
        (a.expire_date - a.effective_date) as duration_days
    FROM applications a
    LEFT JOIN certificates c ON c.application_id = a.id
    WHERE a.status = 'APPROVED'
      AND (a.license_type LIKE '%%ELECTRICITY%%' OR a.approval_no LIKE 'EL%%' OR a.approval_no LIKE 'PEGL%%')
    ORDER BY a.approval_no, a.created_at DESC;
    """

    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    fixes = []

    for r in rows:
        app_id = r['id']
        app_num = r['application_number']
        apprv_no = r['approval_no']
        approval_date = r['approval_date']
        curr_eff = r['effective_date']
        curr_exp = r['expire_date']
        dur_days = r['duration_days']

        # Effective date base
        eff_base = approval_date if approval_date else curr_eff
        if not eff_base:
            continue

        # Target 5 years expiration: eff_base + 5 years - 1 day
        target_exp = eff_base + relativedelta(years=5) - timedelta(days=1)

        # Check if current expire date differs from target 5-year expiration date
        if curr_exp != target_exp or curr_eff != eff_base:
            fixes.append({
                'app_id': app_id,
                'app_num': app_num,
                'apprv_no': apprv_no,
                'cert_id': r['cert_id'],
                'curr_eff': curr_eff,
                'curr_exp': curr_exp,
                'new_eff': eff_base,
                'new_exp': target_exp,
                'dur_days': dur_days
            })

    print(f"Found {len(fixes)} Electrical applications needing 5-year duration alignment:")

    for f in fixes:
        print(f"\nApp ID: {f['app_id']} | App Number: {f['app_num']} | Approval No: {f['apprv_no']}")
        print(f"  Current: Effective={f['curr_eff']}, Expire={f['curr_exp']} (Duration: {f['dur_days']} days)")
        print(f"  Fix -> : Effective={f['new_eff']}, Expire={f['new_exp']} (5 years)")

    if args.apply and fixes:
        print("\nApplying fixes to database...")
        for f in fixes:
            cur.execute("""
                UPDATE applications
                SET effective_date = %s,
                    expire_date = %s,
                    months_eligible = 60,
                    updated_at = now()
                WHERE id = %s;
            """, (f['new_eff'], f['new_exp'], f['app_id']))

            if f['cert_id']:
                cur.execute("""
                    UPDATE certificates
                    SET effective_date = %s,
                        expire_date = %s,
                        months_eligible = 60,
                        updated_at = now()
                    WHERE id = %s;
                """, (f['new_eff'], f['new_exp'], f['cert_id']))

        conn.commit()
        print(f"Successfully updated {len(fixes)} Electrical records in applications & certificates.")

        print("Refreshing materialized views...")
        cur.execute("REFRESH MATERIALIZED VIEW public.website_api_mv;")
        try:
            cur.execute("REFRESH MATERIALIZED VIEW public.tanesco_api_mv;")
        except Exception as e:
            conn.rollback()
            print("Note on tanesco_api_mv refresh:", e)
        else:
            conn.commit()
        print("Materialized views refreshed successfully!")
    elif not args.apply:
        print("\n[DRY RUN COMPLETE] Run with --apply to perform database updates.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
