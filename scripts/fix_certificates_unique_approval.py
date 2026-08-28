#!/usr/bin/env python3
"""
Database Repair Script for certificates & applications:
1. Ensure approval_no is UNIQUE in certificates table (deduplicate duplicate certificate rows).
2. For each unique approval_no, update certificates with the LATEST application's:
   - application_id
   - approval_date
   - effective_date
   - expire_date
   - intimate_date
   - owner_id / license_type / category_license_type / sector
3. Update ALL applications sharing the same approval_no (NEW, RENEW, etc.) so that
   applications.certificate_id references the single primary certificates.id.
4. Add a UNIQUE index / constraint on certificates(approval_no) where approval_no IS NOT NULL.
5. Refresh materialized views (website_api_mv, tanesco_api_mv, npgis_api_mv).

Usage:
    python3 scripts/fix_certificates_unique_approval.py [--apply]
"""

import sys
import argparse
import psycopg2

DB_URL = "postgresql://appuser:ewura%40123@10.1.8.166:5432/eservice_applications"

def main():
    parser = argparse.ArgumentParser(description="Fix certificates table to ensure unique approval_no and latest app dates.")
    parser.add_argument("--apply", action="store_true", help="Apply database changes (default is dry-run)")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("=" * 80)
    print("AUDITING & FIXING CERTIFICATES & APPLICATIONS ON 10.1.8.166")
    print("Mode:", "APPLY FIXES" if args.apply else "DRY-RUN (Pass --apply to execute)")
    print("=" * 80)

    # 1. Audit duplicate approval_no in certificates
    cur.execute("""
        SELECT approval_no, count(*)
        FROM certificates
        WHERE approval_no IS NOT NULL AND approval_no != ''
        GROUP BY approval_no
        HAVING count(*) > 1;
    """)
    duplicates = cur.fetchall()
    print(f"1. Duplicate approval_no in certificates count: {len(duplicates)}")

    # 2. Audit applications missing or mismatched certificate_id
    cur.execute("""
        SELECT count(DISTINCT a.id)
        FROM applications a
        LEFT JOIN certificates c ON c.id = a.certificate_id
        WHERE a.approval_no IS NOT NULL AND a.approval_no != ''
          AND (a.certificate_id IS NULL OR c.approval_no IS DISTINCT FROM a.approval_no);
    """)
    mismatched_apps = cur.fetchone()[0]
    print(f"2. Applications with missing/mismatched certificate_id: {mismatched_apps}")

    # 3. Audit certificates not matching latest application dates or application_id
    cur.execute("""
        WITH latest_app AS (
            SELECT DISTINCT ON (approval_no)
                approval_no,
                id as latest_app_id,
                created_by,
                COALESCE(effective_date, approval_date) as eff_date,
                COALESCE(approval_date, effective_date) as app_date,
                expire_date as exp_date,
                license_type,
                category_license_type
            FROM applications
            WHERE approval_no IS NOT NULL AND approval_no != ''
            ORDER BY approval_no, 
                     CASE WHEN status = 'APPROVED' THEN 1 ELSE 2 END,
                     expire_date DESC NULLS LAST, 
                     created_at DESC
        )
        SELECT count(*)
        FROM certificates c
        JOIN latest_app la ON la.approval_no = c.approval_no
        WHERE c.application_id != la.latest_app_id
           OR c.expire_date IS DISTINCT FROM la.exp_date
           OR c.effective_date IS DISTINCT FROM la.eff_date;
    """)
    outdated_certs = cur.fetchone()[0]
    print(f"3. Certificates out of sync with latest application: {outdated_certs}")

    if not args.apply:
        print("\n[DRY RUN COMPLETE] Pass --apply to perform database updates.")
        cur.close()
        conn.close()
        return

    print("\n" + "=" * 80)
    print("EXECUTING DATABASE FIXES...")
    print("=" * 80)

    # STEP A: Re-link foreign key references in applications and certificate_owner_change_log
    print("Step A: Re-linking foreign keys pointing to duplicate certificate rows...")
    cur.execute("""
        WITH ranked_certs AS (
            SELECT 
                c.id as cert_id,
                c.approval_no,
                c.application_id,
                ROW_NUMBER() OVER (
                    PARTITION BY c.approval_no 
                    ORDER BY 
                        CASE WHEN a.id IS NOT NULL THEN 1 ELSE 2 END,
                        c.created_at ASC, 
                        c.id ASC
                ) as rn
            FROM certificates c
            LEFT JOIN applications a ON a.id = c.application_id
            WHERE c.approval_no IS NOT NULL AND c.approval_no != ''
        ),
        primary_certs AS (
            SELECT approval_no, cert_id as primary_cert_id
            FROM ranked_certs
            WHERE rn = 1
        ),
        duplicate_certs AS (
            SELECT rc.cert_id as dup_cert_id, pc.primary_cert_id, rc.approval_no
            FROM ranked_certs rc
            JOIN primary_certs pc ON pc.approval_no = rc.approval_no
            WHERE rc.rn > 1
        )
        UPDATE applications app
        SET certificate_id = dc.primary_cert_id
        FROM duplicate_certs dc
        WHERE app.certificate_id = dc.dup_cert_id;
    """)
    print("  -> Re-linked applications pointing to duplicate certificate rows.")

    cur.execute("""
        WITH ranked_certs AS (
            SELECT 
                c.id as cert_id,
                c.approval_no,
                ROW_NUMBER() OVER (
                    PARTITION BY c.approval_no 
                    ORDER BY c.created_at ASC, c.id ASC
                ) as rn
            FROM certificates c
            WHERE c.approval_no IS NOT NULL AND c.approval_no != ''
        ),
        primary_certs AS (
            SELECT approval_no, cert_id as primary_cert_id
            FROM ranked_certs
            WHERE rn = 1
        ),
        duplicate_certs AS (
            SELECT rc.cert_id as dup_cert_id, pc.primary_cert_id
            FROM ranked_certs rc
            JOIN primary_certs pc ON pc.approval_no = rc.approval_no
            WHERE rc.rn > 1
        )
        UPDATE certificate_owner_change_log cocl
        SET certificate_id = dc.primary_cert_id
        FROM duplicate_certs dc
        WHERE cocl.certificate_id = dc.dup_cert_id;
    """)
    print("  -> Re-linked certificate_owner_change_log pointing to duplicate certificate rows.")

    # Delete duplicate certificate rows
    cur.execute("""
        WITH ranked_certs AS (
            SELECT 
                c.id as cert_id,
                c.approval_no,
                ROW_NUMBER() OVER (
                    PARTITION BY c.approval_no 
                    ORDER BY c.created_at ASC, c.id ASC
                ) as rn
            FROM certificates c
            WHERE c.approval_no IS NOT NULL AND c.approval_no != ''
        )
        DELETE FROM certificates
        WHERE id IN (
            SELECT cert_id FROM ranked_certs WHERE rn > 1
        );
    """)
    deleted_dups = cur.rowcount
    print(f"  -> Deleted {deleted_dups} duplicate certificate rows.")

    # STEP B: Ensure every application with an approval_no has a certificate record
    print("\nStep B: Creating missing certificate records for applications with approval_no...")
    cur.execute("""
        INSERT INTO certificates (
            id, created_at, updated_at, application_id, owner_id, sector,
            application_number, approval_no, application_certificate_type,
            effective_date, expire_date, intimate_date, months_eligible,
            approval_date, licence_path, license_type, category_license_type,
            certificate_type, zone_id, zone_name
        )
        SELECT DISTINCT ON (a.approval_no)
            gen_random_uuid(), now(), now(), a.id, a.created_by,
            CASE
                WHEN sec.name IS NOT NULL THEN sec.name
                WHEN a.license_type LIKE '%%PETROLEUM' THEN 'PETROLEUM'
                WHEN a.license_type LIKE '%%NATURAL_GAS' THEN 'NATURAL_GAS'
                WHEN a.license_type = 'LICENSE_WATER' THEN 'WATER_SUPPLY'
                WHEN a.license_type LIKE '%%ELECTRICITY%%' THEN 'ELECTRICITY'
                ELSE 'PETROLEUM'
            END,
            a.application_number, a.approval_no, COALESCE(a.application_type, 'NEW'),
            COALESCE(a.effective_date, a.approval_date), a.expire_date,
            COALESCE(a.intimate_date, 
                CASE WHEN a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR cat.name ~* 'Electrical Installation'
                     THEN a.expire_date - INTERVAL '90 days'
                     ELSE a.expire_date - INTERVAL '180 days'
                END
            ),
            a.months_eligible, COALESCE(a.approval_date, a.effective_date),
            a.licence_path, a.license_type, a.category_license_type,
            CASE WHEN cat.category_type IS NOT NULL THEN cat.category_type
                 WHEN a.license_type LIKE 'CONSTRUCTION%%' THEN 'Construction'
                 ELSE 'License'
            END,
            a.zone_id, a.zone_name
        FROM applications a
        LEFT JOIN categories cat ON cat.id = a.category_id
        LEFT JOIN sectors sec ON sec.id = cat.sector_id
        WHERE a.approval_no IS NOT NULL AND a.approval_no != ''
          AND NOT EXISTS (SELECT 1 FROM certificates c WHERE c.approval_no = a.approval_no)
          AND NOT EXISTS (SELECT 1 FROM certificates c2 WHERE c2.application_number = a.application_number)
        ORDER BY a.approval_no, CASE WHEN a.status = 'APPROVED' THEN 1 ELSE 2 END, a.expire_date DESC NULLS LAST, a.created_at DESC;
    """)
    inserted_missing = cur.rowcount
    print(f"  -> Inserted {inserted_missing} missing certificate records.")

    # STEP C: Update certificates with the LATEST application's values for each approval_no
    print("\nStep C: Updating certificates with latest application values...")
    cur.execute("""
        WITH latest_app AS (
            SELECT DISTINCT ON (a.approval_no)
                a.approval_no,
                a.id as latest_app_id,
                a.created_by as latest_owner_id,
                a.application_number as latest_app_num,
                COALESCE(a.effective_date, a.approval_date) as latest_eff_date,
                COALESCE(a.approval_date, a.effective_date) as latest_app_date,
                a.expire_date as latest_exp_date,
                COALESCE(a.intimate_date, 
                    CASE WHEN a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION' OR cat.name ~* 'Electrical Installation'
                         THEN a.expire_date - INTERVAL '90 days'
                         ELSE a.expire_date - INTERVAL '180 days'
                    END
                ) as latest_int_date,
                a.months_eligible as latest_months_eligible,
                a.licence_path as latest_licence_path,
                a.license_type as latest_license_type,
                a.category_license_type as latest_cat_license_type,
                COALESCE(a.application_type, 'NEW') as latest_app_cert_type,
                CASE WHEN cat.category_type IS NOT NULL THEN cat.category_type
                     WHEN a.license_type LIKE 'CONSTRUCTION%%' THEN 'Construction'
                     ELSE 'License'
                END as latest_cert_type,
                CASE
                    WHEN sec.name IS NOT NULL THEN sec.name
                    WHEN a.license_type LIKE '%%PETROLEUM' THEN 'PETROLEUM'
                    WHEN a.license_type LIKE '%%NATURAL_GAS' THEN 'NATURAL_GAS'
                    WHEN a.license_type = 'LICENSE_WATER' THEN 'WATER_SUPPLY'
                    WHEN a.license_type LIKE '%%ELECTRICITY%%' THEN 'ELECTRICITY'
                    ELSE 'PETROLEUM'
                END as latest_sector,
                a.zone_id as latest_zone_id,
                a.zone_name as latest_zone_name
            FROM applications a
            LEFT JOIN categories cat ON cat.id = a.category_id
            LEFT JOIN sectors sec ON sec.id = cat.sector_id
            WHERE a.approval_no IS NOT NULL AND a.approval_no != ''
            ORDER BY a.approval_no, 
                     CASE WHEN a.status = 'APPROVED' THEN 1 ELSE 2 END,
                     COALESCE(a.effective_date, a.approval_date) DESC NULLS LAST,
                     a.expire_date DESC NULLS LAST, 
                     a.created_at DESC
        )
        UPDATE certificates c
        SET
            application_id               = la.latest_app_id,
            owner_id                     = COALESCE(la.latest_owner_id, c.owner_id),
            application_number           = CASE 
                                             WHEN NOT EXISTS (
                                                 SELECT 1 FROM certificates c2 
                                                 WHERE c2.application_number = la.latest_app_num 
                                                   AND c2.id != c.id
                                             ) THEN la.latest_app_num
                                             ELSE c.application_number
                                           END,
            sector                       = la.latest_sector,
            application_certificate_type = la.latest_app_cert_type,
            effective_date               = la.latest_eff_date,
            approval_date                = la.latest_app_date,
            expire_date                  = la.latest_exp_date,
            intimate_date                = la.latest_int_date,
            months_eligible              = COALESCE(la.latest_months_eligible, c.months_eligible),
            licence_path                 = COALESCE(la.latest_licence_path, c.licence_path),
            license_type                 = la.latest_license_type,
            category_license_type        = la.latest_cat_license_type,
            certificate_type             = la.latest_cert_type,
            zone_id                      = COALESCE(la.latest_zone_id, c.zone_id),
            zone_name                    = COALESCE(la.latest_zone_name, c.zone_name),
            updated_at                   = now()
        FROM latest_app la
        WHERE c.approval_no = la.approval_no;
    """)
    updated_certs = cur.rowcount
    print(f"  -> Updated {updated_certs} certificates with latest application details.")

    # STEP D: Link ALL applications sharing approval_no to that unique certificate ID
    print("\nStep D: Linking all applications sharing approval_no to single certificate ID...")
    cur.execute("""
        UPDATE applications a
        SET certificate_id = c.id
        FROM certificates c
        WHERE a.approval_no = c.approval_no
          AND (a.certificate_id IS DISTINCT FROM c.id);
    """)
    updated_apps = cur.rowcount
    print(f"  -> Updated {updated_apps} applications to point to primary certificate ID.")

    # STEP E: Create UNIQUE index on certificates(approval_no) if not present
    print("\nStep E: Creating UNIQUE index on certificates(approval_no)...")
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_certificates_approval_no 
        ON certificates (approval_no) 
        WHERE approval_no IS NOT NULL AND approval_no != '';
    """)
    print("  -> Created unique index uq_certificates_approval_no.")

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

    print("\nAll database updates and materialized view refreshes completed successfully!")

if __name__ == "__main__":
    main()
