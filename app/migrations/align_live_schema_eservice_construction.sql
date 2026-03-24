-- Align LIVE schema (eservice_construction) to match the working auth_migration schema
-- for the core migration tables.
--
-- This script is intentionally additive and low-risk:
-- - Adds missing columns (does not drop columns)
-- - Adjusts defaults needed for migration
-- - Drops UNIQUE constraints that block migration when the requirement is
--   "only application_number is unique".
--
-- Review carefully and run against the *LIVE* database.
-- Recommended: run in a transaction and in a maintenance window.

BEGIN;

---------------------------------------------------------------------
-- Electrical installation imports: align columns used by supervisors/
-- work_experience and self_employed loaders.
--
-- Requirements:
--  - Live DB sometimes has voltage_level fixed to 'NONE' while Excel provides
--    the real voltage in voltagelevel; we store that value in a new `voltage`.
--  - Legacy/live DB may have an FK on work_experience.supervisor_details_id
--    that blocks bulk inserts; drop any FK constraints on that column.
---------------------------------------------------------------------

--  E1) Drop FK constraint(s) on work_experience.supervisor_details_id (any name)
DO $$
DECLARE
    r record;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = 'public.work_experience'::regclass) THEN
        FOR r IN (
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'f'
              AND n.nspname = 'public'
              AND t.relname = 'work_experience'
              AND a.attname = 'supervisor_details_id'
        ) LOOP
            EXECUTE format('ALTER TABLE public.work_experience DROP CONSTRAINT IF EXISTS %I', r.conname);
            RAISE NOTICE 'Dropped FK constraint on public.work_experience.supervisor_details_id: %', r.conname;
        END LOOP;
    END IF;
END $$;

--  E1b) Drop UNIQUE constraint(s) on work_experience.supervisor_details_id (any name)
DO $$
DECLARE
    r record;
    _attnum smallint;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = 'public.work_experience'::regclass) THEN
        SELECT attnum INTO _attnum
        FROM pg_attribute
        WHERE attrelid = 'public.work_experience'::regclass
          AND attname = 'supervisor_details_id';

        IF _attnum IS NOT NULL THEN
            FOR r IN (
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'public.work_experience'::regclass
                  AND contype = 'u'
                  AND conkey = ARRAY[_attnum]
            ) LOOP
                EXECUTE format('ALTER TABLE public.work_experience DROP CONSTRAINT IF EXISTS %I', r.conname);
                RAISE NOTICE 'Dropped UNIQUE constraint on public.work_experience.supervisor_details_id: %', r.conname;
            END LOOP;
        END IF;
    END IF;
END $$;

--  E2) work_experience: add `voltage` and ensure voltage_level default
ALTER TABLE IF EXISTS public.work_experience
    ADD COLUMN IF NOT EXISTS voltage character varying(255);

ALTER TABLE IF EXISTS public.work_experience
    ALTER COLUMN voltage_level SET DEFAULT 'NONE';

--  E3) self_employed: add `voltage` and ensure voltage_level default
ALTER TABLE IF EXISTS public.self_employed
    ADD COLUMN IF NOT EXISTS voltage character varying(255);

-- ── Denormalize application_id on all child tables ───────────────────────
-- Reduces multi-table joins: child tables can reach applications directly
-- without going through application_sector_details first.
-- Column is nullable so it can be back-filled after import.
ALTER TABLE IF EXISTS public.self_employed
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.self_employed
    ALTER COLUMN voltage_level SET DEFAULT 'NONE';

ALTER TABLE IF EXISTS public.custom_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.documents
    ADD COLUMN IF NOT EXISTS application_id uuid;

-- FK back to application_sector_details (needed by backfill + transform)
ALTER TABLE IF EXISTS public.documents
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

ALTER TABLE IF EXISTS public.documents
    ADD COLUMN IF NOT EXISTS documents_order integer;

ALTER TABLE IF EXISTS public.contact_persons
    ADD COLUMN IF NOT EXISTS application_id uuid;

-- FK back to application_sector_details (contact_persons uses app_sector_detail_id)
ALTER TABLE IF EXISTS public.contact_persons
    ADD COLUMN IF NOT EXISTS app_sector_detail_id uuid;

ALTER TABLE IF EXISTS public.fire
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.fire
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

ALTER TABLE IF EXISTS public.insurance_cover_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.insurance_cover_details
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

-- application_id columns: added in the managing_directors / shareholders sections below.

ALTER TABLE IF EXISTS public.ardhi_information
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.ardhi_information
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

-- ── Electrical installation child tables: application_id + aei FK ─────────
ALTER TABLE IF EXISTS public.application_electrical_installation
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.application_electrical_installation
    ADD COLUMN IF NOT EXISTS is_from_lois boolean DEFAULT false;

ALTER TABLE IF EXISTS public.personal_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.personal_details
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.contact_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.contact_details
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.attachments
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.attachments
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.work_experience
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.work_experience
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.self_employed
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.supervisor_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.supervisor_details
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.costumer_details
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.costumer_details
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

ALTER TABLE IF EXISTS public.certificate_verifications
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.certificate_verifications
    ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

-- certificates: columns required by the import pipeline
ALTER TABLE IF EXISTS public.certificates
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.certificates
    ADD COLUMN IF NOT EXISTS application_number text;

ALTER TABLE IF EXISTS public.certificates
    ADD COLUMN IF NOT EXISTS application_certificate_type text;

-- Allow long custom details strings (some uploads exceed 255 chars)
ALTER TABLE IF EXISTS public.custom_details
    ALTER COLUMN name TYPE text;

-- NOTE: Do NOT try to create extensions here.
-- Some managed/Postgres installs don't ship with pgcrypto control files and
-- CREATE EXTENSION will fail even with IF NOT EXISTS.
-- Assumption: required UUID defaults/extensions are handled outside this script.

---------------------------------------------------------------------
-- 0) application_sector_details: fix column types that block migration
--
-- po_box is integer in the live schema but Excel/staging uses text (e.g. "P.O.BOX 1234").
-- Convert it to varchar so migration inserts don't fail with DatatypeMismatch.
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.application_sector_details
    ALTER COLUMN po_box TYPE character varying USING po_box::text;

---------------------------------------------------------------------
-- 1) documents: keep logic_doc_id exactly as in legacy pipeline (normalized)
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.documents
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

---------------------------------------------------------------------
-- 1bb) NEW shareholders (normalized): ensure columns exist
--
-- Shareholders is now linked to applications via application_sector_details.
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS created_at timestamptz;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS created_by uuid;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS deleted_by uuid;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS updated_at timestamptz;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS updated_by uuid;

ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

-- Store source attachment filename and legacy object id from Excel (for traceability)
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS file_name text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS amount_of_shares numeric;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS birth_date date;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS country_of_incorporation text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS country_of_residence text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS email text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS first_name text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS gender text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS individual_company text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS last_name text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS middle_name text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS mobile_no text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS nationality text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS passport_or_nationalid text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS shareholder_name text;
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS amount_of_share_percent numeric;

-- Address info from LOIS shareholders sheet
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS street_address text;

-- 1c) managing_directors: keep legacy file-id columns
--
-- In the normalized schema, managing_directors should still keep:
-- - cpana        (bigint)
-- - work_permit  (bigint)
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS work_permit bigint;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS work_permit_filename text;

-- Columns used by the managing_directors import pipeline
ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS name text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS first_name text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS middle_name text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS last_name text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS email text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS mobile_no text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS country text;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS nationality text;

-- CPANA support requested for managing directors
ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS cpana bigint;

ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS cpana_filename text;

-- application_id is the canonical FK to applications (replaces application_number).
-- Ensure the column exists and drop application_number if it was ever persisted.
ALTER TABLE IF EXISTS public.managing_directors
    ADD COLUMN IF NOT EXISTS application_id uuid;

ALTER TABLE IF EXISTS public.managing_directors
    DROP COLUMN IF EXISTS application_number;

-- Shareholders: application_id is the canonical FK — no application_number column needed.
ALTER TABLE IF EXISTS public.shareholders
    ADD COLUMN IF NOT EXISTS application_id uuid;

---------------------------------------------------------------------
-- 2) applications: ensure migration columns exist and defaults match
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS username character varying;

ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS application_type character varying;

ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS old_parent_application_id character varying;

ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS old_created_by character varying;

-- Provenance flag: distinguish LOIS-migrated rows from native system rows.
ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS is_from_lois boolean NOT NULL DEFAULT false;

---------------------------------------------------------------------
-- 3) Uniqueness policy: ensure application_number is unique
---------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = 'public.applications'::regclass) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = 'public.applications'::regclass
              AND contype = 'u'
              AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
        ) THEN
            EXECUTE 'ALTER TABLE public.applications ADD CONSTRAINT applications_application_number_key UNIQUE (application_number)';
            RAISE NOTICE 'Created unique constraint on applications.application_number';
        END IF;
    END IF;
END $$;

---------------------------------------------------------------------
-- 3b) Relax unique constraints on applications & certificates, then
--     add UNIQUE (application_number) on certificates.
--
-- Business rule:
--   • One certificate row per application_number — this is now the
--     conflict key for all import pipelines (ON CONFLICT (application_number)).
--   • The old composite UNIQUE (approval_no, application_certificate_type)
--     and any other unique on approval_no / application_number are removed
--     first so there is no conflicting constraint.
--
-- Action (for EVERY schema that exists on this DB):
--   REMOVE  single-column UNIQUE on applications.approval_no
--   REMOVE  ANY   unique on certificates that includes approval_no
--   REMOVE  ANY   unique on certificates that includes application_number
--   ADD     UNIQUE (application_number) on certificates
--             — one cert row per application, conflict key for upserts
--
-- All constraint names are Hibernate-generated hashes that differ per
-- environment, so we look them up dynamically from pg_constraint.
-- Schemas that don't exist on a given DB are skipped safely.
---------------------------------------------------------------------
DO $$
DECLARE
    _rec               RECORD;
    _schema            text;
    _app_attnum        smallint;
    _cert_apprv_attnum smallint;
    _cert_appno_attnum smallint;
BEGIN
    FOR _schema IN
        SELECT nspname FROM pg_namespace
        WHERE  nspname IN ('public', 'align_live')
        ORDER BY nspname
    LOOP

        -- ── A. Drop single-column UNIQUE on applications.approval_no ──
        IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname=_schema AND c.relname='applications') THEN

            SELECT attnum INTO _app_attnum
            FROM pg_attribute
            WHERE attrelid = (SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='applications')
              AND attname = 'approval_no';

            IF _app_attnum IS NOT NULL THEN
                FOR _rec IN
                    SELECT con.conname
                    FROM   pg_constraint con
                    JOIN   pg_class cls ON cls.oid = con.conrelid
                    JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                    WHERE  con.contype = 'u'
                      AND  nsp.nspname = _schema
                      AND  cls.relname = 'applications'
                      AND  con.conkey = ARRAY[_app_attnum]
                LOOP
                    EXECUTE format('ALTER TABLE %I.applications DROP CONSTRAINT IF EXISTS %I',
                                   _schema, _rec.conname);
                    RAISE NOTICE 'Dropped %.applications.%', _schema, _rec.conname;
                END LOOP;
            END IF;
        END IF;

        -- ── B & C. Drop ALL unique constraints on certificates that include
        --          approval_no OR application_number (single or composite) ──
        IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname=_schema AND c.relname='certificates') THEN

            SELECT attnum INTO _cert_apprv_attnum
            FROM pg_attribute
            WHERE attrelid = (SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='certificates')
              AND attname = 'approval_no';

            SELECT attnum INTO _cert_appno_attnum
            FROM pg_attribute
            WHERE attrelid = (SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='certificates')
              AND attname = 'application_number';

            -- Drop any unique that mentions approval_no (single OR composite)
            IF _cert_apprv_attnum IS NOT NULL THEN
                FOR _rec IN
                    SELECT con.conname
                    FROM   pg_constraint con
                    JOIN   pg_class cls ON cls.oid = con.conrelid
                    JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                    WHERE  con.contype = 'u'
                      AND  nsp.nspname = _schema
                      AND  cls.relname = 'certificates'
                      AND  _cert_apprv_attnum = ANY(con.conkey)
                LOOP
                    EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                   _schema, _rec.conname);
                    RAISE NOTICE 'Dropped %.certificates.% (approval_no)', _schema, _rec.conname;
                END LOOP;
            END IF;

            -- Drop any existing unique that mentions application_number
            -- (we are about to re-add a clean single-column one below)
            IF _cert_appno_attnum IS NOT NULL THEN
                FOR _rec IN
                    SELECT con.conname
                    FROM   pg_constraint con
                    JOIN   pg_class cls ON cls.oid = con.conrelid
                    JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                    WHERE  con.contype = 'u'
                      AND  nsp.nspname = _schema
                      AND  cls.relname = 'certificates'
                      AND  _cert_appno_attnum = ANY(con.conkey)
                LOOP
                    EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                   _schema, _rec.conname);
                    RAISE NOTICE 'Dropped %.certificates.% (application_number)', _schema, _rec.conname;
                END LOOP;
            END IF;

            -- ── D. Add UNIQUE (application_number) — the upsert conflict key ──
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint con
                JOIN pg_class     cls ON cls.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                WHERE  con.contype = 'u'
                  AND  nsp.nspname = _schema
                  AND  cls.relname = 'certificates'
                  AND  con.conname = 'uq_certificates_application_number'
            ) THEN
                -- Dedup first so the constraint can be created cleanly
                EXECUTE format(
                    'DELETE FROM %I.certificates
                     WHERE id IN (
                         SELECT id FROM (
                             SELECT id,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY application_number
                                        ORDER BY updated_at DESC NULLS LAST,
                                                 created_at  DESC NULLS LAST,
                                                 id
                                    ) AS rn
                             FROM %I.certificates
                             WHERE application_number IS NOT NULL
                         ) ranked
                         WHERE rn > 1
                     )',
                    _schema, _schema
                );
                EXECUTE format(
                    'ALTER TABLE %I.certificates
                     ADD CONSTRAINT uq_certificates_application_number
                     UNIQUE (application_number)',
                    _schema
                );
                RAISE NOTICE 'Added uq_certificates_application_number on %.certificates', _schema;
            END IF;

        END IF; -- certificates table exists

    END LOOP; -- schemas
END $$;

---------------------------------------------------------------------
-- 4) certificates: relax NOT NULL constraints that block migration
--
-- In some live schemas, certificates has NOT NULL on certificate_owner,
-- but historical Excel/staging data doesn't include this value.
-- We make it nullable for migration.
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.certificates
    ALTER COLUMN certificate_owner DROP NOT NULL;

-- Owner is system-derived in the live app and isn't present in historical migration sources.
ALTER TABLE IF EXISTS public.certificates
    ALTER COLUMN owner_id DROP NOT NULL;

-- Additional columns required by the migration pipeline.
ALTER TABLE IF EXISTS public.certificates
    ADD COLUMN IF NOT EXISTS remarks text;

ALTER TABLE IF EXISTS public.certificates
    ADD COLUMN IF NOT EXISTS certificate_status character varying;

---------------------------------------------------------------------
-- 5) Permissions: grant migration role access to all tables (present + future)
--
-- Your FastAPI app runs as DB role: "appuser".
-- Run this script as the schema owner / DB admin so GRANT/ALTER DEFAULT PRIVILEGES succeed.
---------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO "appuser";

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO "appuser";

GRANT USAGE, SELECT, UPDATE
ON ALL SEQUENCES IN SCHEMA public
TO "appuser";

-- Future-proof: new tables/sequences created in public will also be accessible to "user".
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES
TO "appuser";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT, UPDATE ON SEQUENCES
TO "appuser";

---------------------------------------------------------------------
-- 6) Drop incorrect foreign key on application_legal_status
--
-- The table application_legal_status is a lookup table and should not
-- have a foreign key to application_sector_details.
---------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT con.conname
        FROM pg_catalog.pg_constraint con
        INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
        INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
        INNER JOIN pg_catalog.pg_class confrel ON confrel.oid = con.confrelid
        WHERE nsp.nspname = 'public'
          AND rel.relname = 'application_legal_status'
          AND confrel.relname = 'application_sector_details'
          AND con.contype = 'f'
    LOOP
        EXECUTE 'ALTER TABLE public.application_legal_status DROP CONSTRAINT ' || quote_ident(r.conname);
        RAISE NOTICE 'Dropped foreign key constraint: %', r.conname;
    END LOOP;
END $$;

---------------------------------------------------------------------
-- Certificate verification: add from_date / to_date timestamp columns
--
-- The electrical certificate verifications importer stores from_date and
-- to_date as timestamps; add them if they are not present yet.
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.certificate_verification
    ADD COLUMN IF NOT EXISTS from_date  timestamp NULL,
    ADD COLUMN IF NOT EXISTS to_date    timestamp NULL;

COMMENT ON COLUMN public.certificate_verification.from_date
    IS 'Start date of the certificate / qualification period (from Excel fromdate column).';
COMMENT ON COLUMN public.certificate_verification.to_date
    IS 'End date of the certificate / qualification period (from Excel todate column).';

---------------------------------------------------------------------
-- Applications: add completed_at timestamp column
--
-- Stores the date the application was completed/approved as provided in the
-- Excel source file (completed_at column).  Used by the staging-copy importer
-- and the direct import path; included in ON CONFLICT DO UPDATE so re-uploads
-- fill in missing values without overwriting existing ones.
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS completed_at timestamp NULL;

COMMENT ON COLUMN public.applications.completed_at
    IS 'Completion / approval timestamp migrated from the LOIS Excel export (completed_at column).';

---------------------------------------------------------------------
-- Water Supply (and other sector) imports: application columns
--
-- created_by (uuid) — resolved from users.id via userid/username column.
-- Mirrors the electrical-installation importer approach.
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.applications
    ADD COLUMN IF NOT EXISTS created_by uuid NULL;

---------------------------------------------------------------------
-- Water Supply imports: application_sector_details extra columns
--
-- vrn              — VAT Registration Number from vrnno Excel column
-- gn_gazette_on    — Government Gazette date from gngzon Excel column
-- government_notice_no — Notice number from govtnoteno Excel column
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.application_sector_details
    ADD COLUMN IF NOT EXISTS vrn                  text NULL;

ALTER TABLE IF EXISTS public.application_sector_details
    ADD COLUMN IF NOT EXISTS gn_gazette_on         text NULL;

ALTER TABLE IF EXISTS public.application_sector_details
    ADD COLUMN IF NOT EXISTS government_notice_no  text NULL;

---------------------------------------------------------------------
-- Water Supply imports: financial_information extra columns
--
-- application_id              — direct FK to applications (no join via asd)
-- fs                          — financial statement reference
-- amount                      — declared investment/capital amount
-- currency                    — currency of amount (e.g. TZS, USD)
---------------------------------------------------------------------

ALTER TABLE IF EXISTS public.financial_information
    ADD COLUMN IF NOT EXISTS application_id uuid NULL;

ALTER TABLE IF EXISTS public.financial_information
    ADD COLUMN IF NOT EXISTS fs text NULL;

ALTER TABLE IF EXISTS public.financial_information
    ADD COLUMN IF NOT EXISTS amount text NULL;

ALTER TABLE IF EXISTS public.financial_information
    ADD COLUMN IF NOT EXISTS currency text NULL;

---------------------------------------------------------------------
-- Water Supply imports: drop blocking one-to-one UNIQUE and FK
-- constraints on financial_information.
--
-- These Hibernate-generated constraints enforce a 1:1 relationship
-- between financial_information and bank/referee/funding rows, which
-- prevents multiple applications from sharing the same records and
-- blocks idempotent re-imports.  They are replaced by the looser
-- ON CONFLICT (id) DO UPDATE pattern used by the migration pipeline.
--
-- Constraint names are hard-coded Hibernate hashes — drop each with
-- IF NOT EXISTS so the script is safe to re-run.
---------------------------------------------------------------------

DO $$
DECLARE
    _cname text;
    _constraints text[] := ARRAY[
        -- UNIQUE constraints
        'uk3330k9q1sw66jlduqpsrhj0p',   -- UNIQUE bank_details_tz_id
        'uk3bdd17w6dmarkybpxwdqyig3e',   -- UNIQUE bank_details_outside_tz_id
        'ukel2wimvd2s27v1yv6piue4gdt',   -- UNIQUE referee_id
        'ukfa6pmxx6fmxjml7o4tnpiqnk4',   -- UNIQUE sources_of_funding_id
        'ukh0i89cf4bjf1w09xb5phl8shm',   -- UNIQUE applicant_proposed_investment_id
        'uksj2p6a47sf5y8u6uyui21sk8v',   -- UNIQUE audited_financial_statements_id
        -- FK constraints
        'fk1fyiwcuau1xor0biuf5hiy8cf',   -- FK bank_details_tz_id → bank_details_tanzania
        'fk4hpnlnjnfofc7khnkxp0emfqn',   -- FK referee_id → referees
        'fk5uk0tmchnlb1ystu0jpqq42sl',   -- FK application_sector_detail_id → application_sector_details
        'fk66wid3r5eqs7mukb826wj7f3s',   -- FK audited_financial_statements_id
        'fkbp7gwdkep3f5gkn44t1ms7p30',   -- FK sources_of_funding_id
        'fkjmhltcnyhhewyo6hmn8q6hbno',   -- FK bank_details_outside_tz_id → bank_details_outside_tanzania
        'fksmkj5b9iapr68uiw8wrcj5s2m'    -- FK applicant_proposed_investment_id
    ];
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = 'public.financial_information'::regclass) THEN
        FOREACH _cname IN ARRAY _constraints
        LOOP
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.financial_information'::regclass
                  AND conname  = _cname
            ) THEN
                EXECUTE format(
                    'ALTER TABLE public.financial_information DROP CONSTRAINT %I',
                    _cname
                );
                RAISE NOTICE 'Dropped financial_information constraint: %', _cname;
            END IF;
        END LOOP;
    END IF;
END $$;

-- applicant_proposed_investment: migration link columns + FKs
ALTER TABLE IF EXISTS public.applicant_proposed_investment
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL;
ALTER TABLE IF EXISTS public.applicant_proposed_investment
    ADD COLUMN IF NOT EXISTS application_id               uuid NULL;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.applicant_proposed_investment'::regclass AND conname='fk_api_application_id') THEN
        ALTER TABLE public.applicant_proposed_investment DROP CONSTRAINT fk_api_application_id;
    END IF;
    ALTER TABLE public.applicant_proposed_investment ADD CONSTRAINT fk_api_application_id FOREIGN KEY (application_id) REFERENCES public.applications (id) ON DELETE CASCADE;
END $$;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.applicant_proposed_investment'::regclass AND conname='fk_api_asd_id') THEN
        ALTER TABLE public.applicant_proposed_investment DROP CONSTRAINT fk_api_asd_id;
    END IF;
    ALTER TABLE public.applicant_proposed_investment ADD CONSTRAINT fk_api_asd_id FOREIGN KEY (application_sector_detail_id) REFERENCES public.application_sector_details (id) ON DELETE CASCADE;
END $$;

-- project_description: migration link columns + FKs
ALTER TABLE IF EXISTS public.project_description
    ADD COLUMN IF NOT EXISTS application_id               uuid NULL;
ALTER TABLE IF EXISTS public.project_description
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.project_description'::regclass AND conname='fk_pd_application_id') THEN
        ALTER TABLE public.project_description DROP CONSTRAINT fk_pd_application_id;
    END IF;
    ALTER TABLE public.project_description ADD CONSTRAINT fk_pd_application_id FOREIGN KEY (application_id) REFERENCES public.applications (id) ON DELETE CASCADE;
END $$;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.project_description'::regclass AND conname='fk_pd_asd_id') THEN
        ALTER TABLE public.project_description DROP CONSTRAINT fk_pd_asd_id;
    END IF;
    ALTER TABLE public.project_description ADD CONSTRAINT fk_pd_asd_id FOREIGN KEY (application_sector_detail_id) REFERENCES public.application_sector_details (id) ON DELETE CASCADE;
END $$;

-- referees: migration link columns + FKs
ALTER TABLE IF EXISTS public.referees
    ADD COLUMN IF NOT EXISTS application_id               uuid NULL;
ALTER TABLE IF EXISTS public.referees
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.referees'::regclass AND conname='fk_ref_application_id') THEN
        ALTER TABLE public.referees DROP CONSTRAINT fk_ref_application_id;
    END IF;
    ALTER TABLE public.referees ADD CONSTRAINT fk_ref_application_id FOREIGN KEY (application_id) REFERENCES public.applications (id) ON DELETE CASCADE;
END $$;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.referees'::regclass AND conname='fk_ref_asd_id') THEN
        ALTER TABLE public.referees DROP CONSTRAINT fk_ref_asd_id;
    END IF;
    ALTER TABLE public.referees ADD CONSTRAINT fk_ref_asd_id FOREIGN KEY (application_sector_detail_id) REFERENCES public.application_sector_details (id) ON DELETE CASCADE;
END $$;

-- bank_details_tanzania: migration link columns + FKs
ALTER TABLE IF EXISTS public.bank_details_tanzania
    ADD COLUMN IF NOT EXISTS application_id               uuid NULL;
ALTER TABLE IF EXISTS public.bank_details_tanzania
    ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.bank_details_tanzania'::regclass AND conname='fk_bdt_application_id') THEN
        ALTER TABLE public.bank_details_tanzania DROP CONSTRAINT fk_bdt_application_id;
    END IF;
    ALTER TABLE public.bank_details_tanzania ADD CONSTRAINT fk_bdt_application_id FOREIGN KEY (application_id) REFERENCES public.applications (id) ON DELETE CASCADE;
END $$;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.bank_details_tanzania'::regclass AND conname='fk_bdt_asd_id') THEN
        ALTER TABLE public.bank_details_tanzania DROP CONSTRAINT fk_bdt_asd_id;
    END IF;
    ALTER TABLE public.bank_details_tanzania ADD CONSTRAINT fk_bdt_asd_id FOREIGN KEY (application_sector_detail_id) REFERENCES public.application_sector_details (id) ON DELETE CASCADE;
END $$;

COMMIT;
