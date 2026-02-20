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
-- 3b) Relax unique constraints on applications & certificates
--
-- Business rule:
--   • The same approval_no can appear in certificates with DIFFERENT
--     application_certificate_type values (e.g. a NEW and a RENEW
--     certificate can share the same licence/approval number).
--   • The same application_number can therefore have multiple certificate
--     rows — one per certificate type.
--
-- Action (for EVERY schema that exists on this DB):
--   REMOVE  single-column UNIQUE on applications.approval_no
--   REMOVE  single-column UNIQUE on certificates.approval_no
--   REMOVE  single-column UNIQUE on certificates.application_number
--   ADD     composite UNIQUE (approval_no, application_certificate_type)
--           — prevents true duplicates while allowing cross-type reuse
--
-- All constraint names are Hibernate-generated hashes that differ per
-- environment, so we look them up dynamically from pg_constraint.
-- Schemas that don't exist on a given DB are skipped safely.
---------------------------------------------------------------------
DO $$
DECLARE
    _rec              RECORD;
    _schema           text;
    _app_attnum       smallint;
    _cert_attnum      smallint;
    _cert_appno_attnum smallint;
BEGIN
    -- Only iterate over schemas that ACTUALLY EXIST on this database
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

        -- ── B & C. Drop single-column UNIQUEs on certificates ─────────
        IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname=_schema AND c.relname='certificates') THEN

            SELECT attnum INTO _cert_attnum
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

            -- Drop UNIQUE(approval_no)
            IF _cert_attnum IS NOT NULL THEN
                FOR _rec IN
                    SELECT con.conname
                    FROM   pg_constraint con
                    JOIN   pg_class cls ON cls.oid = con.conrelid
                    JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                    WHERE  con.contype = 'u'
                      AND  nsp.nspname = _schema
                      AND  cls.relname = 'certificates'
                      AND  con.conkey = ARRAY[_cert_attnum]
                LOOP
                    EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                   _schema, _rec.conname);
                    RAISE NOTICE 'Dropped %.certificates.% (approval_no)', _schema, _rec.conname;
                END LOOP;
            END IF;

            -- Drop UNIQUE(application_number) — same app can have NEW + RENEW + UPGRADE
            IF _cert_appno_attnum IS NOT NULL THEN
                FOR _rec IN
                    SELECT con.conname
                    FROM   pg_constraint con
                    JOIN   pg_class cls ON cls.oid = con.conrelid
                    JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                    WHERE  con.contype = 'u'
                      AND  nsp.nspname = _schema
                      AND  cls.relname = 'certificates'
                      AND  con.conkey = ARRAY[_cert_appno_attnum]
                LOOP
                    EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                   _schema, _rec.conname);
                    RAISE NOTICE 'Dropped %.certificates.% (application_number)', _schema, _rec.conname;
                END LOOP;
            END IF;

            -- ── D. Add composite UNIQUE (approval_no, application_certificate_type) ──
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint con
                JOIN pg_class     cls ON cls.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                WHERE  con.contype = 'u'
                  AND  nsp.nspname = _schema
                  AND  cls.relname = 'certificates'
                  AND  con.conname = 'certificates_approval_no_cert_type_uq'
            ) THEN
                EXECUTE format(
                    'ALTER TABLE %I.certificates
                     ADD CONSTRAINT certificates_approval_no_cert_type_uq
                     UNIQUE (approval_no, application_certificate_type)',
                    _schema
                );
                RAISE NOTICE 'Added certificates_approval_no_cert_type_uq on %.certificates', _schema;
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

COMMIT;
