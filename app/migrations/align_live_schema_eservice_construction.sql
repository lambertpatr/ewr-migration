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
-- 1) ca_documents: ensure logic_doc_id exists and type matches
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.ca_documents
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

---------------------------------------------------------------------
-- 1b) ca_shareholders: ensure logic_doc_id exists (objectid from Excel)
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.ca_shareholders
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

-- Store original uploaded filename (or source file name) for traceability
ALTER TABLE IF EXISTS public.ca_shareholders
    ADD COLUMN IF NOT EXISTS file_name text;

---------------------------------------------------------------------
-- 1c) ca_managing_directors: columns needed for migration
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.ca_managing_directors
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

ALTER TABLE IF EXISTS public.ca_managing_directors
    ADD COLUMN IF NOT EXISTS work_permit_id bigint;

---------------------------------------------------------------------
-- 2) ca_applications: ensure migration columns exist and defaults match
---------------------------------------------------------------------
ALTER TABLE IF EXISTS public.ca_applications
    ADD COLUMN IF NOT EXISTS username character varying;

ALTER TABLE IF EXISTS public.ca_applications
    ADD COLUMN IF NOT EXISTS application_type character varying;

ALTER TABLE IF EXISTS public.ca_applications
    ADD COLUMN IF NOT EXISTS old_parent_application_id character varying;

ALTER TABLE IF EXISTS public.ca_applications
    ADD COLUMN IF NOT EXISTS old_created_by character varying;

-- Match auth_migration behaviour: id default gen_random_uuid()
-- (safe even if your inserts explicitly set id)
ALTER TABLE IF EXISTS public.ca_applications
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

---------------------------------------------------------------------
-- 3) Uniqueness policy: only application_number should be unique
--
-- Live has lots of unwanted UNIQUE constraints (approval_no, *_information_id, etc.).
-- We drop any unique constraints on ca_applications except the one that enforces
-- application_number uniqueness.
---------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN (
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.ca_applications'::regclass
          AND contype = 'u'
          AND NOT (
              -- Preserve *any* unique constraint that is defined on application_number
              -- (name can differ between environments)
              pg_get_constraintdef(oid) ILIKE '%(application_number)%'
          )
    ) LOOP
        EXECUTE format('ALTER TABLE public.ca_applications DROP CONSTRAINT %I', r.conname);
        RAISE NOTICE 'Dropped unique constraint: %', r.conname;
    END LOOP;
END $$;

-- Ensure application_number has a UNIQUE constraint (create if missing).
-- If it already exists (possibly with a different name), this will fail.
-- In that case, comment this out OR adjust name.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.ca_applications'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
    ) THEN
        EXECUTE 'ALTER TABLE public.ca_applications ADD CONSTRAINT ca_applications_application_number_key UNIQUE (application_number)';
        RAISE NOTICE 'Created unique constraint on application_number';
    END IF;
END $$;

COMMIT;
