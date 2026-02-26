-- ============================================================================
-- Migration: 20260223_extract_phone_email_from_name.sql
--
-- Cleans embedded phone numbers / emails that were packed into the "name"
-- column during LOIS migration.
--
-- Tables affected
-- ───────────────
-- 1. public.custom_details
--      name column contains patterns like:
--        "CASTOR MAGARI 0782355391"
--      → extract phone into mobile_no (only when mobile_no IS NULL / empty)
--
-- 2. public.supervisor_details
--      name column contains comma-separated garbage like:
--        "CLAVERY MABELE,ELECTRICIAL,0754585433,claverymabele1978@gmail.com"
--      → extract phone into mobile_no (only when mobile_no IS NULL / empty)
--      → extract email into email    (only when email     IS NULL / empty)
--      → clean name to the first comma-segment (the actual person name)
--
-- Phone pattern  : +255XXXXXXXXX  |  0[67]XXXXXXXX  (TZ mobile numbers)
-- Email pattern  : RFC-5321 simplified
--
-- Safety rules
-- ────────────
-- • COALESCE logic: never overwrites an existing non-null/non-empty value.
-- • name is trimmed; the first comma-segment is kept as the clean name.
-- • All changes are inside a single transaction — roll back if anything fails.
-- • A DO $$ report $$ block at the end prints how many rows were touched.
-- ============================================================================

BEGIN;

-- Phone pattern: after +255 or 0[67], match 9 digits with optional spaces
-- interspersed in any grouping (+255 715 67 67 70, +255 717 685 943, etc.)
-- Pattern: \+?255[\s]?[0-9]([\s]?[0-9]){8}  (international)
--        | 0[67][0-9]([\s]?[0-9]){7}         (local TZ 10-digit)
-- After extraction REPLACE(..., ' ', '') collapses spaces → compact number.

-- ── 1. custom_details ────────────────────────────────────────────────────────
UPDATE public.custom_details
SET
    mobile_no  = COALESCE(
                     NULLIF(TRIM(mobile_no), ''),
                     NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                         '^.*?(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7}).*$',
                         '\1')), ' ', ''),
                     REPLACE(name, ' ', ''))
                 ),
    updated_at = now()
WHERE
    name ~ '(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7})'
    AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL;


-- ── 2. supervisor_details ─────────────────────────────────────────────────────
-- Separators: pipe ('|') — "NAYMAN CHAVALA | COUNTRY DIRECTOR | ..."
--             comma (',') — "CLAVERY MABELE,ELECTRICIAL,..."
UPDATE public.supervisor_details
SET
    mobile_no  = COALESCE(
                     NULLIF(TRIM(mobile_no), ''),
                     NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                         '^.*?(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7}).*$',
                         '\1')), ' ', ''),
                     REPLACE(name, ' ', ''))
                 ),
    email      = COALESCE(
                     NULLIF(TRIM(email), ''),
                     NULLIF(TRIM(REGEXP_REPLACE(name,
                         '^.*?([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}).*$', '\1')), name)
                 ),
    name       = CASE
                     WHEN name LIKE '%|%'
                     THEN TRIM(SPLIT_PART(name, '|', 1))
                     WHEN name LIKE '%,%'
                     THEN TRIM(SPLIT_PART(name, ',', 1))
                     ELSE TRIM(name)
                 END,
    updated_at = now()
WHERE
    (name LIKE '%|%'
     OR name LIKE '%,%'
     OR name ~ '(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7})'
     OR name ~ '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}');


-- ── 3. Report ─────────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_cd_phone  bigint;
    v_sd_phone  bigint;
    v_sd_email  bigint;
    v_sd_name   bigint;
BEGIN
    SELECT COUNT(*) INTO v_cd_phone
    FROM public.custom_details
    WHERE NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NOT NULL
      AND updated_at >= now() - interval '5 seconds';

    SELECT COUNT(*) INTO v_sd_phone
    FROM public.supervisor_details
    WHERE NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NOT NULL
      AND updated_at >= now() - interval '5 seconds';

    SELECT COUNT(*) INTO v_sd_email
    FROM public.supervisor_details
    WHERE NULLIF(TRIM(COALESCE(email, '')), '') IS NOT NULL
      AND updated_at >= now() - interval '5 seconds';

    SELECT COUNT(*) INTO v_sd_name
    FROM public.supervisor_details
    WHERE updated_at >= now() - interval '5 seconds';

    RAISE NOTICE 'custom_details    : % mobile_no filled', v_cd_phone;
    RAISE NOTICE 'supervisor_details: % mobile_no filled, % email filled, % name cleaned',
                 v_sd_phone, v_sd_email, v_sd_name;
END $$;

COMMIT;
