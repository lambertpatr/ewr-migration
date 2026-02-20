-- Backfill created_by across application tables using applications.username -> users.username
--
-- Rules:
-- - Only update when a.username is present and matches users.username (case-insensitive)
-- - Only set created_by when it is currently NULL (never overwrite existing values)
--
-- Join path:
--   applications.username  ->  users.username
--
-- Tables that reference applications directly (via application_id):
--     applications, application_sector_details, certificates,
--     application_electrical_installation, task_assignments,
--     batch_application_advertisements, transfer_applications,
--     app_evaluation_checklist, application_additional_conditions,
--     application_reviews
--
-- Tables that reference application_sector_details (via application_sector_detail_id):
--     documents, shareholders, managing_directors, fire,
--     insurance_cover_details, ardhi_information
--
-- Special FK name:
--     contact_persons uses  app_sector_detail_id  (not application_sector_detail_id)

BEGIN;

-- -------------------------------------------------------------------------
-- Reusable CTE pattern (repeated per statement — Postgres requires it):
--   resolves application_id -> user_id via username match
-- -------------------------------------------------------------------------

-- 1) applications
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.applications a
SET created_by = u.user_id
FROM u
WHERE a.id = u.application_id
  AND a.created_by IS NULL;

-- 2) application_sector_details  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_sector_details asd
SET created_by = u.user_id
FROM u
WHERE asd.application_id = u.application_id
  AND asd.created_by IS NULL;

-- 3) certificates  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.certificates c
SET created_by = u.user_id
FROM u
WHERE c.application_id = u.application_id
  AND c.created_by IS NULL;

-- 4) application_electrical_installation  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_electrical_installation aei
SET created_by = u.user_id
FROM u
WHERE aei.application_id = u.application_id
  AND aei.created_by IS NULL;

-- 5) task_assignments  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.task_assignments ta
SET created_by = u.user_id
FROM u
WHERE ta.application_id = u.application_id
  AND ta.created_by IS NULL;

-- 6) batch_application_advertisements  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.batch_application_advertisements baa
SET created_by = u.user_id
FROM u
WHERE baa.application_id = u.application_id
  AND baa.created_by IS NULL;

-- 7) transfer_applications  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.transfer_applications tra
SET created_by = u.user_id
FROM u
WHERE tra.application_id = u.application_id
  AND tra.created_by IS NULL;

-- 8) app_evaluation_checklist  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.app_evaluation_checklist aec
SET created_by = u.user_id
FROM u
WHERE aec.application_id = u.application_id
  AND aec.created_by IS NULL;

-- 9) application_additional_conditions  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_additional_conditions aac
SET created_by = u.user_id
FROM u
WHERE aac.application_id = u.application_id
  AND aac.created_by IS NULL;

-- 10) application_reviews  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_reviews ar
SET created_by = u.user_id
FROM u
WHERE ar.application_id = u.application_id
  AND ar.created_by IS NULL;

-- -------------------------------------------------------------------------
-- For the remaining tables we join through application_sector_details
-- to reach the application -> username -> user_id chain.
-- -------------------------------------------------------------------------

-- 11) documents  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.documents d
SET created_by = u.user_id
FROM u
WHERE d.application_sector_detail_id = u.asd_id
  AND d.created_by IS NULL;

-- 12) contact_persons  (FK: app_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.contact_persons cp
SET created_by = u.user_id
FROM u
WHERE cp.app_sector_detail_id = u.asd_id
  AND cp.created_by IS NULL;

-- 13) shareholders  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.shareholders s
SET created_by = u.user_id
FROM u
WHERE s.application_sector_detail_id = u.asd_id
  AND s.created_by IS NULL;

-- 14) managing_directors  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.managing_directors md
SET created_by = u.user_id
FROM u
WHERE md.application_sector_detail_id = u.asd_id
  AND md.created_by IS NULL;

-- 15) fire  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.fire f
SET created_by = u.user_id
FROM u
WHERE f.application_sector_detail_id = u.asd_id
  AND f.created_by IS NULL;

-- 16) insurance_cover_details  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.insurance_cover_details icd
SET created_by = u.user_id
FROM u
WHERE icd.application_sector_detail_id = u.asd_id
  AND icd.created_by IS NULL;

-- 17) ardhi_information  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ardhi_information ai
SET created_by = u.user_id
FROM u
WHERE ai.application_sector_detail_id = u.asd_id
  AND ai.created_by IS NULL;

COMMIT;


BEGIN;

-- -------------------------------------------------------------------------
-- Helper CTE reused in every block:
--   resolves application_id -> user_id via username match
-- -------------------------------------------------------------------------

-- 1) applications
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.applications a
SET created_by = u.user_id
FROM u
WHERE a.id = u.application_id
  AND a.created_by IS NULL;

-- 2) application_sector_details  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_sector_details asd
SET created_by = u.user_id
FROM u
WHERE asd.application_id = u.application_id
  AND asd.created_by IS NULL;

-- 3) certificates  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.certificates c
SET created_by = u.user_id
FROM u
WHERE c.application_id = u.application_id
  AND c.created_by IS NULL;

-- 4) application_electrical_installation  (FK: application_id)
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.application_electrical_installation aei
SET created_by = u.user_id
FROM u
WHERE aei.application_id = u.application_id
  AND aei.created_by IS NULL;

-- -------------------------------------------------------------------------
-- For the remaining tables we join through application_sector_details
-- to reach the application -> username -> user_id chain.
-- -------------------------------------------------------------------------

-- 5) documents  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.documents d
SET created_by = u.user_id
FROM u
WHERE d.application_sector_detail_id = u.asd_id
  AND d.created_by IS NULL;

-- 6) contact_persons  (FK: app_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.contact_persons cp
SET created_by = u.user_id
FROM u
WHERE cp.app_sector_detail_id = u.asd_id
  AND cp.created_by IS NULL;

-- 7) shareholders  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.shareholders s
SET created_by = u.user_id
FROM u
WHERE s.application_sector_detail_id = u.asd_id
  AND s.created_by IS NULL;

-- 8) managing_directors  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.managing_directors md
SET created_by = u.user_id
FROM u
WHERE md.application_sector_detail_id = u.asd_id
  AND md.created_by IS NULL;

-- 9) fire  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.fire f
SET created_by = u.user_id
FROM u
WHERE f.application_sector_detail_id = u.asd_id
  AND f.created_by IS NULL;

-- 10) insurance_cover_details  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.insurance_cover_details icd
SET created_by = u.user_id
FROM u
WHERE icd.application_sector_detail_id = u.asd_id
  AND icd.created_by IS NULL;

-- 11) ardhi_information  (FK: application_sector_detail_id)
WITH u AS (
    SELECT asd.id AS asd_id, usr.id AS user_id
    FROM public.application_sector_details asd
    JOIN public.applications a  ON a.id = asd.application_id
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ardhi_information ai
SET created_by = u.user_id
FROM u
WHERE ai.application_sector_detail_id = u.asd_id
  AND ai.created_by IS NULL;

COMMIT;

