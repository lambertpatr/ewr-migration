-- Backfill created_by across application tables using ca_applications.username -> users.username
--
-- Rules:
-- - Only update when a.username is present and matches users.username
-- - Only set created_by when it is currently NULL
-- - Update:
--     * ca_applications.created_by
--     * ca_documents.created_by   (for docs belonging to applications)
--     * ca_contact_persons.created_by (for contacts belonging to applications)

BEGIN;

-- Applications
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.ca_applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ca_applications a
SET created_by = u.user_id
FROM u
WHERE a.id = u.application_id
  AND a.created_by IS NULL;

-- Documents
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.ca_applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ca_documents d
SET created_by = u.user_id
FROM u
WHERE d.application_id = u.application_id
  AND d.created_by IS NULL;

-- Contact persons
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.ca_applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ca_contact_persons c
SET created_by = u.user_id
FROM u
WHERE c.application_id = u.application_id
  AND c.created_by IS NULL;

-- Shareholders
WITH u AS (
    SELECT a.id AS application_id, usr.id AS user_id
    FROM public.ca_applications a
    JOIN public.users usr
      ON lower(trim(usr.username)) = lower(trim(a.username))
    WHERE a.username IS NOT NULL
      AND trim(a.username) <> ''
)
UPDATE public.ca_shareholders s
SET created_by = u.user_id
FROM u
WHERE s.application_id = u.application_id
  AND s.created_by IS NULL;

COMMIT;
