-- Add provenance flag for migrated LOIS data
-- This column lets us distinguish migrated LOIS rows from native system rows.

BEGIN;

ALTER TABLE public.applications
ADD COLUMN IF NOT EXISTS is_from_lois boolean NOT NULL DEFAULT false;

COMMIT;
