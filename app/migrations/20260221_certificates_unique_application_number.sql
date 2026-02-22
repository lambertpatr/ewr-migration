-- Migration: add UNIQUE constraint on certificates.application_number
-- This lets us use ON CONFLICT (application_number) DO UPDATE in all
-- import pipelines instead of the md5-based surrogate id trick.
-- Safe to run multiple times (IF NOT EXISTS / DO NOTHING guards).

DO $$
BEGIN
    -- 1. Remove any duplicate (application_number) rows, keeping the most-recently
    --    updated row for each application_number so the unique constraint can be added.
    DELETE FROM public.certificates
    WHERE id IN (
        SELECT id
        FROM (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY application_number
                       ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id
                   ) AS rn
            FROM public.certificates
            WHERE application_number IS NOT NULL
        ) ranked
        WHERE rn > 1
    );

    -- 2. Create the unique constraint if it doesn't already exist.
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint c
        JOIN   pg_class t ON t.oid = c.conrelid
        JOIN   pg_namespace n ON n.oid = t.relnamespace
        WHERE  n.nspname = 'public'
          AND  t.relname = 'certificates'
          AND  c.contype = 'u'
          AND  c.conname = 'uq_certificates_application_number'
    ) THEN
        ALTER TABLE public.certificates
            ADD CONSTRAINT uq_certificates_application_number
            UNIQUE (application_number);
        RAISE NOTICE 'Created uq_certificates_application_number';
    ELSE
        RAISE NOTICE 'uq_certificates_application_number already exists – skipped';
    END IF;
END $$;
