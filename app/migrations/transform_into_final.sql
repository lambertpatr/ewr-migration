SET LOCAL synchronous_commit TO OFF;

DO $$
BEGIN
    RAISE NOTICE '[staging-transform] starting (new normalized schema)';
END $$;

-------------------------------------------------------------------------------
-- 0-pre) Drop blocking unique constraints on certificates and applications
--
-- Hibernate generates unique constraints on approval_no (single-column) and
-- sometimes on application_number. These block our upserts which use
-- ON CONFLICT (id). Drop ALL unique constraints that mention approval_no or
-- application_number on certificates, and approval_no on applications.
-- We do NOT re-add them — ON CONFLICT (id) is sufficient and id is always PK.
-------------------------------------------------------------------------------
DO $$
DECLARE
    _rec                  RECORD;
    _schema               text := 'public';
    _app_attnum           smallint;
    _cert_apprv_attnum    smallint;
    _cert_appnum_attnum   smallint;
BEGIN
    -- ── applications.approval_no ─────────────────────────────────────────────
    SELECT attnum INTO _app_attnum
    FROM pg_attribute
    WHERE attrelid = (SELECT c.oid FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = _schema AND c.relname = 'applications')
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
              AND  _app_attnum = ANY(con.conkey)
        LOOP
            EXECUTE format('ALTER TABLE %I.applications DROP CONSTRAINT IF EXISTS %I',
                           _schema, _rec.conname);
            RAISE NOTICE '[staging-transform] Dropped applications unique on approval_no: %', _rec.conname;
        END LOOP;
    END IF;

    -- ── certificates: any unique mentioning approval_no ──────────────────────
    SELECT attnum INTO _cert_apprv_attnum
    FROM pg_attribute
    WHERE attrelid = (SELECT c.oid FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = _schema AND c.relname = 'certificates')
      AND attname = 'approval_no';

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
            RAISE NOTICE '[staging-transform] Dropped certificates unique on approval_no: %', _rec.conname;
        END LOOP;
    END IF;

    -- ── certificates: drop any unique mentioning application_number EXCEPT our own ──
    -- We KEEP uq_certificates_application_number because ON CONFLICT (application_number)
    -- in section 2b depends on it. Drop only other conflicting uniques on that column.
    SELECT attnum INTO _cert_appnum_attnum
    FROM pg_attribute
    WHERE attrelid = (SELECT c.oid FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = _schema AND c.relname = 'certificates')
      AND attname = 'application_number';

    IF _cert_appnum_attnum IS NOT NULL THEN
        FOR _rec IN
            SELECT con.conname
            FROM   pg_constraint con
            JOIN   pg_class cls ON cls.oid = con.conrelid
            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
            WHERE  con.contype = 'u'
              AND  nsp.nspname = _schema
              AND  cls.relname = 'certificates'
              AND  _cert_appnum_attnum = ANY(con.conkey)
              AND  con.conname <> 'uq_certificates_application_number'  -- keep our required constraint
        LOOP
            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                           _schema, _rec.conname);
            RAISE NOTICE '[staging-transform] Dropped certificates unique on application_number: %', _rec.conname;
        END LOOP;
    END IF;

END $$;

-------------------------------------------------------------------------------
-- 0-pre-b) Ensure uq_certificates_application_number exists before section 2b.
-- ON CONFLICT (application_number) requires this unique constraint.
-- Dedup first so the constraint creation does not fail on existing duplicates.
-------------------------------------------------------------------------------
DO $$
BEGIN
    -- Deduplicate certificates by application_number (keep newest row).
    WITH ranked AS (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY application_number
                ORDER BY COALESCE(updated_at, created_at) DESC, created_at DESC, id
            ) AS rn
        FROM public.certificates
        WHERE application_number IS NOT NULL
          AND NULLIF(TRIM(application_number), '') IS NOT NULL
    )
    DELETE FROM public.certificates c
    USING ranked r
    WHERE c.id = r.id
      AND r.rn > 1;

    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint con
        JOIN   pg_class rel ON rel.oid = con.conrelid
        JOIN   pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE  nsp.nspname = 'public'
          AND  rel.relname = 'certificates'
          AND  con.conname = 'uq_certificates_application_number'
    ) THEN
        ALTER TABLE public.certificates
            ADD CONSTRAINT uq_certificates_application_number
            UNIQUE (application_number);
        RAISE NOTICE '[staging-transform] Created uq_certificates_application_number';
    ELSE
        RAISE NOTICE '[staging-transform] uq_certificates_application_number already exists – skipped';
    END IF;
END $$;

-- Transform staging rows into the NEW normalized application schema.
--
-- Assumptions (new schema):
-- - Final tables exist:
--   - public.applications
--   - public.application_sector_details (FK application_id -> applications.id)
--   - public.documents (FK application_sector_detail_id -> application_sector_details.id)
--   - public.contact_persons (FK app_sector_detail_id -> application_sector_details.id)
--
-- Strategy:
-- - Insert applications using stage.generated_id as applications.id (so we can join without RETURNING)
-- - Insert application_sector_details as 1:1 with application for this migration (id = applications.id)
-- - Insert documents by joining stage_docs.application_generated_id -> application_sector_details.id
-- - Insert contact persons by joining stage_contact.application_generated_id -> application_sector_details.id
--
-- Notes:
-- - Staging tables are still named stage_ca_* because the Python pipeline uses those names.
-- - We only insert documents when file_name is present AND the legacy attachment id (logic_doc_id) is present.

-------------------------------------------------------------------------------
-- 0-pre-schema) Schema guard: ADD COLUMN IF NOT EXISTS for all child tables
--
-- This block runs before any DML so that the transform can run safely on
-- partially-aligned DBs (e.g. a DB that was created before application_id was
-- added to documents / contact_persons / fire / insurance_cover_details /
-- shareholders / managing_directors / ardhi_information).
-- All ADD COLUMNs are NO-OPs when the column already exists.
-------------------------------------------------------------------------------
DO $$
BEGIN
    -- documents
    ALTER TABLE public.documents
        ADD COLUMN IF NOT EXISTS application_id                 uuid,
        ADD COLUMN IF NOT EXISTS application_sector_detail_id   uuid,
        ADD COLUMN IF NOT EXISTS logic_doc_id                   bigint,
        ADD COLUMN IF NOT EXISTS documents_order                integer;

    -- contact_persons
    ALTER TABLE public.contact_persons
        ADD COLUMN IF NOT EXISTS application_id                 uuid,
        ADD COLUMN IF NOT EXISTS app_sector_detail_id           uuid;

    -- fire
    ALTER TABLE public.fire
        ADD COLUMN IF NOT EXISTS application_id                 uuid,
        ADD COLUMN IF NOT EXISTS application_sector_detail_id   uuid;

    -- insurance_cover_details
    ALTER TABLE public.insurance_cover_details
        ADD COLUMN IF NOT EXISTS application_id                 uuid,
        ADD COLUMN IF NOT EXISTS application_sector_detail_id   uuid;

    -- shareholders (best-effort — table may not exist yet)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'shareholders') THEN
        EXECUTE 'ALTER TABLE public.shareholders
                 ADD COLUMN IF NOT EXISTS application_id               uuid,
                 ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid';
    END IF;

    -- managing_directors (best-effort)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'managing_directors') THEN
        EXECUTE 'ALTER TABLE public.managing_directors
                 ADD COLUMN IF NOT EXISTS application_id               uuid,
                 ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid';
    END IF;

    -- ardhi_information (best-effort)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'ardhi_information') THEN
        EXECUTE 'ALTER TABLE public.ardhi_information
                 ADD COLUMN IF NOT EXISTS application_id               uuid,
                 ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid';
    END IF;

    -- ── Electrical installation child tables (best-effort) ──────────────

    -- application_electrical_installation
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'application_electrical_installation') THEN
        EXECUTE 'ALTER TABLE public.application_electrical_installation
                 ADD COLUMN IF NOT EXISTS application_id uuid';
    END IF;

    -- personal_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'personal_details') THEN
        EXECUTE 'ALTER TABLE public.personal_details
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- contact_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'contact_details') THEN
        EXECUTE 'ALTER TABLE public.contact_details
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- attachments
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'attachments') THEN
        EXECUTE 'ALTER TABLE public.attachments
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- work_experience
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'work_experience') THEN
        EXECUTE 'ALTER TABLE public.work_experience
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- self_employed
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'self_employed') THEN
        EXECUTE 'ALTER TABLE public.self_employed
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- supervisor_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'supervisor_details') THEN
        EXECUTE 'ALTER TABLE public.supervisor_details
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- costumer_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'costumer_details') THEN
        EXECUTE 'ALTER TABLE public.costumer_details
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    -- certificate_verifications
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'certificate_verifications') THEN
        EXECUTE 'ALTER TABLE public.certificate_verifications
                 ADD COLUMN IF NOT EXISTS application_id                        uuid,
                 ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid';
    END IF;

    RAISE NOTICE '[staging-transform] schema guard: all child table columns ensured';
END $$;

-------------------------------------------------------------------------------
-- 0) Pre-flight: Ensure mandatory categories exist
--
-- The transformation relies on joining categories by name.
-- We insert any missing categories from the known list to ensure foreign keys resolve.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_missing_cats text[] := ARRAY[
        'Petroleum Retail',
        'Township and Village Petroleum Retail',
        'Petroleum Wholesales',
        'Petroleum Station',
        'Village Petroleum Station',
        'Petroleum Storage Deport',
        'Petroleum Storage Business',
        'Petroleum Consumer Installation',
        'Consumer Installation (Agriculture)',
        'Consumer Installation (Mining)',
        'Consumer Installation (Transporter)',
        'Pipeline Transportation (up to 100 km)',
        'Pipeline Transportation (above 100 km)',
        'Petroleum Marine Loading and Offloading facility Construction Approval',
        'Petroleum Waste Oil Recycling Plant Construction Approval',
        'Condensate Storage Construction Approval',
        'LPG Storage and Filling Plant',
        'LPG Wholesalers Business',
        'Petroleum LPG Wholesalers Business',
        'Lubricant Distribution Business',
        'Lubricant Blending Plant',
        'Lubricant Wholesales Business'
    ];
    v_cat text;
    v_sector_name text;
    v_sector_id uuid;
BEGIN
    -- Infer sector from the current upload (stage_categories is populated by the importer).
    -- If stage_categories is empty for any reason, fall back to PETROLEUM.
    SELECT COALESCE(MAX(NULLIF(trim(sector_name), '')), 'PETROLEUM')
      INTO v_sector_name
      FROM public.stage_categories;

    SELECT id
      INTO v_sector_id
      FROM public.sectors
     WHERE LOWER(TRIM(name)) = LOWER(TRIM(v_sector_name))
     LIMIT 1;

    IF v_sector_id IS NULL THEN
        -- Last-resort fallback: still avoid NULL sector_id (categories.sector_id is NOT NULL).
        SELECT id
          INTO v_sector_id
          FROM public.sectors
         WHERE LOWER(TRIM(name)) = 'petroleum'
         LIMIT 1;
    END IF;

    FOREACH v_cat IN ARRAY v_missing_cats
    LOOP
        INSERT INTO public.categories (
            id,
            name,
            sector_id,
            is_approved,
            category_type,
            created_at,
            updated_at,
            created_by,
            updated_by
        )
        SELECT
            gen_random_uuid(),
            v_cat,
            v_sector_id,
            true,
            'License',
            now(),
            now(),
            NULL,
            NULL
        WHERE NOT EXISTS (
            SELECT 1 FROM public.categories c WHERE LOWER(TRIM(c.name)) = LOWER(TRIM(v_cat))
        );
    END LOOP;
END $$;


-------------------------------------------------------------------------------
-- 0b) Auto-create categories from stage_categories
--
-- If the user uploaded new applications with novel license types, they are
-- distinct-collected into public.stage_categories (name, sector_name).
-- We look up the sector_id from public.sectors and insert the new category.
-------------------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
    v_sector_id uuid;
    v_inserted_count int := 0;
BEGIN
    FOR r IN
        SELECT DISTINCT s.name, s.sector_name
        FROM public.stage_categories s
        WHERE NULLIF(trim(s.name), '') IS NOT NULL
    LOOP
        -- Look up sector_id by name (case-insensitive)
        SELECT id INTO v_sector_id
        FROM public.sectors
        WHERE LOWER(TRIM(name)) = LOWER(TRIM(r.sector_name))
        LIMIT 1;
        
        -- If sector not found, we can either skip or use a default.
        -- Here we skip to avoid bad data, or you could raise notice.
        IF v_sector_id IS NOT NULL THEN
            INSERT INTO public.categories (
                id,
                name,
                sector_id,
                is_approved,
                category_type,
                created_at,
                updated_at,
                created_by,
                updated_by
            )
            SELECT
                gen_random_uuid(),
                r.name,
                v_sector_id,
                true,       -- auto-approve imported categories
                'License',  -- default type
                now(),
                now(),
                NULL,
                NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM public.categories c WHERE LOWER(TRIM(c.name)) = LOWER(TRIM(r.name))
            );
            
            IF FOUND THEN
                v_inserted_count := v_inserted_count + 1;
            END IF;
        ELSE
            RAISE NOTICE 'Skipping category "%" because sector "%" was not found in public.sectors', r.name, r.sector_name;
        END IF;

    END LOOP;
    RAISE NOTICE '[staging-transform] Auto-created % categories from staging', v_inserted_count;
END $$;


-------------------------------------------------------------------------------
-- 1) applications
-- zone_id is resolved via napa_regions → zones when both tables exist.
-- On dev DBs that lack these tables, zone_id is inserted as NULL (safe fallback).
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_apps_inserted bigint;
    v_zone_expr     text;
    v_sql           text;
BEGIN
    -- Determine zone_id expression based on whether lookup tables exist.
    IF (SELECT COUNT(*) FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname IN ('napa_regions', 'zones')) = 2
    THEN
        v_zone_expr :=
            '(SELECT z.id FROM public.napa_regions nr'
            ' JOIN public.zones z ON z.id = nr.zone_id'
            ' WHERE lower(trim(nr.name)) = lower(trim(s.region))'
            ' AND nr.zone_id IS NOT NULL LIMIT 1)';
    ELSE
        v_zone_expr := 'NULL::uuid';
    END IF;

    v_sql :=
        'INSERT INTO public.applications ('
        '    id, created_at, created_by, deleted_at, deleted_by, updated_at, updated_by,'
        '    effective_date, expire_date, completed_at,'
        '    application_number, application_type,'
        '    approval_no, status, username, is_from_lois,'
        '    approval_date, category_id, certificate_id, current_step_id,'
        '    intimate_date, months_eligible, workflow_id, zone_id,'
        '    responsible_role_names, licence_path, license_type, category_license_type,'
        '    zone_name, payer_code, current_step_name, pending_actions,'
        '    old_parent_application_id'
        ') SELECT'
        '    s.generated_id,'
        '    now(),'
        '    CASE WHEN NULLIF(trim(s.old_created_by), '''') ~* ''^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'''
        '         THEN NULLIF(trim(s.old_created_by), '''')::uuid ELSE NULL END,'
        '    NULL::timestamp, NULL::uuid, NULL::timestamp, NULL::uuid,'
        '    CASE WHEN NULLIF(NULLIF(trim(s.effective_date),''''),''\N'') IS NOT NULL'
        '         THEN NULLIF(NULLIF(trim(s.effective_date),''''),''\N'')::timestamp::date END,'
        '    CASE WHEN NULLIF(NULLIF(trim(s.expire_date),''''),''\N'') IS NOT NULL'
        '         THEN NULLIF(NULLIF(trim(s.expire_date),''''),''\N'')::timestamp::date END,'
        '    CASE WHEN NULLIF(NULLIF(trim(s.completed_at),''''),''\N'') IS NOT NULL'
        '         THEN NULLIF(NULLIF(trim(s.completed_at),''''),''\N'')::timestamp END,'
        '    NULLIF(s.application_number, ''''),'
        '    CASE UPPER(TRIM(s.application_type))'
        '        WHEN ''EXTENSION''   THEN ''EXTEND'''
        '        WHEN ''RENEWAL''     THEN ''RENEW'''
        '        WHEN ''CHANGE_NAME'' THEN ''CHANGE_OF_NAME'''
        '        WHEN ''CHANGES''     THEN ''CHANGE_OF_NAME'''
        '        ELSE NULLIF(UPPER(TRIM(s.application_type)), '''')'
        '    END,'
        '    NULLIF(trim(s.approval_no), ''''),'
        '    ''APPROVED'','
        '    lower(NULLIF(trim(s.username), '''')),'
        '    true,'
        '    NULL::date, cat.id, NULL::uuid, NULL::uuid,'
        '    NULL::date, NULL::integer, NULL::uuid,'
        '    ' || v_zone_expr || ','
        '    NULL::text, NULL::text,'
        '    CASE'
        '        WHEN TRIM(sec.name) = ''PETROLEUM'' AND cat.category_type = ''Construction'' THEN ''CONSTRUCTION_PETROLEUM'''
        '        WHEN TRIM(sec.name) = ''PETROLEUM'' AND cat.category_type = ''License''      THEN ''LICENSE_PETROLEUM'''
        '        WHEN TRIM(sec.name) = ''NATURAL_GAS'' AND cat.category_type = ''Construction'' THEN ''CONSTRUCTION_NATURAL_GAS'''
        '        WHEN TRIM(sec.name) = ''NATURAL_GAS'' AND cat.category_type = ''License''    THEN ''LICENSE_NATURAL_GAS'''
        '        WHEN TRIM(sec.name) = ''WATER_SUPPLY''                                       THEN ''LICENSE_WATER'''
        '        WHEN TRIM(sec.name) = ''ELECTRICITY''                                        THEN ''LICENSE_ELECTRICITY_SUPPLY'''
        '        ELSE NULL'
        '    END,'
        '    ''OPERATIONAL'','
        '    NULL::text, NULL::text, NULL::text, NULL::text,'
        '    NULLIF(trim(s.old_parent_application_id), '''')'
        ' FROM ('
        '    SELECT s.*, row_number() OVER ('
        '        PARTITION BY COALESCE(NULLIF(s.application_number,''''), s.generated_id::text)'
        '        ORDER BY s.source_row_no NULLS LAST, s.generated_id'
        '    ) AS rn'
        '    FROM public.stage_ca_applications_raw s'
        ' ) s'
        ' LEFT JOIN public.categories cat ON LOWER(TRIM(cat.name)) = LOWER(TRIM(s.license_type))'
        ' LEFT JOIN public.sectors     sec ON sec.id = cat.sector_id'
        ' WHERE s.rn = 1 AND NULLIF(trim(s.application_number), '''') IS NOT NULL'
        ' ON CONFLICT (application_number) DO UPDATE SET'
        '    effective_date   = COALESCE(EXCLUDED.effective_date,   public.applications.effective_date),'
        '    expire_date      = COALESCE(EXCLUDED.expire_date,      public.applications.expire_date),'
        '    completed_at     = COALESCE(EXCLUDED.completed_at,     public.applications.completed_at),'
        '    approval_no      = COALESCE(EXCLUDED.approval_no,      public.applications.approval_no),'
        '    application_type = COALESCE(EXCLUDED.application_type, public.applications.application_type),'
        '    status           = COALESCE(EXCLUDED.status,           public.applications.status),'
        '    username         = COALESCE(EXCLUDED.username,         public.applications.username),'
        '    is_from_lois     = true,'
        '    license_type     = COALESCE(EXCLUDED.license_type,     public.applications.license_type),'
        '    category_id      = COALESCE(EXCLUDED.category_id,      public.applications.category_id),'
        '    zone_id          = COALESCE(EXCLUDED.zone_id,          public.applications.zone_id),'
        '    zone_name        = COALESCE(EXCLUDED.zone_name,        public.applications.zone_name),'
        '    old_parent_application_id = COALESCE(EXCLUDED.old_parent_application_id, public.applications.old_parent_application_id),'
        '    updated_at       = now()';

    EXECUTE v_sql;
    GET DIAGNOSTICS v_apps_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted apps=%', v_apps_inserted;
END $$;



-------------------------------------------------------------------------------
-- 2b) certificates (extended application fields)
-- certificates is keyed by application_id and stores many application-like attributes.
-- We only populate fields we reliably have from staging; system-generated fields remain NULL.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_cert_inserted bigint;
BEGIN
    INSERT INTO public.certificates (
        id,
        created_at,
        created_by,
        deleted_at,
        deleted_by,
        updated_at,
        updated_by,

        application_id,
        owner_id,
        application_number,
        approval_no,
        application_certificate_type,
        effective_date,
        expire_date,
        intimate_date,
        months_eligible,
        approval_date,
        licence_path,
        license_type,
        category_license_type,
        certificate_owner,
        zone_id,
        zone_name
    )
    SELECT
        -- Use gen_random_uuid() – the stable business key for conflict resolution
        -- is now application_number (unique constraint), not the id.
        gen_random_uuid() AS id,
        now() AS created_at,
        NULL::uuid AS created_by,
        NULL::timestamptz AS deleted_at,
        NULL::uuid AS deleted_by,
        now() AS updated_at,
        NULL::uuid AS updated_by,

        a.id AS application_id,
        NULL::uuid AS owner_id,
        a.application_number,
        a.approval_no,
        -- application_certificate_type from staged application_type (NEW/RENEW/UPGRADE etc.)
        COALESCE(
            CASE UPPER(TRIM(s.application_type))
                WHEN 'EXTENSION'   THEN 'EXTEND'
                WHEN 'RENEWAL'     THEN 'RENEW'
                WHEN 'CHANGE_NAME' THEN 'CHANGE_OF_NAME'
                WHEN 'CHANGES'     THEN 'CHANGE_OF_NAME'
                ELSE NULLIF(UPPER(TRIM(s.application_type)), '')
            END,
            'NEW'
        ) AS application_certificate_type,
        a.effective_date,
        a.expire_date,
        a.intimate_date,
        a.months_eligible,
        a.approval_date,
        a.licence_path,
        a.license_type,
        a.category_license_type,
        NULL::text AS certificate_owner,
        a.zone_id,
        a.zone_name
    FROM public.applications a
    -- One certificate row per application_number (first occurrence in staging).
    JOIN (
        SELECT DISTINCT ON (application_number)
               application_number,
               application_type
        FROM   public.stage_ca_applications_raw
        ORDER  BY application_number,
                  source_row_no
    ) s ON s.application_number = a.application_number
    WHERE NULLIF(trim(a.application_number), '') IS NOT NULL
    -- Use the unique constraint on application_number for conflict detection.
    -- This is simpler and more reliable than the old md5-id approach.
    ON CONFLICT (application_number) DO UPDATE
    SET
        application_number   = COALESCE(EXCLUDED.application_number,   public.certificates.application_number),
        approval_no          = COALESCE(EXCLUDED.approval_no,          public.certificates.approval_no),
        application_id       = EXCLUDED.application_id,
        effective_date       = COALESCE(EXCLUDED.effective_date,       public.certificates.effective_date),
        expire_date          = COALESCE(EXCLUDED.expire_date,          public.certificates.expire_date),
        intimate_date        = COALESCE(EXCLUDED.intimate_date,        public.certificates.intimate_date),
        months_eligible      = COALESCE(EXCLUDED.months_eligible,      public.certificates.months_eligible),
        approval_date        = COALESCE(EXCLUDED.approval_date,        public.certificates.approval_date),
        licence_path         = COALESCE(EXCLUDED.licence_path,         public.certificates.licence_path),
        license_type         = EXCLUDED.license_type,
        category_license_type = EXCLUDED.category_license_type,
        application_certificate_type = COALESCE(EXCLUDED.application_certificate_type, public.certificates.application_certificate_type),
        zone_id              = COALESCE(EXCLUDED.zone_id,              public.certificates.zone_id),
        zone_name            = COALESCE(EXCLUDED.zone_name,            public.certificates.zone_name),
        updated_at           = now();

    GET DIAGNOSTICS v_cert_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted certificates=%', v_cert_inserted;
END $$;


-------------------------------------------------------------------------------
-- 2) application_sector_details (1:1 row with applications for this migration)
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_asd_inserted bigint;
BEGIN
    INSERT INTO public.application_sector_details (
        id,
        created_at,
        updated_at,
        application_id,
        applicant_legal_status_id,
        -- location / address fields
        region,
        district,
        ward,
        street,
        road,
        plot_no,
        address_code,
        address_no,
        block_no,
        -- contact / business fields
        mobile_no,
        email,
        website,
        latitude,
        longitude,
        po_box,
        facility_name,
        company_name,
        tin,
        tin_name,
        brela_number,
        brela_registration_type,
        certificate_of_incorporation_no
    )
    SELECT
        a.id AS id,
        now() AS created_at,
        now() AS updated_at,
        a.id AS application_id,
        als.id AS applicant_legal_status_id,
        -- location / address
        NULLIF(trim(s.region), '')          AS region,
        NULLIF(trim(s.district), '')        AS district,
        NULLIF(trim(s.ward), '')            AS ward,
        NULLIF(trim(s.street), '')          AS street,
        NULLIF(trim(s.road), '')            AS road,
        NULLIF(trim(s.plot_no), '')         AS plot_no,
        NULLIF(trim(s.address_code), '')    AS address_code,
        NULLIF(trim(s.address_no), '')      AS address_no,
        NULLIF(trim(s.block_no), '')        AS block_no,
        -- contact / business
        NULLIF(trim(s.mobile_no), '')                       AS mobile_no,
        NULLIF(trim(s.email), '')                           AS email,
        NULLIF(trim(s.website), '')                         AS website,
        NULLIF(trim(s.latitude), '')                        AS latitude,
        NULLIF(trim(s.longitude), '')                       AS longitude,
        NULLIF(trim(s.po_box), '')::varchar                 AS po_box,
        NULLIF(trim(s.facility_name), '')                   AS facility_name,
        NULLIF(trim(s.company_name), '')                    AS company_name,
        NULLIF(trim(s.tin), '')                             AS tin,
        NULLIF(trim(s.tin_name), '')                        AS tin_name,
        NULLIF(trim(s.brela_number), '')                    AS brela_number,
        NULLIF(trim(s.brela_registration_type), '')         AS brela_registration_type,
        NULLIF(trim(s.certificate_of_incorporation_no), '') AS certificate_of_incorporation_no
    FROM public.applications a
    JOIN public.stage_ca_applications_raw s ON s.generated_id = a.id
    LEFT JOIN public.application_legal_status als ON LOWER(TRIM(als.name)) = LOWER(TRIM(s.application_legal_status_raw))
    WHERE a.approval_no IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.application_sector_details x
          WHERE x.id = a.id
      )
    ON CONFLICT (id) DO UPDATE SET
        applicant_legal_status_id  = COALESCE(EXCLUDED.applicant_legal_status_id,  public.application_sector_details.applicant_legal_status_id),
        region                     = COALESCE(EXCLUDED.region,                     public.application_sector_details.region),
        district                   = COALESCE(EXCLUDED.district,                   public.application_sector_details.district),
        ward                       = COALESCE(EXCLUDED.ward,                       public.application_sector_details.ward),
        street                     = COALESCE(EXCLUDED.street,                     public.application_sector_details.street),
        road                       = COALESCE(EXCLUDED.road,                       public.application_sector_details.road),
        plot_no                    = COALESCE(EXCLUDED.plot_no,                    public.application_sector_details.plot_no),
        address_code               = COALESCE(EXCLUDED.address_code,               public.application_sector_details.address_code),
        address_no                 = COALESCE(EXCLUDED.address_no,                 public.application_sector_details.address_no),
        block_no                   = COALESCE(EXCLUDED.block_no,                   public.application_sector_details.block_no),
        mobile_no                  = COALESCE(EXCLUDED.mobile_no,                  public.application_sector_details.mobile_no),
        email                      = COALESCE(EXCLUDED.email,                      public.application_sector_details.email),
        website                    = COALESCE(EXCLUDED.website,                    public.application_sector_details.website),
        latitude                   = COALESCE(EXCLUDED.latitude,                   public.application_sector_details.latitude),
        longitude                  = COALESCE(EXCLUDED.longitude,                  public.application_sector_details.longitude),
        po_box                     = COALESCE(EXCLUDED.po_box,                     public.application_sector_details.po_box),
        facility_name              = COALESCE(EXCLUDED.facility_name,              public.application_sector_details.facility_name),
        company_name               = COALESCE(EXCLUDED.company_name,               public.application_sector_details.company_name),
        tin                        = COALESCE(EXCLUDED.tin,                        public.application_sector_details.tin),
        tin_name                   = COALESCE(EXCLUDED.tin_name,                   public.application_sector_details.tin_name),
        brela_number               = COALESCE(EXCLUDED.brela_number,               public.application_sector_details.brela_number),
        brela_registration_type    = COALESCE(EXCLUDED.brela_registration_type,    public.application_sector_details.brela_registration_type),
        certificate_of_incorporation_no = COALESCE(EXCLUDED.certificate_of_incorporation_no, public.application_sector_details.certificate_of_incorporation_no),
        updated_at                 = now();

    GET DIAGNOSTICS v_asd_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted application_sector_details=%', v_asd_inserted;
END $$;


-------------------------------------------------------------------------------
-- 3) documents (normalized)
-- stage_ca_documents_raw.application_generated_id == applications.id
-- documents.application_sector_detail_id == application_sector_details.id
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_docs_inserted bigint;
BEGIN
    INSERT INTO public.documents (
        id,
        created_at,
        created_by,
        deleted_at,
        deleted_by,
        updated_at,
        updated_by,
        application_id,
        application_sector_detail_id,
        document_url,
        file_name,
        document_name,
        logic_doc_id
    )
    SELECT
        d.id,
        now() AS created_at,
        NULL::uuid AS created_by,
        NULL::timestamptz AS deleted_at,
        NULL::uuid AS deleted_by,
        now() AS updated_at,
        NULL::uuid AS updated_by,
        asd.application_id,
        asd.id AS application_sector_detail_id,
        NULL::text AS document_url,
        NULLIF(trim(d.file_name), '') AS file_name,
        NULLIF(trim(d.document_name), '') AS document_name,
        d.logic_doc_id
    FROM public.stage_ca_documents_raw d
    JOIN public.application_sector_details asd
        ON asd.id = d.application_generated_id
    WHERE NULLIF(trim(d.file_name), '') IS NOT NULL
      AND d.logic_doc_id IS NOT NULL
    ON CONFLICT (id) DO UPDATE SET
        application_id = COALESCE(EXCLUDED.application_id, public.documents.application_id),
        updated_at = now();

    GET DIAGNOSTICS v_docs_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted documents=%', v_docs_inserted;
END $$;


-------------------------------------------------------------------------------
-- 4) contact_persons (normalized)
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_contacts_inserted bigint;
BEGIN
    INSERT INTO public.contact_persons (
        id,
        created_at,
        created_by,
        deleted_at,
        deleted_by,
        updated_at,
        updated_by,
        application_id,
        app_sector_detail_id,
        contact_name,
        email,
        mobile_no,
        title
    )
    SELECT
        c.id,
        now() AS created_at,
        NULL::uuid AS created_by,
        NULL::timestamptz AS deleted_at,
        NULL::uuid AS deleted_by,
        now() AS updated_at,
        NULL::uuid AS updated_by,
        asd.application_id,
        asd.id AS app_sector_detail_id,
        NULLIF(trim(c.contact_name), '') AS contact_name,
        NULLIF(trim(c.email), '') AS email,
        NULLIF(trim(c.mobile_no), '') AS mobile_no,
        NULLIF(trim(c.title), '') AS title
    FROM public.stage_ca_contact_persons_raw c
    JOIN public.application_sector_details asd
        ON asd.id = c.application_generated_id
    WHERE (
        NULLIF(trim(c.contact_name), '') IS NOT NULL
        OR NULLIF(trim(c.email), '') IS NOT NULL
        OR NULLIF(trim(c.mobile_no), '') IS NOT NULL
        OR NULLIF(trim(c.title), '') IS NOT NULL
    )
      AND NOT EXISTS (
          SELECT 1
          FROM public.contact_persons x
          WHERE x.app_sector_detail_id = asd.id
      )
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_contacts_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted contact_persons=%', v_contacts_inserted;
END $$;


-- Shareholders are handled by a separate import endpoint/pipeline for now.
DO $$
BEGIN
    RAISE NOTICE '[staging-transform] shareholders skipped (handled separately)';
END $$;


-------------------------------------------------------------------------------
-- 5) fire (normalized)
-- Insert a fire row only when at least one fire field is present.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_fire_inserted bigint;
BEGIN
    INSERT INTO public.fire (
        id,
        created_at,
        updated_at,
        application_id,
        application_sector_detail_id,
        fire_certificate_control_number,
        premise_name,
        region,
        district,
        ward,
        administrative_area,
        street,
        valid_from,
        valid_to
    )
    SELECT
        gen_random_uuid() AS id,
        now() AS created_at,
        now() AS updated_at,
        asd.application_id,
        asd.id AS application_sector_detail_id,
        NULLIF(trim(s.fire_certificate_control_number), '') AS fire_certificate_control_number,
        NULLIF(trim(s.fire_premise_name), '') AS premise_name,
        NULLIF(trim(s.fire_region), '') AS region,
        NULLIF(trim(s.fire_district), '') AS district,
        NULLIF(trim(s.fire_ward), '') AS ward,
        NULLIF(trim(s.fire_administrative_area), '') AS administrative_area,
        NULLIF(trim(s.fire_street), '') AS street,
        NULLIF(trim(s.fire_valid_from), '') AS valid_from,
        NULLIF(trim(s.fire_valid_to), '') AS valid_to
    FROM public.application_sector_details asd
    JOIN public.stage_ca_applications_raw s ON s.generated_id = asd.id
    WHERE (
        NULLIF(trim(s.fire_certificate_control_number), '') IS NOT NULL
        OR NULLIF(trim(s.fire_premise_name), '') IS NOT NULL
        OR NULLIF(trim(s.fire_region), '') IS NOT NULL
        OR NULLIF(trim(s.fire_district), '') IS NOT NULL
        OR NULLIF(trim(s.fire_ward), '') IS NOT NULL
        OR NULLIF(trim(s.fire_administrative_area), '') IS NOT NULL
        OR NULLIF(trim(s.fire_street), '') IS NOT NULL
        OR NULLIF(trim(s.fire_valid_from), '') IS NOT NULL
        OR NULLIF(trim(s.fire_valid_to), '') IS NOT NULL
    )
      AND NOT EXISTS (
          SELECT 1
          FROM public.fire f
          WHERE f.application_sector_detail_id = asd.id
      )
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS v_fire_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted fire=%', v_fire_inserted;
END $$;


-------------------------------------------------------------------------------
-- 6) insurance_cover_details (normalized)
-- Insert only when at least one insurance field is present.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_ins_inserted bigint;
BEGIN
    INSERT INTO public.insurance_cover_details (
        id,
        created_at,
        updated_at,
        application_id,
        application_sector_detail_id,
        cover_note_number,
        cover_note_ref_no,
        cover_note_start_date,
        cover_note_end_date,
        insurance_ref_no,
        insurer_company_name,
        policy_holder_name,
        risk_name,
        subject_matter_desc
    )
    SELECT
        gen_random_uuid() AS id,
        now() AS created_at,
        now() AS updated_at,
        asd.application_id,
        asd.id AS application_sector_detail_id,
        NULLIF(trim(s.cover_note_number), '') AS cover_note_number,
        NULLIF(trim(s.cover_note_ref_no), '') AS cover_note_ref_no,
        NULLIF(trim(s.cover_note_start_date), '') AS cover_note_start_date,
        NULLIF(trim(s.cover_note_end_date), '') AS cover_note_end_date,
        NULLIF(trim(s.insurance_ref_no), '') AS insurance_ref_no,
        NULLIF(trim(s.insurer_company_name), '') AS insurer_company_name,
        NULLIF(trim(s.policy_holder_name), '') AS policy_holder_name,
        NULLIF(trim(s.risk_name), '') AS risk_name,
        NULLIF(trim(s.subject_matter_desc), '') AS subject_matter_desc
    FROM public.application_sector_details asd
    JOIN public.stage_ca_applications_raw s ON s.generated_id = asd.id
    WHERE (
        NULLIF(trim(s.cover_note_number), '') IS NOT NULL
        OR NULLIF(trim(s.cover_note_ref_no), '') IS NOT NULL
        OR NULLIF(trim(s.cover_note_start_date), '') IS NOT NULL
        OR NULLIF(trim(s.cover_note_end_date), '') IS NOT NULL
        OR NULLIF(trim(s.insurance_ref_no), '') IS NOT NULL
        OR NULLIF(trim(s.insurer_company_name), '') IS NOT NULL
        OR NULLIF(trim(s.policy_holder_name), '') IS NOT NULL
        OR NULLIF(trim(s.risk_name), '') IS NOT NULL
        OR NULLIF(trim(s.subject_matter_desc), '') IS NOT NULL
    )
      AND NOT EXISTS (
          SELECT 1
          FROM public.insurance_cover_details i
          WHERE i.application_sector_detail_id = asd.id
      )
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS v_ins_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted insurance_cover_details=%', v_ins_inserted;
END $$;


-------------------------------------------------------------------------------
-- 7) Backfill application_id on ALL child tables via application_sector_details
--
-- For any existing rows where application_id IS NULL but
-- application_sector_detail_id is set, resolve through asd.application_id.
-- This covers both newly imported and pre-existing historical rows.
-- Idempotent: only updates where application_id IS NULL.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_docs_bf      bigint := 0;
    v_contacts_bf  bigint := 0;
    v_fire_bf      bigint := 0;
    v_insurance_bf bigint := 0;
    v_shareholders_bf  bigint := 0;
    v_directors_bf     bigint := 0;
    v_ardhi_bf         bigint := 0;
BEGIN
    -- documents
    UPDATE public.documents d
    SET    application_id = asd.application_id
    FROM   public.application_sector_details asd
    WHERE  d.application_sector_detail_id = asd.id
      AND  d.application_id IS NULL
      AND  asd.application_id IS NOT NULL;
    GET DIAGNOSTICS v_docs_bf = ROW_COUNT;

    -- contact_persons (uses app_sector_detail_id)
    UPDATE public.contact_persons cp
    SET    application_id = asd.application_id
    FROM   public.application_sector_details asd
    WHERE  cp.app_sector_detail_id = asd.id
      AND  cp.application_id IS NULL
      AND  asd.application_id IS NOT NULL;
    GET DIAGNOSTICS v_contacts_bf = ROW_COUNT;

    -- fire
    UPDATE public.fire f
    SET    application_id = asd.application_id
    FROM   public.application_sector_details asd
    WHERE  f.application_sector_detail_id = asd.id
      AND  f.application_id IS NULL
      AND  asd.application_id IS NOT NULL;
    GET DIAGNOSTICS v_fire_bf = ROW_COUNT;

    -- insurance_cover_details
    UPDATE public.insurance_cover_details icd
    SET    application_id = asd.application_id
    FROM   public.application_sector_details asd
    WHERE  icd.application_sector_detail_id = asd.id
      AND  icd.application_id IS NULL
      AND  asd.application_id IS NOT NULL;
    GET DIAGNOSTICS v_insurance_bf = ROW_COUNT;

    -- shareholders (skip if table doesn't exist yet)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'shareholders') THEN
        UPDATE public.shareholders s
        SET    application_id = asd.application_id
        FROM   public.application_sector_details asd
        WHERE  s.application_sector_detail_id = asd.id
          AND  s.application_id IS NULL
          AND  asd.application_id IS NOT NULL;
        GET DIAGNOSTICS v_shareholders_bf = ROW_COUNT;
    END IF;

    -- managing_directors (skip if table doesn't exist yet)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'managing_directors') THEN
        UPDATE public.managing_directors md
        SET    application_id = asd.application_id
        FROM   public.application_sector_details asd
        WHERE  md.application_sector_detail_id = asd.id
          AND  md.application_id IS NULL
          AND  asd.application_id IS NOT NULL;
        GET DIAGNOSTICS v_directors_bf = ROW_COUNT;
    END IF;

    -- ardhi_information (skip if table doesn't exist yet)
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'ardhi_information') THEN
        UPDATE public.ardhi_information ai
        SET    application_id = asd.application_id
        FROM   public.application_sector_details asd
        WHERE  ai.application_sector_detail_id = asd.id
          AND  ai.application_id IS NULL
          AND  asd.application_id IS NOT NULL;
        GET DIAGNOSTICS v_ardhi_bf = ROW_COUNT;
    END IF;

    RAISE NOTICE '[staging-transform] backfill application_id: documents=%, contact_persons=%, fire=%, insurance=%, shareholders=%, managing_directors=%, ardhi=%',
        v_docs_bf, v_contacts_bf, v_fire_bf, v_insurance_bf, v_shareholders_bf, v_directors_bf, v_ardhi_bf;
END $$;

-------------------------------------------------------------------------------
-- 7b) Backfill application_id on electrical installation child tables
--     via application_electrical_installation → application_id
--
-- SKIPPED when this transform is invoked from the applications-migration
-- endpoint (PETROLEUM, NATURAL_GAS, ELECTRICITY, WATER_SUPPLY).
-- For those sectors the data path is:
--   applications → certificates → application_sector_details → (fire / insurance / documents …)
-- The electrical installation pipeline (electrical_installations_upload) is
-- completely separate and sets application_id itself — do NOT touch those
-- tables here to avoid cross-contamination and the "column does not exist" error.
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_sector        text := current_setting('migration.sector_name', true);
    v_aei_bf        bigint := 0;
    v_pd_bf         bigint := 0;
    v_cd_bf         bigint := 0;
    v_att_bf        bigint := 0;
    v_we_bf         bigint := 0;
    v_se_bf         bigint := 0;
    v_sd_bf         bigint := 0;
    v_cud_bf        bigint := 0;
    v_cv_bf         bigint := 0;
BEGIN
    -- Skip entirely when called from the applications-migration upload.
    -- All four sector uploads (PETROLEUM, NATURAL_GAS, ELECTRICITY, WATER_SUPPLY)
    -- set migration.sector_name; if the variable is set, this is NOT the
    -- electrical-installations pipeline, so leave those tables alone.
    IF v_sector IS NOT NULL AND v_sector != '' THEN
        RAISE NOTICE '[staging-transform] block 7b skipped — sector=% uses applications→certificates path, not electrical_installation',
            v_sector;
        RETURN;
    END IF;

    -- application_electrical_installation: fill via applications.application_number
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'application_electrical_installation') THEN
        UPDATE public.application_electrical_installation aei
        SET    application_id = a.id
        FROM   public.applications a
        WHERE  a.application_number = aei.application_number
          AND  aei.application_id IS NULL
          AND  a.id IS NOT NULL;
        GET DIAGNOSTICS v_aei_bf = ROW_COUNT;
    END IF;

    -- personal_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'personal_details') THEN
        UPDATE public.personal_details pd
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  pd.application_electrical_installation_id = aei.id
          AND  pd.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_pd_bf = ROW_COUNT;
    END IF;

    -- contact_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'contact_details') THEN
        UPDATE public.contact_details cd
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  cd.application_electrical_installation_id = aei.id
          AND  cd.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_cd_bf = ROW_COUNT;
    END IF;

    -- attachments
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'attachments') THEN
        UPDATE public.attachments att
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  att.application_electrical_installation_id = aei.id
          AND  att.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_att_bf = ROW_COUNT;
    END IF;

    -- work_experience
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'work_experience') THEN
        UPDATE public.work_experience we
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  we.application_electrical_installation_id = aei.id
          AND  we.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_we_bf = ROW_COUNT;
    END IF;

    -- self_employed
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'self_employed') THEN
        UPDATE public.self_employed se
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  se.application_electrical_installation_id = aei.id
          AND  se.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_se_bf = ROW_COUNT;
    END IF;

    -- supervisor_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'supervisor_details') THEN
        UPDATE public.supervisor_details sd
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  sd.application_electrical_installation_id = aei.id
          AND  sd.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_sd_bf = ROW_COUNT;
    END IF;

    -- costumer_details
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'costumer_details') THEN
        UPDATE public.costumer_details cud
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  cud.application_electrical_installation_id = aei.id
          AND  cud.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_cud_bf = ROW_COUNT;
    END IF;

    -- certificate_verifications
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'certificate_verifications') THEN
        UPDATE public.certificate_verifications cv
        SET    application_id = aei.application_id
        FROM   public.application_electrical_installation aei
        WHERE  cv.application_electrical_installation_id = aei.id
          AND  cv.application_id IS NULL
          AND  aei.application_id IS NOT NULL;
        GET DIAGNOSTICS v_cv_bf = ROW_COUNT;
    END IF;

    RAISE NOTICE '[staging-transform] backfill application_id (elec): aei=%, personal_details=%, contact_details=%, attachments=%, work_experience=%, self_employed=%, supervisor_details=%, costumer_details=%, certificate_verifications=%',
        v_aei_bf, v_pd_bf, v_cd_bf, v_att_bf, v_we_bf, v_se_bf, v_sd_bf, v_cud_bf, v_cv_bf;
END $$;
