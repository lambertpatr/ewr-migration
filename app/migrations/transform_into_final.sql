SET LOCAL synchronous_commit TO OFF;

DO $$
BEGIN
    RAISE NOTICE '[staging-transform] starting (new normalized schema)';
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
-------------------------------------------------------------------------------
DO $$
DECLARE
    v_apps_inserted bigint;
BEGIN
    INSERT INTO public.applications (
        id,
        created_at,
        created_by,
        deleted_at,
        deleted_by,
        updated_at,
        updated_by,

        effective_date,
        expire_date,

        application_number,
        application_type,

        approval_no,
        status,

    username,

    is_from_lois,

        -- Other columns in applications table (set NULL for now unless you stage them)
        approval_date,
        category_id,
        certificate_id,
        current_step_id,
        intimate_date,
        months_eligible,
        workflow_id,
        zone_id,
        responsible_role_names,
        licence_path,
        license_type,
        category_license_type,
        zone_name,
        payer_code,
        current_step_name,
        pending_actions
    )
    SELECT
        s.generated_id AS id,
        now() AS created_at,
        CASE
            WHEN NULLIF(trim(s.old_created_by), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN NULLIF(trim(s.old_created_by), '')::uuid
            ELSE NULL
        END AS created_by,
        NULL::timestamp AS deleted_at,
        NULL::uuid AS deleted_by,
        NULL::timestamp AS updated_at,
        NULL::uuid AS updated_by,

    NULLIF(NULLIF(s.effective_date, ''), '\\N')::date AS effective_date,
    NULLIF(NULLIF(s.expire_date, ''), '\\N')::date AS expire_date,

        NULLIF(s.application_number, '') AS application_number,
        NULLIF(upper(s.application_type), '') AS application_type,

        NULLIF(trim(s.approval_no), '') AS approval_no,
    'APPROVED'::text AS status,

    NULLIF(trim(s.username), '') AS username,

    true AS is_from_lois,

        NULL::date AS approval_date,
        cat.id AS category_id,
        NULL::uuid AS certificate_id,
        NULL::uuid AS current_step_id,
        NULL::date AS intimate_date,
        NULL::integer AS months_eligible,
        NULL::uuid AS workflow_id,
        NULL::uuid AS zone_id,
        NULL::text AS responsible_role_names,
        NULL::text AS licence_path,
        -- Derive license_type from the sector the category belongs to + the category_type.
        -- sectors.name in DB: 'PETROLEUM', 'NATURAL_GAS', 'ELECTRICITY', 'WATER_SUPPLY'
        -- categories.category_type in DB: 'Construction' or 'License'
        -- The categories JOIN below resolves the raw Excel category name -> category row,
        -- then the sectors JOIN follows sector_id -> sector name, giving us both inputs.
        CASE
            -- ── PETROLEUM ────────────────────────────────────────────────────
            WHEN TRIM(sec.name) = 'PETROLEUM' AND cat.category_type = 'Construction'
                THEN 'CONSTRUCTION_PETROLEUM'
            WHEN TRIM(sec.name) = 'PETROLEUM' AND cat.category_type = 'License'
                THEN 'LICENSE_PETROLEUM'
            -- ── NATURAL GAS ──────────────────────────────────────────────────
            WHEN TRIM(sec.name) = 'NATURAL_GAS' AND cat.category_type = 'Construction'
                THEN 'CONSTRUCTION_NATURAL_GAS'
            WHEN TRIM(sec.name) = 'NATURAL_GAS' AND cat.category_type = 'License'
                THEN 'LICENSE_NATURAL_GAS'
            -- ── WATER SUPPLY ─────────────────────────────────────────────────
            WHEN TRIM(sec.name) = 'WATER_SUPPLY'
                THEN 'LICENSE_WATER'
            -- ── ELECTRICITY ──────────────────────────────────────────────────
            -- Electricity sector only has License categories (LICENSE_ELECTRICITY_SUPPLY).
            -- LICENSE_ELECTRICITY_INSTALLATION is handled exclusively by its own
            -- dedicated service (electrical_installation_import_service.py) and is
            -- never derived here.
            WHEN TRIM(sec.name) = 'ELECTRICITY'
                THEN 'LICENSE_ELECTRICITY_SUPPLY'
            -- ── fallback ─────────────────────────────────────────────────────
            ELSE NULL
        END AS license_type,
        'OPERATIONAL'::text AS category_license_type,
        NULL::text AS zone_name,
        NULL::text AS payer_code,
        NULL::text AS current_step_name,
        NULL::text AS pending_actions
    FROM (
        -- Dedupe inside this load by approval_no first.
        -- This is REQUIRED when using ON CONFLICT(approval_no) DO UPDATE;
        -- otherwise Postgres can attempt to update the same target row multiple times
        -- in one statement and raises CardinalityViolation.
        --
        -- If approval_no is blank (should already be filtered out), fall back to
        -- application_number, else generated_id.
        SELECT s.*,
               row_number() OVER (
                   PARTITION BY COALESCE(
                       NULLIF(trim(s.approval_no), ''),
                       NULLIF(s.application_number, ''),
                       s.generated_id::text
                   )
                   ORDER BY s.source_row_no NULLS LAST, s.generated_id
               ) AS rn
        FROM public.stage_ca_applications_raw s
    ) s
    LEFT JOIN public.categories cat ON LOWER(TRIM(cat.name)) = LOWER(TRIM(s.license_type))
    LEFT JOIN public.sectors     sec ON sec.id = cat.sector_id
    WHERE s.rn = 1
      AND NULLIF(trim(s.approval_no), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
                    FROM public.applications a
                    WHERE a.application_number IS NOT DISTINCT FROM NULLIF(s.application_number, '')
      )
        -- Some DBs enforce UNIQUE(approval_no). If the approval_no already exists,
        -- update the existing record with the staged values (id and metadata are preserved).
        ON CONFLICT (approval_no) DO UPDATE
        SET
            effective_date = EXCLUDED.effective_date,
            expire_date = EXCLUDED.expire_date,
            application_number = COALESCE(EXCLUDED.application_number, public.applications.application_number),
            application_type = COALESCE(EXCLUDED.application_type, public.applications.application_type),
            status = COALESCE(EXCLUDED.status, public.applications.status),
            username = COALESCE(EXCLUDED.username, public.applications.username),
            is_from_lois = true,
            license_type = EXCLUDED.license_type,
            updated_at = now();

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
        -- Stable ID: hash of (application_id, application_certificate_type) so reruns
        -- are idempotent and one application can have multiple certificate type rows.
        md5(a.id::text || '|' || COALESCE(NULLIF(UPPER(TRIM(s.application_type)), ''), 'NEW'))::uuid AS id,
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
        COALESCE(NULLIF(UPPER(TRIM(s.application_type)), ''), 'NEW') AS application_certificate_type,
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
    -- Join back to staging to get application_type for this specific row.
    -- Dedupe: one certificate per (application_number, application_certificate_type).
    JOIN (
        SELECT DISTINCT ON (application_number, COALESCE(NULLIF(UPPER(TRIM(application_type)), ''), 'NEW'))
               application_number,
               application_type
        FROM   public.stage_ca_applications_raw
        ORDER  BY application_number,
                  COALESCE(NULLIF(UPPER(TRIM(application_type)), ''), 'NEW'),
                  source_row_no
    ) s ON s.application_number = a.application_number
    WHERE a.approval_no IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM   public.certificates c
          WHERE  c.application_id = a.id
            AND  c.application_certificate_type = COALESCE(NULLIF(UPPER(TRIM(s.application_type)), ''), 'NEW')
      )
    -- Conflict on the composite unique (approval_no, application_certificate_type).
    -- Same approval_no with a different type (NEW vs RENEW) inserts a new row; same
    -- approval_no + same type refreshes the existing row.
    ON CONFLICT (approval_no, application_certificate_type) DO UPDATE
    SET
        application_number   = EXCLUDED.application_number,
        effective_date       = EXCLUDED.effective_date,
        expire_date          = EXCLUDED.expire_date,
        intimate_date        = EXCLUDED.intimate_date,
        months_eligible      = EXCLUDED.months_eligible,
        approval_date        = EXCLUDED.approval_date,
        licence_path         = EXCLUDED.licence_path,
        license_type         = EXCLUDED.license_type,
        category_license_type = EXCLUDED.category_license_type,
        zone_id              = EXCLUDED.zone_id,
        zone_name            = EXCLUDED.zone_name,
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
    ON CONFLICT (id) DO NOTHING;

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
