-- Transform staging rows into final tables.
-- Assumptions:
-- 1) Final tables exist: public.ca_applications, public.ca_documents
-- 2) Region/District/Ward columns in final are text names. If you store IDs, adjust accordingly.
-- 3) Legal status and license category ID columns are UUID in final tables.
--
-- Strategy:
-- - Insert applications using generated_id as the final id (so no RETURNING required)
-- - Insert documents by joining to final app id via generated_id
--
-- IMPORTANT: Adjust mapping tables below if you have them.

BEGIN;

-- Optional speed settings for the session running this script.
-- These are safe defaults for migration (durability trade-off).
SET LOCAL synchronous_commit TO OFF;

-- Emit counts so you can tell whether rows were inserted or skipped.
-- (Messages show up in Postgres logs and many clients that surface NOTICE.)
DO $$
BEGIN
    RAISE NOTICE '[staging-transform] starting';
END $$;

-- Insert applications
DO $$
DECLARE
    v_apps_inserted bigint;
BEGIN
    INSERT INTO public.ca_applications (
    id,
    created_at,

    application_number,
    application_type,
    address_code,
    address_no,
    block_no,
    plot_no,
    road,
    street,
    region,
    district,
    ward,
    mobile_no,
    email,
    website,
    latitude,
    longitude,
    facility_name,
    company_name,
    tin,
    tin_name,
    brela_number,
    brela_registration_type,
    certificate_of_incorporation_no,
    approval_no,
    effective_date,
    expire_date,
    completed_at,
    old_parent_application_id,
    old_created_by,
    username,

    -- fire
    fire_certificate_control_number,
    fire_premise_name,
    fire_region,
    fire_district,
    fire_administrative_area,
    fire_ward,
    fire_street,
    fire_valid_from,
    fire_valid_to,

    -- tira
    insurance_ref_no,
    cover_note_number,
    cover_note_ref_no,
    policy_holder_name,
    insurer_company_name,
    cover_note_start_date,
    cover_note_end_date,
    risk_name,
    subject_matter_desc,

    application_legal_status_id,
    license_category_id
)
SELECT
    s.generated_id AS id,
    now() AS created_at,

    NULLIF(s.application_number, '') AS application_number,
    NULLIF(upper(s.application_type), '') AS application_type,
    NULLIF(s.address_code, '') AS address_code,
    NULLIF(s.address_no, '') AS address_no,
    NULLIF(s.block_no, '') AS block_no,
    NULLIF(s.plot_no, '') AS plot_no,
    NULLIF(s.road, '') AS road,
    NULLIF(s.street, '') AS street,

    NULLIF(s.region, '') AS region,
    NULLIF(s.district, '') AS district,
    NULLIF(s.ward, '') AS ward,

    NULLIF(s.mobile_no, '') AS mobile_no,
    NULLIF(s.email, '') AS email,
    NULLIF(s.website, '') AS website,
    NULLIF(s.latitude, '') AS latitude,
    NULLIF(s.longitude, '') AS longitude,

    NULLIF(s.facility_name, '') AS facility_name,
    NULLIF(s.company_name, '') AS company_name,
    NULLIF(s.tin, '') AS tin,
    NULLIF(s.tin_name, '') AS tin_name,
    NULLIF(s.brela_number, '') AS brela_number,
    NULLIF(s.brela_registration_type, '') AS brela_registration_type,
    NULLIF(s.certificate_of_incorporation_no, '') AS certificate_of_incorporation_no,

    NULLIF(trim(s.approval_no), '') AS approval_no,
    NULLIF(s.effective_date, '')::date AS effective_date,
    NULLIF(s.expire_date, '')::date AS expire_date,
    NULLIF(s.completed_at, '')::date AS completed_at,

    NULLIF(s.old_parent_application_id, '') AS old_parent_application_id,
    NULLIF(s.old_created_by, '') AS old_created_by,
    NULLIF(s.username, '') AS username,

    -- fire
    NULLIF(s.fire_certificate_control_number, '') AS fire_certificate_control_number,
    NULLIF(s.fire_premise_name, '') AS fire_premise_name,
    NULLIF(s.fire_region, '') AS fire_region,
    NULLIF(s.fire_district, '') AS fire_district,
    NULLIF(s.fire_administrative_area, '') AS fire_administrative_area,
    NULLIF(s.fire_ward, '') AS fire_ward,
    NULLIF(s.fire_street, '') AS fire_street,
    NULLIF(s.fire_valid_from, '')::date AS fire_valid_from,
    NULLIF(s.fire_valid_to, '')::date AS fire_valid_to,

    -- tira
    NULLIF(s.insurance_ref_no, '') AS insurance_ref_no,
    NULLIF(s.cover_note_number, '') AS cover_note_number,
    NULLIF(s.cover_note_ref_no, '') AS cover_note_ref_no,
    NULLIF(s.policy_holder_name, '') AS policy_holder_name,
    NULLIF(s.insurer_company_name, '') AS insurer_company_name,
    NULLIF(s.cover_note_start_date, '')::date AS cover_note_start_date,
    NULLIF(s.cover_note_end_date, '')::date AS cover_note_end_date,
    NULLIF(s.risk_name, '') AS risk_name,
    NULLIF(s.subject_matter_desc, '') AS subject_matter_desc,

    -- These two require mapping. For now, we attempt: if valid UUID then cast else NULL.
    CASE
        WHEN s.application_legal_status_raw ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN s.application_legal_status_raw::uuid
        ELSE NULL
    END AS application_legal_status_id,

    CASE
        WHEN s.license_category_raw ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN s.license_category_raw::uuid
        ELSE NULL
    END AS license_category_id

    FROM (
        -- Dedupe inside this load by application_number only.
        -- If application_number is blank, keep rows distinct by generated_id.
        SELECT s.*,
               row_number() OVER (
                   PARTITION BY COALESCE(NULLIF(s.application_number, ''), s.generated_id::text)
                   ORDER BY s.source_row_no NULLS LAST, s.generated_id
               ) AS rn
        FROM public.stage_ca_applications_raw s
    ) s
    WHERE s.rn = 1
      AND NOT EXISTS (
          SELECT 1
          FROM public.ca_applications a
          WHERE a.application_number IS NOT DISTINCT FROM NULLIF(s.application_number, '')
      )
        -- Requirement: only application_number should control skipping.
        -- Using a conflict target ensures we don't silently skip due to other unique constraints.
        ON CONFLICT (application_number) DO NOTHING;

    GET DIAGNOSTICS v_apps_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted apps=%', v_apps_inserted;
END $$;


-- Insert documents (strict: file_name and logic_doc_id must exist)
DO $$
DECLARE
    v_docs_inserted bigint;
BEGIN
    INSERT INTO public.ca_documents (
    id,
    created_at,
    document_name,
    document_url,
    application_id,
    file_name,
    documents_order,
    logic_doc_id
)
SELECT
    d.id,
    now() as created_at,
    d.document_name,
    NULL::text as document_url,
    a.id as application_id,
    d.file_name,
    d.documents_order,
    d.logic_doc_id
    FROM public.stage_ca_documents_raw d
    JOIN public.ca_applications a
        ON a.id = d.application_generated_id
    WHERE NULLIF(trim(d.file_name), '') IS NOT NULL
        AND d.logic_doc_id IS NOT NULL
        AND a.id IS NOT NULL
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_docs_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted docs=%', v_docs_inserted;
END $$;


-- Insert contact persons (only when Excel provided contact info)
-- Final table has unique(application_id), so for safety we skip if one already exists.
DO $$
DECLARE
    v_contacts_inserted bigint;
BEGIN
    INSERT INTO public.ca_contact_persons (
        id,
        created_at,
        contact_name,
        email,
        mobile_no,
        title,
        application_id
)
SELECT
        c.id,
        now() AS created_at,
        NULLIF(trim(c.contact_name), '') AS contact_name,
        NULLIF(trim(c.email), '') AS email,
        NULLIF(trim(c.mobile_no), '') AS mobile_no,
        NULLIF(trim(c.title), '') AS title,
        a.id AS application_id
    FROM public.stage_ca_contact_persons_raw c
    JOIN public.ca_applications a
        ON a.id = c.application_generated_id
    WHERE
        -- only insert if at least one piece of contact info exists
        (
            NULLIF(trim(c.contact_name), '') IS NOT NULL
            OR NULLIF(trim(c.email), '') IS NOT NULL
            OR NULLIF(trim(c.mobile_no), '') IS NOT NULL
            OR NULLIF(trim(c.title), '') IS NOT NULL
        )
        AND a.id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM public.ca_contact_persons x
            WHERE x.application_id = a.id
        )
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_contacts_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted contacts=%', v_contacts_inserted;
END $$;


-- Ensure ca_shareholders has logic_doc_id (objectid from Excel)
ALTER TABLE IF EXISTS public.ca_shareholders
    ADD COLUMN IF NOT EXISTS logic_doc_id bigint;


-- Insert shareholders
DO $$
DECLARE
    v_shareholders_inserted bigint;
BEGIN
    INSERT INTO public.ca_shareholders (
        id,
        created_at,
        shareholder_name,
        amount_of_shares,
        contact_address,
        country_of_residence,
        country_of_incorporation,
        nationality,
        individual_company,
        passport_or_nationalid,
        shareholder_order,
        application_id,
        logic_doc_id
    )
    SELECT
        s.id,
        now() AS created_at,
        NULLIF(trim(s.shname), '') AS shareholder_name,
        NULLIF(trim(s.amountofshare), '')::numeric AS amount_of_shares,
        NULLIF(trim(s.sconadd), '') AS contact_address,
        NULLIF(trim(s.countryname), '') AS country_of_residence,
        NULLIF(trim(s.countryname), '') AS country_of_incorporation,
        NULLIF(trim(s.countryname), '') AS nationality,
        NULLIF(trim(s.indcomp), '') AS individual_company,
        NULLIF(trim(s.nationality), '') AS passport_or_nationalid,
        COALESCE(NULLIF(trim(s.rowid), '')::int, 1) AS shareholder_order,
        a.id AS application_id,
        CASE
            WHEN NULLIF(trim(s.objectid), '') ~ '^[0-9]+$' THEN trim(s.objectid)::bigint
            ELSE NULL
        END AS logic_doc_id
    FROM public.stage_ca_shareholders_raw s
    JOIN public.ca_applications a
        ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
    WHERE a.id IS NOT NULL
      AND NULLIF(trim(s.shname), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.ca_shareholders x
          WHERE x.application_id = a.id
            AND x.shareholder_order IS NOT DISTINCT FROM COALESCE(NULLIF(trim(s.rowid), '')::int, 1)
      )
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_shareholders_inserted = ROW_COUNT;
    RAISE NOTICE '[staging-transform] inserted shareholders=%', v_shareholders_inserted;
END $$;

COMMIT;
