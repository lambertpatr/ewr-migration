-- Staging schema for high-volume (500k+) Excel imports
-- Safe defaults: keep most fields as TEXT to avoid COPY failures.
-- You can drop these tables after a successful run.

BEGIN;

-- NOTE: stage table name is kept as-is to avoid changing Python pipeline code.
CREATE TABLE IF NOT EXISTS public.stage_ca_applications_raw (
    generated_id uuid PRIMARY KEY,
    source_row_no integer,

    -- Common application columns (store as text; cast in transform)
    application_number text,
    application_type text,
    address_code text,
    address_no text,
    block_no text,
    plot_no text,
    road text,
    street text,
    region text,
    district text,
    ward text,

    mobile_no text,
    email text,
    website text,
    latitude text,
    longitude text,
    po_box text,

    facility_name text,
    company_name text,
    tin text,
    tin_name text,
    brela_number text,
    brela_registration_type text,
    certificate_of_incorporation_no text,

    approval_no text,
    effective_date text,
    expire_date text,
    completed_at text,

    -- legacy mapping targets
    old_parent_application_id text,
    old_created_by text,

    -- mapping columns that may be either UUID or free text in Excel
    application_legal_status_raw text,
    license_category_raw text,
    license_type text,

    username text,

    -- provenance flag (true for LOIS-migrated rows)
    is_from_lois boolean NOT NULL DEFAULT true,

    -- Fire certificate fields
    fire_certificate_control_number text,
    fire_premise_name text,
    fire_region text,
    fire_district text,
    fire_administrative_area text,
    fire_ward text,
    fire_street text,
    fire_valid_from text,
    fire_valid_to text,

    -- Insurance / TIRA
    insurance_ref_no text,
    cover_note_number text,
    cover_note_ref_no text,
    policy_holder_name text,
    insurer_company_name text,
    cover_note_start_date text,
    cover_note_end_date text,
    risk_name text,
    subject_matter_desc text

    ,
    -- Contact person fields coming from Excel (only used if provided)
    cemail text,
    cmobile_no text,
    contact_name text,
    title text
);

-- Ensure new columns exist even if table was already created
ALTER TABLE public.stage_ca_applications_raw ADD COLUMN IF NOT EXISTS license_type text;
ALTER TABLE public.stage_ca_applications_raw ADD COLUMN IF NOT EXISTS is_from_lois boolean NOT NULL DEFAULT true;
ALTER TABLE public.stage_ca_applications_raw ADD COLUMN IF NOT EXISTS po_box text;

-- One contact person per application (matches unique(application_id) in final)
CREATE TABLE IF NOT EXISTS public.stage_ca_contact_persons_raw (
    id uuid PRIMARY KEY,
    application_generated_id uuid NOT NULL,
    contact_name text,
    email text,
    mobile_no text,
    title text,

    CONSTRAINT fk_stage_contact_stage_apps
        FOREIGN KEY (application_generated_id)
        REFERENCES public.stage_ca_applications_raw(generated_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.stage_ca_documents_raw (
    id uuid PRIMARY KEY,
    application_generated_id uuid NOT NULL,
    -- In the NEW schema, documents belong to application_sector_details, not applications.
    -- We stage against the application first, then resolve to the sector-detail row.
    document_name text NOT NULL,
    file_name text NOT NULL,
    documents_order integer,
    logic_doc_id integer,

    CONSTRAINT fk_stage_docs_stage_apps
        FOREIGN KEY (application_generated_id)
        REFERENCES public.stage_ca_applications_raw(generated_id)
        ON DELETE CASCADE
);

-- Helpful indexes for transforms
CREATE INDEX IF NOT EXISTS idx_stage_docs_app_gen_id ON public.stage_ca_documents_raw(application_generated_id);
CREATE INDEX IF NOT EXISTS idx_stage_contact_app_gen_id ON public.stage_ca_contact_persons_raw(application_generated_id);

-------------------------------------------------------------------------------
-- Shareholders staging (used by the normalized shareholders insert)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.stage_ca_shareholders_raw (
    id uuid PRIMARY KEY,

    -- join key back to application
    application_number text,

    -- raw shareholder fields from Excel (kept as text for COPY safety)
    file_name text,
    shname text,
    amountofshare text,
    sconadd text,
    indcomp text,
    nationality text,
    countryname text,
    rowid text,
    objectid text,
    source_row_no bigint
);

CREATE INDEX IF NOT EXISTS idx_stage_shareholders_app_no ON public.stage_ca_shareholders_raw(application_number);

CREATE TABLE IF NOT EXISTS public.stage_categories (
    name text,
    sector_name text
);

COMMIT;
