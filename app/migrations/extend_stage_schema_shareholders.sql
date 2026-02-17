-- Extend staging schema to support importing shareholders from Excel
--
-- Creates a new staging table for shareholders raw rows.

BEGIN;

CREATE TABLE IF NOT EXISTS public.stage_ca_shareholders_raw (
    id uuid PRIMARY KEY,
    application_number text,

    -- Source columns from Excel
    shname text,
    amountofshare text,
    sconadd text,
    countryname text,
    indcomp text,
    nationality text,
    rowid text,
    objectid text,

    -- Metadata
    source_row_no bigint
);

COMMIT;
