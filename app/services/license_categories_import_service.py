from __future__ import annotations

import io
import uuid
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.utils.lookup_cache import load_sector_map


def import_license_categories_and_fees_via_staging_copy(
    db: Session,
    df: pd.DataFrame,
    *,
    sector_name: str,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """Import license categories + application_category_fees from an Excel dataframe.

    Contract
    - Input df: output of read_users_file() (already normalized columns in your codebase).
    - Category name: df['categoryorclass'] -> public.categories.name
    - sector_id: looked up from public.sectors by sector_name
    - If categories row already exists (same sector_id + case-insensitive trimmed name), do not insert.
    - Then create public.application_category_fees rows using mapping columns.

    Dedupe policy
    - categories: unique by (sector_id, lower(trim(name)))
    - application_category_fees: if a row already exists for the same category_id + application_type + prefixes + capacity range + months_eligible, skip.

    Returns: stats dict with inserted/skipped counts.
    """
    def _progress(msg: str):
        if progress_cb:
            progress_cb(msg)

    if df is None or df.empty:
        return {
            "status": "NO_DATA",
            "processed_rows": 0,
            "inserted_categories": 0,
            "inserted_fees": 0,
            "skipped_categories_existing": 0,
            "skipped_fees_existing": 0,
        }

    # Normalize columns to lowercase (read_users_file usually does this, but be defensive)
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    def _first_existing_col(*candidates: str) -> Optional[str]:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    # Natural Gas sheets sometimes don't have these columns; default in SQL when absent.
    capacity_from_col = _first_existing_col("acapacityfrom", "capacityfrom", "capacity_from")
    capacity_to_col = _first_existing_col("acapacityto", "capacityto", "capacity_to")
    # Months column: in practice, files use licenseperiod_x; keep licenseperiod as fallback.
    months_col = (
        "licenseperiod_x" if "licenseperiod_x" in df.columns else ("licenseperiod" if "licenseperiod" in df.columns else None)
    )

    required = [
        "categoryorclass",
        "appfee",
        "licencefee",
        "prefix",
        "licenseprefix",
    ]
    # Sector-specific required column validation.
    # Backward-compatibility rules (derived from the actual Excel formats):
    #
    # Sector       | license_type src  | application_type src | no_customer | capacity
    # -------------|-------------------|----------------------|-------------|------------------
    # Electricity  | licencetype col   | applicationtype col  | —           | capacityfrom/to
    # Petroleum    | licencetype col   | applicationtype col  | —           | — (NULL-safe)
    # Natural Gas  | forced OPERATIONAL| applicationtype col  | —           | NULL-safe
    # Water        | forced OPERATIONAL| applicationtype col  | nocustomerf | NULL-safe
    #
    # All four sectors use applicationtype from the Excel.
    # licencetype is required for Electricity and Petroleum (used as license_type).
    # For Natural Gas and Water it is optional (defaulted to OPERATIONAL in SQL).
    required.append("applicationtype")
    if sector_name in ("Electricity", "Petroleum"):
        # licencetype drives license_type for these sectors
        if "licencetype" not in df.columns:
            raise ValueError("Missing required column in upload: licencetype")
    # optional columns (defaults applied when missing): capacity_from/capacity_to, annualfee, months
    if months_col is None:
        raise ValueError("Missing required columns in upload: licenseperiod_x (or licenseperiod)")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in upload: {missing}")

    # Sector lookup — resolved dynamically from the connected DB so the same
    # code works on test, staging, and production without any UUID changes.
    _progress(f"Looking up sector_id for sector '{sector_name}'...")

    # Map human-readable input names to the new database enum-style names.
    sector_name_map = {
        "Natural Gas": "NATURAL_GAS",
        "Petroleum": "PETROLEUM",
        "Electricity": "ELECTRICITY",
        "Water & Wastewater": "WATER_SUPPLY",
    }
    db_sector_name = sector_name_map.get(sector_name)
    if not db_sector_name:
        raise ValueError(
            f"Invalid sector_name '{sector_name}'. Expected one of: {list(sector_name_map.keys())}"
        )

    sector_map = load_sector_map(db)
    sector_id = sector_map.get(db_sector_name.lower())
    if not sector_id:
        raise ValueError(
            f"Sector '{db_sector_name}' not found in public.sectors. "
            f"Available sectors: {list(sector_map.keys())}. "
            f"Please ensure the sectors table is populated."
        )

    stage = "public.stage_license_category_fees_raw"

    _progress("Creating staging table...")
    db.execute(
        text(
            f"""
            DROP TABLE IF EXISTS {stage};
            CREATE TABLE {stage} (
                categoryorclass  text,
                licencetype      text,
                applicationtype  text,
                nocustomerf      text,
                nocustomerto     text,
                acapacityfrom    text,
                acapacityto      text,
                appfee           text,
                licencefee       text,
                annualfee        text,
                prefix           text,
                licenseprefix    text,
                licenseperiod_y  text,
                has_fee_range    text,
                apply_thereof    text,
                thereof_factor   text,
                thereof_price    text,
                capacity_unit    text,
                voltage_level    text
            );
            """
        )
    )

    # Ensure categories has the columns we need.
    # Each ALTER is committed individually so a later index failure
    # does not roll back the column additions.
    # (Self-contained — works even if DB migrations haven't been applied yet.)
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS sub_sector_type character varying;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS code character varying;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS category_type character varying;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS has_fee_range boolean NOT NULL DEFAULT false;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS has_operation_type boolean NOT NULL DEFAULT false;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS apply_thereof boolean NOT NULL DEFAULT false;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS thereof_factor integer;
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS thereof_price numeric(11,2);
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS capacity_unit character varying(255);
            """
        )
    )
    # fees_category_id: points a category that has no fees to the sibling row
    # (same lower(name), different casing) that does have fees.  This lets the
    # UI/API redirect users away from the "empty" duplicate without deleting it.
    try:
        db.execute(text(
            "ALTER TABLE public.categories ADD COLUMN IF NOT EXISTS fees_category_id uuid NULL"
        ))
        db.commit()
    except Exception:
        db.rollback()

    # uq_categories_name_lower — case-insensitive unique index on active rows.
    # NOTE: We intentionally do NOT soft-delete case-variant duplicates here.
    # Legacy DBs may have both 'generation' and 'Generation' as separate live rows.
    # We keep both alive; the one without fees will get fees_category_id pointing
    # to the sibling that has fees (see post-fee step below).
    # The index creation is best-effort — it will fail if duplicates exist and
    # that is acceptable: the mapping table handles dedup via lower(name) JOIN.

    try:
        db.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_name_lower
                ON public.categories (lower(btrim(name)))
                WHERE deleted_at IS NULL;
                """
            )
        )
        db.commit()
    except Exception:
        db.rollback()
        _progress("Warning: could not create uq_categories_name_lower index (duplicate names exist) — continuing")

    # uq_categories_code — best-effort; skip if duplicate codes already exist in the table.
    try:
        db.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_code
                ON public.categories (code)
                WHERE code IS NOT NULL AND deleted_at IS NULL;
                """
            )
        )
        db.commit()
    except Exception:
        db.rollback()
        _progress("Warning: could not create uq_categories_code index (duplicate codes exist) — continuing")

    # Ensure the destination table has the new column.
    # (Kept here to be self-contained even if schema-sync isn't run.)
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.application_category_fees
                ADD COLUMN IF NOT EXISTS license_type character varying;

            ALTER TABLE IF EXISTS public.application_category_fees
                ADD COLUMN IF NOT EXISTS category_license_type character varying;

            ALTER TABLE IF EXISTS public.application_category_fees
                ADD COLUMN IF NOT EXISTS no_customer_from character varying;
            ALTER TABLE IF EXISTS public.application_category_fees
                ADD COLUMN IF NOT EXISTS no_customer_to character varying;

            ALTER TABLE IF EXISTS public.application_category_fees
                ADD COLUMN IF NOT EXISTS sector_type character varying;
            """
        )
    )

    # COPY into stage
    _progress("Staging with COPY...")
    # Always stage a fixed set of columns; for missing ones, stage NULLs and default later.
    stage_df = pd.DataFrame(
        {
            "categoryorclass": df.get("categoryorclass"),
            "licencetype": df.get("licencetype"),
            "applicationtype": df.get("applicationtype"),
            "nocustomerf": df.get("nocustomerf"),
            "nocustomerto": df.get("nocustomerto"),
            "acapacityfrom": df.get(capacity_from_col) if capacity_from_col else None,
            "acapacityto": df.get(capacity_to_col) if capacity_to_col else None,
            "appfee": df.get("appfee"),
            "licencefee": df.get("licencefee"),
            "annualfee": df.get("annualfee"),
            "prefix": df.get("prefix"),
            "licenseprefix": df.get("licenseprefix"),
            "licenseperiod_y": df.get(months_col),
            "has_fee_range": df.get("has_fee_range"),
            "apply_thereof": df.get("apply_thereof"),
            "thereof_factor": df.get("thereof_factor"),
            "thereof_price": df.get("thereof_price"),
            "capacity_unit": df.get("capacity_unit"),
            "voltage_level": df.get("voltage_level") if df.get("voltage_level") is not None else df.get("voltagelevel"),
        }
    )
    stage_df = stage_df.where(pd.notnull(stage_df), None)

    stage_cols = [
        "categoryorclass",
        "licencetype",
        "applicationtype",
        "nocustomerf",
        "nocustomerto",
        "acapacityfrom",
        "acapacityto",
        "appfee",
        "licencefee",
        "annualfee",
        "prefix",
        "licenseprefix",
        "licenseperiod_y",
        "has_fee_range",
        "apply_thereof",
        "thereof_factor",
        "thereof_price",
        "capacity_unit",
        "voltage_level",
    ]
    stage_df = stage_df.reindex(columns=stage_cols)

    buf = io.StringIO()
    stage_df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    conn = db.connection().connection
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {stage} ("
            "categoryorclass, licencetype, applicationtype, nocustomerf, nocustomerto, "
            "acapacityfrom, acapacityto, appfee, licencefee, annualfee, prefix, licenseprefix, "
            "licenseperiod_y, has_fee_range, apply_thereof, thereof_factor, thereof_price, capacity_unit, "
            "voltage_level"
            ") FROM STDIN WITH (FORMAT CSV)",
            buf,
        )

    staged_rows = db.execute(text(f"SELECT COUNT(*) FROM {stage}"))
    staged_rows = int(staged_rows.scalar() or 0)
    if staged_rows == 0:
        return {
            "status": "NO_DATA",
            "sector_name": sector_name,
            "sector_id": str(sector_id),
            "processed_rows": int(len(df)),
            "staged_rows": 0,
            "inserted_categories": 0,
            "inserted_fees": 0,
            "note": "0 rows staged — check file parse/column names",
        }

    _progress("Upserting/creating license_categories...")

    # Useful diagnostics for why license_type/application_type might become 'NEW'
    blank_licencetype = db.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {stage}
            WHERE licencetype IS NULL OR btrim(licencetype) = ''
            """
        )
    ).scalar() or 0
    blank_applicationtype = db.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {stage}
            WHERE applicationtype IS NULL OR btrim(applicationtype) = ''
            """
        )
    ).scalar() or 0

    # Build a temp mapping table: key_name → ALL category_ids that share the same
    # case-insensitive name.  If both 'generation' and 'Generation' exist as separate
    # rows in public.categories (legacy duplicates), fees will be inserted for BOTH.
    # Primary key is (key_name, category_id) — intentionally one-to-many.
    # NOTE: TEMP tables cannot be created inside the public schema.
    db.execute(
        text(
            """
            DROP TABLE IF EXISTS stage_license_category_ids;
            CREATE TEMP TABLE stage_license_category_ids (
                key_name    text NOT NULL,
                category_id uuid NOT NULL,
                PRIMARY KEY (key_name, category_id)
            );
            """
        )
    )

    # Insert missing categories; update new columns on existing rows only when
    # the staging row contains explicit non-null values (backward-compatible:
    # sectors whose Excel files don't have the new columns keep their existing
    # DB values untouched).
    #
    # Case-insensitive dedup strategy:
    #  1. DISTINCT ON (lower(btrim(...))) collapses duplicates within the same upload.
    #  2. ON CONFLICT (lower(btrim(name))) WHERE deleted_at IS NULL → UPDATE the existing
    #     row when a name collides (case-insensitive). No WHERE NOT EXISTS guard here —
    #     that would prevent the conflict from firing and stop the mapping table from
    #     seeing ALL pre-existing case variants (e.g. both 'generation' and 'Generation').
    #
    # After this INSERT/UPDATE the mapping table is populated with ALL category_id rows
    # that share the same lower(name), so fees are written for every case-variant.
    cat_insert = db.execute(
        text(
            f"""
            WITH enriched AS (
                -- One representative row per category name (first staging row).
                -- key_name is always lowercase for comparison; display_name preserves
                -- the original Excel casing (e.g. 'Generation') for storage.
                SELECT DISTINCT ON (lower(btrim(s.categoryorclass)))
                    lower(btrim(s.categoryorclass))                        AS key_name,
                    btrim(s.categoryorclass)                               AS display_name,
                    NULLIF(btrim(s.licenseprefix), '')    AS first_license_prefix,
                    lower(COALESCE(s.licencetype, ''))    AS licencetype_lower,

                    -- Boolean columns: parse TRUE/1/YES/Y → true, anything else → false.
                    -- NULL in staging (column absent in Excel) also becomes false here,
                    -- but DO UPDATE uses COALESCE/CASE to preserve existing true values.
                    CASE
                        WHEN upper(btrim(COALESCE(s.has_fee_range, ''))) IN ('TRUE','1','YES','Y') THEN true
                        ELSE false
                    END AS has_fee_range,

                    CASE
                        WHEN upper(btrim(COALESCE(s.apply_thereof, ''))) IN ('TRUE','1','YES','Y') THEN true
                        ELSE false
                    END AS apply_thereof,

                    CASE
                        WHEN btrim(s.thereof_factor) ~ '^-?\\d+$'
                        THEN btrim(s.thereof_factor)::integer
                        ELSE NULL
                    END AS thereof_factor,

                    CASE
                        WHEN btrim(s.thereof_price) ~ '^-?\\d+(\\.\\d+)?$'
                        THEN btrim(s.thereof_price)::numeric(11,2)
                        ELSE NULL
                    END AS thereof_price,

                    -- capacity_unit: only store values allowed by the DB check constraint.
                    -- Anything else (NULL, '', 'NONE', unknown) → NULL.
                    CASE
                        WHEN upper(btrim(COALESCE(s.capacity_unit, ''))) = 'MW' THEN 'MW'
                        WHEN upper(btrim(COALESCE(s.capacity_unit, ''))) = 'KV' THEN 'kV'
                        ELSE NULL
                    END AS capacity_unit,

                    -- voltage_level: accept valid values from Excel; default to 'HV' for ELECTRICITY.
                    CASE
                        WHEN upper(btrim(COALESCE(s.voltage_level, ''))) IN ('HV', 'MV', 'LV_1', 'LV_3')
                            THEN upper(btrim(s.voltage_level))
                        WHEN :db_sector_name = 'ELECTRICITY' THEN 'HV'
                        ELSE NULL
                    END AS voltage_level

                FROM {stage} s
                WHERE s.categoryorclass IS NOT NULL AND btrim(s.categoryorclass) <> ''
                ORDER BY lower(btrim(s.categoryorclass))
            )
            INSERT INTO public.categories (
                id,
                name,
                code,
                sector_id,
                sub_sector_type,
                category_type,
                is_approved,
                has_fee_range,
                apply_thereof,
                thereof_factor,
                thereof_price,
                capacity_unit,
                voltage_level,
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                e.display_name,
                COALESCE(e.first_license_prefix, e.display_name),
                :sector_id,
                'OPERATIONAL',
                CASE
                    WHEN e.licencetype_lower LIKE '%construction%' THEN 'Construction'
                    ELSE 'License'
                END,
                false,
                e.has_fee_range,
                e.apply_thereof,
                e.thereof_factor,
                e.thereof_price,
                e.capacity_unit,
                e.voltage_level,
                now(),
                now()
            FROM enriched e
            ON CONFLICT (lower(btrim(name))) WHERE deleted_at IS NULL DO UPDATE SET
                -- Case-insensitive conflict: 'generation' and 'Generation' resolve to the same row.
                -- Update the stored name to the latest Excel value (preserves original casing of
                -- whatever was uploaded most recently, without duplicating the row).
                name           = EXCLUDED.name,
                -- Only overwrite when the Excel file actually supplied a non-empty value.
                -- Booleans: only update when the staging cell was explicitly provided (not absent/null).
                -- Nullables: only update when EXCLUDED has a non-null value (COALESCE keeps existing).
                has_fee_range  = CASE
                                     WHEN EXCLUDED.has_fee_range = true THEN true
                                     WHEN EXCLUDED.has_fee_range = false
                                      AND public.categories.has_fee_range = true THEN true
                                     ELSE false
                                 END,
                apply_thereof  = CASE
                                     WHEN EXCLUDED.apply_thereof = true THEN true
                                     WHEN EXCLUDED.apply_thereof = false
                                      AND public.categories.apply_thereof = true THEN true
                                     ELSE false
                                 END,
                thereof_factor = COALESCE(EXCLUDED.thereof_factor, public.categories.thereof_factor),
                thereof_price  = COALESCE(EXCLUDED.thereof_price,  public.categories.thereof_price),
                capacity_unit  = COALESCE(EXCLUDED.capacity_unit,  public.categories.capacity_unit),
                voltage_level  = COALESCE(EXCLUDED.voltage_level,  public.categories.voltage_level),
                updated_at     = now()
            RETURNING (xmax = 0) AS was_inserted;
            """
        ),
        {"sector_id": sector_id, "db_sector_name": db_sector_name},
    )
    # xmax = 0 → genuine INSERT; xmax != 0 → UPDATE of existing row
    rows = cat_insert.fetchall()
    inserted_categories = sum(1 for r in rows if r[0] is True)
    updated_categories  = sum(1 for r in rows if r[0] is False)

    # Populate mapping table for all names in stage → ALL matching category_id variants.
    # e.g. if both 'generation' and 'Generation' exist in public.categories, both rows
    # are inserted here so fees are written for every case-variant of the same name.
    db.execute(
        text(
            f"""
            INSERT INTO stage_license_category_ids (key_name, category_id)
            SELECT DISTINCT
                lower(btrim(s.categoryorclass)) AS key_name,
                lc.id                           AS category_id
            FROM {stage} s
            JOIN public.categories lc
              ON lower(btrim(lc.name)) = lower(btrim(s.categoryorclass))
             AND lc.deleted_at IS NULL
            WHERE s.categoryorclass IS NOT NULL AND btrim(s.categoryorclass) <> ''
            ON CONFLICT (key_name, category_id) DO NOTHING;
            """
        ),
        {"sector_id": sector_id},
    )

    _progress("Inserting application_category_fees...")

    before_fee_count = db.execute(
        text("SELECT COUNT(*) FROM public.application_category_fees WHERE deleted_at IS NULL")
    ).scalar() or 0

    db.execute(
        text(
            f"""
            WITH typed AS (
                SELECT
                    ids.category_id,
                    -- license_type / category_license_type:
                    --   ELECTRICITY only → PROVISIONAL when licencetype contains 'PROVISIONAL', else OPERATIONAL.
                    --   ALL other sectors (PETROLEUM, NATURAL_GAS, WATER_SUPPLY, etc.) → always OPERATIONAL.
                    --   The raw Excel values like 'Petroleum', 'Construction Approval', etc. are NOT valid
                    --   DB enum values and must never be stored in these columns.
                    CASE
                        WHEN :db_sector_name = 'ELECTRICITY'
                             AND upper(btrim(COALESCE(s.licencetype, ''))) LIKE '%PROVISIONAL%'
                        THEN 'PROVISIONAL'
                        ELSE 'OPERATIONAL'
                    END AS license_type,
                    CASE
                        WHEN :db_sector_name = 'ELECTRICITY'
                             AND upper(btrim(COALESCE(s.licencetype, ''))) LIKE '%PROVISIONAL%'
                        THEN 'PROVISIONAL'
                        ELSE 'OPERATIONAL'
                    END AS category_license_type,
                    NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
                    NULLIF(btrim(s.nocustomerto), '') AS no_customer_to,
                    -- application_type: ALL sectors use applicationtype column from Excel.
                    -- Petroleum/Electricity/Natural Gas/Water all have this column.
                    upper(COALESCE(NULLIF(btrim(s.applicationtype), ''), 'NEW')) AS application_type,

                    -- capacity_from / capacity_to:
                    --   NATURAL_GAS, WATER_SUPPLY → NULL-safe (no capacity in those files)
                    --   ELECTRICITY, PETROLEUM → default to 1/10 when column absent/non-numeric
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE 1 END
                    END AS capacity_from,
                    -- capacity_to is varchar(255) in DB: store raw value as text.
                    -- For NATURAL_GAS/WATER_SUPPLY: NULL when blank.
                    -- For ELECTRICITY/PETROLEUM: keep the raw string (could be 'Above', a number, etc.).
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            NULLIF(btrim(s.acapacityto), '')
                        ELSE
                            COALESCE(NULLIF(btrim(s.acapacityto), ''), '10')
                    END AS capacity_to,
                    CASE WHEN btrim(s.appfee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.appfee))::numeric ELSE 240000 END AS application_fee,
                    CASE WHEN btrim(s.licencefee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.licencefee))::numeric ELSE 120500000 END AS license_fee,
                    CASE WHEN btrim(s.annualfee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.annualfee))::numeric ELSE 0 END AS annual_fee,

                    COALESCE(NULLIF(btrim(s.prefix), ''), 'AECBL') AS application_prefix,
                    COALESCE(NULLIF(btrim(s.licenseprefix), ''), 'ECBL') AS license_prefix,

                    CASE WHEN btrim(s.licenseperiod_y) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.licenseperiod_y))::bigint ELSE 36 END AS months_eligible
                FROM {stage} s
                JOIN stage_license_category_ids ids
                  ON ids.key_name = lower(btrim(s.categoryorclass))
            ),
            deduped AS (
                -- One row per logical fee slot (category + license_type + application_type + capacity range + prefixes + months).
                -- Case-insensitive dedup via lower() in DISTINCT ON key.
                SELECT DISTINCT ON (
                    category_id,
                    lower(btrim(application_type)),
                    lower(btrim(COALESCE(license_type, ''))),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    COALESCE(capacity_from, -1::numeric),
                    COALESCE(capacity_to::text, ''),
                    lower(btrim(COALESCE(application_prefix, ''))),
                    lower(btrim(COALESCE(license_prefix, ''))),
                    COALESCE(months_eligible, 0)
                ) *
                FROM typed
                ORDER BY
                    category_id,
                    lower(btrim(application_type)),
                    lower(btrim(COALESCE(license_type, ''))),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    COALESCE(capacity_from, -1::numeric),
                    COALESCE(capacity_to::text, ''),
                    lower(btrim(COALESCE(application_prefix, ''))),
                    lower(btrim(COALESCE(license_prefix, ''))),
                    COALESCE(months_eligible, 0)
            )
            -- UPDATE existing rows (case-insensitive match on all identity columns).
            UPDATE public.application_category_fees f
            SET
                license_type          = d.license_type,
                category_license_type = d.category_license_type,
                application_type      = d.application_type,
                no_customer_from      = d.no_customer_from,
                no_customer_to        = d.no_customer_to,
                capacity_from         = d.capacity_from,
                capacity_to           = d.capacity_to,
                application_fee       = d.application_fee,
                license_fee           = d.license_fee,
                annual_fee            = d.annual_fee,
                application_prefix    = d.application_prefix,
                license_prefix        = d.license_prefix,
                months_eligible       = d.months_eligible,
                sector_type           = :db_sector_name,
                updated_at            = now()
            FROM deduped d
            WHERE f.category_id = d.category_id
              AND lower(btrim(f.application_type)) = lower(btrim(d.application_type))
              AND lower(btrim(COALESCE(f.license_type, ''))) = lower(btrim(COALESCE(d.license_type, '')))
              AND COALESCE(f.capacity_from, -1::numeric) = COALESCE(d.capacity_from, -1::numeric)
              AND COALESCE(f.capacity_to, '') = COALESCE(d.capacity_to::text, '')
              AND COALESCE(f.no_customer_from, '') = COALESCE(d.no_customer_from, '')
              AND COALESCE(f.no_customer_to, '') = COALESCE(d.no_customer_to, '')
              AND lower(btrim(COALESCE(f.application_prefix, ''))) = lower(btrim(COALESCE(d.application_prefix, '')))
              AND lower(btrim(COALESCE(f.license_prefix, ''))) = lower(btrim(COALESCE(d.license_prefix, '')))
              AND COALESCE(f.months_eligible, 0) = COALESCE(d.months_eligible, 0)
              AND f.deleted_at IS NULL;
            """
        ),
        {"db_sector_name": db_sector_name},
    )

    # INSERT rows that have no existing match (case-insensitive on all identity columns).
    db.execute(
        text(
            f"""
            WITH typed AS (
                SELECT
                    ids.category_id,
                    -- license_type / category_license_type (same rules as UPDATE above):
                    --   ELECTRICITY only → PROVISIONAL or OPERATIONAL from licencetype col.
                    --   ALL other sectors → always OPERATIONAL.
                    CASE
                        WHEN :db_sector_name = 'ELECTRICITY'
                             AND upper(btrim(COALESCE(s.licencetype, ''))) LIKE '%PROVISIONAL%'
                        THEN 'PROVISIONAL'
                        ELSE 'OPERATIONAL'
                    END AS license_type,
                    CASE
                        WHEN :db_sector_name = 'ELECTRICITY'
                             AND upper(btrim(COALESCE(s.licencetype, ''))) LIKE '%PROVISIONAL%'
                        THEN 'PROVISIONAL'
                        ELSE 'OPERATIONAL'
                    END AS category_license_type,
                    NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
                    NULLIF(btrim(s.nocustomerto), '') AS no_customer_to,
                    -- application_type: ALL sectors use applicationtype column from Excel.
                    upper(COALESCE(NULLIF(btrim(s.applicationtype), ''), 'NEW')) AS application_type,
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE 1 END
                    END AS capacity_from,
                    -- capacity_to is varchar(255) in DB: store raw value as text.
                    -- For NATURAL_GAS/WATER_SUPPLY: NULL when blank.
                    -- For ELECTRICITY/PETROLEUM: keep the raw string (could be 'Above', a number, etc.).
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            NULLIF(btrim(s.acapacityto), '')
                        ELSE
                            COALESCE(NULLIF(btrim(s.acapacityto), ''), '10')
                    END AS capacity_to,
                    CASE WHEN btrim(s.appfee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.appfee))::numeric ELSE 240000 END AS application_fee,
                    CASE WHEN btrim(s.licencefee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.licencefee))::numeric ELSE 120500000 END AS license_fee,
                    CASE WHEN btrim(s.annualfee) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.annualfee))::numeric ELSE 0 END AS annual_fee,
                    COALESCE(NULLIF(btrim(s.prefix), ''), 'AECBL') AS application_prefix,
                    COALESCE(NULLIF(btrim(s.licenseprefix), ''), 'ECBL') AS license_prefix,
                    CASE WHEN btrim(s.licenseperiod_y) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.licenseperiod_y))::bigint ELSE 36 END AS months_eligible
                FROM {stage} s
                JOIN stage_license_category_ids ids
                  ON ids.key_name = lower(btrim(s.categoryorclass))
            ),
            deduped AS (
                SELECT DISTINCT ON (
                    category_id,
                    lower(btrim(application_type)),
                    lower(btrim(COALESCE(license_type, ''))),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    COALESCE(capacity_from, -1::numeric),
                    COALESCE(capacity_to::text, ''),
                    lower(btrim(COALESCE(application_prefix, ''))),
                    lower(btrim(COALESCE(license_prefix, ''))),
                    COALESCE(months_eligible, 0)
                ) *
                FROM typed
                ORDER BY
                    category_id,
                    lower(btrim(application_type)),
                    lower(btrim(COALESCE(license_type, ''))),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    COALESCE(capacity_from, -1::numeric),
                    COALESCE(capacity_to::text, ''),
                    lower(btrim(COALESCE(application_prefix, ''))),
                    lower(btrim(COALESCE(license_prefix, ''))),
                    COALESCE(months_eligible, 0)
            )
            INSERT INTO public.application_category_fees (
                id,
                category_id,
                status,
                license_type,
                category_license_type,
                application_type,
                no_customer_from,
                no_customer_to,
                capacity_from,
                capacity_to,
                application_fee,
                license_fee,
                annual_fee,
                application_prefix,
                license_prefix,
                months_eligible,
                sector_type,
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                d.category_id,
                'NOT_ACTIVE',
                NULLIF(d.license_type, ''),
                NULLIF(d.category_license_type, ''),
                d.application_type,
                d.no_customer_from,
                d.no_customer_to,
                d.capacity_from,
                d.capacity_to,
                d.application_fee,
                d.license_fee,
                d.annual_fee,
                d.application_prefix,
                d.license_prefix,
                d.months_eligible,
                :db_sector_name,
                now(),
                now()
            FROM deduped d
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.application_category_fees f
                WHERE f.category_id = d.category_id
                  AND lower(btrim(f.application_type)) = lower(btrim(d.application_type))
                  AND lower(btrim(COALESCE(f.license_type, ''))) = lower(btrim(COALESCE(d.license_type, '')))
                  AND COALESCE(f.capacity_from, -1::numeric) = COALESCE(d.capacity_from, -1::numeric)
                  AND COALESCE(f.capacity_to, '') = COALESCE(d.capacity_to::text, '')
                  AND COALESCE(f.no_customer_from, '') = COALESCE(d.no_customer_from, '')
                  AND COALESCE(f.no_customer_to, '') = COALESCE(d.no_customer_to, '')
                  AND lower(btrim(COALESCE(f.application_prefix, ''))) = lower(btrim(COALESCE(d.application_prefix, '')))
                  AND lower(btrim(COALESCE(f.license_prefix, ''))) = lower(btrim(COALESCE(d.license_prefix, '')))
                  AND COALESCE(f.months_eligible, 0) = COALESCE(d.months_eligible, 0)
                  AND f.deleted_at IS NULL
            )
            RETURNING 1;
            """
        ),
        {"db_sector_name": db_sector_name},
    )

    after_fee_count = db.execute(
        text("SELECT COUNT(*) FROM public.application_category_fees WHERE deleted_at IS NULL")
    ).scalar() or 0
    inserted_fees = int(after_fee_count) - int(before_fee_count)

    # ── Post-fee: link no-fees category variants to their sibling with fees ──
    # For every pair of categories that share the same lower(name) (e.g. 'generation'
    # and 'Generation'), find which one HAS fees and set fees_category_id on the one
    # that has NO fees so the UI can redirect users to the correct category.
    # Safe to re-run: only updates rows where fees_category_id is currently NULL
    # AND no fees exist for that category_id.
    _progress("Linking no-fees category variants to their fees siblings …")
    try:
        db.execute(text(f"""
            UPDATE public.categories c_empty
            SET    fees_category_id = c_has_fees.id,
                   updated_at       = now()
            FROM   public.categories c_has_fees
            WHERE  lower(btrim(c_has_fees.name)) = lower(btrim(c_empty.name))
              AND  c_has_fees.id <> c_empty.id
              AND  c_empty.deleted_at  IS NULL
              AND  c_has_fees.deleted_at IS NULL
              -- c_empty has NO fees
              AND  NOT EXISTS (
                       SELECT 1 FROM public.application_category_fees f
                       WHERE  f.category_id = c_empty.id
                         AND  f.deleted_at IS NULL
                   )
              -- c_has_fees HAS fees
              AND  EXISTS (
                       SELECT 1 FROM public.application_category_fees f
                       WHERE  f.category_id = c_has_fees.id
                         AND  f.deleted_at IS NULL
                   )
              -- only update when not already set (idempotent)
              AND  c_empty.fees_category_id IS NULL;
        """))
        db.commit()
        _progress("fees_category_id back-fill done")
    except Exception as _fce:
        _progress(f"fees_category_id back-fill skipped: {_fce}")
        try:
            db.rollback()
        except Exception:
            pass

    processed = int(len(df))

    # figures for skipped are computed approximately via counts.
    # Count distinct key_names from stage that already had at least one matching
    # category row in the DB (regardless of casing or sector).
    skipped_categories_existing = db.execute(
        text(
            f"""
            SELECT COUNT(DISTINCT lower(btrim(s.categoryorclass)))
            FROM {stage} s
            JOIN public.categories lc
              ON lower(btrim(lc.name)) = lower(btrim(s.categoryorclass))
             AND lc.deleted_at IS NULL
            WHERE s.categoryorclass IS NOT NULL AND btrim(s.categoryorclass) <> '';
            """
        ),
    ).scalar() or 0

    # This includes newly inserted too; so compute existing-only
    skipped_categories_existing = max(0, int(skipped_categories_existing) - int(inserted_categories))

    # Fee rows can be deduped (distinct-on). Provide a safer estimate based on the deduped set size.
    deduped_count = db.execute(
        text(
            f"""
            WITH typed AS (
                SELECT
                    ids.category_id,
                    -- license_type: ELECTRICITY only → PROVISIONAL or OPERATIONAL.
                    -- ALL other sectors → always OPERATIONAL.
                    CASE
                        WHEN :db_sector_name = 'ELECTRICITY'
                             AND upper(btrim(COALESCE(s.licencetype, ''))) LIKE '%PROVISIONAL%'
                        THEN 'PROVISIONAL'
                        ELSE 'OPERATIONAL'
                    END AS license_type,
                    NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
                    NULLIF(btrim(s.nocustomerto), '') AS no_customer_to,
                    upper(COALESCE(NULLIF(btrim(s.applicationtype), ''), 'NEW')) AS application_type,
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE 1 END
                    END AS capacity_from,
                    -- capacity_to is varchar(255) in DB: store raw value as text.
                    -- For NATURAL_GAS/WATER_SUPPLY: NULL when blank.
                    -- For ELECTRICITY/PETROLEUM: keep the raw string (could be 'Above', a number, etc.).
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            NULLIF(btrim(s.acapacityto), '')
                        ELSE
                            COALESCE(NULLIF(btrim(s.acapacityto), ''), '10')
                    END AS capacity_to,
                    COALESCE(NULLIF(btrim(s.prefix), ''), 'AECBL') AS application_prefix,
                    COALESCE(NULLIF(btrim(s.licenseprefix), ''), 'ECBL') AS license_prefix,
                    CASE WHEN btrim(s.licenseperiod_y) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.licenseperiod_y))::bigint ELSE 36 END AS months_eligible
                FROM {stage} s
                JOIN stage_license_category_ids ids
                  ON ids.key_name = lower(btrim(s.categoryorclass))
            )
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT ON (
                    category_id,
                    lower(btrim(application_type)),
                    lower(btrim(COALESCE(license_type, ''))),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    COALESCE(capacity_from, -1::numeric),
                    COALESCE(capacity_to::text, ''),
                    lower(btrim(COALESCE(application_prefix, ''))),
                    lower(btrim(COALESCE(license_prefix, ''))),
                    COALESCE(months_eligible, 0)
                ) 1
                FROM typed
            ) x;
            """
        ),
        {"db_sector_name": db_sector_name},
    ).scalar() or 0
    skipped_fees_existing = max(0, int(deduped_count) - int(inserted_fees))

    return {
        "status": "OK",
        "sector_name": sector_name,
        "sector_id": str(sector_id),
        "processed_rows": processed,
        "staged_rows": staged_rows,
        "blank_licencetype_rows": int(blank_licencetype),
        "blank_applicationtype_rows": int(blank_applicationtype),
        "inserted_categories": int(inserted_categories),
        "updated_categories": int(updated_categories),
        "skipped_categories_existing": int(skipped_categories_existing),
        "inserted_fees": int(inserted_fees),
        "skipped_fees_existing_estimate": int(skipped_fees_existing),
        "fee_count_before": int(before_fee_count),
        "fee_count_after": int(after_fee_count),
        "fees_category_id_linked": "see categories.fees_category_id for no-fees variants",
    }
