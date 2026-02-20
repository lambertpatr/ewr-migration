from __future__ import annotations

import io
import uuid
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session


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
    capacity_from_col = _first_existing_col("acapacityfrom", "capacity_from")
    capacity_to_col = _first_existing_col("acapacityto", "capacity_to")
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
    # Sector-specific required column for application_type mapping
    # - Natural Gas: application_type comes from applicationtype
    # - Water & Wastewater: application_type comes from applicationtype
    # - Electricity: application_type is forced to 'NEW' (no sheet column needed)
    # - Others: application_type comes from licencetype
    if sector_name in ("Natural Gas", "Water & Wastewater"):
        required.append("applicationtype")
    elif sector_name == "Electricity":
        pass
    else:
        required.append("licencetype")
    # optional columns (defaults applied when missing): capacity_from/capacity_to, annualfee, months
    if months_col is None:
        raise ValueError("Missing required columns in upload: licenseperiod_x (or licenseperiod)")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in upload: {missing}")

    # Sector lookup
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

    sector_id = db.execute(
        text("SELECT id FROM public.sectors WHERE name = :name"),
        {"name": db_sector_name},
    ).scalar()
    if not sector_id:
        raise ValueError(
            f"Sector '{db_sector_name}' not found in public.sectors. Please ensure the sectors table is populated."
        )

    stage = "public.stage_license_category_fees_raw"

    _progress("Creating staging table...")
    db.execute(
        text(
            f"""
            DROP TABLE IF EXISTS {stage};
            CREATE TABLE {stage} (
                categoryorclass text,
                licencetype text,
                applicationtype text,
                nocustomerf text,
                nocustomerto text,
                acapacityfrom text,
                acapacityto text,
                appfee text,
                licencefee text,
                annualfee text,
                prefix text,
                licenseprefix text,
                licenseperiod_y text
            );
            """
        )
    )

    # Ensure categories has the columns we want to set during insert.
    # (This keeps the importer self-contained if migrations haven't been run.)
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS sub_sector_type character varying;

            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS code character varying;

            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS category_type character varying;
            """
        )
    )

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
    ]
    stage_df = stage_df.reindex(columns=stage_cols)

    buf = io.StringIO()
    stage_df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    conn = db.connection().connection
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {stage} (categoryorclass, licencetype, applicationtype, nocustomerf, nocustomerto, acapacityfrom, acapacityto, appfee, licencefee, annualfee, prefix, licenseprefix, licenseperiod_y) FROM STDIN WITH (FORMAT CSV)",
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

    # Build a temp mapping table so we can return per-name category_id and use it in fees insert.
    # NOTE: TEMP tables cannot be created inside the public schema.
    db.execute(
        text(
            """
            DROP TABLE IF EXISTS stage_license_category_ids;
            CREATE TEMP TABLE stage_license_category_ids (
                key_name text PRIMARY KEY,
                category_id uuid
            ) ON COMMIT DROP;
            """
        )
    )

    # Insert missing categories
    cat_insert = db.execute(
        text(
            f"""
            WITH raw_names AS (
                SELECT DISTINCT
                    lower(btrim(categoryorclass)) AS key_name,
                    btrim(categoryorclass) AS display_name
                FROM {stage}
                WHERE categoryorclass IS NOT NULL AND btrim(categoryorclass) <> ''
            ),
            missing AS (
                -- Some databases enforce a GLOBAL unique(name) on license_categories.
                -- If the name exists anywhere (even in a different sector), don't insert.
                SELECT
                    rn.*,
                    (
                        SELECT s.licenseprefix
                        FROM {stage} s
                        WHERE lower(btrim(s.categoryorclass)) = rn.key_name
                          AND NULLIF(btrim(s.licenseprefix), '') IS NOT NULL
                        LIMIT 1
                    ) AS first_license_prefix
                FROM raw_names rn
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM public.categories lc
                    WHERE lower(btrim(lc.name)) = rn.key_name
                      AND lc.deleted_at IS NULL
                )
            )
            INSERT INTO public.categories (
                id,
                name,
                code,
                sector_id,
                sub_sector_type,
                category_type,
                is_approved,
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                m.display_name,
                COALESCE(NULLIF(btrim(m.first_license_prefix), ''), m.display_name),
                :sector_id,
                'OPERATIONAL',
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM {stage} ss
                        WHERE lower(btrim(ss.categoryorclass)) = m.key_name
                          AND lower(COALESCE(ss.licencetype, '')) LIKE '%construction%'
                    ) THEN 'Construction'
                    ELSE 'License'
                END,
                false,
                now(),
                now()
            FROM missing m
            ON CONFLICT (name) DO NOTHING
            RETURNING 1;
            """
        ),
        {"sector_id": sector_id},
    )
    inserted_categories = cat_insert.rowcount or 0

    # Populate mapping table for all names in stage (existing + newly inserted)
    db.execute(
        text(
            f"""
            INSERT INTO stage_license_category_ids(key_name, category_id)
            SELECT key_name, category_id
            FROM (
                SELECT
                    lower(btrim(s.categoryorclass)) AS key_name,
                    lc.id AS category_id,
                    row_number() OVER (
                        PARTITION BY lower(btrim(s.categoryorclass))
                        ORDER BY
                            -- Prefer matching sector row when duplicates exist across sectors
                            CASE WHEN lc.sector_id = :sector_id THEN 0 ELSE 1 END,
                            lc.created_at ASC,
                            lc.id
                    ) AS rn
                FROM {stage} s
                JOIN public.categories lc
                  ON lower(btrim(lc.name)) = lower(btrim(s.categoryorclass))
                 AND lc.deleted_at IS NULL
                WHERE s.categoryorclass IS NOT NULL AND btrim(s.categoryorclass) <> ''
            ) x
            WHERE x.rn = 1
            ON CONFLICT (key_name) DO UPDATE SET category_id = EXCLUDED.category_id;
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
                    -- Requested rule:
                    -- - Electricity: use Excel licencetype values.
                    -- - All other sectors: force license_type/category_license_type to OPERATIONAL.
                    NULLIF(
                        btrim(
                            regexp_replace(
                                regexp_replace(
                                    upper(
                                        CASE
                                            WHEN :db_sector_name = 'ELECTRICITY' THEN COALESCE(NULLIF(btrim(s.licencetype), ''), '')
                                            ELSE 'OPERATIONAL'
                                        END
                                    ),
                                    '[^A-Z0-9]+',
                                    '_',
                                    'g'
                                ),
                                '^_+|_+$',
                                '',
                                'g'
                            ),
                            '_'
                        ),
                        ''
                    ) AS license_type,
                    NULLIF(
                        btrim(
                            regexp_replace(
                                regexp_replace(
                                    upper(
                                        CASE
                                            WHEN :db_sector_name = 'ELECTRICITY' THEN COALESCE(NULLIF(btrim(s.licencetype), ''), '')
                                            ELSE 'OPERATIONAL'
                                        END
                                    ),
                                    '[^A-Z0-9]+',
                                    '_',
                                    'g'
                                ),
                                '^_+|_+$',
                                '',
                                'g'
                            ),
                            '_'
                        ),
                        ''
                    ) AS category_license_type,
                    NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
                    NULLIF(btrim(s.nocustomerto), '') AS no_customer_to,
                    upper(
                        CASE
                            WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                                COALESCE(NULLIF(btrim(s.applicationtype), ''), 'NEW')
                            WHEN :db_sector_name = 'ELECTRICITY' THEN
                                'NEW'
                            ELSE
                                COALESCE(NULLIF(btrim(s.licencetype), ''), 'NEW')
                        END
                    ) AS application_type,

                    -- numeric safe casting for capacities/fees
                    -- Natural Gas + Water & Wastewater: do NOT invent capacity ranges; keep NULL when not provided.
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE 1 END
                    END AS capacity_from,
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityto) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityto))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityto) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityto))::numeric ELSE 10 END
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
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    capacity_from,
                    capacity_to,
                    application_prefix,
                    license_prefix,
                    months_eligible
                ) *
                FROM typed
                ORDER BY
                    category_id,
                    lower(btrim(application_type)),
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    capacity_from,
                    capacity_to,
                    application_prefix,
                    license_prefix,
                    months_eligible
            ),
            missing AS (
                SELECT d.*
                FROM deduped d
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM public.application_category_fees f
                    WHERE f.category_id = d.category_id
                      AND lower(btrim(f.application_type)) = lower(btrim(d.application_type))
                AND COALESCE(f.capacity_from, -1) = COALESCE(d.capacity_from, -1)
                AND COALESCE(f.capacity_to, -1) = COALESCE(d.capacity_to, -1)
                      AND COALESCE(f.no_customer_from, '') = COALESCE(d.no_customer_from, '')
                      AND COALESCE(f.no_customer_to, '') = COALESCE(d.no_customer_to, '')
                      AND COALESCE(f.application_prefix, '') = COALESCE(d.application_prefix, '')
                      AND COALESCE(f.license_prefix, '') = COALESCE(d.license_prefix, '')
                      AND COALESCE(f.months_eligible, 0) = COALESCE(d.months_eligible, 0)
                      AND f.deleted_at IS NULL
                )
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
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                m.category_id,
                'NOT_ACTIVE',
                NULLIF(m.license_type, ''),
                NULLIF(m.category_license_type, ''),
                m.application_type,
                m.no_customer_from,
                m.no_customer_to,
                m.capacity_from,
                m.capacity_to,
                m.application_fee,
                m.license_fee,
                m.annual_fee,
                m.application_prefix,
                m.license_prefix,
                m.months_eligible,
                now(),
                now()
            FROM missing m
            RETURNING 1;
            """
        ),
        {"db_sector_name": db_sector_name},
    )

    after_fee_count = db.execute(
        text("SELECT COUNT(*) FROM public.application_category_fees WHERE deleted_at IS NULL")
    ).scalar() or 0
    inserted_fees = int(after_fee_count) - int(before_fee_count)

    processed = int(len(df))

    # figures for skipped are computed approximately via counts
    skipped_categories_existing = db.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT lower(btrim(categoryorclass)) AS key_name
                FROM {stage}
                WHERE categoryorclass IS NOT NULL AND btrim(categoryorclass) <> ''
            ) x
            JOIN public.categories lc
              ON lc.sector_id = :sector_id
             AND lower(btrim(lc.name)) = x.key_name
             AND lc.deleted_at IS NULL;
            """
        ),
        {"sector_id": sector_id},
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
                    -- Keep this typed CTE consistent with the main insert.
                    NULLIF(
                        btrim(
                            regexp_replace(
                                regexp_replace(
                                    upper(
                                        CASE
                                            WHEN :db_sector_name = 'ELECTRICITY' THEN COALESCE(NULLIF(btrim(s.licencetype), ''), '')
                                            ELSE 'OPERATIONAL'
                                        END
                                    ),
                                    '[^A-Z0-9]+',
                                    '_',
                                    'g'
                                ),
                                '^_+|_+$',
                                '',
                                'g'
                            ),
                            '_'
                        ),
                        ''
                    ) AS license_type,
                    NULLIF(
                        btrim(
                            regexp_replace(
                                regexp_replace(
                                    upper(
                                        CASE
                                            WHEN :db_sector_name = 'ELECTRICITY' THEN COALESCE(NULLIF(btrim(s.licencetype), ''), '')
                                            ELSE 'OPERATIONAL'
                                        END
                                    ),
                                    '[^A-Z0-9]+',
                                    '_',
                                    'g'
                                ),
                                '^_+|_+$',
                                '',
                                'g'
                            ),
                            '_'
                        ),
                        ''
                    ) AS category_license_type,
                    NULLIF(btrim(s.nocustomerf), '') AS no_customer_from,
                    NULLIF(btrim(s.nocustomerto), '') AS no_customer_to,
                    upper(
                        CASE
                            WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                                COALESCE(NULLIF(btrim(s.applicationtype), ''), 'NEW')
                            WHEN :db_sector_name = 'ELECTRICITY' THEN
                                'NEW'
                            ELSE
                                COALESCE(NULLIF(btrim(s.licencetype), ''), 'NEW')
                        END
                    ) AS application_type,
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityfrom) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityfrom))::numeric ELSE 1 END
                    END AS capacity_from,
                    CASE
                        WHEN :db_sector_name IN ('NATURAL_GAS', 'WATER_SUPPLY') THEN
                            CASE WHEN btrim(s.acapacityto) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityto))::numeric ELSE NULL END
                        ELSE
                            CASE WHEN btrim(s.acapacityto) ~ '^-?\\d+(\\.\\d+)?$' THEN (btrim(s.acapacityto))::numeric ELSE 10 END
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
                    COALESCE(no_customer_from, ''),
                    COALESCE(no_customer_to, ''),
                    capacity_from,
                    capacity_to,
                    application_prefix,
                    license_prefix,
                    months_eligible
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
        "skipped_categories_existing": int(skipped_categories_existing),
        "inserted_fees": int(inserted_fees),
        "skipped_fees_existing_estimate": int(skipped_fees_existing),
        "fee_count_before": int(before_fee_count),
        "fee_count_after": int(after_fee_count),
    }
