from __future__ import annotations

from typing import Any, Callable, Optional
import uuid
import pandas as pd

# Re-use the countries dict from shareholders_import_service — single source of truth.
from app.services.shareholders_import_service import COUNTRY_ID_TO_NAME


def _map_country_id_to_name(v) -> str:
    """Convert a numeric country-ID (plain int or scientific notation) to its name."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    try:
        key = int(float(s))          # handles "1.54469E+12" and plain integers
    except (ValueError, OverflowError):
        return ""
    return COUNTRY_ID_TO_NAME.get(key, "")


def _to_bigint_str(v) -> str:
    """Convert scientific-notation or plain numeric value to a clean integer string.

    Returns empty string for anything that cannot be parsed (→ NULL in DB).
    """
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    try:
        # NOTE: use float(...) to handle scientific notation (e.g. 2.55756E+11)
        # then int(...) to drop any trailing .0 representation.
        return str(int(float(s)))
    except (ValueError, OverflowError):
        return ""


def import_managing_directors_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """High-volume import of managing directors using staging + COPY + set-based SQL.

    Excel column → DB column mapping
    ─────────────────────────────────
    apprefno           → application_number  (join key)
    name               → name
    demail             → email
    phoneno            → mobile_no  (bigint, may arrive as scientific notation)
    conadd             → contact_address
    countryname        → country
    nationality1       → nationality  (bigint country-ID → name via COUNTRY_ID_TO_NAME)
    workpermit         → work_permit  (bigint, may arrive as scientific notation)
    workpermitfilename → work_permit_filename  (filename text)
    cpana              → cpana  (bigint, may arrive as scientific notation)
    cpanafilename      → cpana_filename  (filename text)
    """

    from sqlalchemy import text
    import io
    import sys

    def _progress(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    df2 = df.copy()
    df2.columns = (
        df2.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    required = {"apprefno", "name"}
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(
            f"Missing required columns for managing directors import: {sorted(missing)}"
        )

    # DEBUG — printed to stderr so it shows in uvicorn logs
    import sys as _sys
    print(
        f"[managing_directors_import] columns ({len(df2.columns)}): {list(df2.columns)}",
        file=_sys.stderr, flush=True,
    )
    if len(df2) > 0:
        print(
            f"[managing_directors_import] first row: {df2.iloc[0].to_dict()}",
            file=_sys.stderr, flush=True,
        )

    # Fill optional columns with empty string so code below never KeyErrors.
    for _opt in ("demail", "phoneno", "conadd", "countryname",
                 "nationality1", "workpermit", "workpermitfilename",
                 "cpana", "cpanafilename"):
        if _opt not in df2.columns:
            df2[_opt] = ""

    total_rows_in_file = int(len(df2))
    if total_rows_in_file == 0:
        return {
            "total_rows_in_file": 0,
            "staged_total": 0,
            "processed_rows": 0,
            "inserted_rows": 0,
            "skipped_total": 0,
            "skipped_breakdown": {
                "missing_application": 0,
                "missing_name": 0,
                "already_exists": 0,
            },
            "diagnostics": {
                "invalid_nationality1": 0,
                "invalid_workpermit": 0,
                "invalid_cpana": 0,
            },
        }

    _progress("managing_directors:staging:create")

    # Always DROP + recreate so column order always matches the COPY CSV.
    db.execute(text("DROP TABLE IF EXISTS public.stage_ca_managing_directors_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_ca_managing_directors_raw (
                id                 uuid PRIMARY KEY,
                application_number text,
                name               text,
                email              text,
                mobile_no          text,
                contact_address    text,
                countryname        text,
                nationality        text,
                workpermit         text,
                workpermitfilename text,
                cpana              text,
                cpanafilename      text,
                source_row_no      bigint
            )
            """
        )
    )

    _progress("managing_directors:prepare:export")

    # Build export frame — resolve scientific-notation numbers in Python before COPY.
    export = pd.DataFrame()
    export["name"]               = df2["name"].astype(str).str.strip()
    export["email"]              = df2["demail"].astype(str).str.strip()
    export["mobile_no"]          = df2["phoneno"].apply(_to_bigint_str)
    export["contact_address"]    = df2["conadd"].astype(str).str.strip()
    export["countryname"]        = df2["countryname"].astype(str).str.strip()
    # nationality1 is a bigint country-ID (may be scientific notation) → resolve to name
    export["nationality"]        = df2["nationality1"].map(_map_country_id_to_name).fillna("")
    # workpermit and cpana are bigint IDs (may be scientific notation) → clean int strings
    export["workpermit"]         = df2["workpermit"].apply(_to_bigint_str)
    export["workpermitfilename"] = df2["workpermitfilename"].astype(str).str.strip()
    export["cpana"]              = df2["cpana"].apply(_to_bigint_str)
    export["cpanafilename"]      = df2["cpanafilename"].astype(str).str.strip()

    n = len(export)
    export.insert(0, "id",                 [str(uuid.uuid4()) for _ in range(n)])
    export.insert(1, "application_number", df2["apprefno"].astype(str).str.strip())
    export["source_row_no"] = range(1, n + 1)

    _progress(f"managing_directors:prepare:done rows={n}")

    # Final column order MUST match CREATE TABLE above.
    export = export[[
        "id",
        "application_number",
        "name",
        "email",
        "mobile_no",
        "contact_address",
        "countryname",
        "nationality",
        "workpermit",
        "workpermitfilename",
        "cpana",
        "cpanafilename",
        "source_row_no",
    ]]

    def _iter_csv_chunks(frame, chunk_rows: int = 50000):
        header_written = False
        for start in range(0, len(frame), chunk_rows):
            chunk = frame.iloc[start : start + chunk_rows]
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=not header_written)
            header_written = True
            buf.seek(0)
            yield buf, min(len(frame), start + len(chunk))

    sa_conn = db.connection()
    raw_conn = sa_conn.connection
    cur = raw_conn.cursor()
    try:
        total = len(export)
        copied = 0
        _progress(f"managing_directors:copy:start total_rows={total}")
        for buf, copied_now in _iter_csv_chunks(export, chunk_rows=50000):
            cur.copy_expert(
                """COPY public.stage_ca_managing_directors_raw (
                    id, application_number, name, email, mobile_no,
                    contact_address, countryname, nationality,
                    workpermit, workpermitfilename,
                    cpana, cpanafilename, source_row_no
                ) FROM STDIN WITH CSV HEADER""",
                buf,
            )
            copied = copied_now
            pct = round((copied / total) * 100, 2) if total else 100.0
            _progress(f"managing_directors:copy {copied}/{total} ({pct}%)")
    finally:
        cur.close()

    staged_total = int(db.execute(text("SELECT COUNT(*) FROM public.stage_ca_managing_directors_raw")).scalar() or 0)
    _progress(f"managing_directors:staged rows={staged_total}")

    _progress("managing_directors:transform:sql")

    transform_sql = text(
        """
        -- Ensure all required columns exist on the target table.
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS work_permit               bigint;
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS work_permit_filename      character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS cpana                     bigint;
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS cpana_filename            character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS national_id_number        character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS passport_number           character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS birth_date                character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS gender                    character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS first_name                character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS middle_name               character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS last_name                 character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS email                         character varying(255);
        ALTER TABLE IF EXISTS public.managing_directors
            ADD COLUMN IF NOT EXISTS nationality                   character varying(255);
        -- NOTE: We intentionally do NOT create/use application_id/contact_address/work_permit/cpana_filename
        -- because your current public.managing_directors table doesn't have those columns.

        WITH s_norm AS (
            SELECT
                s.id,
                NULLIF(trim(s.application_number), '')  AS application_number,
                NULLIF(trim(s.name), '')                AS md_name,
                NULLIF(trim(s.email), '')               AS email,
                NULLIF(trim(s.mobile_no), '')           AS mobile_no,
                NULLIF(trim(s.contact_address), '')     AS contact_address,
                NULLIF(trim(s.countryname), '')         AS country,
                -- nationality already resolved to country name in Python
                -- Normalize to enum: TANZANIAN | NON_TANZANIAN | NULL
                CASE
                    WHEN LOWER(TRIM(s.nationality)) = 'tanzanian'          THEN 'TANZANIAN'
                    WHEN LOWER(TRIM(s.nationality)) LIKE '%non%tanzanian%' THEN 'NON_TANZANIAN'
                    WHEN LOWER(TRIM(s.nationality)) = 'non-tanzanian'      THEN 'NON_TANZANIAN'
                    ELSE NULL
                END                                    AS nationality,
                -- workpermit and cpana are already clean integer strings from Python
                CASE
                    WHEN NULLIF(trim(s.workpermit), '') IS NULL THEN NULL
                    WHEN trim(s.workpermit) ~ '^[0-9]+$'       THEN trim(s.workpermit)::bigint
                    ELSE NULL
                END AS work_permit,
                NULLIF(trim(s.workpermitfilename), '')  AS work_permit_filename,
                CASE
                    WHEN NULLIF(trim(s.cpana), '') IS NULL THEN NULL
                    WHEN trim(s.cpana) ~ '^[0-9]+$'       THEN trim(s.cpana)::bigint
                    ELSE NULL
                END AS cpana_val,
                NULLIF(trim(s.cpanafilename), '')       AS cpana_filename,
                s.source_row_no,
                (NULLIF(trim(s.workpermit), '') IS NOT NULL
                    AND trim(s.workpermit) !~ '^[0-9]+$') AS invalid_workpermit,
                (NULLIF(trim(s.cpana), '') IS NOT NULL
                    AND trim(s.cpana) !~ '^[0-9]+$')      AS invalid_cpana
            FROM public.stage_ca_managing_directors_raw s
        ),
        joined AS (
            SELECT
                sn.*,
                asd.id AS application_sector_detail_id
            FROM s_norm sn
            LEFT JOIN public.applications a
                ON a.application_number IS NOT DISTINCT FROM sn.application_number
            LEFT JOIN public.application_sector_details asd
                ON asd.application_id = a.id
        ),
        eligible AS (
            SELECT j.*
            FROM joined j
            WHERE j.application_sector_detail_id IS NOT NULL
              AND j.md_name IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.managing_directors x
                  WHERE lower(trim(x.name)) = lower(trim(j.md_name))
                    AND x.application_sector_detail_id = j.application_sector_detail_id
              )
        ),
        ins AS (
            INSERT INTO public.managing_directors (
                id,
                created_at,
                updated_at,
                application_sector_detail_id,
                name,
                first_name,
                middle_name,
                last_name,
                email,
                mobile_no,
                country,
                nationality,
                work_permit,
                work_permit_filename,
                cpana,
                cpana_filename
            )
            SELECT
                e.id,
                now()                          AS created_at,
                now()                          AS updated_at,
                e.application_sector_detail_id,
                e.md_name                      AS name,
                split_part(trim(e.md_name), ' ', 1) AS first_name,
                CASE
                    WHEN array_length(regexp_split_to_array(trim(e.md_name), '\\s+'), 1) <= 2
                    THEN NULL
                    ELSE regexp_replace(trim(e.md_name), '^\\S+\\s+(.+)\\s+\\S+$', '\\1')
                END                            AS middle_name,
                CASE
                    WHEN array_length(regexp_split_to_array(trim(e.md_name), '\\s+'), 1) = 1
                    THEN NULL
                    ELSE regexp_replace(trim(e.md_name), '^.*\\s+(\\S+)$', '\\1')
                END                            AS last_name,
                e.email,
                e.mobile_no,
                e.country,
                e.nationality,
                e.work_permit,
                e.work_permit_filename,
                e.cpana_val                    AS cpana,
                e.cpana_filename               AS cpana_filename
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        ),
        stats AS (
            SELECT
                (SELECT COUNT(*) FROM joined)                                    AS processed_rows,
                (SELECT COUNT(*) FROM ins)                                       AS inserted_rows,
                (SELECT COUNT(*) FROM joined WHERE application_sector_detail_id IS NULL) AS skipped_missing_application,
                (SELECT COUNT(*) FROM joined
                    WHERE application_sector_detail_id IS NOT NULL AND md_name IS NULL) AS skipped_missing_name,
                (SELECT COUNT(*) FROM joined j
                    WHERE j.application_sector_detail_id IS NOT NULL
                      AND j.md_name IS NOT NULL
                      AND EXISTS (
                          SELECT 1 FROM public.managing_directors x
                          WHERE lower(trim(x.name)) = lower(trim(j.md_name))
                            AND x.application_sector_detail_id = j.application_sector_detail_id
                      )
                )                                                                AS skipped_already_exists,
                (SELECT COUNT(*) FROM joined WHERE invalid_workpermit)           AS invalid_workpermit,
                (SELECT COUNT(*) FROM joined WHERE invalid_cpana)                AS invalid_cpana
        )
        SELECT
            processed_rows,
            inserted_rows,
            (processed_rows - inserted_rows) AS skipped_total,
            skipped_missing_application,
            skipped_missing_name,
            skipped_already_exists,
            invalid_workpermit,
            invalid_cpana
        FROM stats;
        """
    )

    row = db.execute(transform_sql).first()
    if not row:
        row = (staged_total, 0, staged_total, 0, 0, 0, 0, 0)

    processed_rows              = int(row[0] or 0)
    inserted_rows               = int(row[1] or 0)
    skipped_total               = int(row[2] or 0)
    skipped_missing_application = int(row[3] or 0)
    skipped_missing_name        = int(row[4] or 0)
    skipped_already_exists      = int(row[5] or 0)
    invalid_workpermit          = int(row[6] or 0)
    invalid_cpana               = int(row[7] or 0)

    _progress(
        "managing_directors:transform:done "
        f"processed={processed_rows} inserted={inserted_rows} skipped={skipped_total} "
        f"missing_app={skipped_missing_application} missing_name={skipped_missing_name} "
        f"already_exists={skipped_already_exists}"
    )

    result = {
        "total_rows_in_file": total_rows_in_file,
        "staged_total": staged_total,
        "processed_rows": processed_rows,
        "inserted_rows": inserted_rows,
        "skipped_total": skipped_total,
        "skipped_breakdown": {
            "missing_application": skipped_missing_application,
            "missing_name": skipped_missing_name,
            "already_exists": skipped_already_exists,
        },
        "diagnostics": {
            "invalid_workpermit": invalid_workpermit,
            "invalid_cpana": invalid_cpana,
            "note": (
                "Rows are skipped when application_number has no match in public.applications, "
                "name is blank, or a director with the same name already exists for that application."
            ),
        },
    }

    if inserted_rows == 0 and staged_total > 0:
        # Provide small samples for quick debugging
        result["skip_samples"] = _fetch_skip_samples(db, limit=10)

    return result


def _fetch_skip_samples(db: Any, *, limit: int = 10) -> dict:
    """Return small sample rows from staging to explain why inserts might be 0."""
    from sqlalchemy import text

    reg = db.execute(
        text("SELECT to_regclass('public.stage_ca_managing_directors_raw')")
    ).scalar()
    if not reg:
        return {"note": "staging table public.stage_ca_managing_directors_raw does not exist"}

    samples = {}

    samples["missing_application"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.name, s.email
                FROM public.stage_ca_managing_directors_raw s
                LEFT JOIN public.applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                WHERE a.id IS NULL
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    samples["missing_name"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.name, s.email
                FROM public.stage_ca_managing_directors_raw s
                WHERE NULLIF(trim(s.name), '') IS NULL
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    samples["already_exists"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.name, s.email
                FROM public.stage_ca_managing_directors_raw s
                JOIN public.applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                                JOIN public.application_sector_details asd
                                    ON asd.application_id = a.id
                                JOIN public.managing_directors x
                                    ON x.application_sector_detail_id = asd.id
                                 AND lower(trim(x.name)) = lower(trim(NULLIF(trim(s.name), '')))
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    return samples
