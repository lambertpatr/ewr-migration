from __future__ import annotations

"""Electrical Installation supervisors/work experience import service.

Source Excel columns (case-insensitive; spaces tolerated)
------------------------------------------------------
apprefno, sno, supervisordetail, roleandresponsibility, voltagelevel,
workperformed, position, wfromdate, wtodate

Target tables
-------------
- public.supervisor_details
- public.work_experience

Notes
-----
- Uses a staging table + COPY for performance.
- Response is Swagger-safe by default (counts only), with optional row previews.
"""

from typing import Any, Callable, Optional
import io
import uuid
import logging

logger = logging.getLogger(__name__)

_EMPTY = {"", "nan", "none", "null", "nat"}


def _c(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in _EMPTY else s


def _d(v) -> str:
    """Normalise to ISO date string YYYY-MM-DD, '' if unparseable."""
    import datetime as dt

    if isinstance(v, (dt.datetime, dt.date)):
        return str(v.date()) if isinstance(v, dt.datetime) else str(v)
    s = _c(v)
    if not s:
        return ""
    # Accept both date-only and timestamp-like strings.
    # Note: staging uses `_ts()` which emits 'YYYY-MM-DD 00:00:00'.
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ):
        try:
            return dt.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _ts(v) -> str:
    """Normalise to ISO timestamp string (YYYY-MM-DD 00:00:00), '' if unparseable."""

    d = _d(v)
    return f"{d} 00:00:00" if d else ""


def import_electrical_supervisors_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
    include_rows: bool = False,
    limit_rows: int = 50,
) -> dict:
    from sqlalchemy import text

    def _progress(msg: str):
        logger.info("[electrical_supervisors_import] %s", msg)
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # Normalise columns
    df2 = df.copy()
    df2.columns = (
        df2.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    # Allow common header variants from different templates.
    # We map aliases -> canonical names expected by the transformer.
    col_aliases = {
        # application no
        "app_ref_no": "apprefno",
        "app_refno": "apprefno",
        "app_ref": "apprefno",
        "application_no": "apprefno",
        "application_number": "apprefno",
        "applicationnumber": "apprefno",
        "appno": "apprefno",
        "app_number": "apprefno",
        # supervisor
        "supervisor_detail": "supervisordetail",
        "supervisor_details": "supervisordetail",
        "supervisor": "supervisordetail",
        "supervisordetails": "supervisordetail",
        "name_of_supervisor": "supervisordetail",
        # role/responsibility
        "role_and_responsibility": "roleandresponsibility",
        "role_responsibility": "roleandresponsibility",
        "role": "roleandresponsibility",
        "responsibility": "roleandresponsibility",
        # voltage
        "voltage_level": "voltagelevel",
        "voltage": "voltagelevel",
        # work performed
        "work_performed": "workperformed",
        "workdone": "workperformed",
        "work_done": "workperformed",
        "work": "workperformed",
        "work_description": "workperformed",
        # dates
        "from_date": "wfromdate",
        "fromdate": "wfromdate",
        "work_from_date": "wfromdate",
        "w_from_date": "wfromdate",
        "to_date": "wtodate",
        "todate": "wtodate",
        "work_to_date": "wtodate",
        "w_to_date": "wtodate",
    }
    df2 = df2.rename(columns={c: col_aliases.get(c, c) for c in df2.columns})

    required = {"apprefno", "supervisordetail", "voltagelevel", "workperformed", "wfromdate", "wtodate"}
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    total_rows = len(df2)
    if total_rows == 0:
        return {"total_rows_in_file": 0, "staged_total": 0, "inserted": {}}

    # Staging table
    _progress("staging:create")
    db.execute(text("DROP TABLE IF EXISTS public.stage_elec_supervisors_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_elec_supervisors_raw (
                row_no      integer,
                app_number  character varying(255),
                sno         character varying(255),
                supervisor_detail text,
                role_and_responsibility text,
                voltage_level text,
                work_performed text,
                position text,
                wfromdate text,
                wtodate text,
                source_file_name text,
                staged_at timestamp default now()
            );
            """
        )
    )
    db.commit()

    # Build staging rows
    staging_rows = []
    for i, row in df2.iterrows():
        staging_rows.append(
            (
                i + 1,
                _c(row.get("apprefno")),
                _c(row.get("sno")),
                _c(row.get("supervisordetail")),
                _c(row.get("roleandresponsibility")),
                _c(row.get("voltagelevel")),
                _c(row.get("workperformed")),
                _c(row.get("position")),
                _ts(row.get("wfromdate")),
                _ts(row.get("wtodate")),
                source_file_name or "",
            )
        )

    sio = io.StringIO()
    for r in staging_rows:
        fields = []
        for v in r:
            if v is None or v == "":
                fields.append("")
            else:
                fields.append('"' + str(v).replace('"', '""') + '"')
        sio.write(",".join(fields) + "\n")
    sio.seek(0)

    sa_conn = db.connection()
    raw_conn = sa_conn.connection
    cur = raw_conn.cursor()
    try:
        cur.copy_expert(
            """
            COPY public.stage_elec_supervisors_raw (
                row_no, app_number, sno, supervisor_detail, role_and_responsibility,
                voltage_level, work_performed, position, wfromdate, wtodate,
                source_file_name
            ) FROM STDIN WITH CSV NULL ''
            """,
            sio,
        )
    finally:
        cur.close()

    staged = int(db.execute(text("SELECT COUNT(*) FROM public.stage_elec_supervisors_raw")).scalar() or 0)
    db.commit()
    _progress(f"staging:done rows={staged}")

    # Schema guard: add columns we rely on if they don't exist yet
    _progress("schema:guard")
    db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
    db.execute(
        text(
            """
            -- ── work_experience ──────────────────────────────────────────────
            ALTER TABLE IF EXISTS public.work_experience
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS voltage character varying(255);

            ALTER TABLE IF EXISTS public.work_experience
                ALTER COLUMN work_description TYPE text,
                ALTER COLUMN name_of_employer TYPE text,
                ALTER COLUMN position TYPE text,
                ALTER COLUMN role TYPE text,
                ALTER COLUMN voltage TYPE text,
                ALTER COLUMN voltage_level TYPE text;

            -- ── supervisor_details ───────────────────────────────────────────
            ALTER TABLE IF EXISTS public.supervisor_details
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS work_experience_id uuid;
            """
        )
    )
    db.commit()
    _progress("schema:guard:done")

    # STEP 1: insert work_experience (must exist first; supervisor_details references it)
    _progress("transform:work_experience")
    try:
        r2 = (
            db.execute(
                text(
                    """
                WITH resolved AS (
                    -- AEI is optional during migration; keep application_id as the primary reference.
                    SELECT
                        s.*,
                        a.id   AS app_id,
                        aei.id AS aei_id
                                        FROM public.stage_elec_supervisors_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    LEFT JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    -- Deduplicate so we don't try to upsert the same (id) twice in one INSERT,
                    -- which causes: "ON CONFLICT DO UPDATE command cannot affect row a second time".
                    SELECT DISTINCT ON (we_id)
                        r.*
                    FROM (
                        SELECT
                            r.*,
                            md5(
                                COALESCE(r.aei_id::text, r.app_id::text)
                                || '|we|'
                                || COALESCE(NULLIF(trim(r.wfromdate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.wtodate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.voltage_level), ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.work_performed), ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.position), ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.role_and_responsibility), ''), '')
                            )::uuid AS we_id
                        FROM resolved r
                    ) r
                    ORDER BY we_id, row_no
                ),
                ins AS (
                    INSERT INTO public.work_experience (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        from_date,
                        to_date,
                        work_description,
                        voltage_level,
                        voltage,
                        name_of_employer,
                        position,
                        role,
                        created_at,
                        updated_at
                    )
                    SELECT
                        -- Stable id so reruns are idempotent even when the file is re-uploaded.
                        -- Includes dates + voltage + work fields to allow multiple work rows per aei.
                        e.we_id,
                        e.app_id,
                        e.aei_id,
                        NULLIF(trim(e.wfromdate), '')::timestamp,
                        NULLIF(trim(e.wtodate), '')::timestamp,
                        NULLIF(trim(e.work_performed), ''),
                        'NONE',
                        NULLIF(trim(e.voltage_level), ''),
                        NULLIF(trim(e.supervisor_detail), ''),
                        NULLIF(trim(e.position), ''),
                        NULLIF(trim(e.role_and_responsibility), ''),
                        now(),
                        now()
                    FROM eligible e
                    -- Safety: ensure no duplicate ids can reach the INSERT even if upstream changes.
                    GROUP BY
                        e.we_id,
                        e.app_id,
                        e.aei_id,
                        e.wfromdate,
                        e.wtodate,
                        e.work_performed,
                        e.voltage_level,
                        e.supervisor_detail,
                        e.position,
                        e.role_and_responsibility
                    ON CONFLICT (id) DO UPDATE
                    SET
                        from_date             = COALESCE(EXCLUDED.from_date,             public.work_experience.from_date),
                        to_date               = COALESCE(EXCLUDED.to_date,               public.work_experience.to_date),
                        work_description      = COALESCE(EXCLUDED.work_description, public.work_experience.work_description),
                        voltage_level         = COALESCE(EXCLUDED.voltage_level, public.work_experience.voltage_level),
                        voltage               = COALESCE(EXCLUDED.voltage, public.work_experience.voltage),
                        name_of_employer      = COALESCE(EXCLUDED.name_of_employer, public.work_experience.name_of_employer),
                        position              = COALESCE(EXCLUDED.position, public.work_experience.position),
                        role                  = COALESCE(EXCLUDED.role, public.work_experience.role),
                        updated_at            = now()
                    RETURNING id, application_electrical_installation_id
                )
                SELECT (SELECT COUNT(*) FROM ins) AS cnt;
                    """
                )
            )
            .mappings()
            .first()
            or {}
        )
    except Exception:
        logger.exception("[electrical_supervisors_import] transform:work_experience failed")
        raise
    ins_we = int(r2.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:work_experience inserted={ins_we}")

    # STEP 2: insert supervisor_details (one per (aei, supervisor_detail, position))
    # and link to the (newly inserted / upserted) work_experience row.
    _progress("transform:supervisor_details")
    try:
        r1 = (
            db.execute(
                text(
                    """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id   AS app_id,
                        aei.id AS aei_id,
                        -- work_experience id formula must match the insert above
                        md5(
                            COALESCE(aei.id::text, a.id::text)
                            || '|we|'
                            || COALESCE(NULLIF(trim(s.wfromdate), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(s.wtodate), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(s.voltage_level), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(s.work_performed), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(s.position), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(s.role_and_responsibility), ''), '')
                        )::uuid AS work_experience_id
                    FROM public.stage_elec_supervisors_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    LEFT JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                      AND NULLIF(trim(s.supervisor_detail), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT DISTINCT ON (
                        COALESCE(aei_id, app_id),
                        NULLIF(trim(supervisor_detail), ''),
                        NULLIF(trim(position), '')
                    )
                        *
                    FROM resolved
                    ORDER BY COALESCE(aei_id, app_id),
                             NULLIF(trim(supervisor_detail), ''),
                             NULLIF(trim(position), ''),
                             row_no
                ),
                ins AS (
                    INSERT INTO public.supervisor_details (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        work_experience_id,
                        name,
                        position,
                        email,
                        mobile_no,
                        created_at,
                        updated_at
                    )
                    SELECT
                        md5(
                            COALESCE(e.aei_id::text, e.app_id::text)
                            || '|sd|'
                            || COALESCE(NULLIF(trim(e.supervisor_detail), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(e.position), ''), '')
                        )::uuid,
                        e.app_id,
                        e.aei_id,
                        e.work_experience_id,
                        -- Clean name: strip phone/email garbage embedded by LOIS.
                        -- Pipe-separated: "NAYMAN CHAVALA | COUNTRY DIRECTOR | ..."
                        -- Comma-separated: "CLAVERY MABELE,ELECTRICIAL,..."
                        CASE
                            WHEN NULLIF(trim(e.supervisor_detail), '') LIKE '%|%'
                            THEN NULLIF(trim(SPLIT_PART(e.supervisor_detail, '|', 1)), '')
                            WHEN NULLIF(trim(e.supervisor_detail), '') LIKE '%,%'
                            THEN NULLIF(trim(SPLIT_PART(e.supervisor_detail, ',', 1)), '')
                            ELSE NULLIF(trim(e.supervisor_detail), '')
                        END,
                        NULLIF(trim(e.position), ''),
                        -- Extract email from the raw name string at import time.
                        NULLIF(trim(REGEXP_REPLACE(
                            e.supervisor_detail,
                            '^.*?([a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}).*$',
                            '\1'
                        )), e.supervisor_detail),
                        -- Extract TZ mobile number (with or without spaces) at import time.
                        -- REPLACE(..., ' ', '') collapses "+255 715 67 67 70" → "+255715676770"
                        NULLIF(REPLACE(trim(REGEXP_REPLACE(
                            e.supervisor_detail,
                            '^.*?(\\+?255[\\s]?[0-9]([\\s]?[0-9]){8}|0[67][0-9]([\\s]?[0-9]){7}).*$',
                            '\1'
                        )), ' ', ''), REPLACE(e.supervisor_detail, ' ', '')),
                        now(),
                        now()
                    FROM eligible e
                    ON CONFLICT (id) DO UPDATE
                    SET
                        work_experience_id = COALESCE(EXCLUDED.work_experience_id, public.supervisor_details.work_experience_id),
                        -- Only update name if the stored one still looks like raw LOIS junk
                        -- (contains a separator), otherwise preserve what's already clean.
                        name              = CASE
                                                WHEN public.supervisor_details.name LIKE '%|%'
                                                  OR public.supervisor_details.name LIKE '%,%'
                                                THEN EXCLUDED.name
                                                ELSE COALESCE(public.supervisor_details.name, EXCLUDED.name)
                                            END,
                        position          = COALESCE(EXCLUDED.position,          public.supervisor_details.position),
                        -- Fill mobile_no / email only when currently blank
                        mobile_no         = COALESCE(NULLIF(trim(public.supervisor_details.mobile_no), ''), EXCLUDED.mobile_no),
                        email             = COALESCE(NULLIF(trim(public.supervisor_details.email),     ''), EXCLUDED.email),
                        updated_at        = now()
                    RETURNING 1
                )
                SELECT COUNT(*) AS cnt FROM ins;
                    """
                )
            )
            .mappings()
            .first()
            or {}
        )
    except Exception:
        logger.exception("[electrical_supervisors_import] transform:supervisor_details failed")
        raise
    ins_sd = int(r1.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:supervisor_details inserted={ins_sd}")

    # Optional row previews are not necessary for these endpoints; keep response small
    include_rows = bool(include_rows)
    try:
        limit_rows = int(limit_rows)
    except Exception:
        limit_rows = 50
    limit_rows = max(0, min(limit_rows, 200))

    _progress("done")
    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
        "inserted": {
            "supervisor_details": {"count": ins_sd, "rows": []},
            "work_experience": {"count": ins_we, "rows": []},
        },
    }
