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
- public.self_employed (only when aei.experience_type = 'SELF_EMPLOYED')
- public.costumer_details (not populated by this file; kept for future)

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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


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
                _d(row.get("wfromdate")),
                _d(row.get("wtodate")),
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

    # Schema guard: columns we rely on
    _progress("schema:guard")
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.work_experience
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

            ALTER TABLE IF EXISTS public.supervisor_details
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS work_experience_id uuid;

            ALTER TABLE IF EXISTS public.self_employed
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

            ALTER TABLE IF EXISTS public.costumer_details
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;
            """
        )
    )
    db.commit()
    _progress("schema:guard:done")

    # STEP 1: insert supervisor_details (one per (aei, supervisor_detail, position))
    _progress("transform:supervisor_details")
    r1 = (
        db.execute(
            text(
                """
                WITH eligible AS (
                    SELECT DISTINCT ON (aei.id, NULLIF(trim(s.supervisor_detail), ''), NULLIF(trim(s.position), ''))
                        s.*, a.id AS app_id, aei.id AS aei_id
                    FROM public.stage_elec_supervisors_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                      AND NULLIF(trim(s.supervisor_detail), '') IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM public.supervisor_details sd
                          WHERE sd.application_electrical_installation_id = aei.id
                            AND COALESCE(trim(sd.name), '') = COALESCE(trim(s.supervisor_detail), '')
                            AND COALESCE(trim(sd.position), '') = COALESCE(trim(s.position), '')
                      )
                    ORDER BY aei.id, trim(s.supervisor_detail), trim(s.position), s.row_no
                ),
                ins AS (
                    INSERT INTO public.supervisor_details (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        name,
                        position,
                        email,
                        mobile_no,
                        created_at,
                        updated_at
                    )
                    SELECT
                        gen_random_uuid(),
                        e.app_id,
                        e.aei_id,
                        NULLIF(trim(e.supervisor_detail), ''),
                        NULLIF(trim(e.position), ''),
                        NULL,
                        NULL,
                        now(),
                        now()
                    FROM eligible e
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
    ins_sd = int(r1.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:supervisor_details inserted={ins_sd}")

    # STEP 2: insert work_experience (one per row keyed by aei+dates+voltage+work)
    _progress("transform:work_experience")
    r2 = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id   AS app_id,
                        aei.id AS aei_id,
                        (
                            SELECT sd.id
                            FROM public.supervisor_details sd
                            WHERE sd.application_electrical_installation_id = aei.id
                              AND COALESCE(trim(sd.name), '') = COALESCE(trim(s.supervisor_detail), '')
                              AND COALESCE(trim(sd.position), '') = COALESCE(trim(s.position), '')
                            LIMIT 1
                        ) AS supervisor_details_id
                    FROM public.stage_elec_supervisors_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT r.*
                    FROM resolved r
                    WHERE NULLIF(trim(r.voltage_level), '') IS NOT NULL
                      AND NULLIF(trim(r.wfromdate), '') IS NOT NULL
                      AND NULLIF(trim(r.wtodate), '') IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM public.work_experience we
                          WHERE we.application_electrical_installation_id = r.aei_id
                            AND we.from_date = r.wfromdate::date
                            AND we.to_date = r.wtodate::date
                            AND COALESCE(trim(we.voltage_level), '') = COALESCE(trim(r.voltage_level), '')
                            AND COALESCE(trim(we.work_description), '') = COALESCE(trim(r.work_performed), '')
                            AND COALESCE(trim(we.position), '') = COALESCE(trim(r.position), '')
                            AND COALESCE(trim(we.role), '') = COALESCE(trim(r.role_and_responsibility), '')
                      )
                ),
                ins AS (
                    INSERT INTO public.work_experience (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        supervisor_details_id,
                        from_date,
                        to_date,
                        work_description,
                        voltage_level,
                        name_of_employer,
                        position,
                        role,
                        created_at,
                        updated_at
                    )
                    SELECT
                        gen_random_uuid(),
                        e.app_id,
                        e.aei_id,
                        e.supervisor_details_id,
                        e.wfromdate::date,
                        e.wtodate::date,
                        NULLIF(trim(e.work_performed), ''),
                        NULLIF(trim(e.voltage_level), ''),
                        NULL,
                        NULLIF(trim(e.position), ''),
                        NULLIF(trim(e.role_and_responsibility), ''),
                        now(),
                        now()
                    FROM eligible e
                    RETURNING id, application_electrical_installation_id
                ),
                upd AS (
                    UPDATE public.supervisor_details sd
                    SET work_experience_id = ins.id,
                        updated_at = now()
                    FROM ins
                    WHERE sd.application_electrical_installation_id = ins.application_electrical_installation_id
                      AND sd.work_experience_id IS NULL
                    RETURNING 1
                )
                SELECT (SELECT COUNT(*) FROM ins) AS cnt;
                """
            )
        )
        .mappings()
        .first()
        or {}
    )
    ins_we = int(r2.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:work_experience inserted={ins_we}")

    # STEP 3: if the application's experience_type=SELF_EMPLOYED, store into self_employed
    _progress("transform:self_employed")
    r3 = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id   AS app_id,
                        aei.id AS aei_id,
                        aei.experience_type AS aei_experience_type
                    FROM public.stage_elec_supervisors_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT r.*
                    FROM resolved r
                    WHERE r.aei_experience_type = 'SELF_EMPLOYED'
                      AND NULLIF(trim(r.voltage_level), '') IS NOT NULL
                      AND NULLIF(trim(r.wfromdate), '') IS NOT NULL
                      AND NULLIF(trim(r.wtodate), '') IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM public.self_employed se
                          WHERE se.application_electrical_installation_id = r.aei_id
                            AND se.from_date = r.wfromdate::date
                            AND se.to_date = r.wtodate::date
                            AND COALESCE(trim(se.voltage_level), '') = COALESCE(trim(r.voltage_level), '')
                            AND COALESCE(trim(se.project_performed), '') = COALESCE(trim(r.work_performed), '')
                      )
                ),
                ins AS (
                    INSERT INTO public.self_employed (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        from_date,
                        to_date,
                        project_performed,
                        voltage_level,
                        created_at,
                        updated_at
                    )
                    SELECT
                        gen_random_uuid(),
                        e.app_id,
                        e.aei_id,
                        e.wfromdate::date,
                        e.wtodate::date,
                        NULLIF(trim(e.work_performed), ''),
                        NULLIF(trim(e.voltage_level), ''),
                        now(),
                        now()
                    FROM eligible e
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
    ins_se = int(r3.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:self_employed inserted={ins_se}")

    # costumer_details cannot be populated from this file (no customer name/mobile columns)
    ins_cd = 0

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
            "self_employed": {
                "count": ins_se,
                "rows": [],
                "note": "only for applications where application_electrical_installation.experience_type = SELF_EMPLOYED",
            },
            "costumer_details": {
                "count": ins_cd,
                "rows": [],
                "note": "not populated by this file; requires customer name/mobile columns",
            },
        },
    }
