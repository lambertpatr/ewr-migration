from __future__ import annotations

"""Supervisor Details import service.

Source Excel columns (case-insensitive; spaces tolerated)
---------------------------------------------------------
apprefno, sno, supervisordetail, position, roleandresponsibility,
voltagelevel, workperformed, wfromdate, wtodate

Flow
----
1. Normalise + alias column headers.
2. DROP / CREATE public.stage_supervisor_details_raw (fresh each run).
3. Stream rows via COPY … FROM STDIN.
4. Schema guard: ensure all target columns exist (ADD COLUMN IF NOT EXISTS).
5. STEP 1 – insert into public.supervisor_details
            join apprefno → applications.application_number
            left-join application_electrical_installation
6. STEP 2 – if aei.experience_type = 'SELF_EMPLOYED', also insert into
            public.self_employed from the same staged rows.
"""

from typing import Any, Callable, Optional
import io
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
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
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


def import_supervisor_details_via_staging_copy(
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
        logger.info("[supervisor_details_import] %s", msg)
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # ── 1. Normalise columns ─────────────────────────────────────────────────
    df2 = df.copy()
    df2.columns = (
        df2.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    col_aliases = {
        # application number
        "app_ref_no":          "apprefno",
        "app_refno":           "apprefno",
        "app_ref":             "apprefno",
        "application_no":      "apprefno",
        "application_number":  "apprefno",
        "applicationnumber":   "apprefno",
        "appno":               "apprefno",
        "app_number":          "apprefno",
        # supervisor name
        "supervisor_detail":   "supervisordetail",
        "supervisor_details":  "supervisordetail",
        "supervisor":          "supervisordetail",
        "supervisordetails":   "supervisordetail",
        "name_of_supervisor":  "supervisordetail",
        # role / responsibility
        "role_and_responsibility": "roleandresponsibility",
        "role_responsibility":     "roleandresponsibility",
        "role":                    "roleandresponsibility",
        "responsibility":          "roleandresponsibility",
        # voltage
        "voltage_level": "voltagelevel",
        "voltage":       "voltagelevel",
        # work performed
        "work_performed":    "workperformed",
        "workdone":          "workperformed",
        "work_done":         "workperformed",
        "work":              "workperformed",
        "work_description":  "workperformed",
        # dates
        "from_date":       "wfromdate",
        "fromdate":        "wfromdate",
        "work_from_date":  "wfromdate",
        "w_from_date":     "wfromdate",
        "to_date":         "wtodate",
        "todate":          "wtodate",
        "work_to_date":    "wtodate",
        "w_to_date":       "wtodate",
    }
    df2 = df2.rename(columns={c: col_aliases.get(c, c) for c in df2.columns})

    required = {"apprefno", "supervisordetail"}
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    total_rows = len(df2)
    if total_rows == 0:
        return {"total_rows_in_file": 0, "staged_total": 0, "inserted": {}}

    # ── 2. Staging table ─────────────────────────────────────────────────────
    _progress("staging:create")
    db.execute(text("DROP TABLE IF EXISTS public.stage_supervisor_details_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_supervisor_details_raw (
                row_no                  integer,
                app_number              character varying(255),
                sno                     character varying(255),
                supervisor_detail       text,
                position                text,
                role_and_responsibility text,
                voltage_level           text,
                work_performed          text,
                wfromdate               text,
                wtodate                 text,
                source_file_name        text,
                staged_at               timestamp DEFAULT now()
            );
            """
        )
    )
    db.commit()

    # ── 3. Build staging rows ─────────────────────────────────────────────────
    staging_rows = []
    for i, row in df2.iterrows():
        staging_rows.append(
            (
                i + 1,
                _c(row.get("apprefno")),
                _c(row.get("sno")),
                _c(row.get("supervisordetail")),
                _c(row.get("position")),
                _c(row.get("roleandresponsibility")),
                _c(row.get("voltagelevel")),
                _c(row.get("workperformed")),
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
            COPY public.stage_supervisor_details_raw (
                row_no, app_number, sno, supervisor_detail, position,
                role_and_responsibility, voltage_level, work_performed,
                wfromdate, wtodate, source_file_name
            ) FROM STDIN WITH CSV NULL ''
            """,
            sio,
        )
    finally:
        cur.close()

    staged = int(
        db.execute(text("SELECT COUNT(*) FROM public.stage_supervisor_details_raw")).scalar() or 0
    )
    db.commit()
    _progress(f"staging:done rows={staged}")

    # ── 4. Schema guard ───────────────────────────────────────────────────────
    _progress("schema:guard")
    db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))

    # Drop any FK on work_experience.supervisor_details_id that would block bulk inserts
    db.execute(
        text(
            """
            DO $$
            DECLARE r record;
            BEGIN
                FOR r IN (
                    SELECT conname
                    FROM pg_constraint c
                    JOIN pg_class t  ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
                    WHERE c.contype = 'f'
                      AND n.nspname = 'public'
                      AND t.relname = 'work_experience'
                      AND a.attname = 'supervisor_details_id'
                ) LOOP
                    EXECUTE format(
                        'ALTER TABLE public.work_experience DROP CONSTRAINT IF EXISTS %I',
                        r.conname
                    );
                END LOOP;
            END $$;
            """
        )
    )

    db.execute(
        text(
            """
            -- ── supervisor_details ───────────────────────────────────────────
            ALTER TABLE IF EXISTS public.supervisor_details
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS work_experience_id uuid;

            -- ── self_employed ────────────────────────────────────────────────
            ALTER TABLE IF EXISTS public.self_employed
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS voltage character varying(255);

            ALTER TABLE IF EXISTS public.self_employed
                ALTER COLUMN project_performed TYPE text,
                ALTER COLUMN voltage TYPE text,
                ALTER COLUMN voltage_level TYPE text;
            """
        )
    )
    db.commit()
    _progress("schema:guard:done")

    # ── STEP 1: insert supervisor_details ────────────────────────────────────
    # Join apprefno → applications.application_number
    # Left-join application_electrical_installation (best-effort)
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
                        aei.id AS aei_id
                    FROM public.stage_supervisor_details_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    LEFT JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '')      IS NOT NULL
                      AND NULLIF(trim(s.supervisor_detail), '') IS NOT NULL
                ),
                eligible AS (
                    -- Deduplicate: one supervisor row per (aei/app, name, position)
                    SELECT DISTINCT ON (
                        COALESCE(aei_id, app_id),
                        NULLIF(trim(supervisor_detail), ''),
                        NULLIF(trim(position), '')
                    )
                        *
                    FROM resolved
                    ORDER BY
                        COALESCE(aei_id, app_id),
                        NULLIF(trim(supervisor_detail), ''),
                        NULLIF(trim(position), ''),
                        row_no
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
                        md5(
                            COALESCE(e.aei_id::text, e.app_id::text)
                            || '|sd|'
                            || COALESCE(NULLIF(trim(e.supervisor_detail), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(e.position), ''), '')
                        )::uuid,
                        e.app_id,
                        e.aei_id,
                        NULLIF(trim(e.supervisor_detail), ''),
                        NULLIF(trim(e.position), ''),
                        NULL,   -- email  (not in source file)
                        NULL,   -- mobile (not in source file)
                        now(),
                        now()
                    FROM eligible e
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name       = COALESCE(EXCLUDED.name,     public.supervisor_details.name),
                        position   = COALESCE(EXCLUDED.position, public.supervisor_details.position),
                        updated_at = now()
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
        logger.exception("[supervisor_details_import] transform:supervisor_details failed")
        raise
    ins_sd = int(r1.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:supervisor_details inserted={ins_sd}")

    # ── STEP 2: self_employed (only when experience_type = 'SELF_EMPLOYED') ──
    # Strategy mirrors pandas approach:
    #   join apprefno → applications.application_number (INNER)
    #   left-join aei to get experience_type
    #   qualify with UPPER(TRIM(aei.experience_type)) = 'SELF_EMPLOYED'
    #   use COALESCE(aei_id, app_id) as stable-id seed so NULL aei never breaks md5
    _progress("transform:self_employed")
    try:
        r2 = (
            db.execute(
                text(
                    """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id                                  AS app_id,
                        aei.id                                AS aei_id,
                        UPPER(TRIM(aei.experience_type))      AS aei_experience_type
                    FROM public.stage_supervisor_details_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    LEFT JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    -- Only SELF_EMPLOYED rows; deduplicate by stable se_id
                    SELECT DISTINCT ON (se_id)
                        r.*
                    FROM (
                        SELECT
                            r.*,
                            md5(
                                COALESCE(r.aei_id, r.app_id)::text
                                || '|se|'
                                || COALESCE(NULLIF(trim(r.wfromdate),      ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.wtodate),        ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.voltage_level),  ''), '')
                                || '|'
                                || COALESCE(NULLIF(trim(r.work_performed), ''), '')
                            )::uuid AS se_id
                        FROM resolved r
                        WHERE r.aei_experience_type = 'SELF_EMPLOYED'
                    ) r
                    ORDER BY se_id, row_no
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
                        voltage,
                        created_at,
                        updated_at
                    )
                    SELECT
                        e.se_id,
                        e.app_id,
                        e.aei_id,
                        NULLIF(trim(e.wfromdate), '')::timestamp,
                        NULLIF(trim(e.wtodate), '')::timestamp,
                        NULLIF(trim(e.work_performed), ''),
                        'NONE',
                        NULLIF(trim(e.voltage_level), ''),
                        now(),
                        now()
                    FROM eligible e
                    ON CONFLICT (id) DO UPDATE
                    SET
                        from_date         = COALESCE(EXCLUDED.from_date,         public.self_employed.from_date),
                        to_date           = COALESCE(EXCLUDED.to_date,           public.self_employed.to_date),
                        project_performed = COALESCE(EXCLUDED.project_performed, public.self_employed.project_performed),
                        voltage_level     = COALESCE(EXCLUDED.voltage_level,     public.self_employed.voltage_level),
                        voltage           = COALESCE(EXCLUDED.voltage,           public.self_employed.voltage),
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
        logger.exception("[supervisor_details_import] transform:self_employed failed")
        raise
    ins_se = int(r2.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:self_employed inserted={ins_se}")

    _progress("done")
    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
        "inserted": {
            "supervisor_details": {
                "count": ins_sd,
                "rows": [],
            },
            "self_employed": {
                "count": ins_se,
                "rows": [],
                "note": "only for applications where application_electrical_installation.experience_type = 'SELF_EMPLOYED'",
            },
        },
    }
