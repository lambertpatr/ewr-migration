from __future__ import annotations

"""Self-employed (special Excel) import service.

This is for the separate SELF_EMPLOYED Excel template with columns:
- apprefno, sno, sfevoltagelevel, sfecustomdetails, projectperformed,
  sfefromdate, sfetodate

Target tables:
- public.self_employed
- public.custom_details

Rules / mapping:
- Resolve application_id by joining applications.application_number = apprefno.
- Resolve application_electrical_installation_id by joining
  application_electrical_installation on application_id AND experience_type = 'SELF_EMPLOYED'.
- Insert into self_employed:
    voltage = sfevoltagelevel
    project_performed = projectperformed
    from_date/to_date = sfefromdate/sfetodate
    voltage_level = 'NONE'
- Insert into custom_details:
    name = sfecustomdetails
    (mobile_no left NULL for now; can be parsed later if needed)

Idempotency:
- Uses stable UUIDs from md5() seeds + ON CONFLICT (id) DO UPDATE with COALESCE.
- custom_details enforces unique(self_employed_details_id), so we upsert on that.

Staging table:
- public.stage_self_employed_upload_raw
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
    d = _d(v)
    return f"{d} 00:00:00" if d else ""


def import_self_employed_upload_via_staging_copy(
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
        logger.info("[self_employed_upload_import] %s", msg)
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

    col_aliases = {
        # application
        "app_ref_no": "apprefno",
        "app_refno": "apprefno",
        "application_no": "apprefno",
        "application_number": "apprefno",
        "applicationnumber": "apprefno",
        "app_number": "apprefno",
        # row number
        "sn": "sno",
        "s_n": "sno",
        # fields
        "sfe_voltage_level": "sfevoltagelevel",
        "voltage": "sfevoltagelevel",
        "voltagelevel": "sfevoltagelevel",
        "sfe_custom_details": "sfecustomdetails",
        "custom_details": "sfecustomdetails",
        "project_performed": "projectperformed",
        "projectperformed": "projectperformed",
        "from_date": "sfefromdate",
        "sfefrom_date": "sfefromdate",
        "to_date": "sfetodate",
        "sfeto_date": "sfetodate",
    }
    df2 = df2.rename(columns={k: v for k, v in col_aliases.items() if k in df2.columns})

    required = [
        "apprefno",
        "sno",
        "sfevoltagelevel",
        "sfecustomdetails",
        "projectperformed",
        "sfefromdate",
        "sfetodate",
    ]
    for col in required:
        if col not in df2.columns:
            df2[col] = ""

    df2["apprefno"] = df2["apprefno"].map(_c)
    df2["sno"] = df2["sno"].map(_c)
    df2["sfevoltagelevel"] = df2["sfevoltagelevel"].map(_c)
    df2["sfecustomdetails"] = df2["sfecustomdetails"].map(_c)
    df2["projectperformed"] = df2["projectperformed"].map(_c)
    df2["sfefromdate"] = df2["sfefromdate"].map(_ts)
    df2["sfetodate"] = df2["sfetodate"].map(_ts)

    total_rows = int(len(df2))

    _progress("stage:drop_create")
    db.execute(text("DROP TABLE IF EXISTS public.stage_self_employed_upload_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_self_employed_upload_raw (
                row_no bigint,
                apprefno text,
                sno text,
                sfevoltagelevel text,
                sfecustomdetails text,
                projectperformed text,
                sfefromdate text,
                sfetodate text
            );
            """
        )
    )
    db.commit()

    _progress("stage:copy")
    csv_buf = io.StringIO()
    df2_out = df2[
        [
            "apprefno",
            "sno",
            "sfevoltagelevel",
            "sfecustomdetails",
            "projectperformed",
            "sfefromdate",
            "sfetodate",
        ]
    ].copy()
    df2_out.insert(0, "row_no", range(1, len(df2_out) + 1))
    df2_out.to_csv(csv_buf, index=False, header=False)
    csv_buf.seek(0)

    raw_conn = db.connection().connection
    cur = raw_conn.cursor()
    try:
        cur.copy_expert(
            """
            COPY public.stage_self_employed_upload_raw (
                row_no, apprefno, sno, sfevoltagelevel, sfecustomdetails,
                projectperformed, sfefromdate, sfetodate
            ) FROM STDIN WITH CSV
            """.strip(),
            csv_buf,
        )
    finally:
        cur.close()
    db.commit()

    staged = int(
        db.execute(text("SELECT COUNT(*) FROM public.stage_self_employed_upload_raw")).scalar()
        or 0
    )
    _progress(f"stage:done rows={staged}")

    # ── Diagnostics helpers ────────────────────────────────────────────────
    # Normalise experience_type more defensively (handles 'SELF EMPLOYED',
    # 'SELF-EMPLOYED', mixed case, etc.)
    exp_norm_sql = "UPPER(REPLACE(REPLACE(TRIM(aei.experience_type), '-', '_'), ' ', '_'))"

    # ── Schema guard (live DB drift) ───────────────────────────────────────
    # Some environments are missing these columns even though the target
    # schema expects them. Add them at runtime so imports don't fail.
    _progress("schema_guard")
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.self_employed
                ADD COLUMN IF NOT EXISTS application_id uuid;

            ALTER TABLE IF EXISTS public.self_employed
                ADD COLUMN IF NOT EXISTS voltage character varying(255);

            ALTER TABLE IF EXISTS public.custom_details
                ADD COLUMN IF NOT EXISTS application_id uuid;

            -- Some live schemas have name as varchar(255); this upload can exceed that.
            ALTER TABLE IF EXISTS public.custom_details
                ALTER COLUMN name TYPE text;
            """
        )
    )
    db.commit()

    # STEP 1: self_employed
    _progress("transform:self_employed")
    r1 = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id AS app_id,
                        aei.id AS aei_id,
                        """ + exp_norm_sql + """ AS aei_experience_type
                    FROM public.stage_self_employed_upload_raw s
                    JOIN public.applications a
                        ON a.application_number = TRIM(s.apprefno)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(TRIM(s.apprefno), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT DISTINCT ON (se_id)
                        r.*
                    FROM (
                        SELECT
                            r.*,
                            md5(
                                r.aei_id::text
                                || '|se_upload|'
                                || COALESCE(NULLIF(TRIM(r.sno), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfefromdate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfetodate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfevoltagelevel), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.projectperformed), ''), '')
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
                        NULLIF(TRIM(e.sfefromdate), '')::timestamp,
                        NULLIF(TRIM(e.sfetodate), '')::timestamp,
                        NULLIF(TRIM(e.projectperformed), ''),
                        'NONE',
                        NULLIF(TRIM(e.sfevoltagelevel), ''),
                        now(),
                        now()
                    FROM eligible e
                    ON CONFLICT (id) DO UPDATE
                    SET
                        application_id    = COALESCE(EXCLUDED.application_id,    public.self_employed.application_id),
                        from_date         = COALESCE(EXCLUDED.from_date,         public.self_employed.from_date),
                        to_date           = COALESCE(EXCLUDED.to_date,           public.self_employed.to_date),
                        project_performed = COALESCE(EXCLUDED.project_performed, public.self_employed.project_performed),
                        voltage_level     = COALESCE(EXCLUDED.voltage_level,     public.self_employed.voltage_level),
                        voltage           = COALESCE(EXCLUDED.voltage,           public.self_employed.voltage),
                        updated_at        = now()
                    RETURNING id
                )
                SELECT COUNT(*) AS cnt FROM ins;
                """
            )
        )
        .mappings()
        .first()
        or {}
    )
    ins_se = int(r1.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:self_employed inserted={ins_se}")

    # STEP 2: custom_details (1 per self_employed)
    _progress("transform:custom_details")
    r2 = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    SELECT
                        s.*,
                        a.id AS app_id,
                        aei.id AS aei_id,
                        """ + exp_norm_sql + """ AS aei_experience_type
                    FROM public.stage_self_employed_upload_raw s
                    JOIN public.applications a
                        ON a.application_number = TRIM(s.apprefno)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(TRIM(s.apprefno), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT DISTINCT ON (se_id)
                        r.*
                    FROM (
                        SELECT
                            r.*,
                            md5(
                                r.aei_id::text
                                || '|se_upload|'
                                || COALESCE(NULLIF(TRIM(r.sno), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfefromdate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfetodate), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.sfevoltagelevel), ''), '')
                                || '|'
                                || COALESCE(NULLIF(TRIM(r.projectperformed), ''), '')
                            )::uuid AS se_id
                        FROM resolved r
                        WHERE r.aei_experience_type = 'SELF_EMPLOYED'
                    ) r
                    ORDER BY se_id, row_no
                ),
                ins AS (
                    INSERT INTO public.custom_details (
                        id,
                        application_id,
                        self_employed_details_id,
                        name,
                        mobile_no,
                        created_at,
                        updated_at
                    )
                    SELECT
                        md5(e.se_id::text || '|custom_details')::uuid,
                        e.app_id,
                        e.se_id,
                        NULLIF(LEFT(TRIM(e.sfecustomdetails), 10000), ''),
                        NULL,
                        now(),
                        now()
                    FROM eligible e
                    ON CONFLICT (self_employed_details_id) DO UPDATE
                    SET
                        application_id = COALESCE(EXCLUDED.application_id, public.custom_details.application_id),
                        name       = COALESCE(EXCLUDED.name, public.custom_details.name),
                        mobile_no  = COALESCE(EXCLUDED.mobile_no, public.custom_details.mobile_no),
                        updated_at = now()
                    RETURNING id
                )
                SELECT COUNT(*) AS cnt FROM ins;
                """
            )
        )
        .mappings()
        .first()
        or {}
    )
    ins_cd = int(r2.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:custom_details inserted={ins_cd}")

    # ── Diagnostics summary (helps when inserted=0) ────────────────────────
    _progress("diagnostics")
    diag = {
        "staged_total": staged,
        "matched_applications": 0,
        "matched_aei": 0,
        "matched_self_employed_aei": 0,
        "missing_application_numbers_sample": [],
        "self_employed_apprefno_sample": [],
    }
    try:
        row = (
            db.execute(
                text(
                    """
                    WITH s AS (
                        SELECT DISTINCT TRIM(apprefno) AS apprefno
                        FROM public.stage_self_employed_upload_raw
                        WHERE NULLIF(TRIM(apprefno), '') IS NOT NULL
                    )
                    SELECT
                        (SELECT COUNT(*) FROM s) AS staged_distinct_apprefno,
                        (SELECT COUNT(*) FROM s JOIN public.applications a ON a.application_number = s.apprefno) AS matched_applications,
                        (SELECT COUNT(*)
                           FROM s
                           JOIN public.applications a ON a.application_number = s.apprefno
                           JOIN public.application_electrical_installation aei ON aei.application_id = a.id
                        ) AS matched_aei,
                        (SELECT COUNT(*)
                           FROM s
                           JOIN public.applications a ON a.application_number = s.apprefno
                           JOIN public.application_electrical_installation aei ON aei.application_id = a.id
                          WHERE """ + exp_norm_sql + """ = 'SELF_EMPLOYED'
                        ) AS matched_self_employed_aei
                    """
                )
            )
            .mappings()
            .first()
            or {}
        )
        diag["matched_applications"] = int(row.get("matched_applications", 0) or 0)
        diag["matched_aei"] = int(row.get("matched_aei", 0) or 0)
        diag["matched_self_employed_aei"] = int(row.get("matched_self_employed_aei", 0) or 0)

        missing = (
            db.execute(
                text(
                    """
                    SELECT DISTINCT TRIM(s.apprefno) AS apprefno
                    FROM public.stage_self_employed_upload_raw s
                    LEFT JOIN public.applications a
                      ON a.application_number = TRIM(s.apprefno)
                    WHERE NULLIF(TRIM(s.apprefno), '') IS NOT NULL
                      AND a.id IS NULL
                    ORDER BY apprefno
                    LIMIT 25;
                    """
                )
            )
            .scalars()
            .all()
        )
        diag["missing_application_numbers_sample"] = list(missing or [])

        se_matches = (
            db.execute(
                text(
                    """
                    SELECT DISTINCT a.application_number
                    FROM public.stage_self_employed_upload_raw s
                    JOIN public.applications a
                      ON a.application_number = TRIM(s.apprefno)
                    JOIN public.application_electrical_installation aei
                      ON aei.application_id = a.id
                    WHERE NULLIF(TRIM(s.apprefno), '') IS NOT NULL
                      AND """ + exp_norm_sql + """ = 'SELF_EMPLOYED'
                    ORDER BY a.application_number
                    LIMIT 25;
                    """
                )
            )
            .scalars()
            .all()
        )
        diag["self_employed_apprefno_sample"] = list(se_matches or [])
    except Exception:
        logger.exception("[self_employed_upload_import] diagnostics failed")

    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
        "inserted": {
            "self_employed": {"count": ins_se, "rows": []},
            "custom_details": {"count": ins_cd, "rows": []},
        },
        "diagnostics": diag,
        "note": "Only rows where application_electrical_installation.experience_type = 'SELF_EMPLOYED' are inserted.",
        "source_file_name": source_file_name,
    }
