from __future__ import annotations

"""Electrical Installation certificate verifications import service.

Source Excel columns (case-insensitive; spaces tolerated)
------------------------------------------------------
apprefno, sno, fromdate, todate, institutenameaddress, award, objectid, filename

Target table
------------
- public.certificate_verifications

Mapping
-------
- institutenameaddress -> education_regulatory_body (NOT NULL)
- award                -> education_regulatory_body_category (NOT NULL)
- objectid             -> logic_doc_id (bigint)
- filename             -> file_name

graduation_year / registration_number / is_external are not present in this file
and are inserted as NULL.
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


def _n_bigint(v) -> Optional[int]:
    s = _c(v)
    if not s:
        return None
    try:
        # handle scientific notation
        return int(float(s))
    except Exception:
        try:
            return int(s)
        except Exception:
            return None


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


def import_electrical_certificate_verifications_via_staging_copy(
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
        logger.info("[electrical_cert_verifications_import] %s", msg)
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

    required = {"apprefno", "institutenameaddress", "award"}
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    total_rows = len(df2)
    if total_rows == 0:
        return {"total_rows_in_file": 0, "staged_total": 0, "inserted": {}}

    # Ensure target table exists
    reg = db.execute(text("SELECT to_regclass('public.certificate_verifications')")).scalar()
    if reg is None:
        raise RuntimeError("public.certificate_verifications table does not exist in this database")

    # Schema guard for new columns
    _progress("schema:guard")
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.certificate_verifications
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS logic_doc_id bigint,
                ADD COLUMN IF NOT EXISTS file_name text;
            """
        )
    )
    db.commit()
    _progress("schema:guard:done")

    # Staging
    _progress("staging:create")
    db.execute(text("DROP TABLE IF EXISTS public.stage_elec_cert_verifications_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_elec_cert_verifications_raw (
                row_no integer,
                app_number character varying(255),
                sno character varying(255),
                fromdate text,
                todate text,
                institutenameaddress text,
                award text,
                objectid text,
                filename text,
                source_file_name text,
                staged_at timestamp default now()
            );
            """
        )
    )
    db.commit()

    staging_rows = []
    for i, row in df2.iterrows():
        staging_rows.append(
            (
                i + 1,
                _c(row.get("apprefno")),
                _c(row.get("sno")),
                _d(row.get("fromdate")),
                _d(row.get("todate")),
                _c(row.get("institutenameaddress")),
                _c(row.get("award")),
                _c(row.get("objectid")),
                _c(row.get("filename")),
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
            COPY public.stage_elec_cert_verifications_raw (
                row_no, app_number, sno, fromdate, todate,
                institutenameaddress, award, objectid, filename,
                source_file_name
            ) FROM STDIN WITH CSV NULL ''
            """,
            sio,
        )
    finally:
        cur.close()

    staged = int(db.execute(text("SELECT COUNT(*) FROM public.stage_elec_cert_verifications_raw")).scalar() or 0)
    db.commit()
    _progress(f"staging:done rows={staged}")

    # Insert
    _progress("transform:certificate_verifications")
    r = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    SELECT
                        s.*, a.id AS app_id, aei.id AS aei_id
                    FROM public.stage_elec_cert_verifications_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    SELECT r.*
                    FROM resolved r
                    WHERE NULLIF(trim(r.institutenameaddress), '') IS NOT NULL
                      AND NULLIF(trim(r.award), '') IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM public.certificate_verifications cv
                          WHERE cv.application_electrical_installation_id = r.aei_id
                            AND COALESCE(trim(cv.education_regulatory_body), '') = COALESCE(trim(r.institutenameaddress), '')
                            AND COALESCE(trim(cv.education_regulatory_body_category), '') = COALESCE(trim(r.award), '')
                            AND COALESCE(cv.logic_doc_id, -1) = COALESCE(NULLIF(trim(r.objectid), '')::bigint, -1)
                      )
                ),
                ins AS (
                    INSERT INTO public.certificate_verifications (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        education_regulatory_body,
                        education_regulatory_body_category,
                        graduation_year,
                        is_external,
                        registration_number,
                        logic_doc_id,
                        file_name,
                        created_at,
                        updated_at
                    )
                    SELECT
                        gen_random_uuid(),
                        e.app_id,
                        e.aei_id,
                        NULLIF(trim(e.institutenameaddress), ''),
                        NULLIF(trim(e.award), ''),
                        NULL,
                        NULL,
                        NULL,
                        CASE WHEN NULLIF(trim(e.objectid), '') IS NOT NULL THEN trim(e.objectid)::bigint END,
                        NULLIF(trim(e.filename), ''),
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
    ins_cv = int(r.get("cnt", 0) or 0)
    db.commit()
    _progress(f"transform:certificate_verifications inserted={ins_cv}")

    _progress("done")
    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
        "inserted": {
            "certificate_verifications": {
                "count": ins_cv,
                "rows": [],
                "note": "row previews are disabled by default to keep responses small",
            }
        },
    }
