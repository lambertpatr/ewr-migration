from __future__ import annotations

"""Electrical Installation certificate verifications import service.

Source Excel columns (case-insensitive; spaces tolerated)
-------------------------------------------------------
apprefno, sno, fromdate, todate, institutenameaddress, award, objectid, filename

Notes
-----
The target table `public.certificate_verification` has CHECK constraints on
`education_regulatory_body` and `education_regulatory_body_category`. For
migration uploads we force both to the allowed value `'NONE'`.
"""

from typing import Any, Callable, Optional

import io
import csv
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
    reg = db.execute(text("SELECT to_regclass('public.certificate_verification')")).scalar()
    if reg is None:
        raise RuntimeError("public.certificate_verification table does not exist in this database")

    # Schema guard for new columns
    _progress("schema:guard")
    db.execute(
        text(
            """
            ALTER TABLE IF EXISTS public.certificate_verification
                ADD COLUMN IF NOT EXISTS application_id uuid,
                ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid,
                ADD COLUMN IF NOT EXISTS logic_doc_id bigint,
                ADD COLUMN IF NOT EXISTS file_name text,
                ADD COLUMN IF NOT EXISTS from_date timestamp NULL,
                ADD COLUMN IF NOT EXISTS to_date   timestamp NULL;
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

    # Drop any FK constraints that point from certificate_verification
    # to other tables (e.g. other_training, application_electrical_installation)
    # so we can insert with application_id as the primary reference only.
    _progress("schema:drop_fk")
    db.execute(
        text(
            """
            DO $$
            DECLARE r record;
            BEGIN
                FOR r IN (
                    SELECT conname, conrelid::regclass AS tbl
                    FROM pg_constraint
                    WHERE contype = 'f'
                      AND (conrelid = 'public.certificate_verification'::regclass
                           OR confrelid = 'public.certificate_verification'::regclass)
                      AND conname NOT IN (
                          'certificate_verification_pkey'
                      )
                ) LOOP
                    EXECUTE format(
                        'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
                        r.tbl, r.conname
                    );
                END LOOP;
            END $$;
            """
        )
    )
    db.commit()
    _progress("schema:drop_fk:done")

    # Insert
    _progress("transform:certificate_verifications")
    r = (
        db.execute(
            text(
                """
                WITH resolved AS (
                    -- application_id is the primary reference (INNER JOIN).
                    -- application_electrical_installation_id is best-effort (LEFT JOIN).
                    SELECT
                        s.*,
                        a.id   AS app_id,
                        aei.id AS aei_id
                    FROM public.stage_elec_cert_verifications_raw s
                    JOIN public.applications a
                        ON a.application_number = trim(s.app_number)
                    LEFT JOIN public.application_electrical_installation aei
                        ON aei.application_id = a.id
                    WHERE NULLIF(trim(s.app_number), '') IS NOT NULL
                ),
                eligible AS (
                    -- Deduplicate across possible AEI rows from the LEFT JOIN.
                    -- Prefer rows that have aei_id, then earliest staged row_no.
                    SELECT DISTINCT ON (
                        app_id,
                        COALESCE(NULLIF(trim(objectid), '')::bigint, -1),
                        COALESCE(NULLIF(trim(filename), ''), ''),
                        COALESCE(NULLIF(trim(fromdate), ''), ''),
                        COALESCE(NULLIF(trim(todate), ''), ''),
                        COALESCE(NULLIF(trim(institutenameaddress), ''), ''),
                        COALESCE(NULLIF(trim(award), ''), '')
                    )
                        r.*
                    FROM resolved r
                    ORDER BY
                        app_id,
                        COALESCE(NULLIF(trim(objectid), '')::bigint, -1),
                        COALESCE(NULLIF(trim(filename), ''), ''),
                        COALESCE(NULLIF(trim(fromdate), ''), ''),
                        COALESCE(NULLIF(trim(todate), ''), ''),
                        COALESCE(NULLIF(trim(institutenameaddress), ''), ''),
                        COALESCE(NULLIF(trim(award), ''), ''),
                        (aei_id IS NULL),
                        row_no
                ),
                ins AS (
                    INSERT INTO public.certificate_verification (
                        id,
                        application_id,
                        application_electrical_installation_id,
                        from_date,
                        to_date,
                        education_regulatory_body,
                        education_regulatory_body_category,
                        graduation_year,
                        registration_number,
                        is_external,
                        logic_doc_id,
                        file_name,
                        created_at,
                        updated_at
                    )
                    SELECT
                        md5(
                            app_id::text
                            || '|cv|'
                            || COALESCE(NULLIF(trim(institutenameaddress), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(award), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(objectid), ''), '')
                            || '|'
                            || COALESCE(NULLIF(trim(filename), ''), '')
                        )::uuid,
                        app_id,
                        aei_id,
                        NULLIF(trim(fromdate), '')::timestamp,
                        NULLIF(trim(todate), '')::timestamp,
                        -- Force constrained columns to NONE
                        'NONE',
                        'NONE',
                        NULL,
                        NULL,
                        NULL,
                        NULLIF(trim(objectid), '')::bigint,
                        NULLIF(trim(filename), ''),
                        now(),
                        now()
                    FROM eligible
                    ON CONFLICT (id) DO UPDATE
                    SET
                        from_date = COALESCE(EXCLUDED.from_date, public.certificate_verification.from_date),
                        to_date = COALESCE(EXCLUDED.to_date, public.certificate_verification.to_date),
                        education_regulatory_body = 'NONE',
                        education_regulatory_body_category = 'NONE',
                        logic_doc_id = COALESCE(EXCLUDED.logic_doc_id, public.certificate_verification.logic_doc_id),
                        file_name = COALESCE(EXCLUDED.file_name, public.certificate_verification.file_name),
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
