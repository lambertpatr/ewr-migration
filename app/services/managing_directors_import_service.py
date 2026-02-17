from __future__ import annotations

from typing import Any, Callable, Optional
import uuid


def import_managing_directors_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """High-volume import of managing directors using staging + COPY + set-based SQL.

    Excel → DB mapping
    - apprefno -> ca_applications.application_number -> ca_managing_directors.application_id
    - demail -> email
    - phoneno -> mobile_no
    - conadd -> contact_address
    - name -> name
    - countryname -> country
    - nationality1 (country id) -> countries.countriesid -> countries.countryname -> nationality (TEXT)
    - workpermit -> work_permit_id (BIGINT)
    - workpermitfilename -> work_permit (store filename)
    - cpana -> logic_doc_id (BIGINT)
    - cpanafilename -> copy_of_id (store filename)

    Returns JSON-friendly stats including inserted/skipped breakdown and examples.
    """

    from sqlalchemy import text
    import io

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

    required = {
        "apprefno",
        "demail",
        "phoneno",
        "conadd",
        "name",
        "workpermit",
        "workpermitfilename",
        "cpana",
        "cpanafilename",
        "nationality1",
        "countryname",
    }
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns for managing directors import: {sorted(missing)}")

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

    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.stage_ca_managing_directors_raw (
                id uuid PRIMARY KEY,
                application_number text,
                name text,
                email text,
                mobile_no text,
                contact_address text,
                countryname text,
                nationality1 text,
                workpermit text,
                workpermitfilename text,
                cpana text,
                cpanafilename text,
                file_name text,
                source_row_no bigint
            );
            """
        )
    )

    db.execute(text("TRUNCATE TABLE public.stage_ca_managing_directors_raw"))

    _progress("managing_directors:prepare:export")

    export = df2[[
        "apprefno",
        "name",
        "demail",
        "phoneno",
        "conadd",
        "countryname",
        "nationality1",
        "workpermit",
        "workpermitfilename",
        "cpana",
        "cpanafilename",
    ]].copy()

    export.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(export))])
    export.insert(1, "application_number", export["apprefno"].astype(str).str.strip())
    export.insert(2, "file_name", (source_file_name or "").strip())
    export.insert(len(export.columns), "source_row_no", range(1, len(export) + 1))

    _progress(f"managing_directors:prepare:done rows={len(export)}")

    export = export[[
        "id",
        "application_number",
        "name",
        "demail",
        "phoneno",
        "conadd",
        "countryname",
        "nationality1",
        "workpermit",
        "workpermitfilename",
        "cpana",
        "cpanafilename",
        "file_name",
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
                "COPY public.stage_ca_managing_directors_raw FROM STDIN WITH CSV HEADER",
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
        -- Ensure target columns exist
        ALTER TABLE IF EXISTS public.ca_managing_directors
            ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

        ALTER TABLE IF EXISTS public.ca_managing_directors
            ADD COLUMN IF NOT EXISTS work_permit_id bigint;

        WITH s_norm AS (
            SELECT
                s.id,
                NULLIF(trim(s.application_number), '') AS application_number,
                NULLIF(trim(s.name), '') AS md_name,
                NULLIF(trim(s.email), '') AS email,
                NULLIF(trim(s.mobile_no), '') AS mobile_no,
                NULLIF(trim(s.contact_address), '') AS contact_address,
                NULLIF(trim(s.countryname), '') AS country,
                NULLIF(trim(s.nationality1), '') AS nationality1_raw,
                NULLIF(trim(s.workpermit), '') AS workpermit_raw,
                NULLIF(trim(s.workpermitfilename), '') AS work_permit_filename,
                NULLIF(trim(s.cpana), '') AS cpana_raw,
                NULLIF(trim(s.cpanafilename), '') AS copy_of_id_filename,
                COALESCE(NULLIF(trim(s.source_row_no::text), '')::bigint, 0) AS source_row_no
            FROM public.stage_ca_managing_directors_raw s
        ),
        joined AS (
            SELECT
                sn.*,
                a.id AS application_id,
                -- nationality: map nationality1 (id) -> countries.countryname
                COALESCE(
                    c.countryname,
                    sn.country
                ) AS nationality_name,
                -- safe bigint parsing
                CASE
                    WHEN sn.workpermit_raw ~ '^[0-9]+$' THEN sn.workpermit_raw::bigint
                    WHEN sn.workpermit_raw ~ '^[0-9]+\\.0$' THEN replace(sn.workpermit_raw, '.0', '')::bigint
                    ELSE NULL
                END AS work_permit_id,
                CASE
                    WHEN sn.cpana_raw ~ '^[0-9]+$' THEN sn.cpana_raw::bigint
                    WHEN sn.cpana_raw ~ '^[0-9]+\\.0$' THEN replace(sn.cpana_raw, '.0', '')::bigint
                    ELSE NULL
                END AS logic_doc_id,
                (sn.nationality1_raw IS NOT NULL AND sn.nationality1_raw !~ '^[0-9]+(\\.0)?$') AS invalid_nationality1,
                (sn.workpermit_raw IS NOT NULL AND sn.workpermit_raw !~ '^[0-9]+(\\.0)?$') AS invalid_workpermit,
                (sn.cpana_raw IS NOT NULL AND sn.cpana_raw !~ '^[0-9]+(\\.0)?$') AS invalid_cpana
            FROM s_norm sn
            LEFT JOIN public.ca_applications a
                ON a.application_number IS NOT DISTINCT FROM sn.application_number
            LEFT JOIN public.countries c
                ON (
                    sn.nationality1_raw ~ '^[0-9]+$'
                    AND c.countriesid = sn.nationality1_raw::bigint
                )
        ),
        eligible AS (
            SELECT j.*
            FROM joined j
            WHERE j.application_id IS NOT NULL
              AND j.md_name IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.ca_managing_directors x
                  WHERE x.application_id = j.application_id
                    AND lower(trim(x.name)) = lower(trim(j.md_name))
              )
        ),
        ins AS (
            INSERT INTO public.ca_managing_directors (
                id,
                created_at,
                updated_at,
                application_id,
                name,
                email,
                mobile_no,
                contact_address,
                country,
                nationality,
                work_permit,
                work_permit_id,
                copy_of_id,
                logic_doc_id
            )
            SELECT
                e.id,
                now() AS created_at,
                now() AS updated_at,
                e.application_id,
                e.md_name AS name,
                e.email,
                e.mobile_no,
                e.contact_address,
                e.country,
                e.nationality_name AS nationality,
                e.work_permit_filename AS work_permit,
                e.work_permit_id,
                e.copy_of_id_filename AS copy_of_id,
                e.logic_doc_id
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        ),
        stats AS (
            SELECT
                (SELECT COUNT(*) FROM joined) AS processed_rows,
                (SELECT COUNT(*) FROM ins) AS inserted_rows,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NULL) AS skipped_missing_application,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NOT NULL AND md_name IS NULL) AS skipped_missing_name,
                (SELECT COUNT(*) FROM joined j
                    WHERE j.application_id IS NOT NULL
                      AND j.md_name IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM public.ca_managing_directors x
                          WHERE x.application_id = j.application_id
                            AND lower(trim(x.name)) = lower(trim(j.md_name))
                      )
                ) AS skipped_already_exists,
                (SELECT COUNT(*) FROM joined WHERE invalid_nationality1) AS invalid_nationality1,
                (SELECT COUNT(*) FROM joined WHERE invalid_workpermit) AS invalid_workpermit,
                (SELECT COUNT(*) FROM joined WHERE invalid_cpana) AS invalid_cpana
        )
        SELECT
            processed_rows,
            inserted_rows,
            (processed_rows - inserted_rows) AS skipped_total,
            skipped_missing_application,
            skipped_missing_name,
            skipped_already_exists,
            invalid_nationality1,
            invalid_workpermit,
            invalid_cpana
        FROM stats;
        """
    )

    row = db.execute(transform_sql).first()
    if not row:
        row = (staged_total, 0, staged_total, 0, 0, 0, 0, 0, 0)

    processed_rows = int(row[0] or 0)
    inserted_rows = int(row[1] or 0)
    skipped_total = int(row[2] or 0)
    skipped_missing_application = int(row[3] or 0)
    skipped_missing_name = int(row[4] or 0)
    skipped_already_exists = int(row[5] or 0)
    invalid_nationality1 = int(row[6] or 0)
    invalid_workpermit = int(row[7] or 0)
    invalid_cpana = int(row[8] or 0)

    _progress(
        "managing_directors:transform:done "
        f"processed={processed_rows} inserted={inserted_rows} skipped={skipped_total} "
        f"missing_app={skipped_missing_application} missing_name={skipped_missing_name} already_exists={skipped_already_exists}"
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
            "invalid_nationality1": invalid_nationality1,
            "invalid_workpermit": invalid_workpermit,
            "invalid_cpana": invalid_cpana,
            "note": "Rows are skipped when application_number doesn't match ca_applications, name is blank, or an existing managing director with same name already exists for that application.",
        },
    }

    if inserted_rows == 0 and staged_total > 0:
        # Provide small samples for quick debugging
        result["skip_samples"] = _fetch_skip_samples(db, limit=10)

    return result


def _fetch_skip_samples(db: Any, *, limit: int = 10) -> dict:
    """Return small sample rows from staging to explain why inserts might be 0."""
    from sqlalchemy import text

    reg = db.execute(text("select to_regclass('public.stage_ca_managing_directors_raw')")).scalar()
    if not reg:
        return {"note": "staging table public.stage_ca_managing_directors_raw does not exist in this DB/session"}

    samples = {}

    samples["missing_application"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.name, s.demail
                FROM public.stage_ca_managing_directors_raw s
                LEFT JOIN public.ca_applications a
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
                SELECT s.source_row_no, s.application_number, s.name, s.demail
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
                SELECT s.source_row_no, s.application_number, s.name, s.demail
                FROM public.stage_ca_managing_directors_raw s
                JOIN public.ca_applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                JOIN public.ca_managing_directors x
                  ON x.application_id = a.id
                 AND lower(trim(x.name)) = lower(trim(NULLIF(trim(s.name), '')))
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    return samples
