from __future__ import annotations

from typing import Any, Callable, Optional
import uuid


def import_shareholders_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """High-volume import of shareholders using staging + COPY + SQL transform.

    Contract
    - Input df: dataframe containing (at least) the Excel columns used by the mapping:
        apprefno, countryname, amountofshare, objectid, nationality, indcomp,
        sconadd, shname, rowid
    - Output: dict of counts.

    Notes
    - Stages into public.stage_ca_shareholders_raw.
    - Transforms into public.ca_shareholders by joining:
        stage.application_number (apprefno) -> ca_applications.application_number
    """

    from sqlalchemy import text
    import io

    def _progress(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # Normalize columns
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
        "countryname",
        "amountofshare",
        "objectid",
        "nationality",
        "indcomp",
        "sconadd",
        "shname",
        "rowid",
    }
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns for shareholders import: {sorted(missing)}")

    total_rows_in_file = int(len(df2))
    if total_rows_in_file == 0:
        return {
            "total_rows_in_file": 0,
            "staged_total": 0,
            "inserted_shareholders": 0,
            "skipped_total": 0,
            "skipped_breakdown": {
                "missing_application": 0,
                "missing_shareholder_name": 0,
                "already_exists": 0,
            },
            "diagnostics": {
                "invalid_objectid": 0,
            },
        }

    _progress("shareholders:staging:create")

    # Ensure staging table exists
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.stage_ca_shareholders_raw (
                id uuid PRIMARY KEY,
                application_number text,
                shname text,
                amountofshare text,
                sconadd text,
                countryname text,
                indcomp text,
                nationality text,
                rowid text,
                objectid text,
                file_name text,
                source_row_no bigint
            );
            """
        )
    )

    db.execute(text("TRUNCATE TABLE public.stage_ca_shareholders_raw"))

    _progress("shareholders:prepare:export")

    # Build export frame
    export = df2[[
        "apprefno",
        "shname",
        "amountofshare",
        "sconadd",
        "countryname",
        "indcomp",
        "nationality",
        "rowid",
        "objectid",
    ]].copy()

    export.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(export))])
    export.insert(1, "application_number", export["apprefno"].astype(str).str.strip())
    export.insert(2, "file_name", (source_file_name or "").strip())
    export.insert(len(export.columns), "source_row_no", range(1, len(export) + 1))

    _progress(f"shareholders:prepare:done rows={len(export)}")

    # Match staging column order
    export = export[[
        "id",
        "application_number",
        "file_name",
        "shname",
        "amountofshare",
        "sconadd",
        "countryname",
        "indcomp",
        "nationality",
        "rowid",
        "objectid",
        "source_row_no",
    ]]

    # Stream COPY in chunks
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
        _progress(f"shareholders:copy:start total_rows={total}")
        for buf, copied_now in _iter_csv_chunks(export, chunk_rows=50000):
            cur.copy_expert(
                "COPY public.stage_ca_shareholders_raw FROM STDIN WITH CSV HEADER",
                buf,
            )
            copied = copied_now
            pct = round((copied / total) * 100, 2) if total else 100.0
            _progress(f"shareholders:copy {copied}/{total} ({pct}%)")
    finally:
        cur.close()

    staged_total = int(db.execute(text("SELECT COUNT(*) FROM public.stage_ca_shareholders_raw")).scalar() or 0)
    _progress(f"shareholders:staged rows={staged_total}")

    # Transform into final tables.
    # We reuse the global transform script if it already includes shareholders insert.
    _progress("shareholders:transform:start")

    # Run transform and compute detailed stats.
    transform_sql = text(
        """
        -- ensure ca_shareholders has logic_doc_id and file_name
        ALTER TABLE IF EXISTS public.ca_shareholders
            ADD COLUMN IF NOT EXISTS logic_doc_id bigint;

        ALTER TABLE IF EXISTS public.ca_shareholders
            ADD COLUMN IF NOT EXISTS file_name text;

        -- Normalize staging and compute eligibility/skips.
        WITH s_norm AS (
            SELECT
                s.id,
                NULLIF(trim(s.application_number), '') AS application_number,
                NULLIF(trim(s.shname), '') AS shareholder_name,
                NULLIF(trim(s.amountofshare), '') AS amountofshare_raw,
                NULLIF(trim(s.sconadd), '') AS contact_address,
                NULLIF(trim(s.countryname), '') AS countryname,
                NULLIF(trim(s.indcomp), '') AS individual_company,
                NULLIF(trim(s.nationality), '') AS nationality_raw,
                -- rowid sometimes comes as huge numbers or float-like strings; cast safely
                CASE
                    WHEN NULLIF(trim(s.rowid), '') IS NULL THEN 1
                    WHEN trim(s.rowid) ~ '^[0-9]+$' THEN LEAST(trim(s.rowid)::bigint, 2147483647)::int
                    WHEN trim(s.rowid) ~ '^[0-9]+\\.0$' THEN LEAST(replace(trim(s.rowid), '.0', '')::bigint, 2147483647)::int
                    ELSE 1
                END AS shareholder_order,
                NULLIF(trim(s.objectid), '') AS objectid_raw,
                NULLIF(trim(s.file_name), '') AS file_name
            FROM public.stage_ca_shareholders_raw s
        ),
        joined AS (
            SELECT
                sn.*,
                a.id AS application_id,
                CASE
                    WHEN sn.objectid_raw ~ '^[0-9]+$' THEN sn.objectid_raw::bigint
                    ELSE NULL
                END AS logic_doc_id,
                (sn.objectid_raw IS NOT NULL AND sn.objectid_raw !~ '^[0-9]+$') AS invalid_objectid,
                -- Nationality mapping: Excel provides an integer country id.
                -- If it is numeric, map it to countries.name using countries.countriesid.
                -- If it is already a name (non-numeric), keep it.
                COALESCE(
                    -- Map Excel nationality (integer) -> countries.countryname
                    c.countryname,
                    -- If nationality_raw is already a name (non-numeric), keep it
                    sn.nationality_raw
                ) AS nationality_name_mapped
            FROM s_norm sn
            LEFT JOIN public.ca_applications a
                ON a.application_number IS NOT DISTINCT FROM sn.application_number
            LEFT JOIN public.countries c
                ON (
                    -- only attempt mapping when nationality_raw is numeric-like
                    (sn.nationality_raw ~ '^[0-9]+$' OR sn.nationality_raw ~ '^[0-9]+\\.0$')
                    AND c.countriesid = (
                        CASE
                            WHEN sn.nationality_raw ~ '^[0-9]+$' THEN sn.nationality_raw::bigint
                            WHEN sn.nationality_raw ~ '^[0-9]+\\.0$' THEN replace(sn.nationality_raw, '.0', '')::bigint
                            ELSE NULL
                        END
                    )
                )
        ),
        eligible AS (
            SELECT j.*
            FROM joined j
            WHERE j.application_id IS NOT NULL
              AND j.shareholder_name IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.ca_shareholders x
                  WHERE x.application_id = j.application_id
                    AND x.shareholder_order IS NOT DISTINCT FROM j.shareholder_order
              )
        ),
        ins AS (
            INSERT INTO public.ca_shareholders (
                id,
                created_at,
                updated_at,
                shareholder_name,
                amount_of_shares,
                contact_address,
                country_of_residence,
                country_of_incorporation,
                nationality,
                individual_company,
                passport_or_nationalid,
                shareholder_order,
                application_id,
                logic_doc_id,
                file_name
            )
            SELECT
                e.id,
                now() AS created_at,
                now() AS updated_at,
                e.shareholder_name,
                CASE
                    WHEN e.amountofshare_raw ~ '^[0-9]+(\\.[0-9]+)?$' THEN e.amountofshare_raw::numeric
                    ELSE NULL
                END AS amount_of_shares,
                e.contact_address,
                e.countryname AS country_of_residence,
                e.countryname AS country_of_incorporation,
                e.nationality_name_mapped AS nationality,
                e.individual_company,
                NULL AS passport_or_nationalid,
                e.shareholder_order,
                e.application_id,
                e.logic_doc_id,
                e.file_name
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        ),
        stats AS (
            SELECT
                (SELECT COUNT(*) FROM joined) AS processed_rows,
                (SELECT COUNT(*) FROM ins) AS inserted_rows,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NULL) AS skipped_missing_application,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NOT NULL AND shareholder_name IS NULL) AS skipped_missing_shareholder_name,
                (SELECT COUNT(*) FROM joined j
                    WHERE j.application_id IS NOT NULL
                      AND j.shareholder_name IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM public.ca_shareholders x
                          WHERE x.application_id = j.application_id
                            AND x.shareholder_order IS NOT DISTINCT FROM j.shareholder_order
                      )
                ) AS skipped_already_exists,
                (SELECT COUNT(*) FROM joined WHERE invalid_objectid) AS invalid_objectid
        )
        SELECT
            processed_rows,
            inserted_rows,
            (processed_rows - inserted_rows) AS skipped_total,
            skipped_missing_application,
            skipped_missing_shareholder_name,
            skipped_already_exists,
            invalid_objectid
        FROM stats;
        """
    )

    _progress("shareholders:transform:sql")
    row = db.execute(transform_sql).first()
    if not row:
        # Shouldn't happen, but keep API response stable.
        row = (staged_total, 0, staged_total, 0, 0, 0, 0)

    processed_rows = int(row[0] or 0)
    inserted_rows = int(row[1] or 0)
    skipped_total = int(row[2] or 0)
    skipped_missing_application = int(row[3] or 0)
    skipped_missing_shareholder_name = int(row[4] or 0)
    skipped_already_exists = int(row[5] or 0)
    invalid_objectid = int(row[6] or 0)

    _progress(
        "shareholders:transform:done "
        f"processed={processed_rows} inserted={inserted_rows} skipped={skipped_total} "
        f"missing_app={skipped_missing_application} missing_name={skipped_missing_shareholder_name} already_exists={skipped_already_exists}"
    )

    result = {
        "total_rows_in_file": total_rows_in_file,
        "staged_total": staged_total,
        "processed_rows": processed_rows,
        "inserted_rows": inserted_rows,
        "skipped_total": skipped_total,
        "skipped_breakdown": {
            "missing_application": skipped_missing_application,
            "missing_shareholder_name": skipped_missing_shareholder_name,
            "already_exists": skipped_already_exists,
        },
        "diagnostics": {
            "invalid_objectid": invalid_objectid,
            "note": "Rows are skipped when application_number doesn't match ca_applications, shareholder_name is blank, or (application_id + shareholder_order) already exists.",
        },
    }

    # If nothing inserted, attach a small sample of rows for each skip reason
    # so it's easy to see what's wrong without querying the DB manually.
    if inserted_rows == 0 and staged_total > 0:
        try:
            result["skip_samples"] = _fetch_skip_samples(db, limit=10)
        except Exception as e:
            result["skip_samples"] = {"error": str(e)}

    return result


def _fetch_skip_samples(db: Any, *, limit: int = 10) -> dict:
    """Return small sample rows from staging to explain why inserts might be 0."""
    from sqlalchemy import text

    reg = db.execute(text("select to_regclass('public.stage_ca_shareholders_raw')")).scalar()
    if not reg:
        return {"note": "staging table public.stage_ca_shareholders_raw does not exist in this DB/session"}

    # Note: we keep this lightweight: only return a few rows for each skip reason.
    samples = {}

    samples["missing_application"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.shname, s.rowid
                FROM public.stage_ca_shareholders_raw s
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

    samples["missing_shareholder_name"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.shname, s.rowid
                FROM public.stage_ca_shareholders_raw s
                JOIN public.ca_applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                WHERE NULLIF(trim(s.shname), '') IS NULL
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
                SELECT s.source_row_no, s.application_number, s.shname, s.rowid
                FROM public.stage_ca_shareholders_raw s
                JOIN public.ca_applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                JOIN public.ca_shareholders x
                  ON x.application_id = a.id
                 AND x.shareholder_order IS NOT DISTINCT FROM COALESCE(NULLIF(trim(s.rowid), '')::int, 1)
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    return samples
