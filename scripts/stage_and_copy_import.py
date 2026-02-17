"""High-volume importer: Excel -> staging tables via COPY -> SQL transform.

Designed for remote Postgres + 500k rows.

This script is intentionally simple and explicit so it can be run as:
- a standalone script
- or called from a FastAPI endpoint/service

Key points:
- Uses COPY for staging inserts (few network round-trips)
- Uses SQL set-based inserts for final tables
- Only inserts documents when BOTH file_name and logic_doc_id are valid

NOTE:
This script requires direct access to the SQLAlchemy Session used in the app.
"""

from __future__ import annotations

import io
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import text

import logging

logger = logging.getLogger(__name__)


_EMPTY_MARKERS = {"", "nan", "nat", "none", "null"}


def _as_clean_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.lower() in _EMPTY_MARKERS:
        return None
    return s


def _parse_int_like(val: Any) -> Optional[int]:
    s = _as_clean_str(val)
    if s is None:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _convert_excel_date_to_iso(val: Any) -> Optional[str]:
    """Return YYYY-MM-DD string or None.

    Matches behavior in application_migrations_service but returns string
    because staging table keeps dates as text.
    """
    if val is None:
        return None

    # Pandas Timestamp/datetime
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y-%m-%d")
        except Exception:
            pass

    s = str(val).strip()
    if not s or s.lower() in _EMPTY_MARKERS:
        return None

    # Excel serial float dates
    try:
        f = float(s)
        if f > 1000:  # serial dates are large numbers
            # Excel serial date: day 0 is 1899-12-30 in pandas
            dt = pd.to_datetime(f, unit="D", origin="1899-12-30")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Already looks like date
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue

    return None


def _get_raw_conn(db: Any):
    """Get a DBAPI connection from a SQLAlchemy Session."""
    try:
        return db.connection().connection
    except Exception:
        return None


def _copy_dataframe_to_table(
    db: Any,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
):
    """COPY rows into table using CSV streamed over the DBAPI connection."""
    raw_conn = _get_raw_conn(db)
    if raw_conn is None or not hasattr(raw_conn, "cursor"):
        raise RuntimeError("COPY requires a DBAPI connection with cursor (psycopg2)")

    sio = io.StringIO()
    for r in rows:
        out = []
        for v in r:
            if v is None:
                out.append("")
            else:
                s = str(v).replace('"', '""')
                out.append(s)
        sio.write(",".join(f'"{x}"' for x in out) + "\n")
    sio.seek(0)

    cols_sql = ",".join(columns)
    copy_sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH CSV"

    cur = raw_conn.cursor()
    cur.copy_expert(copy_sql, sio)


def ensure_staging_schema(db: Any):
    sql_path = os.path.join(os.path.dirname(__file__), "..", "app", "migrations", "staging_schema.sql")
    sql_path = os.path.abspath(sql_path)
    with open(sql_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    # Some DBs/drivers don't allow CREATE TABLE inside a transaction block.
    # We try normal execute+commit first, then fall back to autocommit.
    try:
        db.execute(text(ddl))
        db.commit()
        return
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    raw_conn = _get_raw_conn(db)
    if raw_conn is None:
        raise
    prev = getattr(raw_conn, "autocommit", None)
    try:
        if prev is not None:
            raw_conn.autocommit = True
        cur = raw_conn.cursor()
        cur.execute(ddl)
    finally:
        try:
            if prev is not None:
                raw_conn.autocommit = prev
        except Exception:
            pass


def run_transform_into_final(db: Any):
    sql_path = os.path.join(os.path.dirname(__file__), "..", "app", "migrations", "transform_into_final.sql")
    sql_path = os.path.abspath(sql_path)
    with open(sql_path, "r", encoding="utf-8") as f:
        db.execute(text(f.read()))
    db.commit()


def truncate_staging(db: Any):
    # Be tolerant: first-run may not have staging tables yet.
    # We use DO blocks so missing tables don't error.
    db.execute(
        text(
            """
DO $$
BEGIN
    IF to_regclass('public.stage_ca_documents_raw') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_ca_documents_raw RESTART IDENTITY CASCADE';
    END IF;
    IF to_regclass('public.stage_ca_applications_raw') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_ca_applications_raw RESTART IDENTITY CASCADE';
    END IF;
END $$;
            """
        )
    )
    db.commit()


def stage_and_copy_import(
    db: Any,
    df: pd.DataFrame,
    attachments_spec: List[Tuple[str, Optional[str], str]],
    excel_to_stage: Dict[str, str],
    progress_cb=None,
    *,
    chunk_rows: int = 50000,
    truncate_first: bool = True,
):
    """Main pipeline.

    - Builds a "stage apps" frame with generated_id.
    - Builds a "stage docs" long table (rows for valid attachments only).
    - COPY both into staging.
    - Run SQL transform.

    For remote DB, chunk_rows=50k is usually a good balance.
    """

    def _progress(msg: str):
        logger.info("[staging-import] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress(f"start rows={len(df)} chunk_rows={chunk_rows}")

    # Ensure staging exists before any TRUNCATE/COPY.
    ensure_staging_schema(db)

    _progress("staging schema ensured")

    if truncate_first:
        truncate_staging(db)
        _progress("staging truncated")

    # Normalize df columns once
    df_cols = [str(c).strip() for c in df.columns]
    df.columns = df_cols

    # Build staging apps dataframe (only the columns we know)
    stage_cols = list(set(excel_to_stage.values()))
    # Keep deterministic order: generated_id first + source_row_no
    stage_cols = [c for c in stage_cols if c not in ("generated_id", "source_row_no")]
    stage_cols = ["generated_id", "source_row_no"] + sorted(stage_cols)

    # Filter to real staging columns (avoid COPY failing on unknown columns)
    stage_table_cols = {
        r[0]
        for r in db.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='stage_ca_applications_raw'
                """
            )
        ).fetchall()
    }
    stage_cols = [c for c in stage_cols if c in stage_table_cols]

    df_stage = pd.DataFrame(index=df.index)
    df_stage["generated_id"] = [uuid.uuid4() for _ in range(len(df))]
    df_stage["source_row_no"] = [int(i) + 2 for i in range(len(df))]  # +2 for header/1-index

    for src, dst in excel_to_stage.items():
        if dst not in stage_table_cols:
            # mapped destination doesn't exist in staging schema -> ignore
            continue
        if src in df.columns:
            df_stage[dst] = df[src]
        else:
            df_stage[dst] = None

    # Clean common text null markers
    for c in df_stage.columns:
        if c in ("generated_id", "source_row_no"):
            continue
        df_stage[c] = df_stage[c].apply(_as_clean_str)

    # Date columns: convert to ISO so transform.sql can cast reliably
    for dc in ("effective_date", "expire_date", "completed_at", "fire_valid_from", "fire_valid_to", "cover_note_start_date", "cover_note_end_date"):
        if dc in df_stage.columns:
            df_stage[dc] = df_stage[dc].apply(_convert_excel_date_to_iso)

    # Stage mappings raw fields
    # (these should be populated by caller mapping excel_to_stage)

    # Build contact-person staging rows (1 per application) - vectorized (fast)
    contact_rows: List[Tuple[Any, ...]] = []
    contact_cols = ["id", "application_generated_id", "contact_name", "email", "mobile_no", "title"]

    # Excel columns are: cemail, cmobile_no, title, contact_name
    if any(c in df.columns for c in ("cemail", "cmobile_no", "title", "contact_name")):
        contact_df = pd.DataFrame(index=df.index)
        contact_df["application_generated_id"] = df_stage["generated_id"]
        contact_df["email"] = df["cemail"] if "cemail" in df.columns else None
        contact_df["mobile_no"] = df["cmobile_no"] if "cmobile_no" in df.columns else None
        contact_df["title"] = df["title"] if "title" in df.columns else None
        contact_df["contact_name"] = df["contact_name"] if "contact_name" in df.columns else None

        for c in ("email", "mobile_no", "title", "contact_name"):
            contact_df[c] = contact_df[c].apply(_as_clean_str)

        contact_df = contact_df[
            contact_df[["email", "mobile_no", "title", "contact_name"]].notna().any(axis=1)
        ]
        if not contact_df.empty:
            contact_df["id"] = [uuid.uuid4() for _ in range(len(contact_df))]
            contact_df = contact_df[["id", "application_generated_id", "contact_name", "email", "mobile_no", "title"]]
            contact_rows = list(contact_df.itertuples(index=False, name=None))

    _progress(f"staged contact persons={len(contact_rows)}")

    # Now build documents rows.
    docs_rows: List[Tuple[Any, ...]] = []
    docs_cols = ["id", "application_generated_id", "document_name", "file_name", "documents_order", "logic_doc_id"]

    # Big speed win: if the sheet doesn't have any filename columns at all,
    # don't do the expensive per-row scan.
    df_col_set = set(df.columns)
    possible_filename_cols = [fname for _, fname, _ in attachments_spec if fname and fname in df_col_set]
    if not possible_filename_cols:
        _progress("no attachment filename columns found in Excel; skipping docs scan")
        docs_rows = []
        # continue to COPY apps/contacts and run transform
    else:
        _progress(f"attachment filename columns present={len(possible_filename_cols)}; scanning rows for valid docs")

        # Build per row attachments, only keep valid ones
        # This can be heavy for 500k rows; we only do it if filename columns exist.
        total = len(df)
        log_every = max(1, total // 20)  # ~5% increments
        for i in range(total):
            row = df.iloc[i]
            app_gen_id = df_stage.at[i, "generated_id"]
            order = 1
            for id_col, filename_col, label in attachments_spec:
                if filename_col is None or filename_col not in df_col_set:
                    continue
                if id_col is None or id_col not in df_col_set:
                    continue

                fname = _as_clean_str(row.get(filename_col))
                if not fname:
                    continue

                logic_id = _parse_int_like(row.get(id_col))
                if logic_id is None:
                    continue

                docs_rows.append((uuid.uuid4(), app_gen_id, label, fname, order, logic_id))
                order += 1

            if (i + 1) % log_every == 0 or (i + 1) == total:
                _progress(
                    f"scanned rows {i+1}/{total} ({((i+1)/total)*100:.0f}%), staged_docs_so_far={len(docs_rows)}"
                )

    # COPY in chunks to reduce memory and keep network streaming efficient
    # session speedups for remote COPY
    db.execute(text("SET synchronous_commit TO OFF"))

    _progress("COPY stage_ca_applications_raw begin")

    # COPY stage apps (use columns confirmed to exist in staging)
    app_copy_cols = [c for c in df_stage.columns if c in stage_table_cols]
    total_apps = len(df_stage)
    for start in range(0, total_apps, chunk_rows):
        chunk = df_stage.iloc[start : start + chunk_rows]
        rows = chunk[app_copy_cols].itertuples(index=False, name=None)
        _copy_dataframe_to_table(db, "public.stage_ca_applications_raw", app_copy_cols, rows)
        db.commit()
        done = min(start + chunk_rows, total_apps)
    _progress(f"COPY apps {done}/{total_apps} ({(done/total_apps)*100:.0f}%)")

    # COPY stage contacts (optional)
    if contact_rows:
        _progress("COPY stage_ca_contact_persons_raw begin")
        total_contacts = len(contact_rows)
        for start in range(0, total_contacts, chunk_rows * 10):
            chunk = contact_rows[start : start + (chunk_rows * 10)]
            _copy_dataframe_to_table(db, "public.stage_ca_contact_persons_raw", contact_cols, chunk)
            db.commit()
            done = min(start + (chunk_rows * 10), total_contacts)
            _progress(f"COPY contacts {done}/{total_contacts} ({(done/total_contacts)*100:.0f}%)")

    _progress(f"COPY stage_ca_documents_raw begin total_docs={len(docs_rows)}")

    # COPY stage docs
    # docs_rows is already compact, chunk it
    total_docs = len(docs_rows)
    doc_chunk = chunk_rows * 5
    for start in range(0, total_docs, doc_chunk):
        chunk = docs_rows[start : start + (chunk_rows * 5)]
        _copy_dataframe_to_table(db, "public.stage_ca_documents_raw", docs_cols, chunk)
        db.commit()
        done = min(start + doc_chunk, total_docs)
        if total_docs:
            _progress(f"COPY docs {done}/{total_docs} ({(done/total_docs)*100:.0f}%)")
        else:
            _progress("COPY docs skipped (0 docs)")

    # Transform into final
    _progress("transform into final begin")
    run_transform_into_final(db)
    _progress("transform into final done")

    # Make outcomes explicit (helps troubleshoot partial inserts)
    inserted_apps = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_applications_raw s
            JOIN public.ca_applications a
              ON a.id = s.generated_id
            """
        )
    ).scalar() or 0

    inserted_docs = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_documents_raw d
            JOIN public.ca_documents cd
              ON cd.id = d.id
            """
        )
    ).scalar() or 0

    inserted_contacts = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_contact_persons_raw c
            JOIN public.ca_contact_persons cp
              ON cp.id = c.id
            """
        )
    ).scalar() or 0

    # Diagnostics: why apps were skipped?
    # 1) Conflicts with existing rows in ca_applications (already in DB)
    skipped_due_to_existing_approval_no = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_applications_raw s
            WHERE NULLIF(s.approval_no, '') IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM public.ca_applications a
                WHERE a.approval_no IS NOT DISTINCT FROM NULLIF(s.approval_no, '')
              )
            """
        )
    ).scalar() or 0

    skipped_due_to_existing_application_number = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_applications_raw s
            WHERE NULLIF(s.application_number, '') IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM public.ca_applications a
                WHERE a.application_number IS NOT DISTINCT FROM NULLIF(s.application_number, '')
              )
            """
        )
    ).scalar() or 0

    # 2) Duplicates inside the staging batch itself (Excel duplicates)
    dup_approval_no_in_stage = db.execute(
        text(
            """
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT NULLIF(s.approval_no, '') AS k, COUNT(*) AS cnt
                FROM public.stage_ca_applications_raw s
                WHERE NULLIF(s.approval_no, '') IS NOT NULL
                GROUP BY NULLIF(s.approval_no, '')
                HAVING COUNT(*) > 1
            ) x
            """
        )
    ).scalar() or 0

    dup_application_number_in_stage = db.execute(
        text(
            """
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT NULLIF(s.application_number, '') AS k, COUNT(*) AS cnt
                FROM public.stage_ca_applications_raw s
                WHERE NULLIF(s.application_number, '') IS NOT NULL
                GROUP BY NULLIF(s.application_number, '')
                HAVING COUNT(*) > 1
            ) x
            """
        )
    ).scalar() or 0

    # Diagnostics for docs/contacts: how many staged rows point to skipped applications?
    staged_docs_for_skipped_apps = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_documents_raw d
            LEFT JOIN public.ca_applications a
              ON a.id = d.application_generated_id
            WHERE a.id IS NULL
            """
        )
    ).scalar() or 0

    staged_contacts_for_skipped_apps = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_contact_persons_raw c
            LEFT JOIN public.ca_applications a
              ON a.id = c.application_generated_id
            WHERE a.id IS NULL
            """
        )
    ).scalar() or 0

    staged_apps = int(len(df_stage))
    staged_docs = int(len(docs_rows))
    staged_contacts = int(len(contact_rows))
    skipped_apps = staged_apps - int(inserted_apps)

    _progress(
        f"summary: staged_apps={staged_apps}, inserted_apps={int(inserted_apps)}, skipped_apps={skipped_apps}; "
        f"staged_docs={staged_docs}, inserted_docs={int(inserted_docs)}; "
        f"staged_contacts={staged_contacts}, inserted_contacts={int(inserted_contacts)}"
    )

    _progress(
        "skip diagnostics: "
        f"existing_approval_no={int(skipped_due_to_existing_approval_no)}, "
        f"existing_application_number={int(skipped_due_to_existing_application_number)}, "
        f"dup_approval_no_in_stage={int(dup_approval_no_in_stage)}, "
        f"dup_application_number_in_stage={int(dup_application_number_in_stage)}, "
        f"staged_docs_for_skipped_apps={int(staged_docs_for_skipped_apps)}, "
        f"staged_contacts_for_skipped_apps={int(staged_contacts_for_skipped_apps)}"
    )

    return {
        "staged_app_rows": staged_apps,
        "inserted_app_rows": int(inserted_apps),
        "skipped_app_rows": int(skipped_apps),

        "skipped_due_to_existing_approval_no": int(skipped_due_to_existing_approval_no),
        "skipped_due_to_existing_application_number": int(skipped_due_to_existing_application_number),
        "dup_approval_no_in_stage": int(dup_approval_no_in_stage),
        "dup_application_number_in_stage": int(dup_application_number_in_stage),

        "staged_doc_rows": staged_docs,
        "inserted_doc_rows": int(inserted_docs),
        "staged_doc_rows_for_skipped_apps": int(staged_docs_for_skipped_apps),

        "staged_contact_rows": staged_contacts,
        "inserted_contact_rows": int(inserted_contacts),
        "staged_contact_rows_for_skipped_apps": int(staged_contacts_for_skipped_apps),
    }
