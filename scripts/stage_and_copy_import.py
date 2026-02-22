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
    # Some exports use placeholder tokens instead of blanks.
    # For example: old_parent_application_id may contain the literal 'papprefno'
    # meaning "no parent".
    if s.lower() in _EMPTY_MARKERS or s.lower() == "papprefno":
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

    Handles:
    - pandas Timestamp / datetime objects  (before or after _as_clean_str)
    - YYYY-MM-DD HH:MM:SS  (pandas stringifies Timestamps this way)
    - YYYY-MM-DD
    - DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY, YYYY/MM/DD
    - Excel serial float (e.g. 44927.0)
    """
    if val is None:
        return None

    # Pandas Timestamp / datetime — handle BEFORE converting to string
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y-%m-%d")
        except Exception:
            pass

    s = str(val).strip()
    if not s or s.lower() in _EMPTY_MARKERS:
        return None

    # Excel serial float dates (e.g. 43979.0 → 2020-06-15)
    try:
        f = float(s)
        if f > 1000:  # serial dates are large numbers; guards against tiny ints
            dt = pd.to_datetime(f, unit="D", origin="1899-12-30")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Try all common string formats (include timestamp variants so
    # _as_clean_str("2022-11-30 00:00:00") still parses correctly)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",   # pandas Timestamp stringified → "2022-11-30 00:00:00"
        "%Y-%m-%d %H:%M:%S.%f",# with microseconds
        "%Y-%m-%dT%H:%M:%S",   # ISO-8601
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
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
        out_fields: List[str] = []
        for v in r:
            if v is None:
                # Important: emit an *unquoted empty field* for NULL.
                # With COPY ... NULL '' (below) this becomes a real SQL NULL.
                out_fields.append("")
            else:
                s = str(v).replace('"', '""')
                out_fields.append(f'"{s}"')
        sio.write(",".join(out_fields) + "\n")
    sio.seek(0)

    cols_sql = ",".join(columns)
    # We keep CSV and quote non-NULL fields. NULLs are represented by empty *unquoted* fields.
    # Setting NULL '' instructs Postgres to treat those as SQL NULL.
    copy_sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH CSV NULL ''"

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


def _drain_psycopg_notices(db: Any) -> List[str]:
    """Collect and clear server NOTICE messages (psycopg2).

    We use this to return table-level insert counts emitted by transform SQL
    (via RAISE NOTICE) without printing massive SQL errors.
    """

    try:
        raw_conn = _get_raw_conn(db)
        if raw_conn is None:
            return []
        notices = list(getattr(raw_conn, "notices", []) or [])
        # Clear buffer so the next run doesn't duplicate.
        # Note: we only call this at controlled points (start of run, and after transform).
        try:
            raw_conn.notices.clear()
        except Exception:
            # Some drivers expose notices as a plain list.
            setattr(raw_conn, "notices", [])
        # psycopg2 includes trailing newlines; normalize.
        return [n.strip() for n in notices if str(n).strip()]
    except Exception:
        return []


def truncate_staging(db: Any):
    # Be tolerant: first-run may not have staging tables yet.
    # We use DO blocks so missing tables don't error.
    db.execute(
        text(
            """
DO $$
BEGIN
    -- Truncate the parent table first; CASCADE clears dependent staging tables.
    IF to_regclass('public.stage_ca_applications_raw') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_ca_applications_raw RESTART IDENTITY CASCADE';
    END IF;
    -- Be explicit as well (safe if CASCADE already cleared them).
    IF to_regclass('public.stage_ca_contact_persons_raw') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_ca_contact_persons_raw RESTART IDENTITY CASCADE';
    END IF;
    IF to_regclass('public.stage_ca_documents_raw') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_ca_documents_raw RESTART IDENTITY CASCADE';
    END IF;
    IF to_regclass('public.stage_categories') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.stage_categories RESTART IDENTITY CASCADE';
    END IF;
END $$;
            """
        )
    )
    db.commit()


def drop_staging_schema(db: Any):
    """Drop staging tables.

    Use this only when you've changed the staging schema (added/renamed columns)
    and want to guarantee the DB matches the current staging_schema.sql.

    For normal runs, truncate_staging() is faster and safer.
    """
    db.execute(
        text(
            """
DO $$
BEGIN
    IF to_regclass('public.stage_ca_documents_raw') IS NOT NULL THEN
        EXECUTE 'DROP TABLE public.stage_ca_documents_raw CASCADE';
    END IF;
    IF to_regclass('public.stage_ca_contact_persons_raw') IS NOT NULL THEN
        EXECUTE 'DROP TABLE public.stage_ca_contact_persons_raw CASCADE';
    END IF;
    IF to_regclass('public.stage_ca_shareholders_raw') IS NOT NULL THEN
        EXECUTE 'DROP TABLE public.stage_ca_shareholders_raw CASCADE';
    END IF;
    IF to_regclass('public.stage_categories') IS NOT NULL THEN
        EXECUTE 'DROP TABLE public.stage_categories CASCADE';
    END IF;
    IF to_regclass('public.stage_ca_applications_raw') IS NOT NULL THEN
        EXECUTE 'DROP TABLE public.stage_ca_applications_raw CASCADE';
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
    drop_and_recreate_staging: bool = True,   # always drop+recreate staging on every import
    sector_name: str = "PETROLEUM"
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

    # Load region/district/ward dictionaries (CSV-backed) so we can store names in staging.
    # Requirement: Only application-level region/district/ward should be mapped.
    # Fire delimitations (fire_region/fire_district/fire_ward) must NOT be mapped.
    #
    # Import _normalize_numeric_string once here so it is in scope for ALL helper
    # functions below (_build_normalized_map AND _map_region/_map_district/_map_ward).
    try:
        from app.services.application_migrations_service import _normalize_numeric_string
    except Exception as _e:
        logger.warning("[staging-import] could not import _normalize_numeric_string: %s", _e)
        def _normalize_numeric_string(val: str) -> str:  # type: ignore[misc]
            return str(val).strip() if val is not None else val

    def _build_normalized_map(source: Dict[str, str]) -> Dict[str, str]:
        """Build a lookup dict from id-to-name CSV map.

        Adds ALL of these key variants for every entry so pandas float strings
        (e.g. '1553779224293.0') always resolve:
          - raw CSV key as-is                  e.g. '1553779224293'
          - normalized integer string           e.g. '1553779224293'   (same here)
          - float-string variant                e.g. '1553779224293.0'
        """
        nm: Dict[str, str] = {}
        for k, v in (source or {}).items():
            ks = str(k).strip()
            if not ks:
                continue
            nm[ks] = v
            # Normalized integer string (handles scientific notation from other sources)
            try:
                nk = _normalize_numeric_string(ks)
                if nk and nk != ks:
                    nm[nk] = v
            except Exception:
                pass
            # Float-string variant: pandas often reads integer Excel cells as '12345.0'
            # Add that key explicitly so direct dict lookup succeeds without needing
            # _normalize_numeric_string at map time.
            try:
                float_key = f"{float(ks):.1f}"  # e.g. '1553779224293.0'
                if float_key not in nm:
                    nm[float_key] = v
                # Also add without the .0 suffix (already added as ks above, but be safe)
                int_key = str(int(float(ks)))
                if int_key not in nm:
                    nm[int_key] = v
            except (ValueError, OverflowError):
                pass
        return nm

    try:
        from app.services.application_migrations_service import region_map_csv as _region_csv
        norm_region_map = _build_normalized_map(_region_csv)
        logger.info("[staging-import] region map loaded: %d entries", len(norm_region_map))
    except Exception as _e:
        logger.warning("[staging-import] region map failed to load: %s", _e)
        norm_region_map = {}

    try:
        from app.services.application_migrations_service import district_map_csv as _district_csv
        norm_district_map = _build_normalized_map(_district_csv)
        logger.info("[staging-import] district map loaded: %d entries", len(norm_district_map))
    except Exception as _e:
        logger.warning("[staging-import] district map failed to load: %s", _e)
        norm_district_map = {}

    try:
        from app.services.application_migrations_service import ward_map_csv as _ward_csv
        norm_ward_map = _build_normalized_map(_ward_csv)
        logger.info("[staging-import] ward map loaded: %d entries", len(norm_ward_map))
    except Exception as _e:
        logger.warning("[staging-import] ward map failed to load: %s", _e)
        norm_ward_map = {}


    # Clear any previous server-side NOTICE buffer early.
    _drain_psycopg_notices(db)

    # Always drop and recreate staging tables on every import.
    # This guarantees:
    #   - no stale rows from a previous run
    #   - schema always matches current staging_schema.sql (no column drift)
    #   - no COPY conflicts from old data
    _progress("drop+recreate staging schema (every import)")
    drop_staging_schema(db)
    ensure_staging_schema(db)
    _progress("staging schema recreated")

    # ── Schema guard: ensure applications.completed_at exists ────────────────
    # This is needed when the live DB was provisioned before align_live_schema
    # added the column.  Safe to re-run (ADD COLUMN IF NOT EXISTS is idempotent).
    try:
        db.execute(text("""
            ALTER TABLE IF EXISTS public.applications
                ADD COLUMN IF NOT EXISTS completed_at timestamp NULL
        """))
        db.commit()
    except Exception as _sg_err:
        logger.warning("[staging-import] schema guard completed_at skipped: %s", _sg_err)
        try:
            db.rollback()
        except Exception:
            pass

    # Normalize df columns: strip whitespace AND lowercase so header matching is robust.
    # e.g. "FControlNo" matches "fcontrolno", "Region " matches "region", etc.
    df_cols_original = [str(c) for c in df.columns]
    df_cols_normalized = [str(c).strip().lower() for c in df.columns]
    df.columns = df_cols_normalized

    # Also normalize the keys of excel_to_stage so all lookups are case/space-insensitive.
    excel_to_stage = {str(k).strip().lower(): v for k, v in excel_to_stage.items()}

    # Log the original→normalized header pairs that actually changed (for transparency).
    changed_headers = [(o, n) for o, n in zip(df_cols_original, df_cols_normalized) if o != n]
    if changed_headers:
        _progress(f"diagnostic: normalized {len(changed_headers)} excel headers (strip+lower): {changed_headers[:20]}")

    # Diagnostics: identify mapped Excel columns that are missing in this file.
    # Missing source columns are the most common reason staging ends up with NULLs.
    expected_excel_cols = sorted({k for k in excel_to_stage.keys()})
    missing_excel_cols = [c for c in expected_excel_cols if c not in df.columns]
    if missing_excel_cols:
        _progress(f"diagnostic: excel missing mapped columns count={len(missing_excel_cols)}")
        _progress(f"diagnostic: excel missing mapped columns sample={missing_excel_cols[:50]}")
    else:
        _progress(f"diagnostic: all {len(expected_excel_cols)} mapped excel columns found in file")

    # Business rule: only migrate approved applications.
    # If approval_no is blank/null in Excel, skip the row entirely (do not stage, do not scan attachments).
    if "approval_no" in df.columns:
        _progress("filter: dropping rows with empty approval_no")
        before = len(df)
        approval_series = df["approval_no"].apply(_as_clean_str)
        df = df[approval_series.notna()].copy()
        _progress(f"filter: approval_no not null kept={len(df)}/{before}")

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

    # IMPORTANT: create all staging columns up-front.
    # Without this, any destination column that is missing from excel_to_stage
    # (or missing in a particular Excel file) might not exist on df_stage at all,
    # causing it to be omitted from COPY and end up NULL downstream.
    for c in stage_cols:
        if c in ("generated_id", "source_row_no"):
            continue
        if c not in df_stage.columns:
            df_stage[c] = None

    for src, dst in excel_to_stage.items():
        if dst not in stage_table_cols:
            # mapped destination doesn't exist in staging schema -> ignore
            continue
        if src in df.columns:
            # Only set if destination is still empty (first alias match wins).
            # This prevents a later absent alias from overwriting a value already
            # populated by an earlier alias key (e.g. approval_no set by
            # 'approval_no', then wiped by 'approvalno' which is absent).
            if dst not in df_stage.columns or df_stage[dst].isna().all():
                df_stage[dst] = df[src]
        # If src is absent from df, leave dst as-is (already initialised to None above).

    # ── Normalise date columns to YYYY-MM-DD BEFORE _as_clean_str ────────────
    # pandas keeps Excel date cells as Timestamp objects; _as_clean_str would
    # turn them into "2022-11-30 00:00:00" which the later apply() block used
    # to miss.  Running _convert_excel_date_to_iso first (on the raw Timestamps)
    # guarantees we always store clean ISO strings in the staging table.
    _DATE_STAGING_COLS = (
        "effective_date", "expire_date", "completed_at",
        "fire_valid_from", "fire_valid_to",
        "cover_note_start_date", "cover_note_end_date",
    )
    for dc in _DATE_STAGING_COLS:
        if dc in df_stage.columns:
            df_stage[dc] = df_stage[dc].apply(_convert_excel_date_to_iso)

    # Clean common text null markers
    for c in df_stage.columns:
        if c in ("generated_id", "source_row_no"):
            continue
        df_stage[c] = df_stage[c].apply(_as_clean_str)

    # Lowercase username in the DataFrame before COPY so mixed-case duplicates
    # (e.g. ALLY.GANZO / Ally.Ganzo / ally.ganzo) are already normalised when
    # they land in the staging table — no post-COPY UPDATE needed.
    if "username" in df_stage.columns:
        df_stage["username"] = df_stage["username"].apply(
            lambda v: v.lower().strip() if isinstance(v, str) and v.strip() else None
        )

    # Normalise integer-like float strings in location columns.
    # Pandas reads integer Excel cells as floats, so str() gives '1553779224293.0'.
    # Strip the '.0' suffix so the value is a plain integer string '1553779224293'
    # before it hits the CSV map lookup.  Only the app-level columns are affected;
    # fire_* delimitations are left untouched (they are text names, not IDs).
    def _strip_dot_zero(v: Any) -> Any:
        """Convert '12345.0' -> '12345', leave everything else unchanged."""
        if v is None:
            return None
        s = str(v).strip()
        if s.endswith(".0"):
            try:
                return str(int(float(s)))
            except (ValueError, OverflowError):
                pass
        return v

    for _loc_col in ("region", "district", "ward"):
        if _loc_col in df_stage.columns:
            df_stage[_loc_col] = df_stage[_loc_col].apply(_strip_dot_zero)

    # Map application-level region/district/ward IDs -> names (store names in staging).
    # IMPORTANT: do not map fire_* delimitations.
    # The norm_*_map dicts already contain all key variants:
    #   - '1553779224293'    (integer string, as in CSV)
    #   - '1553779224293.0'  (float string, how pandas reads integer Excel cells)
    # So a simple dict.get() is sufficient — no _normalize_numeric_string needed at lookup time.
    def _map_value(v: Any, lookup: Dict[str, str]) -> Any:
        """Return name from lookup, or original value if not found."""
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in _EMPTY_MARKERS:
            return None
        # Direct lookup (handles '1553779224293' and '1553779224293.0')
        if s in lookup:
            return lookup[s]
        # Last-resort: normalise and retry (scientific notation, etc.)
        try:
            nk = _normalize_numeric_string(s)
            if nk and nk != s and nk in lookup:
                return lookup[nk]
        except Exception:
            pass
        return s  # return unchanged if not in map

    try:
        if "region" in df_stage.columns and norm_region_map:
            before_unique = df_stage["region"].nunique()
            df_stage["region"] = df_stage["region"].apply(lambda v: _map_value(v, norm_region_map))
            after_unique = df_stage["region"].nunique()
            mapped_count = int((df_stage["region"].notna()).sum())
            logger.info("[staging-import] region mapped: %d unique values before=%d after=%d",
                        mapped_count, before_unique, after_unique)

        if "district" in df_stage.columns and norm_district_map:
            before_unique = df_stage["district"].nunique()
            df_stage["district"] = df_stage["district"].apply(lambda v: _map_value(v, norm_district_map))
            after_unique = df_stage["district"].nunique()
            mapped_count = int((df_stage["district"].notna()).sum())
            logger.info("[staging-import] district mapped: %d unique values before=%d after=%d",
                        mapped_count, before_unique, after_unique)

        if "ward" in df_stage.columns and norm_ward_map:
            before_unique = df_stage["ward"].nunique()
            df_stage["ward"] = df_stage["ward"].apply(lambda v: _map_value(v, norm_ward_map))
            after_unique = df_stage["ward"].nunique()
            mapped_count = int((df_stage["ward"].notna()).sum())
            logger.info("[staging-import] ward mapped: %d unique values before=%d after=%d",
                        mapped_count, before_unique, after_unique)

        # Log a few sample values so we can confirm names (not IDs) are staged.
        for col in ("region", "district", "ward"):
            if col in df_stage.columns:
                samples = df_stage[col].dropna().unique()[:5].tolist()
                logger.info("[staging-import] %s sample staged values: %s", col, samples)

    except Exception:
        logger.exception("location mapping: failed to map region/district/ward")


    # Diagnostics: report staged fill-rate for mapped staging columns.
    # This helps detect where values are being turned into NULL by cleaning.
    try:
        mapped_stage_cols = sorted({dst for dst in excel_to_stage.values() if dst in df_stage.columns})
        if mapped_stage_cols:
            fill_stats = []
            for c in mapped_stage_cols:
                non_null = int(df_stage[c].notna().sum())
                nulls = int(df_stage[c].isna().sum())
                fill_stats.append((nulls, non_null, c))
            fill_stats.sort(reverse=True)  # most nulls first
            _progress("diagnostic: staging null/non-null counts (top nulls first):")
            for nulls, non_null, c in fill_stats[:30]:
                _progress(f"  {c}: null={nulls} non_null={non_null}")
    except Exception:
        logger.exception("diagnostic: failed to compute staging fill rates")

    # Diagnostic: print one representative staged row to help spot where data disappears.
    # We prefer a row that has fire/insurance columns present (or at least the src row no)
    # so the user can cross-check directly in Excel.
    try:
        debug_cols = [
            c
            for c in (
                "source_row_no",
                "approval_no",
                "application_number",
                "region",
                "district",
                "ward",
                "fire_certificate_control_number",
                "cover_note_number",
                "cover_note_ref_no",
                "insurance_ref_no",
            )
            if c in df_stage.columns
        ]

        def _looks_numeric_id(x: Any) -> bool:
            s = _as_clean_str(x)
            if s is None:
                return False
            return bool(pd.Series([s]).astype(str).str.match(r"^[0-9]{10,}$").iloc[0])

        candidate = df_stage
        if {"region", "district", "ward"}.issubset(df_stage.columns):
            mask = (
                df_stage["region"].apply(_looks_numeric_id)
                | df_stage["district"].apply(_looks_numeric_id)
                | df_stage["ward"].apply(_looks_numeric_id)
            )
            if mask.any():
                candidate = df_stage[mask]

        if not candidate.empty:
            sample = candidate.iloc[0][debug_cols].to_dict()
            _progress(f"diagnostic: sample staged row={sample}")
    except Exception:
        logger.exception("diagnostic: failed to print sample staged row")

    # NOTE: date columns were already normalised to YYYY-MM-DD above (before
    # _as_clean_str), so no second pass is needed here.

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
        # After filtering, df/df_stage might not have a 0..N-1 index.
        # Use positional indexing to avoid KeyError on .at[0, ...].
        gen_ids = df_stage["generated_id"].to_numpy()
        log_every = max(1, total // 20)  # ~5% increments
        for i in range(total):
            row = df.iloc[i]
            app_gen_id = gen_ids[i]
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

    # Stage Categories
    if "license_type" in df.columns:
        valid_cats = df["license_type"].dropna().astype(str).str.strip()
        # Filter out empty strings
        valid_cats = valid_cats[valid_cats != ""]
        unique_cats = valid_cats.unique()
        
        cat_rows = []
        for c in unique_cats:
            cat_rows.append((c, sector_name))

        if cat_rows:
            _copy_dataframe_to_table(db, "public.stage_categories", ["name", "sector_name"], cat_rows)
            db.commit()
            _progress(f"COPY categories {len(cat_rows)} distinct license types")

    # ── Provision users from staged usernames ──────────────────────────────────
    # Must run BEFORE transform so that created_by backfill inside the transform
    # (or subsequent backfill_created_by_from_username) can resolve UUIDs.
    # Skip entirely if the users/roles tables are unreachable (FDW not configured).
    _progress("provisioning users begin")
    _inserted_users = 0
    _skipped_users = 0
    _inserted_user_roles = 0
    _skipped_user_roles = 0
    try:
        db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        db.commit()
    except Exception:
        pass

    # Quick reachability check: if public.users is an FDW table and the remote
    # server is unreachable, skip all three provisioning steps rather than
    # logging three scary errors.
    _users_reachable = False
    try:
        db.execute(text("SELECT 1 FROM public.users LIMIT 1"))
        _users_reachable = True
    except Exception as _reach_err:
        logger.info(
            "User provisioning skipped — public.users unreachable (FDW not configured "
            "for this host). To fix: add '10.1.8.157/32' to pg_hba.conf on 10.1.8.144. "
            "Error: %s", _reach_err
        )
        try:
            db.rollback()
        except Exception:
            pass

    if not _users_reachable:
        _progress("provisioning users skipped (FDW unreachable)")
    else:
        # Normalize usernames to lowercase in staging table before any user insert
        # so that mixed-case duplicates (e.g. Ally.Ganzo / ALLY.GANZO) collapse to one row.
        try:
            db.execute(text("""
                UPDATE public.stage_ca_applications_raw
                SET username = lower(trim(username))
                WHERE username IS NOT NULL
                  AND username <> lower(trim(username))
            """))
            db.commit()
        except Exception as _ln:
            logger.warning("Could not lowercase staging usernames: %s", _ln)
            try:
                db.rollback()
            except Exception:
                pass

        _total_usernames = 0

        try:
            _ur = db.execute(text("""
                WITH u AS (
                    SELECT DISTINCT lower(TRIM(username)) AS username
                    FROM public.stage_ca_applications_raw
                    WHERE NULLIF(TRIM(username), '') IS NOT NULL
                )
                INSERT INTO public.users (
                    id, full_name, username, password_hash, status,
                    phone_number, email_address, user_category,
                    account_type, auth_mode, failed_attempts,
                    is_first_login, deleted, created_at, updated_at
                )
                SELECT
                    gen_random_uuid(), u.username, u.username, '',
                    'ACTIVE', NULL, NULL, 'EXTERNAL', 'INDIVIDUAL', 'DB',
                    0, false, false, now(), now()
                FROM u
                WHERE NOT EXISTS (
                    SELECT 1 FROM public.users eu
                    WHERE lower(trim(eu.username)) = u.username
                )
            """))
            db.commit()
            _inserted_users = _ur.rowcount or 0
            # Distinct usernames in staging minus newly inserted = already existed
            _total_usernames = db.execute(text(
                "SELECT COUNT(DISTINCT lower(TRIM(username))) FROM public.stage_ca_applications_raw "
                "WHERE NULLIF(TRIM(username), '') IS NOT NULL"
            )).scalar() or 0
            _skipped_users = max(0, int(_total_usernames) - int(_inserted_users))
            _progress(f"provisioning users done: inserted={_inserted_users}, already_existed={_skipped_users}")
        except Exception as _ue:
            logger.warning("Provision users failed: %s", _ue)
            try:
                db.rollback()
            except Exception:
                pass

        # Look up APPLICANT ROLE id without scanning public.roles.
        # public.roles is a FDW foreign table whose remote name differs on the
        # remote side — any scan fails with "relation public.role does not exist".
        # We pick the role_id from an existing user_roles row (best-effort).
        # If no row exists yet, role assignment is skipped gracefully.
        _applicant_role_id = None
        try:
            # Strategy 1: pick any role_id already in user_roles — fastest, no FDW scan.
            _ur_row = db.execute(text(
                "SELECT role_id FROM public.user_roles LIMIT 1"
            )).fetchone()
            if _ur_row:
                _applicant_role_id = str(_ur_row[0])
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        if not _applicant_role_id:
            # public.roles FDW always fails (remote table is named public.role).
            # We cannot safely scan roles — skip role assignment silently.
            logger.info("APPLICANT ROLE id not found from user_roles; role assignment skipped (FDW limitation)")
            _skipped_user_roles = int(_total_usernames)

        # Assign APPLICANT ROLE to all staged users that don't have it yet.
        # Remote user_roles FDW table only exposes (user_id, role_id).
        if _applicant_role_id:
            try:
                _rr = db.execute(text("""
                    WITH u AS (
                        SELECT DISTINCT NULLIF(TRIM(username), '') AS username
                        FROM public.stage_ca_applications_raw
                        WHERE NULLIF(TRIM(username), '') IS NOT NULL
                    )
                    INSERT INTO public.user_roles (user_id, role_id)
                    SELECT usr.id, :role_id
                    FROM u
                    JOIN public.users usr ON lower(trim(usr.username)) = lower(trim(u.username))
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.user_roles ur2
                        WHERE ur2.user_id = usr.id AND ur2.role_id = :role_id
                    )
                """), {"role_id": _applicant_role_id})
                db.commit()
                _inserted_user_roles = _rr.rowcount or 0
                _skipped_user_roles = max(0, int(_total_usernames) - int(_inserted_user_roles))
                _progress(f"role assignment done: inserted={_inserted_user_roles}, already_had_role={_skipped_user_roles}")
            except Exception as _ure:
                logger.warning("Role assignment skipped (non-fatal): %s", _ure)
                _skipped_user_roles = int(_total_usernames)
                try:
                    db.rollback()
                except Exception:
                    pass

    # Transform into final
    # Ensure uq_certificates_application_number exists before running the
    # transform, which now uses ON CONFLICT (application_number) DO UPDATE.
    _progress("ensure certificates unique constraint begin")
    try:
        db.execute(text("""
            DO $$
            DECLARE
                _rec               RECORD;
                _schema            text;
                _cert_apprv_attnum  smallint;
                _cert_appnum_attnum smallint;
            BEGIN
                FOR _schema IN
                    SELECT nspname FROM pg_namespace
                    WHERE  nspname IN ('public', 'align_live')
                    ORDER BY nspname
                LOOP
                    IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                                   WHERE n.nspname=_schema AND c.relname='certificates') THEN
                        CONTINUE;
                    END IF;

                    SELECT attnum INTO _cert_apprv_attnum
                    FROM pg_attribute
                    WHERE attrelid = (SELECT c.oid FROM pg_class c
                                      JOIN pg_namespace n ON n.oid=c.relnamespace
                                      WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'approval_no';

                    SELECT attnum INTO _cert_appnum_attnum
                    FROM pg_attribute
                    WHERE attrelid = (SELECT c.oid FROM pg_class c
                                      JOIN pg_namespace n ON n.oid=c.relnamespace
                                      WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'application_number';

                    -- Drop any existing unique that mentions approval_no (blocks the new one)
                    IF _cert_apprv_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname FROM pg_constraint con
                            JOIN pg_class cls ON cls.oid = con.conrelid
                            JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE con.contype = 'u'
                              AND nsp.nspname = _schema AND cls.relname = 'certificates'
                              AND _cert_apprv_attnum = ANY(con.conkey)
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                        END LOOP;
                    END IF;

                    -- Drop any existing unique on application_number (we re-add cleanly below)
                    IF _cert_appnum_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname FROM pg_constraint con
                            JOIN pg_class cls ON cls.oid = con.conrelid
                            JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE con.contype = 'u'
                              AND nsp.nspname = _schema AND cls.relname = 'certificates'
                              AND _cert_appnum_attnum = ANY(con.conkey)
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                        END LOOP;
                    END IF;

                    -- Add UNIQUE (application_number) if not present
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint con
                        JOIN pg_class cls ON cls.oid = con.conrelid
                        JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                        WHERE con.contype = 'u'
                          AND nsp.nspname = _schema AND cls.relname = 'certificates'
                          AND con.conname = 'uq_certificates_application_number'
                    ) THEN
                        -- Dedup first so constraint creation succeeds
                        EXECUTE format(
                            'DELETE FROM %I.certificates
                             WHERE id IN (
                                 SELECT id FROM (
                                     SELECT id,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY application_number
                                                ORDER BY updated_at DESC NULLS LAST,
                                                         created_at  DESC NULLS LAST, id
                                            ) AS rn
                                     FROM %I.certificates
                                     WHERE application_number IS NOT NULL
                                 ) ranked WHERE rn > 1
                             )',
                            _schema, _schema
                        );
                        EXECUTE format(
                            'ALTER TABLE %I.certificates
                             ADD CONSTRAINT uq_certificates_application_number
                             UNIQUE (application_number)',
                            _schema
                        );
                        RAISE NOTICE '[staging-import] Added uq_certificates_application_number on %.certificates', _schema;
                    END IF;

                END LOOP;
            END $$;
        """))
        db.commit()
        _progress("ensure certificates unique constraint done")
    except Exception as _uce:
        logger.warning("ensure certificates unique constraint failed (non-fatal): %s", _uce)
        try:
            db.rollback()
        except Exception:
            pass

    _progress("transform into final begin")
    run_transform_into_final(db)
    _progress("transform into final done")

    # Collect server notices after the commit so all DO blocks have flushed their RAISE NOTICE.
    transform_notices = _drain_psycopg_notices(db)
    inserted_from_transform: Dict[str, int] = {}
    for n in transform_notices:
        # example: "NOTICE:  [staging-transform] inserted apps=123"
        if "[staging-transform]" not in n or "inserted" not in n or "=" not in n:
            continue
        try:
            left, right = n.rsplit("=", 1)
            cnt = int(str(right).strip())
            key = left.split("inserted", 1)[1].strip()
            key = key.replace("NOTICE:", "").replace(":", "").strip()
            inserted_from_transform[key] = cnt
        except Exception:
            continue

    # Make outcomes explicit (helps troubleshoot partial inserts)
    inserted_apps = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_applications_raw s
            JOIN public.applications a
              ON a.id = s.generated_id
            """
        )
    ).scalar() or 0

    inserted_docs = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_documents_raw d
                        JOIN public.application_sector_details asd
                            ON asd.id = d.application_generated_id
                        JOIN public.documents cd
                            ON cd.id = d.id
                         AND cd.application_sector_detail_id = asd.id
            """
        )
    ).scalar() or 0

    inserted_contacts = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_contact_persons_raw c
                        JOIN public.application_sector_details asd
                            ON asd.id = c.application_generated_id
                        JOIN public.contact_persons cp
                            ON cp.id = c.id
                         AND cp.app_sector_detail_id = asd.id
            """
        )
    ).scalar() or 0

    # Diagnostics: why apps were skipped?
    # 1) Conflicts with existing rows in applications (already in DB)
    skipped_due_to_existing_approval_no = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_applications_raw s
            WHERE NULLIF(s.approval_no, '') IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM public.applications a
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
                                SELECT 1 FROM public.applications a
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
            LEFT JOIN public.application_sector_details sd
              ON sd.id = d.application_generated_id
            WHERE sd.id IS NULL
            """
        )
    ).scalar() or 0

    staged_contacts_for_skipped_apps = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM public.stage_ca_contact_persons_raw c
            LEFT JOIN public.application_sector_details sd
              ON sd.id = c.application_generated_id
            WHERE sd.id IS NULL
            """
        )
    ).scalar() or 0

    staged_apps = int(len(df_stage))
    staged_docs = int(len(docs_rows))
    staged_contacts = int(len(contact_rows))
    skipped_apps = staged_apps - int(inserted_apps)

    tables_summary = {
        "applications": {
            "staged": staged_apps,
            "inserted": int(inserted_apps),
            "skipped": int(skipped_apps),
        },
        "documents": {
            "staged": staged_docs,
            "inserted": int(inserted_docs),
            "skipped": int(staged_docs - int(inserted_docs)),
        },
        "contact_persons": {
            "staged": staged_contacts,
            "inserted": int(inserted_contacts),
            "skipped": int(staged_contacts - int(inserted_contacts)),
        },
    }

    # Merge in transform-level metrics for tables that aren't directly counted above.
    # Example keys from SQL notices: "application_sector_details", "certificates".
    for k, v in inserted_from_transform.items():
        tables_summary.setdefault(k, {})
        # Don't overwrite if we already computed inserted.
        tables_summary[k].setdefault("inserted", v)

    _progress(
        f"summary: staged_apps={staged_apps}, inserted_apps={int(inserted_apps)}, skipped_apps={skipped_apps}; "
        f"staged_docs={staged_docs}, inserted_docs={int(inserted_docs)}; "
        f"staged_contacts={staged_contacts}, inserted_contacts={int(inserted_contacts)}; "
        f"inserted_users={_inserted_users}, skipped_users={_skipped_users}; "
        f"inserted_user_roles={_inserted_user_roles}, skipped_user_roles={_skipped_user_roles}"
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
        "tables": tables_summary,
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

        "inserted_users": int(_inserted_users),
        "skipped_users": int(_skipped_users),
        "inserted_user_roles": int(_inserted_user_roles),
        "skipped_user_roles": int(_skipped_user_roles),

        # Extra: what the SQL transform itself reported (per-table inserted counts).
        "transform_inserted": inserted_from_transform,
        # Full NOTICE list can be useful for debugging but is kept in the response (not logs).
        "transform_notices": transform_notices,
    }
