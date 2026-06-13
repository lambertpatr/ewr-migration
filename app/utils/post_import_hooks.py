"""Post-import hooks: align live schema + backfill created_by.

These helpers are meant to be called at the end of every migration import
(application migrations, electrical installations, etc.) so the DB is
always left in a consistent state without requiring manual admin-tools calls.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# ── Path to the align-schema DDL script ─────────────────────────────────────
_ALIGN_SCHEMA_SQL_PATH = (
    Path(__file__).resolve().parents[1] / "migrations" / "align_live_schema_eservice_construction.sql"
)


def run_align_live_schema(db: Any, *, progress_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Execute the align_live_schema DDL script against *db*.

    The script may contain its own ``BEGIN / COMMIT`` blocks, so we execute it
    via the raw DBAPI connection in autocommit mode — exactly like the
    ``/admin-tools/sync-schemas`` endpoint does.

    Returns a small diagnostic dict (or ``{"skipped": reason}`` on error).
    """
    def _progress(msg: str):
        logger.info("[post-import:align-schema] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    if not _ALIGN_SCHEMA_SQL_PATH.exists():
        _progress(f"schema script not found: {_ALIGN_SCHEMA_SQL_PATH}")
        return {"skipped": f"script not found: {_ALIGN_SCHEMA_SQL_PATH}"}

    sql_text = _ALIGN_SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    _progress("executing align_live_schema …")

    try:
        sa_conn = db.connection()
        raw_conn = sa_conn.connection
        prev_autocommit = getattr(raw_conn, "autocommit", None)
        try:
            if prev_autocommit is not None:
                raw_conn.autocommit = True
            cur = raw_conn.cursor()
            try:
                cur.execute(sql_text)
            finally:
                cur.close()
        finally:
            try:
                if prev_autocommit is not None:
                    raw_conn.autocommit = prev_autocommit
            except Exception:
                pass

        # Leave session in a clean state
        try:
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        _progress("align_live_schema completed")
        return {"status": "ok"}

    except Exception as exc:
        logger.warning("[post-import:align-schema] failed (non-fatal): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"skipped": str(exc)}


def run_backfill_created_by(db: Any, *, progress_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Run ``backfill_created_by_from_username`` and return the result dict.

    Non-fatal: logs a warning on error and returns ``{"skipped": reason}``.
    """
    def _progress(msg: str):
        logger.info("[post-import:backfill-created-by] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress("running backfill_created_by_from_username …")
    try:
        from app.services.application_migrations_service import backfill_created_by_from_username
        result = backfill_created_by_from_username(db)
        _progress(f"backfill done: {result}")
        return result
    except Exception as exc:
        logger.warning("[post-import:backfill-created-by] failed (non-fatal): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"skipped": str(exc)}


def run_backfill_application_id(db: Any, *, progress_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Backfill ``application_id`` on all child tables.

    Delegates to the canonical ``backfill_application_id_on_child_tables``
    which runs 3 resolution passes (via ASD, 2nd-pass ASD join, then by
    application_number) so every row is covered regardless of import path.
    Idempotent and non-fatal.
    """
    def _progress(msg: str):
        logger.info("[post-import:backfill-app-id] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress("running backfill application_id on child tables …")
    try:
        from app.services.application_migrations_service import backfill_application_id_on_child_tables
        counts = backfill_application_id_on_child_tables(db)
        _progress(f"backfill application_id done: {counts}")
        return counts
    except Exception as exc:
        logger.warning("[post-import:backfill-app-id] failed (non-fatal): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"skipped": str(exc)}


def run_backfill_category_type(db: Any, *, progress_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Backfill ``applications.category_type`` from the ``categories`` table.

    Joins ``applications.category_id`` → ``categories.id`` to copy
    ``categories.category_type``.  Falls back to ``'License'`` when the join
    finds no match or the stored value is NULL / blank.  Idempotent and
    non-fatal.
    """
    def _progress(msg: str):
        logger.info("[post-import:backfill-category-type] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress("backfilling applications.category_type from categories …")
    try:
        result = db.execute(text("""
            WITH resolved AS (
                SELECT
                    a.id,
                    COALESCE(
                        NULLIF(TRIM(c.category_type), ''),
                        'License'
                    ) AS resolved_type
                FROM applications a
                LEFT JOIN categories c ON c.id = a.category_id
                WHERE
                    COALESCE(NULLIF(TRIM(a.category_type), ''), '') IS DISTINCT FROM
                    COALESCE(NULLIF(TRIM(c.category_type), ''), 'License')
            )
            UPDATE applications a
            SET    category_type = r.resolved_type
            FROM   resolved r
            WHERE  a.id = r.id
        """))
        db.commit()
        updated = result.rowcount
        _progress(f"category_type backfill done: {updated} rows updated")
        return {"updated": updated}
    except Exception as exc:
        logger.warning("[post-import:backfill-category-type] failed (non-fatal): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"skipped": str(exc)}


def run_backfill_certificates_sector_owner(
    db: Any, *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Backfill ``certificates.sector`` (from ``applications.license_type``)
    and ``certificates.owner_id`` (from ``applications.created_by``).

    Idempotent and non-fatal.
    """
    def _progress(msg: str):
        logger.info("[post-import:backfill-certs-sector-owner] %s", msg)
        if callable(progress_cb):
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress("backfilling certificates.sector + owner_id + certificate_type …")
    try:
        result = db.execute(text("""
            UPDATE public.certificates c
            SET    sector = CASE
                               WHEN a.license_type LIKE '%PETROLEUM'      THEN 'PETROLEUM'
                               WHEN a.license_type LIKE '%NATURAL_GAS'    THEN 'NATURAL_GAS'
                               WHEN a.license_type = 'LICENSE_WATER'       THEN 'WATER_SUPPLY'
                               WHEN a.license_type LIKE '%ELECTRICITY%'   THEN 'ELECTRICITY'
                               ELSE NULL
                           END,
                                     owner_id   = a.created_by,
                                     certificate_type = COALESCE(a.category_type, c.certificate_type),
                   updated_at = now()
            FROM   public.applications a
            WHERE  a.application_number = c.application_number
                            AND  (
                                     c.sector IS NULL OR c.sector = ''
                                OR c.owner_id IS DISTINCT FROM a.created_by
                                OR (a.category_type IS NOT NULL AND a.category_type <> '' AND c.certificate_type IS DISTINCT FROM a.category_type)
                            )
        """))
        db.commit()
        updated = result.rowcount
        _progress(f"certificates backfilled: {updated} rows updated")
        return {"updated": updated}
    except Exception as exc:
        logger.warning("[post-import:backfill-certs-sector-owner] failed (non-fatal): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"skipped": str(exc)}


def run_post_import_hooks(
    db: Any,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Run **all** post-import hooks in order:

    1. ``align_live_schema`` — ensures columns / constraints exist
    2. ``backfill_application_id`` — fills ``application_id`` on child tables
    3. ``backfill_created_by`` — fills ``created_by`` from ``username``
    4. ``backfill_category_type`` — fills ``category_type`` from ``categories``
    5. ``backfill_certificates_sector_owner`` — fills ``sector`` + ``owner_id``

    Returns a dict with the result of each hook.
    """
    results: Dict[str, Any] = {}

    results["align_live_schema"] = run_align_live_schema(db, progress_cb=progress_cb)
    results["backfill_application_id"] = run_backfill_application_id(db, progress_cb=progress_cb)
    results["backfill_created_by"] = run_backfill_created_by(db, progress_cb=progress_cb)
    results["backfill_category_type"] = run_backfill_category_type(db, progress_cb=progress_cb)
    results["backfill_certificates_sector_owner"] = run_backfill_certificates_sector_owner(db, progress_cb=progress_cb)

    return results
