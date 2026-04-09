from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin-tools")

# ── Simple time-based cache ───────────────────────────────────────────────────
# Stores: { cache_key: {"data": ..., "fetched_at": float} }
_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes — adjust as needed


def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and (time.time() - entry["fetched_at"]) < _CACHE_TTL_SECONDS:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any):
    _CACHE[key] = {"data": data, "fetched_at": time.time()}


def _cache_clear(key: str = None):
    if key:
        _CACHE.pop(key, None)
    else:
        _CACHE.clear()

# SQL constants for sync-license-type
_SQL_UPDATE_APPLICATIONS = """
UPDATE public.applications a
SET
    license_type =
        CASE
            WHEN c.category_type = 'Construction' AND s.name = 'NATURAL_GAS'
                THEN 'CONSTRUCTION_NATURAL_GAS'
            WHEN c.category_type = 'Construction' AND s.name = 'PETROLEUM'
                THEN 'CONSTRUCTION_PETROLEUM'
            WHEN c.category_type = 'License' AND s.name = 'NATURAL_GAS'
                THEN 'LICENSE_NATURAL_GAS'
            WHEN c.category_type = 'License' AND s.name = 'PETROLEUM'
                THEN 'LICENSE_PETROLEUM'
            WHEN c.category_type = 'License' AND s.name = 'WATER_SUPPLY'
                THEN 'LICENSE_WATER'
            WHEN s.name = 'ELECTRICITY'
                 AND a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION'
                THEN 'LICENSE_ELECTRICITY_INSTALLATION'
            WHEN c.category_type = 'License' AND s.name = 'ELECTRICITY'
                THEN 'LICENSE_ELECTRICITY_SUPPLY'
            ELSE a.license_type
        END,
    category_license_type = 'OPERATIONAL'
FROM public.categories c
JOIN public.sectors s ON s.id = c.sector_id
WHERE a.category_id = c.id
  AND c.deleted_at IS NULL
"""

_SQL_UPDATE_CERTIFICATES = """
UPDATE public.certificates cert
SET
    license_type          = a.license_type,
    category_license_type = a.category_license_type
FROM public.applications a
WHERE cert.application_id = a.id
  AND (
      cert.license_type          IS DISTINCT FROM a.license_type
   OR cert.category_license_type IS DISTINCT FROM a.category_license_type
  )
"""

_SQL_PREVIEW_APPLICATIONS = """
SELECT
    CASE
        WHEN c.category_type = 'Construction' AND s.name = 'NATURAL_GAS'  THEN 'CONSTRUCTION_NATURAL_GAS'
        WHEN c.category_type = 'Construction' AND s.name = 'PETROLEUM'    THEN 'CONSTRUCTION_PETROLEUM'
        WHEN c.category_type = 'License'      AND s.name = 'NATURAL_GAS'  THEN 'LICENSE_NATURAL_GAS'
        WHEN c.category_type = 'License'      AND s.name = 'PETROLEUM'    THEN 'LICENSE_PETROLEUM'
        WHEN c.category_type = 'License'      AND s.name = 'WATER_SUPPLY' THEN 'LICENSE_WATER'
        WHEN s.name = 'ELECTRICITY'
             AND a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION'      THEN 'LICENSE_ELECTRICITY_INSTALLATION'
        WHEN c.category_type = 'License'      AND s.name = 'ELECTRICITY'  THEN 'LICENSE_ELECTRICITY_SUPPLY'
        ELSE a.license_type
    END                         AS new_license_type,
    COUNT(*)                    AS application_count
FROM public.applications a
JOIN public.categories c ON c.id = a.category_id AND c.deleted_at IS NULL
JOIN public.sectors     s ON s.id = c.sector_id
GROUP BY 1
ORDER BY 1
"""

_SQL_PREVIEW_CERTIFICATES = """
SELECT COUNT(*) AS certificates_to_update
FROM public.certificates cert
JOIN public.applications a ON a.id = cert.application_id
WHERE cert.license_type          IS DISTINCT FROM a.license_type
   OR cert.category_license_type IS DISTINCT FROM a.category_license_type
"""

_PHONE_RE = r'(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7})'
_EMAIL_RE = r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'


def _get_new_session():
    from app.core import database as db_module
    db_module._init_engine()
    return db_module._SessionLocal()


@router.post('/sync-schemas', tags=["01 - Schema Sync"])
def sync_schemas(dry_run: bool = True):
    """Sync DB schema for live migration.

    Runs `app/migrations/align_live_schema_eservice_construction.sql`.

    - dry_run=true  -> returns SQL without executing
    - dry_run=false -> executes SQL
    """
    sql_path = Path(__file__).resolve().parents[2] / "migrations" / "align_live_schema_eservice_construction.sql"
    if not sql_path.exists():
        raise HTTPException(status_code=500, detail=f"schema sync script not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    if dry_run:
        return {"status": "DRY_RUN", "script": str(sql_path), "sql": sql_text}

    db = _get_new_session()
    try:
        diag = db.execute(
            text("SELECT current_database(), current_schema(), current_user, inet_server_addr(), inet_server_port()")
        ).first()

        before = {
            "applications_unique_constraints": None,
            "documents_has_logic_doc_id": None,
            "shareholders_has_application_sector_detail_id": None,
            "applications_has_application_number_unique": None,
        }
        try:
            before["applications_unique_constraints"] = int(
                db.execute(text("SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.applications'::regclass AND contype = 'u'")).scalar() or 0
            )
            before["documents_has_logic_doc_id"] = bool(
                db.execute(text("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='logic_doc_id'")).first()
            )
            before["shareholders_has_application_sector_detail_id"] = bool(
                db.execute(text("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='shareholders' AND column_name='application_sector_detail_id'")).first()
            )
            before["applications_has_application_number_unique"] = bool(
                db.execute(text("""
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.applications'::regclass
                      AND contype = 'u'
                      AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
                    LIMIT 1
                """)).first()
            )
        except Exception:
            pass

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
        try:
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        after = {
            "applications_unique_constraints": None,
            "documents_has_logic_doc_id": None,
            "shareholders_has_application_sector_detail_id": None,
            "applications_has_application_number_unique": None,
        }
        try:
            after["applications_unique_constraints"] = int(
                db.execute(text("SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.applications'::regclass AND contype = 'u'")).scalar() or 0
            )
            after["documents_has_logic_doc_id"] = bool(
                db.execute(text("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='logic_doc_id'")).first()
            )
            after["shareholders_has_application_sector_detail_id"] = bool(
                db.execute(text("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='shareholders' AND column_name='application_sector_detail_id'")).first()
            )
            after["applications_has_application_number_unique"] = bool(
                db.execute(text("""
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.applications'::regclass
                      AND contype = 'u'
                      AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
                    LIMIT 1
                """)).first()
            )
        except Exception:
            pass

        return {
            "status": "SUCCESS",
            "script": str(sql_path),
            "before": before,
            "after": after,
            "changes": {
                "dropped_unique_constraints": (
                    (before.get("applications_unique_constraints") - after.get("applications_unique_constraints"))
                    if isinstance(before.get("applications_unique_constraints"), int)
                    and isinstance(after.get("applications_unique_constraints"), int)
                    else None
                ),
                "added_documents_logic_doc_id": (
                    (not before.get("documents_has_logic_doc_id")) and bool(after.get("documents_has_logic_doc_id"))
                    if before.get("documents_has_logic_doc_id") is not None and after.get("documents_has_logic_doc_id") is not None
                    else None
                ),
                "added_shareholders_application_sector_detail_id": (
                    (not before.get("shareholders_has_application_sector_detail_id"))
                    and bool(after.get("shareholders_has_application_sector_detail_id"))
                    if before.get("shareholders_has_application_sector_detail_id") is not None
                    and after.get("shareholders_has_application_sector_detail_id") is not None
                    else None
                ),
            },
            "db_diag": {
                "current_database": diag[0] if diag else None,
                "current_schema": diag[1] if diag else None,
                "current_user": diag[2] if diag else None,
                "server_addr": str(diag[3]) if diag and diag[3] is not None else None,
                "server_port": int(diag[4]) if diag and diag[4] is not None else None,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    except SQLAlchemyError as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post('/repair-and-backfill', tags=["99 - Backfill created_by"])
def repair_and_backfill():
    """Repair missing users + backfill created_by in one shot."""
    from app.services.application_migrations_service import backfill_created_by_from_username

    db = _get_new_session()
    try:
        try:
            db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            db.commit()
        except Exception:
            pass

        missing_rows = db.execute(text("""
            SELECT DISTINCT lower(trim(a.username)) AS username
            FROM public.applications a
            WHERE a.username IS NOT NULL
              AND lower(trim(a.username)) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM public.users u
                  WHERE lower(trim(u.username)) = lower(trim(a.username))
              )
            ORDER BY 1
        """)).fetchall()

        missing_usernames = [r[0] for r in missing_rows]
        logger.info("repair-missing-users: found %d usernames with no user row", len(missing_usernames))

        if not missing_usernames:
            bf = backfill_created_by_from_username(db)
            return {
                "inserted_users": 0,
                "missing_usernames": [],
                "backfill": bf,
                "message": "No missing users found. Backfill ran on existing users."
            }

        inserted = []
        failed = []
        for uname in missing_usernames:
            try:
                db.execute(text("""
                    INSERT INTO public.users (
                        id, full_name, username, password_hash, status,
                        phone_number, email_address, user_category,
                        account_type, auth_mode, failed_attempts,
                        is_first_login, deleted, created_at, updated_at
                    )
                    SELECT
                        gen_random_uuid(), :uname, :uname, '',
                        'ACTIVE', NULL, NULL, 'EXTERNAL', 'INDIVIDUAL', 'DB',
                        0, false, false, now(), now()
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.users eu
                        WHERE lower(trim(eu.username)) = :uname
                    )
                """), {"uname": uname})
                db.commit()
                inserted.append(uname)
            except Exception as _ue:
                logger.error("repair-missing-users: failed to insert user '%s': %s", uname, _ue)
                failed.append({"username": uname, "error": str(_ue)})
                try:
                    db.rollback()
                except Exception:
                    pass

        _admin_role_id = None
        try:
            _ur = db.execute(text("SELECT role_id FROM public.user_roles LIMIT 1")).fetchone()
            if _ur:
                _admin_role_id = str(_ur[0])
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        if not _admin_role_id:
            logger.info("repair-missing-users: role assignment skipped (FDW limitation)")

        roles_assigned = []
        if _admin_role_id:
            for uname in inserted:
                try:
                    db.execute(text("""
                        INSERT INTO public.user_roles (user_id, role_id, deleted, created_at)
                        SELECT u.id, :role_id, false, now()
                        FROM public.users u
                        WHERE lower(trim(u.username)) = :uname
                        AND NOT EXISTS (
                            SELECT 1 FROM public.user_roles ex
                            WHERE ex.user_id = u.id AND ex.role_id = :role_id
                        )
                    """), {"uname": uname, "role_id": _admin_role_id})
                    db.commit()
                    roles_assigned.append(uname)
                except Exception as _ure:
                    logger.error("repair-missing-users: failed to assign role for '%s': %s", uname, _ure)
                    try:
                        db.rollback()
                    except Exception:
                        pass

        deduped_certs = 0
        try:
            r_dedup = db.execute(text("""
                DELETE FROM public.certificates
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY application_number
                                   ORDER BY updated_at DESC NULLS LAST,
                                            created_at DESC NULLS LAST, id
                               ) AS rn
                        FROM public.certificates
                        WHERE application_number IS NOT NULL
                    ) ranked
                    WHERE rn > 1
                )
            """))
            deduped_certs = r_dedup.rowcount or 0
            db.commit()
        except Exception as _cde:
            logger.error("repair-and-backfill: certificate dedup failed (non-fatal): %s", _cde)
            try:
                db.rollback()
            except Exception:
                pass

        from app.services.application_migrations_service import backfill_application_id_on_child_tables
        abf = backfill_application_id_on_child_tables(db)
        bf = backfill_created_by_from_username(db)

        return {
            "inserted_users": len(inserted),
            "inserted_usernames": inserted,
            "roles_assigned": len(roles_assigned),
            "deduped_certificates": deduped_certs,
            "failed": failed,
            "backfill_application_id": abf,
            "backfill": bf,
            "message": (
                f"Inserted {len(inserted)} missing users, assigned {len(roles_assigned)} roles, "
                f"removed {deduped_certs} duplicate certificate rows, "
                f"backfilled application_id + created_by across all child tables."
            )
        }

    except Exception as e:
        logger.exception("repair-and-backfill: unexpected error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post('/backfill-application-id', tags=["99 - Backfill created_by"])
def backfill_application_id():
    """Propagate application_id to all child tables."""
    from app.services.application_migrations_service import (
        backfill_application_id_on_child_tables,
        _ensure_child_table_columns,
    )
    db = _get_new_session()
    try:
        _ensure_child_table_columns(db)
        result = backfill_application_id_on_child_tables(db)
        return {
            "status": "ok",
            "backfill_application_id": result,
            "message": "application_id backfilled on all child tables.",
        }
    except Exception as e:
        logger.exception("backfill-application-id: unexpected error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


_PHONE_RE_PY = r'(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7})'
_EMAIL_RE_PY = r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'


@router.post('/clean-name-fields', tags=["01 - Schema Sync"])
def clean_name_fields(dry_run: bool = True):
    """Extract phone numbers and emails embedded in name columns."""
    db = _get_new_session()
    try:
        cd_affected = int(db.execute(text(f"""
            SELECT COUNT(*) FROM public.custom_details
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """)).scalar() or 0)
        sd_phone_affected = int(db.execute(text(f"""
            SELECT COUNT(*) FROM public.supervisor_details
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """)).scalar() or 0)
        sd_email_affected = int(db.execute(text(f"""
            SELECT COUNT(*) FROM public.supervisor_details
            WHERE name ~ '{_EMAIL_RE}'
              AND NULLIF(TRIM(COALESCE(email, '')), '') IS NULL
        """)).scalar() or 0)
        sd_name_affected = int(db.execute(text("""
            SELECT COUNT(*) FROM public.supervisor_details
            WHERE name LIKE '%,%' OR name LIKE '%|%'
        """)).scalar() or 0)

        preview = {
            "custom_details_phone_to_fill":    cd_affected,
            "supervisor_details_phone_to_fill": sd_phone_affected,
            "supervisor_details_email_to_fill": sd_email_affected,
            "supervisor_details_name_to_clean": sd_name_affected,
        }

        if dry_run:
            return {"status": "DRY_RUN", "preview": preview}

        r_cd = db.execute(text(f"""
            UPDATE public.custom_details
            SET
                mobile_no  = COALESCE(
                                 NULLIF(TRIM(mobile_no), ''),
                                 NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                                     '^.*?({_PHONE_RE[1:-1]}).*$', E'\\\\1')), ' ', ''), REPLACE(name, ' ', ''))
                             ),
                updated_at = now()
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """))
        cd_updated = r_cd.rowcount or 0
        db.commit()

        r_sd_phone = db.execute(text(f"""
            UPDATE public.supervisor_details
            SET
                mobile_no  = NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                                 '^.*?({_PHONE_RE[1:-1]}).*$', E'\\\\1')), ' ', ''),
                             REPLACE(name, ' ', '')),
                updated_at = now()
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """))
        sd_phone_updated = r_sd_phone.rowcount or 0
        db.commit()

        r_sd_email = db.execute(text(f"""
            UPDATE public.supervisor_details
            SET
                email      = NULLIF(TRIM(REGEXP_REPLACE(name,
                                 '^.*?({_EMAIL_RE[1:-1]}).*$', E'\\\\1')), name),
                updated_at = now()
            WHERE name ~ '{_EMAIL_RE}'
              AND NULLIF(TRIM(COALESCE(email, '')), '') IS NULL
        """))
        sd_email_updated = r_sd_email.rowcount or 0
        db.commit()

        r_sd_name = db.execute(text("""
            UPDATE public.supervisor_details
            SET
                name       = CASE
                                 WHEN name LIKE '%|%' THEN TRIM(SPLIT_PART(name, '|', 1))
                                 WHEN name LIKE '%,%' THEN TRIM(SPLIT_PART(name, ',', 1))
                                 ELSE TRIM(name)
                             END,
                updated_at = now()
            WHERE name LIKE '%|%' OR name LIKE '%,%'
        """))
        sd_name_updated = r_sd_name.rowcount or 0
        db.commit()

        return {
            "status": "APPLIED",
            "preview": preview,
            "custom_details_updated":             cd_updated,
            "supervisor_details_phone_filled":    sd_phone_updated,
            "supervisor_details_email_filled":    sd_email_updated,
            "supervisor_details_name_cleaned":    sd_name_updated,
            "message": (
                f"custom_details: {cd_updated} rows had phone extracted into mobile_no. "
                f"supervisor_details: {sd_phone_updated} mobile_no filled, "
                f"{sd_email_updated} email filled, {sd_name_updated} names cleaned."
            ),
        }

    except Exception as e:
        logger.exception("clean-name-fields: error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/fix-certificates", tags=["01 - Schema Sync"])
def fix_certificates():
    """Post-upload certificate maintenance."""
    db = _get_new_session()
    try:
        r1 = db.execute(text("""
            UPDATE public.applications a
            SET intimate_date = a.expire_date - INTERVAL '6 months',
                updated_at    = now()
            FROM public.categories c
            WHERE a.category_id = c.id
              AND c.category_type = 'License'
              AND a.intimate_date IS NULL
              AND a.expire_date IS NOT NULL
        """))
        intimate_date_updated = r1.rowcount or 0
        db.commit()

        r2 = db.execute(text("""
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY approval_no
                           ORDER BY expire_date DESC NULLS LAST, created_at DESC NULLS LAST
                       ) AS rn
                FROM public.certificates
                WHERE approval_no IS NOT NULL AND TRIM(approval_no) <> ''
            )
            DELETE FROM public.certificates WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
        """))
        certificates_deleted = r2.rowcount or 0
        db.commit()

        r3 = db.execute(text("""
            UPDATE public.applications a
            SET certificate_id = c.id, updated_at = now()
            FROM public.certificates c
            WHERE TRIM(a.approval_no) = TRIM(c.approval_no)
              AND a.approval_no IS NOT NULL AND TRIM(a.approval_no) <> ''
        """))
        certificate_id_updated = r3.rowcount or 0
        db.commit()

        try:
            db.execute(text("""
                ALTER TABLE public.certificates
                    ADD COLUMN IF NOT EXISTS sector           character varying NULL,
                    ADD COLUMN IF NOT EXISTS certificate_type character varying NULL
            """))
            db.commit()
        except Exception:
            db.rollback()

        r4 = db.execute(text("""
            UPDATE public.certificates c
            SET sector = s.name, certificate_type = cat.category_type, updated_at = now()
            FROM public.applications a
            JOIN public.categories cat ON cat.id = a.category_id
            JOIN public.sectors    s   ON s.id   = cat.sector_id
            WHERE c.application_id = a.id
              AND (c.sector IS NULL OR c.certificate_type IS NULL)
        """))
        certificates_sector_updated = r4.rowcount or 0
        db.commit()

        return {
            "status": "OK",
            "intimate_date_backfilled": intimate_date_updated,
            "duplicate_certificates_deleted": certificates_deleted,
            "applications_certificate_id_updated": certificate_id_updated,
            "certificates_sector_and_type_updated": certificates_sector_updated,
        }

    except Exception as e:
        logger.exception("fix-certificates: error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/update-zone-id", tags=["01 - Schema Sync"])
def update_zone_id():
    """Backfill applications.zone_id from napa_regions via application_sector_details.region."""
    db = _get_new_session()
    try:
        r = db.execute(text("""
            UPDATE public.applications a
            SET zone_id = nr.id, updated_at = now()
            FROM public.application_sector_details asd
            JOIN public.napa_regions nr ON lower(trim(nr.name)) = lower(trim(asd.region))
            WHERE a.id = asd.application_id AND a.zone_id IS NULL
        """))
        updated = r.rowcount or 0
        db.commit()
        return {"status": "OK", "applications_zone_id_updated": updated}
    except Exception as e:
        logger.exception("update-zone-id: error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/update-natural-gas-category-id", tags=["01 - Schema Sync"])
def update_natural_gas_category_id():
    """Backfill applications.category_id from natural_gas_mapping table."""
    db = _get_new_session()
    try:
        r = db.execute(text("""
            UPDATE public.applications a
            SET category_id = c.id, updated_at = now()
            FROM public.natural_gas_mapping ngm
            JOIN public.sectors s ON s.name = 'NATURAL_GAS'
            JOIN public.categories c ON c.name = ngm.license_category_name AND c.sector_id = s.id
            WHERE a.application_number = ngm.application_number
        """))
        updated = r.rowcount or 0
        db.commit()
        return {"status": "OK", "applications_category_id_updated": updated}
    except Exception as e:
        logger.exception("update-natural-gas-category-id: error: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/sync-license-type", tags=["01 - Schema Sync"])
def sync_license_type_admin_tools(dry_run: bool = Query(default=True)):
    """
    Synchronise license_type and category_license_type across applications and certificates.

    - dry_run=true  (default): returns a preview of what would change, no writes.
    - dry_run=false: executes the UPDATEs and reports rows affected.
    """
    db = _get_new_session()
    try:
        if dry_run:
            breakdown = [
                dict(r._mapping)
                for r in db.execute(text(_SQL_PREVIEW_APPLICATIONS)).fetchall()
            ]
            certs_pending = db.execute(text(_SQL_PREVIEW_CERTIFICATES)).scalar() or 0
            return {
                "status": "DRY_RUN",
                "note": "Pass dry_run=false to apply changes.",
                "applications_breakdown": breakdown,
                "certificates_to_update": int(certs_pending),
            }
        else:
            updated_apps = db.execute(text(_SQL_UPDATE_APPLICATIONS)).rowcount or 0
            updated_certs = db.execute(text(_SQL_UPDATE_CERTIFICATES)).rowcount or 0
            db.commit()
            return {
                "status": "OK",
                "updated_applications": updated_apps,
                "updated_certificates": updated_certs,
            }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ── Copy category attachment configuration ────────────────────────────────────

@router.post("/copy-category-config", tags=["06 - Admin Utilities"])
def copy_category_config(
    source_category_id: str = Query(..., description="Category ID to copy FROM"),
    destination_category_id: str = Query(..., description="Category ID to copy TO"),
):
    """
    Copy all rows from `application_category_attachments` for the **source** category
    into the **destination** category.

    Rows that already exist on the destination (matched on
    `application_type + attachment_rule + application_attachment_id`) are silently skipped.
    Safe to run multiple times — fully idempotent.
    """
    if source_category_id == destination_category_id:
        raise HTTPException(status_code=400, detail="source and destination category IDs must be different.")

    db = _get_new_session()
    try:
        source_count = db.execute(
            text("SELECT COUNT(*) FROM application_category_attachments WHERE category_id = :src"),
            {"src": source_category_id},
        ).scalar() or 0

        if source_count == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Source category '{source_category_id}' has no attachment configuration rows.",
            )

        result = db.execute(
            text("""
                INSERT INTO application_category_attachments
                    (id, category_id, application_type, attachment_rule,
                     application_attachment_id, created_at)
                SELECT
                    gen_random_uuid(),
                    :dest,
                    application_type,
                    attachment_rule,
                    application_attachment_id,
                    now()
                FROM application_category_attachments src
                WHERE src.category_id = :src
                  AND NOT EXISTS (
                      SELECT 1
                      FROM application_category_attachments existing
                      WHERE existing.category_id                = :dest
                        AND existing.application_type           = src.application_type
                        AND existing.attachment_rule            = src.attachment_rule
                        AND existing.application_attachment_id  = src.application_attachment_id
                  )
            """),
            {"src": source_category_id, "dest": destination_category_id},
        )
        db.commit()

        rows_copied  = result.rowcount
        rows_skipped = source_count - rows_copied

        logger.info(
            "copy-category-config: src=%s dest=%s copied=%d skipped=%d",
            source_category_id, destination_category_id, rows_copied, rows_skipped,
        )

        return {
            "status": "OK",
            "source_category_id": source_category_id,
            "destination_category_id": destination_category_id,
            "source_total_rows": source_count,
            "rows_copied": rows_copied,
            "rows_skipped": rows_skipped,
            "message": (
                f"Copied {rows_copied} row(s) to destination. "
                f"{rows_skipped} row(s) already existed and were skipped."
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("copy-category-config failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── Assign DEFAULT role to all APPLICANT ROLE users ───────────────────────────

@router.post("/assign-default-role", tags=["06 - Admin Utilities"])
def assign_default_role(
    applicant_role_name: str = Query(default="APPLICANT ROLE", description="Source role name"),
    default_role_name: str   = Query(default="DEFAULT",        description="Role to assign"),
):
    """
    Finds every user who holds the **applicant role** but does **not** yet have
    the **default role**, then inserts the missing `user_roles` rows.

    Idempotent — running it twice has no additional effect.
    """
    db = _get_new_session()
    try:
        # Verify both roles exist
        found = {
            r[0]
            for r in db.execute(
                text("SELECT name FROM roles WHERE name IN (:ar, :dr)"),
                {"ar": applicant_role_name, "dr": default_role_name},
            ).fetchall()
        }
        missing = {applicant_role_name, default_role_name} - found
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"Role(s) not found in the database: {sorted(missing)}",
            )

        result = db.execute(
            text("""
                WITH applicant_users AS (
                    SELECT ur.user_id
                    FROM user_roles ur
                    JOIN roles r ON r.id = ur.role_id
                    WHERE r.name = :ar
                ),
                default_role AS (
                    SELECT id FROM roles WHERE name = :dr
                )
                INSERT INTO user_roles (user_id, role_id)
                SELECT au.user_id, dr.id
                FROM applicant_users au
                CROSS JOIN default_role dr
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM user_roles ur
                    WHERE ur.user_id = au.user_id
                      AND ur.role_id = dr.id
                )
            """),
            {"ar": applicant_role_name, "dr": default_role_name},
        )
        db.commit()

        rows_inserted = result.rowcount
        logger.info(
            "assign-default-role: applicant_role=%s default_role=%s inserted=%d",
            applicant_role_name, default_role_name, rows_inserted,
        )

        return {
            "status": "OK",
            "applicant_role_name": applicant_role_name,
            "default_role_name": default_role_name,
            "rows_inserted": rows_inserted,
            "message": (
                f"Assigned '{default_role_name}' to {rows_inserted} user(s). "
                "Users who already had the role were left unchanged."
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("assign-default-role failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── Internal fetch helpers (populate cache) ───────────────────────────────────

def _fetch_all_categories(db) -> list[dict]:
    rows = db.execute(text("""
        SELECT
            c.id,
            c.name,
            c.category_type,
            s.name AS sector_name
        FROM categories c
        LEFT JOIN sectors s ON s.id = c.sector_id
        WHERE c.deleted_at IS NULL
        ORDER BY s.name NULLS LAST, c.name
    """)).fetchall()
    return [
        {
            "value": str(r[0]),
            "label": r[1],
            "category_type": r[2],
            "sector": r[3],
        }
        for r in rows
    ]


def _fetch_all_roles(db) -> list[dict]:
    rows = db.execute(text("""
        SELECT
            r.id,
            r.name,
            COUNT(ur.user_id) AS user_count
        FROM roles r
        LEFT JOIN user_roles ur ON ur.role_id = r.id
        GROUP BY r.id, r.name
        ORDER BY r.name
    """)).fetchall()
    return [
        {
            "value": str(r[0]),
            "label": r[1],
            "user_count": int(r[2]),
        }
        for r in rows
    ]


# ── GET /list-categories ──────────────────────────────────────────────────────

@router.get("/list-categories", tags=["06 - Admin Utilities"])
def list_categories(
    search: str = Query(default="", description="Filter by name or sector (case-insensitive)"),
    sector: str = Query(default="", description="Filter by sector name e.g. ELECTRICITY, PETROLEUM"),
    include_deleted: bool = Query(default=False, description="Include soft-deleted categories"),
):
    """
    Returns categories as **select options** (`value` = id, `label` = name).

    Results are **cached for 5 minutes** — the cache refreshes automatically
    when data grows. Use `POST /refresh-cache` to force an immediate refresh.
    """
    # For filtered queries (search/sector/include_deleted) skip cache and query live
    filtered = bool(search or sector or include_deleted)

    if not filtered:
        cached = _cache_get("categories")
        if cached is not None:
            return {
                "total": len(cached),
                "cached": True,
                "options": cached,
            }

    db = _get_new_session()
    try:
        if not filtered:
            # Full unfiltered load — store in cache
            options = _fetch_all_categories(db)
            _cache_set("categories", options)
        else:
            # Filtered — query live, do not overwrite cache
            rows = db.execute(
                text("""
                    SELECT
                        c.id,
                        c.name,
                        c.category_type,
                        s.name AS sector_name,
                        c.deleted_at IS NOT NULL AS is_deleted
                    FROM categories c
                    LEFT JOIN sectors s ON s.id = c.sector_id
                    WHERE (:search  = '' OR lower(c.name)  LIKE '%' || lower(:search) || '%'
                                        OR lower(s.name)   LIKE '%' || lower(:search) || '%')
                      AND (:sector  = '' OR lower(s.name)  LIKE '%' || lower(:sector) || '%')
                      AND (:include_deleted = TRUE OR c.deleted_at IS NULL)
                    ORDER BY s.name NULLS LAST, c.name
                """),
                {"search": search, "sector": sector, "include_deleted": include_deleted},
            ).fetchall()
            options = [
                {
                    "value":         str(r[0]),
                    "label":         r[1],
                    "category_type": r[2],
                    "sector":        r[3],
                    "is_deleted":    r[4],
                }
                for r in rows
            ]

        return {
            "total": len(options),
            "cached": False,
            "options": options,
        }

    except Exception as exc:
        logger.exception("list-categories failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── GET /list-roles ───────────────────────────────────────────────────────────

@router.get("/list-roles", tags=["06 - Admin Utilities"])
def list_roles(
    search: str = Query(default="", description="Filter by role name (case-insensitive)"),
):
    """
    Returns roles as **select options** (`value` = id, `label` = name)
    plus a `user_count` showing how many users hold each role.

    Results are **cached for 5 minutes**. Use `POST /refresh-cache` to force refresh.
    """
    filtered = bool(search)

    if not filtered:
        cached = _cache_get("roles")
        if cached is not None:
            return {
                "total": len(cached),
                "cached": True,
                "options": cached,
            }

    db = _get_new_session()
    try:
        if not filtered:
            options = _fetch_all_roles(db)
            _cache_set("roles", options)
        else:
            rows = db.execute(
                text("""
                    SELECT
                        r.id,
                        r.name,
                        COUNT(ur.user_id) AS user_count
                    FROM roles r
                    LEFT JOIN user_roles ur ON ur.role_id = r.id
                    WHERE lower(r.name) LIKE '%' || lower(:search) || '%'
                    GROUP BY r.id, r.name
                    ORDER BY r.name
                """),
                {"search": search},
            ).fetchall()
            options = [
                {
                    "value":      str(r[0]),
                    "label":      r[1],
                    "user_count": int(r[2]),
                }
                for r in rows
            ]

        return {
            "total": len(options),
            "cached": False,
            "options": options,
        }

    except Exception as exc:
        logger.exception("list-roles failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── POST /refresh-cache ───────────────────────────────────────────────────────

@router.post("/refresh-cache", tags=["06 - Admin Utilities"])
def refresh_cache(
    target: str = Query(
        default="all",
        description="Which cache to refresh: 'categories', 'roles', or 'all'",
    ),
):
    """
    Force-refresh the in-memory select-option caches without waiting for the
    5-minute TTL to expire. Useful after bulk inserts into `categories` or `roles`.
    """
    valid = {"all", "categories", "roles"}
    if target not in valid:
        raise HTTPException(status_code=400, detail=f"target must be one of: {sorted(valid)}")

    db = _get_new_session()
    refreshed = []
    try:
        if target in ("all", "categories"):
            data = _fetch_all_categories(db)
            _cache_set("categories", data)
            refreshed.append({"cache": "categories", "count": len(data)})

        if target in ("all", "roles"):
            data = _fetch_all_roles(db)
            _cache_set("roles", data)
            refreshed.append({"cache": "roles", "count": len(data)})

        return {
            "status": "OK",
            "refreshed": refreshed,
            "ttl_seconds": _CACHE_TTL_SECONDS,
        }

    except Exception as exc:
        logger.exception("refresh-cache failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── POST /password-troubleshoot/start ─────────────────────────────────────────

@router.post("/password-troubleshoot/start", tags=["06 - Admin Utilities"])
def password_troubleshoot_start(
    problem_username: str = Query(..., description="Username of the user who cannot log in"),
    admin_username: str   = Query(default="mkulasi.robert", description="Admin whose password will be temporarily applied"),
):
    """
    **Step 1 of 2 — Save & replace password.**

    1. Creates a permanent table `public.password_troubleshoot_backup` (if it doesn't exist).
    2. Saves the problematic user's current `username + password_hash` into that table.
    3. Updates the problematic user's `password_hash` with the admin's hash so they
       can log in for testing.

    Call `/password-troubleshoot/restore` after you have finished testing.
    """
    db = _get_new_session()
    try:
        # Verify both users exist
        users_found = {
            r[0]
            for r in db.execute(
                text("SELECT username FROM users WHERE username IN (:pu, :au)"),
                {"pu": problem_username, "au": admin_username},
            ).fetchall()
        }
        missing = {problem_username, admin_username} - users_found
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"User(s) not found: {sorted(missing)}",
            )

        # Ensure backup table exists (permanent — survives session restarts)
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS public.password_troubleshoot_backup (
                id            serial       PRIMARY KEY,
                username      text         NOT NULL UNIQUE,
                password_hash text         NOT NULL,
                backed_up_at  timestamptz  NOT NULL DEFAULT now(),
                backed_up_by  text         NOT NULL
            )
        """))
        db.commit()

        # Guard: don't overwrite an existing backup for this user
        already = db.execute(
            text("SELECT 1 FROM public.password_troubleshoot_backup WHERE username = :pu"),
            {"pu": problem_username},
        ).first()
        if already:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"A backup already exists for '{problem_username}'. "
                    "Call /password-troubleshoot/restore first to restore and clear it."
                ),
            )

        # Save original hash
        db.execute(
            text("""
                INSERT INTO public.password_troubleshoot_backup
                    (username, password_hash, backed_up_by)
                SELECT username, password_hash, :au
                FROM public.users
                WHERE username = :pu
            """),
            {"pu": problem_username, "au": admin_username},
        )

        # Replace with admin's hash
        result = db.execute(
            text("""
                UPDATE public.users
                SET
                    password_hash = (SELECT password_hash FROM public.users WHERE username = :au),
                    updated_at    = now()
                WHERE username = :pu
                RETURNING username, updated_at
            """),
            {"pu": problem_username, "au": admin_username},
        ).first()
        db.commit()

        logger.info(
            "password-troubleshoot/start: problem_user=%s admin=%s",
            problem_username, admin_username,
        )

        return {
            "status": "OK",
            "message": (
                f"Original password hash for '{problem_username}' saved to "
                f"public.password_troubleshoot_backup. "
                f"User can now log in with '{admin_username}' password. "
                f"Call /password-troubleshoot/restore when done."
            ),
            "problem_username": result[0],
            "updated_at": str(result[1]),
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("password-troubleshoot/start failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── POST /password-troubleshoot/restore ───────────────────────────────────────

@router.post("/password-troubleshoot/restore", tags=["06 - Admin Utilities"])
def password_troubleshoot_restore(
    problem_username: str = Query(..., description="Username to restore the original password for"),
    admin_username: str   = Query(default="mkulasi.robert", description="Admin performing the restoration"),
    drop_backup_table: bool = Query(
        default=False,
        description="Drop the entire backup table after restoring (only if no other backups remain).",
    ),
):
    """
    **Step 2 of 2 — Restore original password & clean up.**

    1. Reads the original `password_hash` from `public.password_troubleshoot_backup`.
    2. Writes it back to `public.users`.
    3. Deletes the backup row for this user.
    4. If `drop_backup_table=true` AND no other rows remain in the table, drops it entirely.
    """
    db = _get_new_session()
    try:
        # Check backup exists for this user
        backup_row = db.execute(
            text("""
                SELECT username, password_hash, backed_up_at
                FROM public.password_troubleshoot_backup
                WHERE username = :pu
            """),
            {"pu": problem_username},
        ).first()

        if not backup_row:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No backup found for '{problem_username}'. "
                    "Either it was never started or already restored."
                ),
            )

        # Restore original hash
        result = db.execute(
            text("""
                UPDATE public.users
                SET
                    password_hash = :original_hash,
                    updated_at    = now()
                WHERE username = :pu
                RETURNING username, updated_at
            """),
            {"original_hash": backup_row[1], "pu": problem_username},
        ).first()

        # Remove backup row
        db.execute(
            text("DELETE FROM public.password_troubleshoot_backup WHERE username = :pu"),
            {"pu": problem_username},
        )
        db.commit()

        # Optionally drop the table if it is now empty
        table_dropped = False
        if drop_backup_table:
            remaining = db.execute(
                text("SELECT COUNT(*) FROM public.password_troubleshoot_backup")
            ).scalar() or 0
            if remaining == 0:
                db.execute(text("DROP TABLE public.password_troubleshoot_backup"))
                db.commit()
                table_dropped = True
            else:
                logger.info(
                    "password-troubleshoot/restore: %d backup row(s) still remain — table NOT dropped.",
                    remaining,
                )

        logger.info(
            "password-troubleshoot/restore: problem_user=%s restored, table_dropped=%s",
            problem_username, table_dropped,
        )

        return {
            "status": "OK",
            "message": (
                f"Original password hash for '{problem_username}' has been restored. "
                + ("Backup table dropped." if table_dropped else "Backup row removed.")
            ),
            "problem_username": result[0],
            "restored_at": str(result[1]),
            "backed_up_at": str(backup_row[2]),
            "backup_table_dropped": table_dropped,
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("password-troubleshoot/restore failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
#  TRANSFER APPLICATION OWNERSHIP
# ─────────────────────────────────────────────────────────────────────────────
#
#  What this does (dry_run=false):
#    1. Look up the target user (by username OR user UUID)
#    2. Snapshot current state of every affected row into
#       public.transfer_ownership_backup  (created on first use)
#    3. applications:  SET created_by=<new_uuid>, username=<new_username>
#    4. certificates:  SET owner_id=<new_uuid>   (where application_id matches)
#    5. Every other table that FK-references applications(id):
#       SET created_by=<new_uuid> WHERE application_id=<app_id> AND created_by IS NOT NULL
#
#  Rollback: POST /transfer-ownership/restore  with the same application_number
# ─────────────────────────────────────────────────────────────────────────────

# Child tables that carry a created_by column and FK → applications(id)
_TRANSFER_CHILD_TABLES = [
    "task_assignments",
    "batch_application_advertisements",
    "application_electrical_installation",
    "transfer_applications",
    "application_installation_approval_classes",
    "applicant_proposed_investment",
    "bank_details_tanzania",
    "project_description",
    "referees",
    "app_evaluation_checklist",
    "application_additional_conditions",
    "application_sector_details",
    "application_approval_classes",
    "application_reviews",
]


def _ensure_transfer_backup_table(db) -> None:
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS public.transfer_ownership_backup (
            id                  bigserial PRIMARY KEY,
            backed_up_at        timestamp NOT NULL DEFAULT now(),
            application_number  varchar,
            application_id      uuid,
            table_name          varchar NOT NULL,
            row_id              uuid,
            old_created_by      uuid,
            old_username        varchar,
            old_owner_id        uuid
        )
    """))
    db.commit()


@router.get('/transfer-ownership/preview', tags=["06 - Transfer Ownership"])
def transfer_ownership_preview(
    application_number: str = Query(..., description="Application number to transfer"),
    new_username: str = Query(None, description="Target username (either this or new_user_id)"),
    new_user_id: str = Query(None, description="Target user UUID (either this or new_username)"),
):
    """
    Preview what will change when ownership of an application is transferred.
    Does NOT modify anything.
    """
    if not new_username and not new_user_id:
        raise HTTPException(status_code=422, detail="Provide new_username or new_user_id")

    db = _get_new_session()
    try:
        # ── Resolve application ───────────────────────────────────────────────
        app_row = db.execute(text("""
            SELECT id, application_number, created_by, username
            FROM public.applications
            WHERE application_number = :appno
              AND deleted_at IS NULL
        """), {"appno": application_number}).fetchone()

        if not app_row:
            raise HTTPException(status_code=404, detail=f"Application '{application_number}' not found")

        app_id       = str(app_row[0])
        current_cby  = str(app_row[2]) if app_row[2] else None
        current_uname = app_row[3]

        # ── Resolve target user ───────────────────────────────────────────────
        if new_user_id:
            user_row = db.execute(text("""
                SELECT id, username FROM public.users
                WHERE id = :uid AND deleted = false
            """), {"uid": new_user_id}).fetchone()
        else:
            user_row = db.execute(text("""
                SELECT id, username FROM public.users
                WHERE lower(trim(username)) = lower(trim(:uname)) AND deleted = false
            """), {"uname": new_username}).fetchone()

        if not user_row:
            raise HTTPException(status_code=404, detail="Target user not found or is deleted")

        target_user_id   = str(user_row[0])
        target_username  = user_row[1]

        # ── Count child rows ──────────────────────────────────────────────────
        child_counts: dict[str, int] = {}
        for tbl in _TRANSFER_CHILD_TABLES:
            try:
                col_exists = db.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:tbl AND column_name='created_by'
                """), {"tbl": tbl}).fetchone()
                if col_exists:
                    cnt = db.execute(text(
                        f"SELECT COUNT(*) FROM public.{tbl} WHERE application_id = :aid AND created_by IS NOT NULL"
                    ), {"aid": app_id}).scalar() or 0
                    child_counts[tbl] = cnt
            except Exception:
                pass

        cert_count = db.execute(text("""
            SELECT COUNT(*) FROM public.certificates
            WHERE application_id = :aid AND owner_id IS NOT NULL
        """), {"aid": app_id}).scalar() or 0

        return {
            "status": "PREVIEW",
            "application_number": application_number,
            "application_id": app_id,
            "current_created_by": current_cby,
            "current_username": current_uname,
            "target_user_id": target_user_id,
            "target_username": target_username,
            "changes": {
                "applications": {"created_by": True, "username": True, "rows": 1},
                "certificates": {"owner_id": True, "rows": cert_count},
                "child_tables": child_counts,
            },
            "message": (
                f"Will transfer application '{application_number}' from "
                f"'{current_uname}' → '{target_username}'. "
                f"Run with dry_run=false on the POST endpoint to apply."
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("transfer-ownership/preview failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


@router.post('/transfer-ownership', tags=["06 - Transfer Ownership"])
def transfer_ownership(
    application_number: str = Query(..., description="Application number to transfer"),
    new_username: str = Query(None, description="Target username (either this or new_user_id)"),
    new_user_id: str = Query(None, description="Target user UUID (either this or new_username)"),
    dry_run: bool = Query(True, description="true = preview only, false = apply changes"),
):
    """
    Transfer ownership of an application to another user.

    Updates:
    - `applications.created_by` + `applications.username`
    - `certificates.owner_id` for all certificates linked to the application
    - `created_by` on every child table that references `applications(id)`

    A full snapshot is saved to `public.transfer_ownership_backup` before any
    changes are made so you can roll back with POST /transfer-ownership/restore.
    """
    if not new_username and not new_user_id:
        raise HTTPException(status_code=422, detail="Provide new_username or new_user_id")

    db = _get_new_session()
    try:
        _ensure_transfer_backup_table(db)

        # ── Purge any previous backup for this application (fresh slate) ──────
        deleted_old = db.execute(text("""
            DELETE FROM public.transfer_ownership_backup
            WHERE application_number = :appno
        """), {"appno": application_number}).rowcount or 0
        if deleted_old:
            logger.info(
                "transfer-ownership: cleared %d stale backup row(s) for %s",
                deleted_old, application_number,
            )
        db.commit()

        # ── Resolve application ───────────────────────────────────────────────
        app_row = db.execute(text("""
            SELECT id, application_number, created_by, username
            FROM public.applications
            WHERE application_number = :appno
              AND deleted_at IS NULL
        """), {"appno": application_number}).fetchone()

        if not app_row:
            raise HTTPException(status_code=404, detail=f"Application '{application_number}' not found")

        app_id           = str(app_row[0])
        current_cby      = str(app_row[2]) if app_row[2] else None
        current_username = app_row[3]

        # ── Resolve target user ───────────────────────────────────────────────
        if new_user_id:
            user_row = db.execute(text("""
                SELECT id, username FROM public.users
                WHERE id = :uid AND deleted = false
            """), {"uid": new_user_id}).fetchone()
        else:
            user_row = db.execute(text("""
                SELECT id, username FROM public.users
                WHERE lower(trim(username)) = lower(trim(:uname)) AND deleted = false
            """), {"uname": new_username}).fetchone()

        if not user_row:
            raise HTTPException(status_code=404, detail="Target user not found or is deleted")

        target_user_id  = str(user_row[0])
        target_username = user_row[1]

        if dry_run:
            # Re-use preview logic
            return transfer_ownership_preview(
                application_number=application_number,
                new_username=new_username,
                new_user_id=new_user_id,
            )

        # ── Backup: applications row ──────────────────────────────────────────
        db.execute(text("""
            INSERT INTO public.transfer_ownership_backup
                (application_number, application_id, table_name, row_id, old_created_by, old_username, old_owner_id)
            SELECT
                :appno, id, 'applications', id, created_by, username, NULL
            FROM public.applications
            WHERE id = :aid
        """), {"appno": application_number, "aid": app_id})

        # ── Backup: certificates ──────────────────────────────────────────────
        db.execute(text("""
            INSERT INTO public.transfer_ownership_backup
                (application_number, application_id, table_name, row_id, old_created_by, old_username, old_owner_id)
            SELECT
                :appno, :aid, 'certificates', id, created_by, NULL, owner_id
            FROM public.certificates
            WHERE application_id = :aid
        """), {"appno": application_number, "aid": app_id})

        # ── Backup: child tables ──────────────────────────────────────────────
        for tbl in _TRANSFER_CHILD_TABLES:
            try:
                col_exists = db.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:tbl AND column_name='created_by'
                """), {"tbl": tbl}).fetchone()
                if col_exists:
                    db.execute(text(f"""
                        INSERT INTO public.transfer_ownership_backup
                            (application_number, application_id, table_name, row_id, old_created_by, old_username, old_owner_id)
                        SELECT
                            :appno, :aid, :tbl, id, created_by, NULL, NULL
                        FROM public.{tbl}
                        WHERE application_id = :aid AND created_by IS NOT NULL
                    """), {"appno": application_number, "aid": app_id, "tbl": tbl})
            except Exception as bk_err:
                logger.warning("transfer-ownership: backup skipped for %s: %s", tbl, bk_err)

        db.commit()
        logger.info("transfer-ownership: backup snapshot saved for application %s", application_number)

        # ── Apply: applications ───────────────────────────────────────────────
        r_app = db.execute(text("""
            UPDATE public.applications
            SET created_by = CAST(:new_uid AS uuid),
                username   = :new_uname,
                updated_at = now()
            WHERE id = CAST(:aid AS uuid)
        """), {"new_uid": target_user_id, "new_uname": target_username, "aid": app_id})
        db.commit()

        # ── Apply: certificates (owner_id + created_by) ───────────────────────
        r_cert = db.execute(text("""
            UPDATE public.certificates
            SET owner_id   = CAST(:new_uid AS uuid),
                created_by = CAST(:new_uid AS uuid),
                updated_at = now()
            WHERE application_id = CAST(:aid AS uuid)
        """), {"new_uid": target_user_id, "aid": app_id})
        db.commit()

        # ── Apply: child tables ───────────────────────────────────────────────
        child_results: dict[str, int] = {}
        for tbl in _TRANSFER_CHILD_TABLES:
            try:
                col_exists = db.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:tbl AND column_name='created_by'
                """), {"tbl": tbl}).fetchone()
                if col_exists:
                    r = db.execute(text(f"""
                        UPDATE public.{tbl}
                        SET created_by = CAST(:new_uid AS uuid)
                        WHERE application_id = CAST(:aid AS uuid)
                          AND created_by IS NOT NULL
                    """), {"new_uid": target_user_id, "aid": app_id})
                    db.commit()
                    child_results[tbl] = r.rowcount or 0
            except Exception as upd_err:
                logger.warning("transfer-ownership: update skipped for %s: %s", tbl, upd_err)
                try:
                    db.rollback()
                except Exception:
                    pass

        logger.info(
            "transfer-ownership: application=%s transferred from '%s' → '%s'",
            application_number, current_username, target_username,
        )

        return {
            "status": "OK",
            "application_number": application_number,
            "application_id": app_id,
            "previous_owner": {"created_by": current_cby, "username": current_username},
            "new_owner": {"user_id": target_user_id, "username": target_username},
            "rows_updated": {
                "applications": r_app.rowcount or 0,
                "certificates": r_cert.rowcount or 0,
                "child_tables": child_results,
            },
            "backup_table": "public.transfer_ownership_backup",
            "message": (
                f"Application '{application_number}' ownership transferred from "
                f"'{current_username}' to '{target_username}'. "
                f"Backup saved — use POST /transfer-ownership/restore to roll back."
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("transfer-ownership failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


@router.post('/transfer-ownership/restore', tags=["06 - Transfer Ownership"])
def transfer_ownership_restore(
    application_number: str = Query(..., description="Application number to roll back"),
    dry_run: bool = Query(True, description="true = preview what will be restored, false = apply"),
):
    """
    Roll back a previous transfer by restoring values from the backup snapshot
    in `public.transfer_ownership_backup`.
    """
    db = _get_new_session()
    try:
        # ── Check backup exists ───────────────────────────────────────────────
        backup_rows = db.execute(text("""
            SELECT id, table_name, row_id, old_created_by, old_username, old_owner_id, backed_up_at
            FROM public.transfer_ownership_backup
            WHERE application_number = :appno
            ORDER BY backed_up_at DESC, id DESC
        """), {"appno": application_number}).fetchall()

        if not backup_rows:
            raise HTTPException(
                status_code=404,
                detail=f"No backup snapshot found for application '{application_number}'"
            )

        backed_up_at = str(backup_rows[0][6])
        summary = {}
        for r in backup_rows:
            summary.setdefault(r[1], 0)
            summary[r[1]] += 1

        if dry_run:
            return {
                "status": "PREVIEW",
                "application_number": application_number,
                "backed_up_at": backed_up_at,
                "backup_rows_by_table": summary,
                "message": "Set dry_run=false to apply the restore.",
            }

        # ── Restore: applications ─────────────────────────────────────────────
        app_backup = next((r for r in backup_rows if r[1] == "applications"), None)
        r_app = 0
        if app_backup:
            res = db.execute(text("""
                UPDATE public.applications
                SET created_by = CAST(:old_cby AS uuid),
                    username   = :old_uname,
                    updated_at = now()
                WHERE id = CAST(:rid AS uuid)
            """), {
                "old_cby":   str(app_backup[3]) if app_backup[3] else None,
                "old_uname": app_backup[4],
                "rid":       str(app_backup[2]),
            })
            r_app = res.rowcount or 0
        db.commit()

        # ── Restore: certificates ─────────────────────────────────────────────
        r_cert = 0
        for r in backup_rows:
            if r[1] != "certificates":
                continue
            res = db.execute(text("""
                UPDATE public.certificates
                SET owner_id   = CAST(:old_owner AS uuid),
                    created_by = CAST(:old_cby AS uuid),
                    updated_at = now()
                WHERE id = CAST(:rid AS uuid)
            """), {
                "old_owner": str(r[5]) if r[5] else None,
                "old_cby":   str(r[3]) if r[3] else None,
                "rid":       str(r[2]),
            })
            r_cert += res.rowcount or 0
        db.commit()

        # ── Restore: child tables ─────────────────────────────────────────────
        child_results: dict[str, int] = {}
        for r in backup_rows:
            tbl = r[1]
            if tbl in ("applications", "certificates"):
                continue
            try:
                res = db.execute(text(f"""
                    UPDATE public.{tbl}
                    SET created_by = CAST(:old_cby AS uuid)
                    WHERE id = CAST(:rid AS uuid)
                """), {"old_cby": str(r[3]) if r[3] else None, "rid": str(r[2])})
                db.commit()
                child_results[tbl] = child_results.get(tbl, 0) + (res.rowcount or 0)
            except Exception as re_err:
                logger.warning("transfer-ownership/restore: skipped %s row %s: %s", tbl, r[2], re_err)
                try:
                    db.rollback()
                except Exception:
                    pass

        # ── Clean up backup rows ──────────────────────────────────────────────
        db.execute(text("""
            DELETE FROM public.transfer_ownership_backup
            WHERE application_number = :appno
        """), {"appno": application_number})
        db.commit()

        logger.info("transfer-ownership/restore: application=%s rolled back", application_number)

        return {
            "status": "RESTORED",
            "application_number": application_number,
            "rows_restored": {
                "applications": r_app,
                "certificates": r_cert,
                "child_tables": child_results,
            },
            "backup_rows_deleted": len(backup_rows),
            "message": (
                f"Ownership of '{application_number}' has been rolled back to the snapshot "
                f"taken at {backed_up_at}."
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("transfer-ownership/restore failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── SQL: fix-region-zones ─────────────────────────────────────────────────────

_SQL_FIX_REGION_UUID = """
UPDATE public.contact_details cd
SET region = nr.name
FROM public.napa_regions nr
WHERE cd.region::uuid = nr.id
  AND cd.region ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
"""

_SQL_FIX_ZONE_FROM_SECTOR = """
UPDATE public.applications a
SET
    zone_id   = z.id,
    zone_name = z.name
FROM
    public.application_sector_details asd,
    public.napa_regions nr,
    public.zones z
WHERE
    a.id = asd.application_id
    AND asd.region = nr.name
    AND nr.zone_id = z.id
    AND asd.region IS NOT NULL
"""

_SQL_FIX_ZONE_FROM_ELECTRICAL = """
UPDATE public.applications a
SET
    zone_id   = z.id,
    zone_name = z.name
FROM
    public.application_electrical_installation aei,
    public.contact_details cd,
    public.napa_regions nr,
    public.zones z
WHERE
    a.id = aei.application_id
    AND cd.application_electrical_installation_id = aei.id
    AND cd.region = nr.name
    AND nr.zone_id = z.id
"""

_SQL_PREVIEW_REGION_UUID = """
SELECT COUNT(*) AS contact_details_to_fix
FROM public.contact_details cd
WHERE cd.region ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND EXISTS (
      SELECT 1 FROM public.napa_regions nr
      WHERE cd.region::uuid = nr.id
  )
"""

_SQL_PREVIEW_ZONE_FROM_SECTOR = """
SELECT COUNT(*) AS applications_to_fix_via_sector
FROM public.applications a
JOIN public.application_sector_details asd ON a.id = asd.application_id
JOIN public.napa_regions nr ON asd.region = nr.name
JOIN public.zones z ON nr.zone_id = z.id
WHERE asd.region IS NOT NULL
  AND (a.zone_id IS DISTINCT FROM z.id OR a.zone_name IS DISTINCT FROM z.name)
"""

_SQL_PREVIEW_ZONE_FROM_ELECTRICAL = """
SELECT COUNT(*) AS applications_to_fix_via_electrical
FROM public.applications a
JOIN public.application_electrical_installation aei ON a.id = aei.application_id
JOIN public.contact_details cd ON cd.application_electrical_installation_id = aei.id
JOIN public.napa_regions nr ON cd.region = nr.name
JOIN public.zones z ON nr.zone_id = z.id
WHERE a.zone_id IS DISTINCT FROM z.id
   OR a.zone_name IS DISTINCT FROM z.name
"""


@router.post('/fix-region-zones', tags=["06 - Data Fixes"])
def fix_region_zones(dry_run: bool = True):
    """Fix UUID region values in contact_details and backfill zone_id/zone_name in applications.

    Runs three updates in order:

    1. **contact_details** — replaces UUID strings in `region` with proper region names
       from `napa_regions`.
    2. **applications (sector path)** — sets `zone_id`/`zone_name` via
       `application_sector_details → napa_regions → zones`.
    3. **applications (electrical path)** — sets `zone_id`/`zone_name` via
       `application_electrical_installation → contact_details → napa_regions → zones`.

    - `dry_run=true`  → returns row counts of what *would* be updated (no changes)
    - `dry_run=false` → executes all three updates and returns affected row counts
    """
    db = _get_new_session()
    try:
        if dry_run:
            region_uuid_count = db.execute(text(_SQL_PREVIEW_REGION_UUID)).scalar() or 0
            zone_sector_count = db.execute(text(_SQL_PREVIEW_ZONE_FROM_SECTOR)).scalar() or 0
            zone_elec_count   = db.execute(text(_SQL_PREVIEW_ZONE_FROM_ELECTRICAL)).scalar() or 0
            return {
                "status": "DRY_RUN",
                "would_update": {
                    "contact_details_uuid_regions":            region_uuid_count,
                    "applications_zone_via_sector_details":    zone_sector_count,
                    "applications_zone_via_electrical":        zone_elec_count,
                },
            }

        # ── Step 1: fix UUID region values in contact_details ────────────────
        r1 = db.execute(text(_SQL_FIX_REGION_UUID))
        db.commit()

        # ── Step 2: backfill zone from application_sector_details ────────────
        r2 = db.execute(text(_SQL_FIX_ZONE_FROM_SECTOR))
        db.commit()

        # ── Step 3: backfill zone from electrical installation contact ───────
        r3 = db.execute(text(_SQL_FIX_ZONE_FROM_ELECTRICAL))
        db.commit()

        rows = {
            "contact_details_uuid_regions_fixed":       r1.rowcount,
            "applications_zone_via_sector_details":     r2.rowcount,
            "applications_zone_via_electrical":         r3.rowcount,
        }

        logger.info("fix-region-zones: %s", rows)

        return {
            "status": "OK",
            "rows_updated": rows,
            "total": sum(rows.values()),
        }

    except Exception as exc:
        db.rollback()
        logger.exception("fix-region-zones failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()


# ── SQL: fix-owner-id ─────────────────────────────────────────────────────────

_SQL_PREVIEW_OWNER_ID = """
SELECT COUNT(*) AS certificates_missing_owner_id
FROM public.certificates cert
JOIN public.applications a ON a.id = cert.application_id
JOIN public.users u ON u.id = a.created_by
WHERE cert.owner_id IS NULL
  AND a.created_by IS NOT NULL
"""

_SQL_FIX_OWNER_ID = """
UPDATE public.certificates cert
SET
    owner_id   = a.created_by,
    updated_at = now()
FROM public.applications a
JOIN public.users u ON u.id = a.created_by
WHERE cert.application_id = a.id
  AND cert.owner_id IS NULL
  AND a.created_by IS NOT NULL
"""


@router.post('/fix-owner-id', tags=["06 - Data Fixes"])
def fix_owner_id(dry_run: bool = True):
    """Backfill `certificates.owner_id` from `applications.created_by`.

    For every certificate whose `owner_id` is NULL, looks up the linked
    application's `created_by` (which is `users.id`) and sets `owner_id`
    to that value.  Only updates rows where a valid user actually exists.

    - `dry_run=true`  → returns the count of certificates that would be updated
    - `dry_run=false` → applies the update and returns affected row count
    """
    db = _get_new_session()
    try:
        missing = int(db.execute(text(_SQL_PREVIEW_OWNER_ID)).scalar() or 0)

        if dry_run:
            return {
                "status": "DRY_RUN",
                "certificates_missing_owner_id": missing,
                "message": (
                    f"{missing} certificate(s) have owner_id=NULL and a resolvable "
                    f"created_by on their application. Re-run with dry_run=false to fix."
                ),
            }

        r = db.execute(text(_SQL_FIX_OWNER_ID))
        updated = r.rowcount or 0
        db.commit()

        logger.info("fix-owner-id: updated %d certificate rows", updated)

        return {
            "status": "OK",
            "certificates_updated": updated,
            "message": (
                f"owner_id backfilled on {updated} certificate row(s) "
                f"using applications.created_by."
            ),
        }

    except Exception as exc:
        db.rollback()
        logger.exception("fix-owner-id failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        db.close()
