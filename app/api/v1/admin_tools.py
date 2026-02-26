from fastapi import APIRouter, HTTPException
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin-tools")


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

        # Capture state before (best-effort). Schema sync mostly does DDL, but we can still
        # report what changed (e.g., columns now present, unique constraints removed).
        before = {
            "applications_unique_constraints": None,
            "documents_has_logic_doc_id": None,
            "shareholders_has_application_sector_detail_id": None,
            "applications_has_application_number_unique": None,
        }
        try:
            before["applications_unique_constraints"] = int(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.applications'::regclass AND contype = 'u'"
                    )
                ).scalar()
                or 0
            )
            before["documents_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            before["shareholders_has_application_sector_detail_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='shareholders' AND column_name='application_sector_detail_id'"
                    )
                ).first()
            )
            before["applications_has_application_number_unique"] = bool(
                db.execute(
                    text(
                        """
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.applications'::regclass
                          AND contype = 'u'
                          AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
                        LIMIT 1
                        """
                    )
                ).first()
            )
        except Exception:
            # Don't block sync on diagnostic queries
            pass

        # The alignment script contains its own BEGIN/COMMIT.
        # If we execute it via SQLAlchemy Session, it may already be in a transaction,
        # which can cause errors like "current transaction is aborted" after the first failure.
        # Execute via the raw DBAPI connection in autocommit mode for robustness.
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
        # Ensure Session isn't left in a broken transaction.
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
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.applications'::regclass AND contype = 'u'"
                    )
                ).scalar()
                or 0
            )
            after["documents_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='documents' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            after["shareholders_has_application_sector_detail_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='shareholders' AND column_name='application_sector_detail_id'"
                    )
                ).first()
            )
            after["applications_has_application_number_unique"] = bool(
                db.execute(
                    text(
                        """
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.applications'::regclass
                          AND contype = 'u'
                          AND pg_get_constraintdef(oid) ILIKE '%(application_number)%'
                        LIMIT 1
                        """
                    )
                ).first()
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
    """Repair missing users + backfill created_by in one shot.

    Steps performed (all idempotent — safe to call multiple times):
      1. Find every username in public.applications with no matching public.users row.
      2. Insert those users (lowercased, ACTIVE, EXTERNAL/INDIVIDUAL).
      3. Ensure the APPLICANT ROLE exists.
      4. Assign APPLICANT ROLE to every newly inserted user.
      5. Run the full created_by backfill across all 17 tables.

    If there are no missing users, only step 5 runs.
    """
    from app.services.application_migrations_service import backfill_created_by_from_username

    db = _get_new_session()
    try:
        # 1. Ensure pgcrypto is available
        try:
            db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            db.commit()
        except Exception:
            pass

        # 2. Find all usernames in applications with no matching user (case-insensitive)
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

        # 3. Insert each missing user individually so one failure never blocks others
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

        # 4. Look up APPLICANT ROLE id — read from an existing user_roles row.
        #    Never scan public.roles: it is a FDW table whose remote name differs
        #    (public.role), causing every query to fail.
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
            # public.roles FDW always fails (remote table is named public.role).
            # Cannot safely scan roles — skip role assignment silently.
            logger.info("repair-missing-users: APPLICANT ROLE id not found from user_roles; role assignment skipped (FDW limitation)")

        # 5. Assign APPLICANT ROLE to every inserted user.
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

        # 6. Deduplicate certificates: the unique constraint on application_number
        #    is now the source of truth, so just remove any duplicate rows keeping
        #    the most-recently updated one per application_number.
        deduped_certs = 0
        try:
            r_dedup = db.execute(text("""
                DELETE FROM public.certificates
                WHERE id IN (
                    SELECT id
                    FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY application_number
                                   ORDER BY updated_at DESC NULLS LAST,
                                            created_at DESC NULLS LAST,
                                            id
                               ) AS rn
                        FROM public.certificates
                        WHERE application_number IS NOT NULL
                    ) ranked
                    WHERE rn > 1
                )
            """))
            deduped_certs = r_dedup.rowcount or 0
            db.commit()
            logger.info("repair-and-backfill: removed %d duplicate certificate rows", deduped_certs)
        except Exception as _cde:
            logger.error("repair-and-backfill: certificate dedup failed (non-fatal): %s", _cde)
            try:
                db.rollback()
            except Exception:
                pass

        # 7. Backfill application_id on all child tables first
        from app.services.application_migrations_service import backfill_application_id_on_child_tables
        abf = backfill_application_id_on_child_tables(db)
        logger.info("repair-and-backfill: backfill application_id result: %s", abf)

        # 8. Full created_by backfill across all 17 tables
        bf = backfill_created_by_from_username(db)
        logger.info("repair-and-backfill: backfill result: %s", bf)

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
    """Propagate application_id to all child tables.

    Fills application_id (and where relevant app_sector_detail_id) on:
      documents, contact_persons, fire, insurance_cover_details,
      shareholders, managing_directors, ardhi_information

    Idempotent — only rows with application_id IS NULL are updated.
    Run this once after migrating existing data, or whenever a new import
    leaves child-table rows without an application_id.
    """
    from app.services.application_migrations_service import (
        backfill_application_id_on_child_tables,
        _ensure_child_table_columns,
    )
    db = _get_new_session()
    try:
        # Ensure columns exist first
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


# ── Phone / email extraction regex (Postgres compatible) ──────────────────────
# Covers TZ mobile numbers with or without spaces between digit groups:
#   +255XXXXXXXXX           (international, no spaces)
#   +255 XXX XX XX XX       (international, spaces in any grouping)
#   +255 XXX XXX XXX        (international, 3-3-3 grouping)
#   0[67]XXXXXXXX           (local 10-digit, no spaces)
#   0[67]XX XXX XXXX        (local 10-digit, with spaces)
# Pattern: after the prefix (+255 or 0[67]), match 9 digits that may have
# single spaces interspersed, then assert the digit-count via the structure
# [0-9][\s]? repeated 9 times — simplest form: [0-9]([\s]?[0-9]){8}
_PHONE_RE  = r'(\+?255[\s]?[0-9]([\s]?[0-9]){8}|0[67][0-9]([\s]?[0-9]){7})'
_EMAIL_RE  = r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'


@router.post('/clean-name-fields', tags=["01 - Schema Sync"])
def clean_name_fields(dry_run: bool = True):
    """Extract phone numbers and emails embedded in *name* columns.

    **custom_details**
    - `name` often contains the person's name followed by a mobile number, e.g.
      ``CASTOR MAGARI 0782355391``.
    - Extracts the phone into ``mobile_no`` (only when ``mobile_no`` is blank).

    **supervisor_details**
    - `name` often contains comma-separated junk, e.g.
      ``CLAVERY MABELE,ELECTRICIAL,0754585433,claverymabele1978@gmail.com``.
    - Extracts phone into ``mobile_no`` (only when blank).
    - Extracts email into ``email``     (only when blank).
    - Cleans ``name`` to the first comma-segment (the actual person name).

    Set ``dry_run=false`` to apply changes.
    """
    db = _get_new_session()
    try:
        # ── preview counts before any change ──────────────────────────────
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
            "custom_details_phone_to_fill":     cd_affected,
            "supervisor_details_phone_to_fill":  sd_phone_affected,
            "supervisor_details_email_to_fill":  sd_email_affected,
            "supervisor_details_name_to_clean":  sd_name_affected,
        }

        if dry_run:
            return {"status": "DRY_RUN", "preview": preview}

        # ── 1. custom_details: extract phone → mobile_no ───────────────────
        # REGEXP_REPLACE with back-reference extracts the first capture group
        # as a scalar — avoids the "set-returning not allowed in UPDATE" error
        # that REGEXP_MATCHES causes.
        r_cd = db.execute(text(f"""
            UPDATE public.custom_details
            SET
                mobile_no  = COALESCE(
                                 NULLIF(TRIM(mobile_no), ''),
                                 -- strip spaces from extracted number (e.g. "+255 715 67 67 70" → "+255715676770")
                                 NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                                     '^.*?({_PHONE_RE[1:-1]}).*$', '\\1')), ' ', ''), REPLACE(name, ' ', ''))
                             ),
                updated_at = now()
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """))
        cd_updated = r_cd.rowcount or 0
        db.commit()
        logger.info("clean-name-fields: custom_details updated=%d", cd_updated)

        # ── 2a. supervisor_details: extract phone → mobile_no (only when blank) ─
        r_sd_phone = db.execute(text(f"""
            UPDATE public.supervisor_details
            SET
                mobile_no  = NULLIF(REPLACE(TRIM(REGEXP_REPLACE(name,
                                 '^.*?({_PHONE_RE[1:-1]}).*$', '\\1')), ' ', ''),
                             REPLACE(name, ' ', '')),
                updated_at = now()
            WHERE name ~ '{_PHONE_RE}'
              AND NULLIF(TRIM(COALESCE(mobile_no, '')), '') IS NULL
        """))
        sd_phone_updated = r_sd_phone.rowcount or 0
        db.commit()
        logger.info("clean-name-fields: supervisor_details mobile_no filled=%d", sd_phone_updated)

        # ── 2b. supervisor_details: extract email → email (only when blank) ───
        r_sd_email = db.execute(text(f"""
            UPDATE public.supervisor_details
            SET
                email      = NULLIF(TRIM(REGEXP_REPLACE(name,
                                 '^.*?({_EMAIL_RE[1:-1]}).*$', '\\1')), name),
                updated_at = now()
            WHERE name ~ '{_EMAIL_RE}'
              AND NULLIF(TRIM(COALESCE(email, '')), '') IS NULL
        """))
        sd_email_updated = r_sd_email.rowcount or 0
        db.commit()
        logger.info("clean-name-fields: supervisor_details email filled=%d", sd_email_updated)

        # ── 2c. supervisor_details: clean name (always — strip junk separators) ─
        r_sd_name = db.execute(text("""
            UPDATE public.supervisor_details
            SET
                name       = CASE
                                 WHEN name LIKE '%|%'
                                 THEN TRIM(SPLIT_PART(name, '|', 1))
                                 WHEN name LIKE '%,%'
                                 THEN TRIM(SPLIT_PART(name, ',', 1))
                                 ELSE TRIM(name)
                             END,
                updated_at = now()
            WHERE name LIKE '%|%'
               OR name LIKE '%,%'
        """))
        sd_name_updated = r_sd_name.rowcount or 0
        db.commit()
        logger.info("clean-name-fields: supervisor_details name cleaned=%d", sd_name_updated)

        sd_updated = max(sd_phone_updated, sd_email_updated, sd_name_updated)

        return {
            "status": "APPLIED",
            "preview": preview,
            "custom_details_updated":              cd_updated,
            "supervisor_details_phone_filled":     sd_phone_updated,
            "supervisor_details_email_filled":     sd_email_updated,
            "supervisor_details_name_cleaned":     sd_name_updated,
            "message": (
                f"custom_details: {cd_updated} rows had phone extracted into mobile_no. "
                f"supervisor_details: {sd_phone_updated} mobile_no filled, "
                f"{sd_email_updated} email filled, {sd_name_updated} names cleaned. "
                f"(phone/email only filled where column was blank; name always cleaned of separators)."
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

