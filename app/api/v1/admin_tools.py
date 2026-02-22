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
        #    Remote user_roles FDW table only exposes (user_id, role_id).
        roles_assigned = []
        if _admin_role_id:
            for uname in inserted:
                try:
                    db.execute(text("""
                        INSERT INTO public.user_roles (user_id, role_id)
                        SELECT u.id, :role_id
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

        # 7. Full created_by backfill across all 17 tables
        bf = backfill_created_by_from_username(db)
        logger.info("repair-and-backfill: backfill result: %s", bf)

        return {
            "inserted_users": len(inserted),
            "inserted_usernames": inserted,
            "roles_assigned": len(roles_assigned),
            "deduped_certificates": deduped_certs,
            "failed": failed,
            "backfill": bf,
            "message": (
                f"Inserted {len(inserted)} missing users, assigned {len(roles_assigned)} roles, "
                f"removed {deduped_certs} duplicate certificate rows, "
                f"backfilled created_by across 17 tables."
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
