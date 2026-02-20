from fastapi import APIRouter, HTTPException
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

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


@router.post('/backfill-created-by', tags=["99 - Backfill created_by"])
def backfill_created_by():
    """Backfill created_by UUID using users.username mapping.

    Updates:
    - applications.created_by
    - documents.created_by (by application_id)
    - contact_persons.created_by (by application_id)
    """

    from app.services.application_migrations_service import backfill_created_by_from_username

    db = _get_new_session()
    try:
        result = backfill_created_by_from_username(db)
        return {"status": "SUCCESS", "result": result}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
