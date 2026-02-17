from fastapi import APIRouter, HTTPException
from pathlib import Path
from sqlalchemy import text

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
            "ca_applications_unique_constraints": None,
            "ca_documents_has_logic_doc_id": None,
            "ca_shareholders_has_logic_doc_id": None,
            "ca_applications_has_application_number_unique": None,
        }
        try:
            before["ca_applications_unique_constraints"] = int(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.ca_applications'::regclass AND contype = 'u'"
                    )
                ).scalar()
                or 0
            )
            before["ca_documents_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_documents' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            before["ca_shareholders_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_shareholders' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            before["ca_applications_has_application_number_unique"] = bool(
                db.execute(
                    text(
                        """
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.ca_applications'::regclass
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

        db.execute(text(sql_text))
        db.commit()

        after = {
            "ca_applications_unique_constraints": None,
            "ca_documents_has_logic_doc_id": None,
            "ca_shareholders_has_logic_doc_id": None,
            "ca_applications_has_application_number_unique": None,
        }
        try:
            after["ca_applications_unique_constraints"] = int(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.ca_applications'::regclass AND contype = 'u'"
                    )
                ).scalar()
                or 0
            )
            after["ca_documents_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_documents' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            after["ca_shareholders_has_logic_doc_id"] = bool(
                db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_shareholders' AND column_name='logic_doc_id'"
                    )
                ).first()
            )
            after["ca_applications_has_application_number_unique"] = bool(
                db.execute(
                    text(
                        """
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.ca_applications'::regclass
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
                    (before.get("ca_applications_unique_constraints") - after.get("ca_applications_unique_constraints"))
                    if isinstance(before.get("ca_applications_unique_constraints"), int)
                    and isinstance(after.get("ca_applications_unique_constraints"), int)
                    else None
                ),
                "added_ca_documents_logic_doc_id": (
                    (not before.get("ca_documents_has_logic_doc_id")) and bool(after.get("ca_documents_has_logic_doc_id"))
                    if before.get("ca_documents_has_logic_doc_id") is not None and after.get("ca_documents_has_logic_doc_id") is not None
                    else None
                ),
                "added_ca_shareholders_logic_doc_id": (
                    (not before.get("ca_shareholders_has_logic_doc_id")) and bool(after.get("ca_shareholders_has_logic_doc_id"))
                    if before.get("ca_shareholders_has_logic_doc_id") is not None and after.get("ca_shareholders_has_logic_doc_id") is not None
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
    finally:
        db.close()


@router.post('/backfill-created-by', tags=["99 - Backfill created_by"])
def backfill_created_by():
    """Backfill created_by UUID using users.username mapping.

    Updates:
    - ca_applications.created_by
    - ca_documents.created_by (by application_id)
    - ca_contact_persons.created_by (by application_id)
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
