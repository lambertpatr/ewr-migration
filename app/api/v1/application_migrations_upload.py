from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import logging
import uuid as uuid_mod
from datetime import datetime
from pathlib import Path
from enum import Enum

from app.core.database import get_db
from app.utils.file_reader import read_users_file
from app.services.application_migrations_service import (
    import_applications_from_df,
    import_applications_via_staging_copy,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter(prefix="/api/v1/application-migrations", tags=["03 - Applications Migration"])

class SectorName(str, Enum):
    PETROLEUM = "PETROLEUM"
    NATURAL_GAS = "NATURAL_GAS"
    ELECTRICITY = "ELECTRICITY"
    WATER_SUPPLY = "WATER_SUPPLY"

# Simple in-memory job status tracker
_job_status: dict = {}


def _get_new_session():
    """Get a new DB session, initializing the engine if needed."""
    from app.core import database as db_module
    db_module._init_engine()
    return db_module._SessionLocal()


def _run_import_job(job_id: str, df, sector_name: str = "PETROLEUM"):
    """Background task that runs the import and updates job status."""
    global _job_status
    _job_status[job_id] = {"status": "RUNNING", "started_at": datetime.utcnow().isoformat(), "progress": "Starting..."}
    logger.info("[Job %s] Starting import of %d rows (sector=%s)", job_id, len(df), sector_name)
    
    try:
        db = _get_new_session()
        try:
            def _progress(msg: str):
                # lightweight last-known progress for polling
                _job_status[job_id]["progress"] = msg

            # For large files (e.g. 500k rows) on a remote DB, staging+COPY is dramatically faster.
            result = import_applications_via_staging_copy(db, df, progress_cb=_progress, sector_name=sector_name)
            db.commit()
            _job_status[job_id] = {
                "status": "COMPLETED",
                "completed_at": datetime.utcnow().isoformat(),
                "result": result
            }
            logger.info("[Job %s] Import completed: %s", job_id, result)
        except Exception as e:
            db.rollback()
            _job_status[job_id] = {
                "status": "FAILED",
                "error": str(e),
                "failed_at": datetime.utcnow().isoformat()
            }
            # Keep logs concise by default: SQLAlchemy exceptions can include huge SQL strings.
            # Full traceback/SQL is still available by enabling DEBUG logs.
            logger.error("[Job %s] Import failed: %s: %s", job_id, type(e).__name__, str(e))
            logger.debug("[Job %s] Import failed (debug traceback)", job_id, exc_info=True)
        finally:
            db.close()
    except Exception as e:
        _job_status[job_id] = {"status": "FAILED", "error": str(e)}
        logger.exception("[Job %s] Failed to initialize DB: %s", job_id, e)


@router.post('/upload')
def upload_application_migrations(
    sector_name: SectorName = Form(SectorName.PETROLEUM),
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    sync: bool = False
):
    """
    Upload and import applications from Excel/CSV.
    
    - Use sector_name (NATURAL_GAS, PETROLEUM, ELECTRICITY, WATER_SUPPLY) 
      to associate new categories with the correct sector.
    - By default (sync=false), the import runs in the background and you get a job_id immediately.
    - Use GET /status/{job_id} to check progress.
    - Set sync=true to wait for the import to complete (may timeout for large files).
    """
    logger.info("Received upload request: %s (sector=%s) (sync=%s)", file.filename, sector_name, sync)
    
    # Use the enum value (string) for downstream logic
    sector_str = sector_name.value

    try:
        df = read_users_file(file.filename, file.file)
        logger.info("Read %d rows from file", len(df))
    except Exception as e:
        logger.exception("Failed to read uploaded file")
        raise HTTPException(status_code=400, detail=f"failed to read uploaded file: {e}")

    if sync:
        # Synchronous mode: wait for completion (may timeout)
        db = _get_new_session()
        try:
            result = import_applications_via_staging_copy(db, df, sector_name=sector_str)
            db.commit()
            logger.info("Import completed: %s", result)
            return {"status": "SUCCESS", "result": result}
        except Exception as e:
            db.rollback()
            logger.exception("Import failed")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            db.close()
    else:
        # Async mode: run in background and return job_id immediately
        job_id = str(uuid_mod.uuid4())
        _job_status[job_id] = {"status": "QUEUED", "queued_at": datetime.utcnow().isoformat()}
        background_tasks.add_task(_run_import_job, job_id, df, sector_str)
        logger.info("Queued background import job: %s", job_id)
        return JSONResponse(
            status_code=202,
            content={
                "status": "ACCEPTED",
                "job_id": job_id,
                "message": "Import started in background. Check status at /api/v1/application-migrations/status/{job_id}"
            }
        )


@router.get('/status/{job_id}')
def get_job_status(job_id: str):
    """Check the status of a background import job."""
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get('/jobs')
def list_jobs():
    """List all import jobs and their statuses."""
    return _job_status



