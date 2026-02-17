from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import uuid as uuid_mod
from datetime import datetime

from app.core.database import get_db
from app.utils.file_reader import read_users_file
from app.services.shareholders_import_service import import_shareholders_via_staging_copy

router = APIRouter(prefix="/api/v1/shareholders", tags=["04 - Shareholders Migration"])

# Simple in-memory job status tracker
_job_status: dict = {}


def _get_new_session():
    """Get a new DB session, initializing the engine if needed."""
    from app.core import database as db_module
    db_module._init_engine()
    return db_module._SessionLocal()


def _run_shareholders_job(job_id: str, df):
    global _job_status
    _job_status[job_id] = {
        "status": "RUNNING",
        "started_at": datetime.utcnow().isoformat(),
        "progress": "Starting...",
    }

    db = _get_new_session()
    try:
        def _progress(msg: str):
            _job_status[job_id]["progress"] = msg

        result = import_shareholders_via_staging_copy(
            db,
            df,
            source_file_name=_job_status[job_id].get("source_file_name"),
            progress_cb=_progress,
        )
        db.commit()
        _job_status[job_id] = {
            "status": "COMPLETED",
            "completed_at": datetime.utcnow().isoformat(),
            "result": result,
        }
    except Exception as e:
        db.rollback()
        # Store error details for debugging, including constraint errors.
        _job_status[job_id] = {
            "status": "FAILED",
            "failed_at": datetime.utcnow().isoformat(),
            "error": str(e),
        }
    finally:
        db.close()


@router.post("/upload")
def upload_shareholders(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    sync: bool = True,
    background: bool = False,
):
    """Upload Excel/CSV containing shareholders and import into ca_shareholders.

    - By default (sync=false), runs in background and returns a job_id.
    - Poll GET /status/{job_id} for progress.
    - Set sync=true to run synchronously.
    """

    try:
        df = read_users_file(file.filename, file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to read uploaded file: {e}")

    # Default behavior: return detailed stats immediately.
    # If you want async/background mode, pass background=true (or sync=false).
    if sync and not background:
        db = _get_new_session()
        try:
            result = import_shareholders_via_staging_copy(db, df, source_file_name=file.filename)
            db.commit()
            return {"status": "SUCCESS", "result": result}
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            db.close()

    job_id = str(uuid_mod.uuid4())
    _job_status[job_id] = {
        "status": "QUEUED",
        "queued_at": datetime.utcnow().isoformat(),
        "source_file_name": file.filename,
    }
    background_tasks.add_task(_run_shareholders_job, job_id, df)
    return JSONResponse(
        status_code=202,
        content={
            "status": "ACCEPTED",
            "job_id": job_id,
            "message": "Shareholders import started in background. Check status at /api/v1/shareholders/status/{job_id}",
        },
    )


@router.get("/status/{job_id}")
def get_shareholders_job_status(job_id: str):
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get("/jobs")
def list_shareholders_jobs():
    return _job_status
