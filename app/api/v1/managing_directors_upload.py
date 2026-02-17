from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uuid as uuid_mod
from datetime import datetime

from app.utils.file_reader import read_users_file
from app.services.managing_directors_import_service import import_managing_directors_via_staging_copy

router = APIRouter(prefix="/api/v1/managing-directors", tags=["05 - Managing Directors Migration"])

_job_status: dict = {}


def _get_new_session():
    from app.core import database as db_module
    db_module._init_engine()
    return db_module._SessionLocal()


def _run_job(job_id: str, df, source_file_name: str):
    _job_status[job_id] = {
        "status": "RUNNING",
        "started_at": datetime.utcnow().isoformat(),
        "progress": "Starting...",
        "source_file_name": source_file_name,
    }

    db = _get_new_session()
    try:
        def _progress(msg: str):
            _job_status[job_id]["progress"] = msg

        result = import_managing_directors_via_staging_copy(
            db,
            df,
            source_file_name=source_file_name,
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
        _job_status[job_id] = {
            "status": "FAILED",
            "failed_at": datetime.utcnow().isoformat(),
            "error": str(e),
        }
    finally:
        db.close()


@router.post("/upload")
def upload_managing_directors(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    sync: bool = True,
    background: bool = False,
):
    """Upload Excel/CSV containing managing directors and import into ca_managing_directors.

    - Default: synchronous (returns detailed stats immediately).
    - To run in background: background=true (or sync=false)
    """

    try:
        df = read_users_file(file.filename, file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to read uploaded file: {e}")

    if sync and not background:
        db = _get_new_session()
        try:
            result = import_managing_directors_via_staging_copy(db, df, source_file_name=file.filename)
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
    background_tasks.add_task(_run_job, job_id, df, file.filename)
    return JSONResponse(
        status_code=202,
        content={
            "status": "ACCEPTED",
            "job_id": job_id,
            "message": "Managing directors import started in background. Check status at /api/v1/managing-directors/status/{job_id}",
        },
    )


@router.get("/status/{job_id}")
def get_job_status(job_id: str):
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get("/jobs")
def list_jobs():
    return _job_status
