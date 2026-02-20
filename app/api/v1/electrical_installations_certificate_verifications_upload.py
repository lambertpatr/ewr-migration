from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uuid as uuid_mod
from datetime import datetime

from app.utils.file_reader import read_users_file
from app.services.electrical_certificate_verifications_import_service import (
    import_electrical_certificate_verifications_via_staging_copy,
)


router = APIRouter(
    prefix="/api/v1/electrical-installations",
    tags=["06 - Electrical Installations Migration"],
)


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

        result = import_electrical_certificate_verifications_via_staging_copy(
            db,
            df,
            source_file_name=source_file_name,
            progress_cb=_progress,
            include_rows=False,
            limit_rows=50,
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


@router.post("/upload-certificate-verifications")
def upload_certificate_verifications(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    sync: bool = True,
    background: bool = False,
    include_rows: bool = False,
    limit_rows: int = 50,
):
    """Upload certificate verifications Excel.

    Expected columns (case-insensitive):
    - apprefno, sno, fromdate, todate, institutenameaddress, award, objectid, filename

    Maps:
    - institutenameaddress -> education_regulatory_body
    - award -> education_regulatory_body_category
    - objectid -> logic_doc_id
    - filename -> file_name
    """

    try:
        df = read_users_file(file.filename, file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to read uploaded file: {e}")

    if sync and not background:
        db = _get_new_session()
        try:
            result = import_electrical_certificate_verifications_via_staging_copy(
                db,
                df,
                source_file_name=file.filename,
                include_rows=include_rows,
                limit_rows=limit_rows,
            )
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
            "message": "Import started in background. Check status at /api/v1/electrical-installations/cert-verifications-status/{job_id}",
        },
    )


@router.get("/cert-verifications-status/{job_id}")
def get_cert_verifications_job_status(job_id: str):
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get("/cert-verifications-jobs")
def list_cert_verifications_jobs():
    return _job_status
