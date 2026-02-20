from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uuid as uuid_mod
from datetime import datetime

from app.utils.file_reader import read_users_file
from app.services.electrical_installation_import_service import (
    import_electrical_installations_via_staging_copy,
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

        result = import_electrical_installations_via_staging_copy(
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


@router.post("/upload")
def upload_electrical_installations(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    sync: bool = True,
    background: bool = False,
    include_rows: bool = False,
    limit_rows: int = 50,
):
    """Upload Excel/CSV containing electrical installation records and import into
    `application_electrical_installation`.

    **Required Excel columns** (header names, case-insensitive):

    | Excel column       | Maps to DB column        |
    |--------------------|--------------------------|
    | apprefno           | application_number (join) |
    | installationname   | installation_name        |

    **Optional Excel columns:**

    | Excel column        | Maps to DB column     |
    |---------------------|-----------------------|
    | installationtype    | installation_type     |
    | voltagelevel        | voltage_level         |
    | capacity            | capacity (numeric)    |
    | capacityunit        | capacity_unit         |
    | locationregion      | location_region       |
    | locationdistrict    | location_district     |
    | locationward        | location_ward         |
    | locationstreet      | location_street       |
    | locationplotno      | location_plot_no      |
    | locationhouseno     | location_house_no     |
    | contractorname      | contractor_name       |
    | contractorregno     | contractor_reg_no     |
    | contractoremail     | contractor_email      |
    | contractorphone     | contractor_phone      |
    | supervisorname      | supervisor_name       |
    | supervisorregno     | supervisor_reg_no     |
    | supervisoremail     | supervisor_email      |
    | supervisorphone     | supervisor_phone      |
    | inspectiondate      | inspection_date (date)|
    | completiondate      | completion_date (date)|
    | remarks             | remarks               |
    | filename            | filename              |
    | objectid            | object_id (bigint)    |
    | rowid               | row_id                |

    **Deduplication:** rows are skipped when an existing record already exists for the
    same `application_id` + `installation_name` (case-insensitive).

    - **sync=true** (default): waits for import to complete and returns stats.
    - **background=true**: runs in background and returns a `job_id` immediately.
      Poll `GET /api/v1/electrical-installations/status/{job_id}` for progress.
    """

    try:
        df = read_users_file(file.filename, file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to read uploaded file: {e}")

    if sync and not background:
        # Synchronous path — wait for completion
        db = _get_new_session()
        try:
            result = import_electrical_installations_via_staging_copy(
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
    else:
        # Background path — return job_id immediately
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
                "message": (
                    "Import started in background. "
                    "Check status at /api/v1/electrical-installations/status/{job_id}"
                ),
            },
        )


@router.get("/status/{job_id}")
def get_job_status(job_id: str):
    """Check the status of a background electrical installations import job."""
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get("/jobs")
def list_jobs():
    """List all electrical installation import jobs and their statuses."""
    return _job_status
