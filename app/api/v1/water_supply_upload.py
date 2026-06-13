"""POST /api/v1/water-supply/upload

Upload the Water Supply Excel/CSV and import into:
  - applications
  - application_sector_details  (vrn, gn_gazette_on, government_notice_no, …)
  - bank_details_tanzania
  - financial_information       (application_sector_detail_id + application_id only)
  - documents                   (one row per attachment pair)
"""

import logging
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File, Form, Query

from app.core.database import _db_env_var, DBEnv
from app.utils.file_reader import read_users_file
from app.services.water_supply_import_service import import_water_supply_via_staging
from app.utils.post_import_hooks import run_post_import_hooks

router = APIRouter(prefix="/api/v1/water-supply", tags=["07 - Water Supply Migration"])

logger = logging.getLogger(__name__)

_job_status: dict = {}


def _get_new_session():
    from app.core import database as db_module
    return db_module.new_session()


# ── Background job runner ─────────────────────────────────────────────────────

def _run_job(job_id: str, df, source_file_name: str, db_env: str = "dev"):
    _db_env_var.set(db_env)
    _job_status[job_id] = {
        "status": "RUNNING",
        "started_at": datetime.utcnow().isoformat(),
        "progress": "Starting…",
        "source_file_name": source_file_name,
    }

    db = _get_new_session()
    try:
        def _cb(msg: str):
            _job_status[job_id]["progress"] = msg

        result = import_water_supply_via_staging(db, df, progress_cb=_cb)
        db.commit()
        hooks_result = run_post_import_hooks(db, progress_cb=_cb)
        result["post_import_hooks"] = hooks_result
        _job_status[job_id] = {
            "status": "COMPLETED",
            "message": "Water Supply import finished successfully.",
            "completed_at": datetime.utcnow().isoformat(),
            "source_file_name": source_file_name,
            # Flat summary — every key from the service result at the top level
            **result,
        }
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        _job_status[job_id] = {
            "status": "FAILED",
            "completed_at": datetime.utcnow().isoformat(),
            "error": str(exc),
        }
    finally:
        db.close()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/upload")
def upload_water_supply(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    background: bool = True,
    db_env: DBEnv = Form(DBEnv.dev, description="Target database environment (dev / staging / prod)"),
):
    """Upload a Water Supply Excel/CSV file and import all records.

    - **background=true** (default): returns a job_id immediately; poll `/status/{job_id}`.
    - **background=false**: runs synchronously and returns the result directly.

    Imported tables
    ---------------
    - `applications`
    - `application_sector_details`  (vrn → vrn, gngzon → gn_gazette_on, govtnoteno → government_notice_no)
    - `bank_details_tanzania`       (bname, bposition, bconpername, ateleno)
    - `financial_information`       (application_sector_detail_id + application_id only)
    - `documents`                   (all *filename attachment pairs)
    """
    try:
        df = read_users_file(file.filename, file.file)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read file: {exc}")

    _db_env_var.set(db_env.value)

    # Filter: only process rows where approval_no is a valid licence number
    # (must be non-null, non-blank, and contain at least one '-' or '/' — bare numbers like 33 are rejected)
    if "approval_no" in df.columns:
        before = len(df)
        mask = (
            df["approval_no"].notna()
            & (df["approval_no"].astype(str).str.strip() != "")
            & (df["approval_no"].astype(str).str.contains(r'[-/]', regex=True))
        )
        df = df[mask]
        skipped = before - len(df)
        if skipped:
            logger.info("Skipped %d rows with invalid/missing approval_no (kept %d)", skipped, len(df))
    if df.empty:
        raise HTTPException(status_code=400, detail="No rows with a valid approval_no found in the uploaded file.")

    if background:
        job_id = f"water-supply-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
        current_db_env = db_env.value
        background_tasks.add_task(_run_job, job_id, df, file.filename or "upload", current_db_env)
        return {"job_id": job_id, "status": "QUEUED", "rows": len(df)}

    # Synchronous path
    db = _get_new_session()
    try:
        result = import_water_supply_via_staging(db, df)
        db.commit()
        hooks_result = run_post_import_hooks(db)
        result["post_import_hooks"] = hooks_result
        return {
            "status": "COMPLETED",
            "message": "Water Supply import finished successfully.",
            "completed_at": datetime.utcnow().isoformat(),
            "source_file_name": file.filename or "upload",
            **result,
        }
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        db.close()


@router.get("/status/{job_id}")
def get_job_status(job_id: str):
    """Poll the status of a background water-supply import job."""
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")
    return _job_status[job_id]
