from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import logging
import traceback
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
from app.utils.post_import_hooks import run_post_import_hooks

# ── Persistent boot-scoped log ──────────────────────────────────────────────────
# Every server restart appends a clear "=== SERVER BOOT ===" separator so you can
# grep/tail and immediately know where each run begins.
_LOG_FILE = "/tmp/ewura_migration.log"
_LOG_FMT  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=_LOG_FMT,
    handlers=[
        logging.StreamHandler(),                           # terminal (stdout)
        logging.FileHandler(_LOG_FILE, encoding="utf-8"), # /tmp/ewura_migration.log
    ],
)

logger = logging.getLogger(__name__)

# Write a boot marker — makes it trivial to find where this server run starts
_boot_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
with open(_LOG_FILE, "a", encoding="utf-8") as _lf:
    _lf.write(
        f"\n{'='*80}\n"
        f"=== SERVER BOOT  {_boot_ts} ===\n"
        f"{'='*80}\n\n"
    )
logger.info("application_migrations_upload loaded — full logs at %s", _LOG_FILE)


class _JobLogHandler(logging.Handler):
    """Captures every log record emitted while a job is running into a list."""
    def __init__(self, lines: list):
        super().__init__()
        self.lines = lines
        self.setFormatter(logging.Formatter(_LOG_FMT))

    def emit(self, record: logging.LogRecord):
        try:
            self.lines.append(self.format(record))
        except Exception:
            pass

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

    # ── Per-job log capture ────────────────────────────────────────────────────
    # Every INFO/WARNING/ERROR line logged anywhere in the process during this
    # job will be captured in `_job_lines` and written to the per-job file,
    # giving a self-contained story of exactly what happened.
    _job_lines: list = []
    _job_handler = _JobLogHandler(_job_lines)
    root_logger = logging.getLogger()
    root_logger.addHandler(_job_handler)
    # ──────────────────────────────────────────────────────────────────────────

    _job_status[job_id] = {"status": "RUNNING", "started_at": datetime.utcnow().isoformat(), "progress": "Starting..."}
    logger.info("[Job %s] Starting import of %d rows (sector=%s)", job_id, len(df), sector_name)

    try:
        db = _get_new_session()
        try:
            def _progress(msg: str):
                # lightweight last-known progress for polling
                _job_status[job_id]["progress"] = msg
                logger.info("[Job %s] progress: %s", job_id, msg)

            # For large files (e.g. 500k rows) on a remote DB, staging+COPY is dramatically faster.
            result = import_applications_via_staging_copy(db, df, progress_cb=_progress, sector_name=sector_name)
            db.commit()

            # ── Post-import hooks: align schema + backfill created_by ──────
            _progress("Running post-import hooks (align schema + backfill) …")
            hooks_result = run_post_import_hooks(db, progress_cb=_progress)
            result["post_import_hooks"] = hooks_result

            _job_status[job_id] = {
                "status": "COMPLETED",
                "completed_at": datetime.utcnow().isoformat(),
                "result": result
            }
            logger.info("[Job %s] Import completed: %s", job_id, result)
        except Exception as e:
            db.rollback()
            tb = traceback.format_exc()
            _job_status[job_id] = {
                "status": "FAILED",
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": tb,
                "failed_at": datetime.utcnow().isoformat(),
            }
            # Log full traceback — DB constraint errors bury the DETAIL line in here
            logger.error("[Job %s] Import FAILED — full traceback below:\n%s", job_id, tb)

            # Write per-job error file: header + every log line from this job + traceback
            err_path = f"/tmp/ewura_job_{job_id}_error.log"
            try:
                with open(err_path, "w", encoding="utf-8") as _ef:
                    _ef.write(
                        f"{'='*80}\n"
                        f"Job:       {job_id}\n"
                        f"Sector:    {sector_name}\n"
                        f"Rows:      {len(df)}\n"
                        f"Failed at: {_job_status[job_id]['failed_at']}\n"
                        f"Error:     {type(e).__name__}: {e}\n"
                        f"{'='*80}\n\n"
                        f"--- FULL JOB LOG (from job start) ---\n\n"
                    )
                    for line in _job_lines:
                        _ef.write(line + "\n")
                    _ef.write(
                        f"\n--- PYTHON TRACEBACK ---\n\n"
                        f"{tb}"
                    )
                logger.error("[Job %s] Full job log saved to %s", job_id, err_path)
            except Exception:
                pass
        finally:
            db.close()
    except Exception as e:
        _job_status[job_id] = {"status": "FAILED", "error": str(e)}
        logger.exception("[Job %s] Failed to initialize DB: %s", job_id, e)
    finally:
        # Always detach the per-job handler so it doesn't leak into other jobs
        root_logger.removeHandler(_job_handler)


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

            # ── Post-import hooks: align schema + backfill created_by ──────
            hooks_result = run_post_import_hooks(db)
            result["post_import_hooks"] = hooks_result

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


@router.get('/traceback/{job_id}', response_class=JSONResponse)
def get_job_traceback(job_id: str):
    """Return the full traceback for a FAILED job (plain text, no truncation)."""
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    job = _job_status[job_id]
    if job.get("status") != "FAILED":
        return {"status": job.get("status"), "message": "Job has not failed — no traceback available"}
    return {
        "job_id":     job_id,
        "error_type": job.get("error_type"),
        "error":      job.get("error"),
        "failed_at":  job.get("failed_at"),
        "traceback":  job.get("traceback"),
        "log_file":   f"/tmp/ewura_job_{job_id}_error.log",
    }


@router.get('/jobs')
def list_jobs():
    """List all import jobs and their statuses (traceback omitted — use /traceback/{job_id})."""
    # Strip traceback from list view to keep the response readable
    summary = {}
    for jid, jdata in _job_status.items():
        summary[jid] = {k: v for k, v in jdata.items() if k != "traceback"}
    return summary
