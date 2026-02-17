# app/api/v1/lois_users_upload.py
try:
    from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
except Exception:
    # Provide lightweight fallbacks so this module can be imported in
    # environments where FastAPI isn't installed (useful for scripts,
    # static analysis, or tests that don't run the server). These
    # fallbacks are import-time helpers only; running the FastAPI app
    # requires installing FastAPI.
    class APIRouter:
        def __init__(self, *a, **k):
            pass
        def post(self, *a, **k):
            def _decor(f):
                return f
            return _decor

    class UploadFile:
        pass

    File = None

    def Depends(x=None):
        return None

    class HTTPException(Exception):
        def __init__(self, status_code=400, detail=None):
            super().__init__(detail)

from sqlalchemy.orm import Session

from app.core.database import get_db
from app.utils.file_reader import read_lois_users_file
from app.services.lois_users_import_service import LoisUsersImportService

router = APIRouter(prefix="/api/v1/lois-users", tags=["02 - LOIS Users Migration"])

@router.post("/upload")
def upload_lois_users(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        df = read_lois_users_file(file.filename, file.file)
        result = LoisUsersImportService.import_users(db, df)
        return {"status": "SUCCESS", "result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
