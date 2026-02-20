from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import io
import uuid
from datetime import datetime
from app.core.database import get_db
from app.utils.file_reader import read_users_file

router = APIRouter(prefix="/api/v1/categories", tags=["07 - Categories Upload"])

@router.post("/upload")
def upload_categories(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Upload categories from Excel.
    Expects columns: 'category_name', 'sector_name'.
    If category name (case-insensitive) does not exist, insert it.
    The sector_id is looked up from the public.sectors table using sector_name.
    Set is_approved = True by default.
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")

    try:
        content = file.file.read()
        df = read_users_file(file.filename, io.BytesIO(content))
        
        required_cols = {'category_name', 'sector_name'}
        if not required_cols.issubset(set(df.columns)):
             raise HTTPException(status_code=400, detail=f"Excel must contain columns: {required_cols}")

             
        stats = {
            "processed": 0,
            "inserted": 0,
            "skipped_existing": 0,
            "skipped_no_sector": 0
        }
        
        # Pre-fetch all sectors for lookup
        sectors_res = db.execute(text("SELECT id, name FROM public.sectors")).fetchall()
        # Map lower(name) -> id
        sector_map = {str(row.name).lower().strip(): row.id for row in sectors_res}
        # Also map raw name just in case
        for row in sectors_res:
             sector_map[str(row.name).strip()] = row.id

        for _, row in df.iterrows():
            cat_name = str(row['category_name']).strip()
            sec_name = str(row['sector_name']).strip()
            
            if not cat_name or cat_name.lower() in ('nan', 'none', ''):
                continue
            
            stats["processed"] += 1
            
            # Lookup sector
            sector_uuid = sector_map.get(sec_name.lower())
            if not sector_uuid:
                # Try exact match or other variations if needed, but lowercase map covers most
                stats["skipped_no_sector"] += 1
                continue

            # Check existence
            existing = db.execute(
                text("SELECT 1 FROM public.categories WHERE LOWER(TRIM(name)) = LOWER(TRIM(:name))"),
                {"name": cat_name}
            ).fetchone()
            
            if existing:
                stats["skipped_existing"] += 1
                continue
                
            # Insert
            new_id = uuid.uuid4()
            now = datetime.now()
            
            insert_sql = text("""
                INSERT INTO public.categories (
                    id, 
                    name, 
                    sector_id, 
                    is_approved, 
                    category_type, 
                    created_at, 
                    updated_at
                ) VALUES (
                    :id, 
                    :name, 
                    :sector_id, 
                    true, 
                    'License', 
                    :created_at, 
                    :updated_at
                )
            """)
            
            db.execute(insert_sql, {
                "id": new_id,
                "name": cat_name,
                "sector_id": sector_uuid,
                "created_at": now,
                "updated_at": now
            })
            stats["inserted"] += 1
            
        db.commit()
        return {"message": "Upload complete", "stats": stats}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
