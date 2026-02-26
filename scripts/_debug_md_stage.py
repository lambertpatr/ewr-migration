"""Check if stage_ca_managing_directors_raw still exists and has apprefno."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.core.database import get_db
from sqlalchemy import text
db = next(get_db())

exists = db.execute(text(
    "SELECT to_regclass('public.stage_ca_managing_directors_raw')"
)).scalar()
print(f"stage table exists: {exists}")

if exists:
    cols = db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='stage_ca_managing_directors_raw'"
    )).fetchall()
    print(f"columns: {[r[0] for r in cols]}")
    sample = db.execute(text(
        "SELECT * FROM public.stage_ca_managing_directors_raw LIMIT 3"
    )).fetchall()
    for r in sample:
        print(dict(r._mapping))

db.close()
