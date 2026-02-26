"""
One-shot backfill: populate managing_directors.application_id for all existing
rows that have a NULL application_id.

application_number is no longer stored on the child table — application_id is
the canonical FK.  This script recovers via the staging table (still present on
disk) then fixes any orphaned application_sector_detail_id values.

Run once from the project root:
    python3 scripts/backfill_md_application_id.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.core.database import get_db
from sqlalchemy import text

db = next(get_db())

print("=== Step 1: drop application_number column if it still exists ===")
db.execute(text("""
    ALTER TABLE public.managing_directors
        DROP COLUMN IF EXISTS application_number
"""))
db.commit()
print("  done")

print()
print("=== Step 2: backfill application_id via stage_ca_managing_directors_raw ===")
r = db.execute(text("""
    UPDATE public.managing_directors md
    SET    application_id = a.id
    FROM   public.stage_ca_managing_directors_raw s
    JOIN   public.applications a
           ON a.application_number = NULLIF(trim(s.application_number), '')
    WHERE  md.application_id IS NULL
      AND  lower(trim(md.name)) = lower(trim(s.name))
"""))
cnt2 = r.rowcount
db.commit()
print(f"  updated {cnt2} rows with application_id")

print()
print("=== Step 3: fix orphaned application_sector_detail_id ===")
r = db.execute(text("""
    UPDATE public.managing_directors md
    SET    application_sector_detail_id = asd.id
    FROM   (
        SELECT DISTINCT ON (application_id) id, application_id
        FROM   public.application_sector_details
        ORDER  BY application_id, created_at DESC
    ) asd
    WHERE  md.application_id = asd.application_id
      AND  (
           md.application_sector_detail_id IS NULL
        OR NOT EXISTS (
               SELECT 1 FROM public.application_sector_details
               WHERE id = md.application_sector_detail_id
           )
      )
"""))
cnt3 = r.rowcount
db.commit()
print(f"  updated {cnt3} rows with correct application_sector_detail_id")

print()
print("=== Final counts ===")
remaining = db.execute(text(
    "SELECT COUNT(*) FROM public.managing_directors WHERE application_id IS NULL"
)).scalar()
total = db.execute(text(
    "SELECT COUNT(*) FROM public.managing_directors"
)).scalar()
print(f"  total rows:              {total}")
print(f"  still NULL application_id: {remaining}")
print(f"  filled:                  {total - remaining}")

db.close()
