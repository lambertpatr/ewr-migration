"""
Backfill managing_directors.application_id and application_sector_detail_id
for rows where the stored application_sector_detail_id is orphaned (doesn't
exist in application_sector_details).

Resolution path:
  managing_directors.name + apprefno (via stage) is unavailable post-import,
  so we join via application_sector_detail_id -> find the application_number
  from the original applications table by doing the reverse:
    managing_directors -> applications via application_number stored in the
    staging table is gone. Instead we use:
    1. Try asd join (may work for some rows)
    2. For rows where asd join fails, try matching by looking at what
       application has a managing_director with this asd_id as a best guess
       (won't work either since asd doesn't exist)
    3. Real fix: the managing_directors table must carry application_number
       so we can join directly. Since it doesn't, use the backfill approach:
       find applications that share the same application_sector_detail_id
       pattern or match by name+apprefno from the excel file.

PRACTICAL FIX: Add application_number column to managing_directors staging
and final table, populated during import, so backfill can work directly.

This script does the immediate fix differently:
  - managing_directors has NO application_number column
  - BUT the application_sector_detail_id was set during the original import
    using the SAME lateral join we have now (but the ASD was deleted/recreated)
  - The only reliable path: re-import from Excel

HOWEVER: we can do a best-effort fix right now using the backfill that
matches managing_directors to applications via application_sector_details
for rows where the ASD DOES exist, and for the rest, report what's orphaned.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.core.database import get_db
from sqlalchemy import text

db = next(get_db())

print("=== Checking ASD coverage ===")
r = db.execute(text("""
    SELECT
        COUNT(*) FILTER (WHERE asd.id IS NOT NULL) AS asd_matched,
        COUNT(*) FILTER (WHERE asd.id IS NULL) AS asd_orphaned,
        COUNT(*) AS total
    FROM public.managing_directors md
    LEFT JOIN public.application_sector_details asd ON asd.id = md.application_sector_detail_id
    WHERE md.application_id IS NULL
""")).first()
print(f"  asd_matched={r.asd_matched}, asd_orphaned={r.asd_orphaned}, total={r.total}")

print()
print("=== Checking if managing_directors has application_number column ===")
has_appnum = db.execute(text("""
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema='public' AND table_name='managing_directors'
    AND column_name='application_number'
""")).scalar()
print(f"  has application_number column: {bool(has_appnum)}")

db.close()
