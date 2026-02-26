"""Debug: check why managing_directors.application_id stays NULL after re-upload."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.core.database import get_db
from sqlalchemy import text

db = next(get_db())

print("=== ASD rows for the 3 sample managing_directors ===")
rows = db.execute(text(
    "SELECT asd.id, asd.application_id, a.application_number "
    "FROM public.application_sector_details asd "
    "LEFT JOIN public.applications a ON a.id = asd.application_id "
    "WHERE asd.id IN ("
    "  '5f575952-5d91-48c5-9d56-6f953042a4cc',"
    "  'e731a13e-d2fe-46ef-9631-25bbeb9a2349',"
    "  '6777a65a-0321-450e-b7bb-1da7fa6b9790'"
    ")"
)).fetchall()
if not rows:
    print("  NO ROWS FOUND - those ASD ids do not exist in application_sector_details!")
for r in rows:
    print(dict(r._mapping))

print()
print("=== Total managing_directors with NULL application_id ===")
cnt = db.execute(text(
    "SELECT COUNT(*) FROM public.managing_directors WHERE application_id IS NULL"
)).scalar()
print(f"  NULL application_id count: {cnt}")

print()
print("=== Sample: do the application_sector_detail_ids resolve via ASD? ===")
sample = db.execute(text(
    "SELECT md.id, md.name, md.application_sector_detail_id, "
    "       asd.id AS asd_exists, asd.application_id AS asd_app_id "
    "FROM public.managing_directors md "
    "LEFT JOIN public.application_sector_details asd ON asd.id = md.application_sector_detail_id "
    "WHERE md.application_id IS NULL "
    "LIMIT 5"
)).fetchall()
for r in sample:
    print(dict(r._mapping))

print()
print("=== Check if md5-based id would match existing row ===")
# The old rows have uuid4 ids — md5 id is NEW, so conflict never fires
# New rows computed from md5 will be new inserts — but eligible filter
# blocks them if application_sector_detail_id is NULL in joined CTE
md_sample = db.execute(text(
    "SELECT md.id, md.name, md.application_sector_detail_id, "
    "       md5(md.application_sector_detail_id::text || '|' || lower(trim(md.name)))::uuid AS md5_id "
    "FROM public.managing_directors md "
    "WHERE md.application_id IS NULL LIMIT 3"
)).fetchall()
for r in md_sample:
    print(dict(r._mapping))

db.close()
