"""
One-shot backfill: populate shareholders.application_id for rows that still have NULL.

Strategy:
  Step 1 — Via staging table (stage_ca_shareholders_raw) if it still exists on disk.
           Match on lower(trim(shname)) + application_number.
  Step 2 — Via other child tables that share the same orphaned application_sector_detail_id
           (e.g. managing_directors, documents) and already have application_id filled.
  Step 3 — Fix orphaned application_sector_detail_id values using the current ASD for
           each application_id that was just recovered.

Run once from the project root:
    python3 scripts/backfill_sh_application_id.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.database import get_db
from sqlalchemy import text

db = next(get_db())

# ── Step 1: update application_id via staging table ─────────────────────
print("Step 1: backfill application_id via stage_ca_shareholders_raw ...")
r = db.execute(text("""
    UPDATE public.shareholders sh
    SET    application_id = a.id
    FROM   public.stage_ca_shareholders_raw stg
    JOIN   public.applications a
           ON a.application_number = NULLIF(trim(stg.application_number), '')
    WHERE  sh.application_id IS NULL
      AND  lower(trim(sh.shareholder_name)) = lower(trim(stg.shname))
"""))
print(f"  rows updated: {r.rowcount}")
db.commit()

# ── Step 2: backfill via sibling child tables that share the same orphaned ASD id ─
# managing_directors, documents etc. may already have application_id filled
# for the same (now-orphaned) application_sector_detail_id.
print("Step 2: backfill application_id via managing_directors (shared orphaned ASD) ...")
r = db.execute(text("""
    UPDATE public.shareholders sh
    SET    application_id = md.application_id
    FROM   public.managing_directors md
    WHERE  sh.application_id IS NULL
      AND  md.application_id IS NOT NULL
      AND  sh.application_sector_detail_id = md.application_sector_detail_id
"""))
print(f"  rows updated: {r.rowcount}")
db.commit()

print("Step 2b: backfill application_id via documents (shared orphaned ASD) ...")
r = db.execute(text("""
    UPDATE public.shareholders sh
    SET    application_id = d.application_id
    FROM   public.documents d
    WHERE  sh.application_id IS NULL
      AND  d.application_id IS NOT NULL
      AND  sh.application_sector_detail_id = d.application_sector_detail_id
"""))
print(f"  rows updated: {r.rowcount}")
db.commit()

# ── Step 3: fix orphaned application_sector_detail_id ───────────────────
print("Step 3: fix orphaned application_sector_detail_id ...")
r = db.execute(text("""
    UPDATE public.shareholders sh
    SET    application_sector_detail_id = asd_new.id
    FROM   public.applications a
    JOIN   LATERAL (
        SELECT id FROM public.application_sector_details
        WHERE  application_id = a.id
        LIMIT  1
    ) asd_new ON true
    WHERE  sh.application_id = a.id
      AND  asd_new.id IS NOT NULL
      AND  NOT EXISTS (
          SELECT 1 FROM public.application_sector_details
          WHERE id = sh.application_sector_detail_id
      )
"""))
print(f"  rows updated: {r.rowcount}")
db.commit()

# ── Final check ─────────────────────────────────────────────────────────
row = db.execute(text("""
    SELECT
        COUNT(*)                                       AS total_rows,
        COUNT(*) FILTER (WHERE application_id IS NULL) AS null_app_id
    FROM public.shareholders
""")).first()
print("\n=== Final counts ===")
print(f"  total rows:              {row[0]}")
print(f"  still NULL application_id: {row[1]}")
print(f"  filled:                  {row[0] - row[1]}")
