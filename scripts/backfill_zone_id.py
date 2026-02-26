"""
backfill_zone_id.py
───────────────────
Backfill applications.zone_id for all rows where it is currently NULL,
by joining region names → public.napa_regions → public.zones.

Region source priority (first non-NULL match wins):
  1. public.application_sector_details.region  (linked via application_id)
  2. public.stage_ca_applications_raw.region   (linked via application_number, current batch only)

Run:
    python3 scripts/backfill_zone_id.py
"""

import logging
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    log.error("DATABASE_URL not set")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

with engine.begin() as conn:

    # ── Step 1: backfill via application_sector_details.region ─────────────
    result = conn.execute(text("""
        UPDATE public.applications a
        SET
            zone_id    = z.id,
            updated_at = now()
        FROM public.application_sector_details asd
        JOIN public.napa_regions nr
          ON lower(trim(nr.name)) = lower(trim(asd.region))
         AND nr.zone_id IS NOT NULL
        JOIN public.zones z ON z.id = nr.zone_id
        WHERE asd.application_id = a.id
          AND a.zone_id IS NULL
          AND NULLIF(trim(asd.region), '') IS NOT NULL
    """))
    step1 = result.rowcount
    log.info(f"Step 1 (via application_sector_details): updated {step1} rows")

    # ── Step 2: backfill remaining via stage_ca_applications_raw.region ────
    # Only rows still staged (current batch) — covers anything missed by step 1.
    result2 = conn.execute(text("""
        UPDATE public.applications a
        SET
            zone_id    = z.id,
            updated_at = now()
        FROM public.stage_ca_applications_raw s
        JOIN public.napa_regions nr
          ON lower(trim(nr.name)) = lower(trim(s.region))
         AND nr.zone_id IS NOT NULL
        JOIN public.zones z ON z.id = nr.zone_id
        WHERE s.application_number = a.application_number
          AND a.zone_id IS NULL
          AND NULLIF(trim(s.region), '') IS NOT NULL
    """))
    step2 = result2.rowcount
    log.info(f"Step 2 (via stage_ca_applications_raw):  updated {step2} rows")

    # ── Summary ─────────────────────────────────────────────────────────────
    totals = conn.execute(text("""
        SELECT
            COUNT(*)            AS total,
            COUNT(zone_id)      AS with_zone_id,
            COUNT(*) - COUNT(zone_id) AS still_null
        FROM public.applications
    """)).fetchone()
    log.info(
        f"\nResult: total={totals[0]}  with_zone_id={totals[1]}  still_null={totals[2]}"
    )
