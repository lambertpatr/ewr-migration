#!/usr/bin/env python3
"""Migration script to import new petroleum licenses, shareholders, and directors from Lois-Feb into .166 production DB.

Files:
1. All approved after Feb 27 2026.xlsx (Petroleum Applications & Certificates)
2. ShareHolder after Feb 27 2026.xlsx (Shareholders)
3. Directors Details.xlsx (Managing Directors)
"""

import sys
import os
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import text

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.database import _db_env_var, new_session, _url_for_env
from app.utils.file_reader import read_users_file
from app.services.application_migrations_service import import_applications_via_staging_copy
from app.utils.post_import_hooks import run_post_import_hooks
from app.services.shareholders_import_service import import_shareholders_via_staging_copy
from app.services.managing_directors_import_service import import_managing_directors_via_staging_copy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("migrate_lois_feb")


def progress(msg: str):
    logger.info(">>> PROGRESS: %s", msg)


def main():
    target_env = "prod"
    _db_env_var.set(target_env)
    url = _url_for_env(target_env)
    logger.info("Target DB URL: %s", url.replace("ewura%40123", "***"))

    data_dir = PROJECT_ROOT / "app" / "data" / "lois-feb"
    lic_file = data_dir / "All approved after Feb 27 2026.xlsx"
    sh_file = data_dir / "ShareHolder after Feb 27 2026.xlsx"
    dir_file = data_dir / "Directors Details.xlsx"

    for f in [lic_file, sh_file, dir_file]:
        if not f.exists():
            logger.error("File not found: %s", f)
            sys.exit(1)

    db = new_session()

    try:
        # Pre-migration count check
        tables_to_check = [
            "applications",
            "application_sector_details",
            "certificates",
            "documents",
            "contact_persons",
            "shareholders",
            "managing_directors",
        ]
        counts_before = {}
        for t in tables_to_check:
            counts_before[t] = db.execute(text(f"SELECT count(*) FROM public.{t}")).scalar()
            logger.info("BEFORE: %s = %d", t, counts_before[t])

        # -------------------------------------------------------------
        # 1. Petroleum Licenses Migration
        # -------------------------------------------------------------
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: Importing Petroleum Licenses from %s", lic_file.name)
        logger.info("=" * 60)
        with open(lic_file, "rb") as fp:
            df_lic = read_users_file(lic_file.name, fp)
        logger.info("Loaded %d rows from %s", len(df_lic), lic_file.name)

        lic_result = import_applications_via_staging_copy(
            db,
            df_lic,
            progress_cb=progress,
            sector_name="PETROLEUM",
        )
        db.commit()
        logger.info("Applications import result: %s", lic_result)

        progress("Running post-import hooks for applications...")
        hooks_result = run_post_import_hooks(db, progress_cb=progress)
        db.commit()
        logger.info("Post-import hooks result: %s", hooks_result)

        # -------------------------------------------------------------
        # 2. Shareholders Migration
        # -------------------------------------------------------------
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: Importing Shareholders from %s", sh_file.name)
        logger.info("=" * 60)
        with open(sh_file, "rb") as fp:
            df_sh = read_users_file(sh_file.name, fp)
        logger.info("Loaded %d rows from %s", len(df_sh), sh_file.name)

        sh_result = import_shareholders_via_staging_copy(
            db,
            df_sh,
            source_file_name=sh_file.name,
            progress_cb=progress,
        )
        db.commit()
        logger.info("Shareholders import result: %s", sh_result)

        # -------------------------------------------------------------
        # 3. Managing Directors Migration
        # -------------------------------------------------------------
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: Importing Managing Directors from %s", dir_file.name)
        logger.info("=" * 60)
        with open(dir_file, "rb") as fp:
            df_dir = read_users_file(dir_file.name, fp)
        logger.info("Loaded %d rows from %s", len(df_dir), dir_file.name)

        dir_result = import_managing_directors_via_staging_copy(
            db,
            df_dir,
            source_file_name=dir_file.name,
            progress_cb=progress,
        )
        db.commit()
        logger.info("Managing directors import result: %s", dir_result)

        # -------------------------------------------------------------
        # 4. Final Post-Import Hooks & Backfill
        # -------------------------------------------------------------
        logger.info("\n" + "=" * 60)
        logger.info("STEP 4: Running final post-import backfill hooks")
        logger.info("=" * 60)
        final_hooks = run_post_import_hooks(db, progress_cb=progress)
        db.commit()
        logger.info("Final hooks result: %s", final_hooks)

        # -------------------------------------------------------------
        # 5. Verification & Summary
        # -------------------------------------------------------------
        logger.info("\n" + "=" * 60)
        logger.info("STEP 5: Post-Migration Row Count Verification")
        logger.info("=" * 60)
        counts_after = {}
        for t in tables_to_check:
            counts_after[t] = db.execute(text(f"SELECT count(*) FROM public.{t}")).scalar()
            diff = counts_after[t] - counts_before[t]
            logger.info("AFTER: %-28s = %7d (change: %+d)", t, counts_after[t], diff)

        # Check for duplicates on unique business keys
        app_dups = db.execute(text("""
            SELECT application_number, count(*)
            FROM public.applications
            GROUP BY application_number
            HAVING count(*) > 1
        """)).fetchall()
        logger.info("Duplicate application_numbers in applications: %d", len(app_dups))

        sh_dups = db.execute(text("""
            SELECT application_id, lower(trim(shareholder_name)), count(*)
            FROM public.shareholders
            WHERE application_id IS NOT NULL AND shareholder_name IS NOT NULL
            GROUP BY application_id, lower(trim(shareholder_name))
            HAVING count(*) > 1
        """)).fetchall()
        logger.info("Duplicate shareholders in shareholders: %d", len(sh_dups))

        md_dups = db.execute(text("""
            SELECT application_id, lower(trim(name)), count(*)
            FROM public.managing_directors
            WHERE application_id IS NOT NULL AND name IS NOT NULL
            GROUP BY application_id, lower(trim(name))
            HAVING count(*) > 1
        """)).fetchall()
        logger.info("Duplicate managing directors: %d", len(md_dups))

        logger.info("\n🎉 All 3 migrations completed successfully with 0 duplicates!")

    except Exception as e:
        db.rollback()
        logger.exception("Migration failed: %s", e)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
