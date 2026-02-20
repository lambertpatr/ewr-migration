"""
POST /api/v1/sync-license-type

Backfills `license_type` and `category_license_type` on:
  1. applications  — derived from categories.category_type + sectors.name
  2. certificates  — inherits both values from the linked applications row

The ELECTRICITY sector is special:
  - If applications.license_type already contains 'INSTALLATION'   → LICENSE_ELECTRICITY_INSTALLATION
  - If applications.license_type already contains 'SUPPLY' (or anything else) → LICENSE_ELECTRICITY_SUPPLY
  - This preserves the hard-coded values set by electrical_installation_import_service.py.

Query params
  dry_run=true  (default) – returns counts without writing
  dry_run=false           – executes the UPDATE
"""

from fastapi import APIRouter, Query
from sqlalchemy import text

router = APIRouter(prefix="/api/v1", tags=["05 - Sync License Type"])


def _get_db():
    from app.core import database as db_module
    db_module._init_engine()
    return db_module._SessionLocal()


# ---------------------------------------------------------------------------
# SQL: UPDATE applications
# ---------------------------------------------------------------------------
_SQL_UPDATE_APPLICATIONS = """
UPDATE public.applications a
SET
    license_type =
        CASE
            -- ── CONSTRUCTION ──────────────────────────────────────────────
            WHEN c.category_type = 'Construction' AND s.name = 'NATURAL_GAS'
                THEN 'CONSTRUCTION_NATURAL_GAS'

            WHEN c.category_type = 'Construction' AND s.name = 'PETROLEUM'
                THEN 'CONSTRUCTION_PETROLEUM'

            -- ── LICENSE ───────────────────────────────────────────────────
            WHEN c.category_type = 'License' AND s.name = 'NATURAL_GAS'
                THEN 'LICENSE_NATURAL_GAS'

            WHEN c.category_type = 'License' AND s.name = 'PETROLEUM'
                THEN 'LICENSE_PETROLEUM'

            WHEN c.category_type = 'License' AND s.name = 'WATER_SUPPLY'
                THEN 'LICENSE_WATER'

            -- ── ELECTRICITY: preserve installation vs supply ───────────────
            -- electrical_installation_import_service.py hard-codes INSTALLATION;
            -- keep it.  Everything else in the electricity sector → SUPPLY.
            WHEN s.name = 'ELECTRICITY'
                 AND a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION'
                THEN 'LICENSE_ELECTRICITY_INSTALLATION'

            WHEN c.category_type = 'License' AND s.name = 'ELECTRICITY'
                THEN 'LICENSE_ELECTRICITY_SUPPLY'

            -- ── fallback: keep whatever is already there ──────────────────
            ELSE a.license_type
        END,
    category_license_type = 'OPERATIONAL'
FROM public.categories c
JOIN public.sectors s ON s.id = c.sector_id
WHERE a.category_id = c.id
  AND c.deleted_at IS NULL
"""

# ---------------------------------------------------------------------------
# SQL: propagate to certificates (inherit from applications)
# ---------------------------------------------------------------------------
_SQL_UPDATE_CERTIFICATES = """
UPDATE public.certificates cert
SET
    license_type          = a.license_type,
    category_license_type = a.category_license_type
FROM public.applications a
WHERE cert.application_id = a.id
  AND (
      cert.license_type          IS DISTINCT FROM a.license_type
   OR cert.category_license_type IS DISTINCT FROM a.category_license_type
  )
"""

# ---------------------------------------------------------------------------
# SQL: preview counts (dry-run)
# ---------------------------------------------------------------------------
_SQL_PREVIEW_APPLICATIONS = """
SELECT
    CASE
        WHEN c.category_type = 'Construction' AND s.name = 'NATURAL_GAS'  THEN 'CONSTRUCTION_NATURAL_GAS'
        WHEN c.category_type = 'Construction' AND s.name = 'PETROLEUM'    THEN 'CONSTRUCTION_PETROLEUM'
        WHEN c.category_type = 'License'      AND s.name = 'NATURAL_GAS'  THEN 'LICENSE_NATURAL_GAS'
        WHEN c.category_type = 'License'      AND s.name = 'PETROLEUM'    THEN 'LICENSE_PETROLEUM'
        WHEN c.category_type = 'License'      AND s.name = 'WATER_SUPPLY' THEN 'LICENSE_WATER'
        WHEN s.name = 'ELECTRICITY'
             AND a.license_type = 'LICENSE_ELECTRICITY_INSTALLATION'      THEN 'LICENSE_ELECTRICITY_INSTALLATION'
        WHEN c.category_type = 'License'      AND s.name = 'ELECTRICITY'  THEN 'LICENSE_ELECTRICITY_SUPPLY'
        ELSE a.license_type
    END                         AS new_license_type,
    COUNT(*)                    AS application_count
FROM public.applications a
JOIN public.categories c ON c.id = a.category_id AND c.deleted_at IS NULL
JOIN public.sectors     s ON s.id = c.sector_id
GROUP BY 1
ORDER BY 1
"""

_SQL_PREVIEW_CERTIFICATES = """
SELECT COUNT(*) AS certificates_to_update
FROM public.certificates cert
JOIN public.applications a ON a.id = cert.application_id
WHERE cert.license_type          IS DISTINCT FROM a.license_type
   OR cert.category_license_type IS DISTINCT FROM a.category_license_type
"""


@router.post("/sync-license-type")
def sync_license_type(dry_run: bool = Query(default=True)):
    """
    Synchronise `license_type` and `category_license_type` across
    `applications` and `certificates`.

    - **dry_run=true**  (default): returns a preview of what would change — no writes.
    - **dry_run=false**: executes the UPDATEs and reports rows affected.
    """
    db = _get_db()
    try:
        if dry_run:
            # ── Preview ───────────────────────────────────────────────────
            breakdown = [
                dict(r._mapping)
                for r in db.execute(text(_SQL_PREVIEW_APPLICATIONS)).fetchall()
            ]
            certs_pending = db.execute(text(_SQL_PREVIEW_CERTIFICATES)).scalar() or 0

            return {
                "status": "DRY_RUN",
                "note": "Pass dry_run=false to apply changes.",
                "applications_breakdown": breakdown,
                "certificates_to_update": int(certs_pending),
            }

        else:
            # ── Step 1: update applications ───────────────────────────────
            result_apps = db.execute(text(_SQL_UPDATE_APPLICATIONS))
            updated_apps = result_apps.rowcount or 0

            # ── Step 2: propagate to certificates ─────────────────────────
            result_certs = db.execute(text(_SQL_UPDATE_CERTIFICATES))
            updated_certs = result_certs.rowcount or 0

            db.commit()

            return {
                "status": "OK",
                "updated_applications": updated_apps,
                "updated_certificates": updated_certs,
            }

    except Exception as exc:
        db.rollback()
        raise
    finally:
        db.close()
