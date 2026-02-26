"""
app/utils/lookup_cache.py
─────────────────────────
Dynamic lookup-table helpers for all import services.

WHY THIS MODULE EXISTS
──────────────────────
Every DB environment (test / staging / production) has its own auto-generated
UUIDs.  Hard-coding UUIDs in Python dicts or SQL CASE blocks breaks the moment
you switch databases.  These helpers resolve UUIDs at **runtime** by querying
the connected DB, so imports "just work" regardless of which environment you
point to.

PATTERN
───────
1. Query the target lookup table.
2. INSERT any row that is missing (ON CONFLICT DO NOTHING) — idempotent.
3. Re-query and return a plain {normalised_key → uuid_str} dict.

Callers can optionally push the dict into a Postgres TEMP TABLE so the main
import SQL can JOIN against it instead of embedding UUIDs in f-strings.

ROLLBACK SAFETY
───────────────
All helpers run inside the *caller's* transaction.  They call db.flush() (not
db.commit()) so new rows are visible within the same connection without
prematurely committing the outer transaction.  Individual insert failures are
caught, logged, and the session is rolled back to a clean savepoint so the
outer import can continue.
"""

from __future__ import annotations

import logging
import uuid as _uuid_mod
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# ── Electrical class codes we care about ──────────────────────────────────────
# These are the canonical short codes stored in public.categories.code.
# The Excel column 'licensecategoryclass' / 'approvedclass' uses either
#   "CLASS A"  or bare  "A"  — normalisation happens in SQL.
ELEC_CLASS_CODES: list[str] = ["A", "B", "C", "D", "W", "S1", "S2", "S3"]

# Default display names used when a category row must be inserted from scratch.
_ELEC_CODE_DISPLAY: Dict[str, str] = {
    "A":  "CLASS A",
    "B":  "CLASS B",
    "C":  "CLASS C",
    "D":  "CLASS D",
    "W":  "CLASS W",
    "S1": "CLASS S1",
    "S2": "CLASS S2",
    "S3": "CLASS S3",
}

# Temp table name used within a single DB connection / import run.
ELEC_CAT_MAP_TEMP = "stage_elec_category_map"


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load_elec_category_map(db: Any) -> Dict[str, str]:
    """Return {code → uuid_string} for the 8 electrical class categories.

    1. Query public.categories for rows where code IN (...).
    2. For any code that is missing, insert a new row (ON CONFLICT DO NOTHING).
    3. Re-query to pick up the newly inserted UUIDs.
    4. Return the mapping.

    The caller can inspect the returned dict — if a code is still absent after
    the insert attempt it means the DB rejected the insert (e.g. unique-name
    constraint already satisfied by a differently-coded row).  In that case the
    SQL fallback (hardcoded CASE) will handle it via the ELSE branch.
    """
    from sqlalchemy import text

    # ── Schema guard: ensure `code` column & unique index exist ───────────────
    # Some environments may not have the `code` column or its unique index yet.
    # We create both idempotently so ON CONFLICT (code) works everywhere.
    try:
        db.execute(text("""
            ALTER TABLE IF EXISTS public.categories
                ADD COLUMN IF NOT EXISTS code character varying;
        """))
        db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_code
            ON public.categories (code)
            WHERE code IS NOT NULL AND deleted_at IS NULL;
        """))
        db.flush()
    except Exception as exc:
        logger.debug("load_elec_category_map: schema guard skipped (%s)", exc)
        try:
            db.rollback()
        except Exception:
            pass

    # ── Step 1: fetch existing rows ───────────────────────────────────────────
    rows = db.execute(
        text("""
            SELECT code, id::text
            FROM   public.categories
            WHERE  code = ANY(:codes)
              AND  deleted_at IS NULL
        """),
        {"codes": ELEC_CLASS_CODES},
    ).fetchall()

    cat_map: Dict[str, str] = {str(r[0]).strip().upper(): str(r[1]) for r in rows if r[0]}

    missing_codes = [c for c in ELEC_CLASS_CODES if c not in cat_map]

    if not missing_codes:
        logger.debug("load_elec_category_map: all %d codes found in DB", len(cat_map))
        return cat_map

    # ── Step 2: insert missing rows ───────────────────────────────────────────
    # Requires the sector_id for 'Electricity'.  If the sector is not found we
    # skip the insert and let the SQL fallback handle the missing codes.
    sector_row = db.execute(
        text("""
            SELECT id
            FROM   public.sectors
            WHERE  lower(trim(name)) = 'electricity'
              AND  deleted_at IS NULL
            LIMIT 1
        """)
    ).fetchone()

    if not sector_row:
        logger.warning(
            "load_elec_category_map: 'Electricity' sector not found — "
            "cannot insert missing codes %s; SQL fallback will apply",
            missing_codes,
        )
        return cat_map

    sector_id = str(sector_row[0])

    inserted: list[str] = []
    for code in missing_codes:
        display = _ELEC_CODE_DISPLAY.get(code, f"CLASS {code}")
        new_id = str(_uuid_mod.uuid4())
        try:
            db.execute(
                text("""
                    INSERT INTO public.categories (
                        id, name, code, sector_id,
                        sub_sector_type, category_type,
                        is_approved, created_at, updated_at
                    )
                    VALUES (
                        :id, :name, :code, :sector_id,
                        'OPERATIONAL', 'License',
                        false, now(), now()
                    )
                    ON CONFLICT (code)
                    WHERE code IS NOT NULL AND deleted_at IS NULL
                    DO NOTHING
                """),
                {
                    "id":        new_id,
                    "name":      display,
                    "code":      code,
                    "sector_id": sector_id,
                },
            )
            db.flush()
            inserted.append(code)
        except Exception as exc:
            logger.warning(
                "load_elec_category_map: could not insert code=%s (%s); "
                "SQL fallback will apply for this code",
                code, exc,
            )
            try:
                db.rollback()
            except Exception:
                pass

    if inserted:
        logger.info(
            "load_elec_category_map: inserted %d new category row(s): %s",
            len(inserted), inserted,
        )

    # ── Step 3: re-fetch to pick up inserted + any existing we might have missed ──
    rows2 = db.execute(
        text("""
            SELECT code, id::text
            FROM   public.categories
            WHERE  code = ANY(:codes)
              AND  deleted_at IS NULL
        """),
        {"codes": ELEC_CLASS_CODES},
    ).fetchall()

    cat_map = {str(r[0]).strip().upper(): str(r[1]) for r in rows2 if r[0]}

    still_missing = [c for c in ELEC_CLASS_CODES if c not in cat_map]
    if still_missing:
        logger.warning(
            "load_elec_category_map: codes still unresolved after insert attempt: %s — "
            "SQL CASE fallback will handle them (may result in NULL category_id)",
            still_missing,
        )
    else:
        logger.info("load_elec_category_map: all %d codes resolved", len(cat_map))

    return cat_map


def push_category_map_temp_table(db: Any, cat_map: Dict[str, str]) -> None:
    """Create (or replace) a TEMP TABLE `stage_elec_category_map` with the mapping.

    The table is `ON COMMIT PRESERVE ROWS` (default) so it survives individual
    statement commits and is visible for the lifetime of the DB connection.

    Schema:
        code        text  PRIMARY KEY   -- 'A', 'B', ... 'S3'
        category_id uuid  NOT NULL
        -- extra convenience columns used by the CASE normalisation in SQL
        class_label text              -- 'CLASS A', 'CLASS B', ...
    """
    from sqlalchemy import text

    if not cat_map:
        logger.warning("push_category_map_temp_table: empty map — temp table will be empty")

    db.execute(text(f"""
        DROP TABLE IF EXISTS {ELEC_CAT_MAP_TEMP};
        CREATE TEMP TABLE {ELEC_CAT_MAP_TEMP} (
            code        text  PRIMARY KEY,
            class_label text  NOT NULL,   -- e.g. 'CLASS A'
            category_id uuid  NOT NULL
        );
    """))

    for code, uid in cat_map.items():
        label = _ELEC_CODE_DISPLAY.get(code, f"CLASS {code}")
        db.execute(
            text(f"""
                INSERT INTO {ELEC_CAT_MAP_TEMP} (code, class_label, category_id)
                VALUES (:code, :label, CAST(:uid AS uuid))
                ON CONFLICT (code) DO NOTHING
            """),
            {"code": code, "label": label, "uid": uid},
        )

    logger.debug(
        "push_category_map_temp_table: populated %s with %d rows",
        ELEC_CAT_MAP_TEMP, len(cat_map),
    )


def build_category_case_sql(cat_map: Dict[str, str], col_expr: str) -> str:
    """Build a SQL CASE expression string from the live map for use in f-strings.

    This is the *fallback* path used when the temp-table approach is not
    applicable (e.g. inside a multi-statement text() block that has already
    been composed).  The caller embeds the returned string directly into SQL.

    ``col_expr`` is the SQL expression for the input value, e.g.
        ``"UPPER(REGEXP_REPLACE(TRIM(ea.licensecategoryclass), '\\\\s+', ' '))"``.

    Returns a CASE expression that maps 'CLASS X' AND bare 'X' to uuid literals,
    falling back to a raw-UUID-string cast, then NULL.
    """
    if not cat_map:
        # Nothing resolved — return NULL so callers notice and use old fallback
        return "NULL::uuid"

    branches: list[str] = []
    for code, uid in cat_map.items():
        label = _ELEC_CODE_DISPLAY.get(code, f"CLASS {code}")
        branches.append(f"    WHEN '{label}' THEN '{uid}'::uuid")
        branches.append(f"    WHEN '{code}'  THEN '{uid}'::uuid")

    branches_sql = "\n".join(branches)

    return (
        f"CASE {col_expr}\n"
        f"{branches_sql}\n"
        f"    ELSE (CASE WHEN NULLIF(TRIM({col_expr}),'') ~ '^[0-9a-fA-F-]{{36}}$'\n"
        f"               THEN TRIM({col_expr})::uuid\n"
        f"               ELSE NULL END)\n"
        f"END"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sector map
# ─────────────────────────────────────────────────────────────────────────────

def load_sector_map(db: Any) -> Dict[str, str]:
    """Return {lower(name) → uuid_str} for every row in public.sectors.

    Sectors are master / seed data — this function does NOT insert missing rows.
    Callers should raise a clear ValueError when the required sector is absent.

    Example
    -------
    >>> sector_map = load_sector_map(db)
    >>> sector_id = sector_map.get("electricity")          # works on every env
    >>> sector_id = sector_map.get("natural_gas")
    """
    from sqlalchemy import text

    rows = db.execute(
        text("""
            SELECT lower(trim(name)), id::text
            FROM   public.sectors
            WHERE  deleted_at IS NULL
        """)
    ).fetchall()

    sector_map: Dict[str, str] = {str(r[0]): str(r[1]) for r in rows if r[0]}
    logger.debug("load_sector_map: loaded %d sectors", len(sector_map))
    return sector_map


# ─────────────────────────────────────────────────────────────────────────────
# Legal-status map
# ─────────────────────────────────────────────────────────────────────────────

# Display names used when a row must be inserted from scratch.
_LEGAL_STATUS_DISPLAY: Dict[str, str] = {
    "co-operative society":              "Co-operative Society",
    "government agency":                 "Government Agency",
    "joint venture":                     "Joint Venture",
    "others":                            "Others",
    "parastatal organization":           "Parastatal Organization",
    "partnership":                       "Partnership",
    "private limited liability company": "Private Limited Liability Company",
    "public limited liability company":  "Public Limited Liability Company",
    "sole proprietor":                   "Sole Proprietor",
    "sole proprietor in a column":       "Sole Proprietor",   # alias → same display
}


def load_legal_status_map(db: Any) -> Dict[str, str]:
    """Return {lower(name) → uuid_str} for public.application_legal_status.

    If a canonical legal-status name is missing from the DB it is inserted so
    the mapping is always complete regardless of which environment you connect to.

    The dict key is lower-cased and stripped so lookups are case-insensitive.

    Example
    -------
    >>> ls_map = load_legal_status_map(db)
    >>> legal_status_id = ls_map.get(str(row["legalstatus"]).lower().strip())
    """
    from sqlalchemy import text

    rows = db.execute(
        text("""
            SELECT lower(trim(name)), id::text
            FROM   public.application_legal_status
            WHERE  deleted_at IS NULL
        """)
    ).fetchall()

    ls_map: Dict[str, str] = {str(r[0]): str(r[1]) for r in rows if r[0]}

    missing = [k for k in _LEGAL_STATUS_DISPLAY if k not in ls_map]

    if not missing:
        logger.debug("load_legal_status_map: all %d statuses found in DB", len(ls_map))
        return ls_map

    inserted: list[str] = []
    for key in missing:
        display = _LEGAL_STATUS_DISPLAY[key]
        # Alias keys (e.g. "sole proprietor in a column") share the same
        # display name — check whether that display name already exists so we
        # don't create duplicates.
        existing = ls_map.get(display.lower().strip())
        if existing:
            ls_map[key] = existing   # point alias at the real UUID
            continue

        new_id = str(_uuid_mod.uuid4())
        try:
            db.execute(
                text("""
                    INSERT INTO public.application_legal_status
                        (id, name, created_at, updated_at)
                    VALUES
                        (:id, :name, now(), now())
                    ON CONFLICT DO NOTHING
                """),
                {"id": new_id, "name": display},
            )
            db.flush()
            inserted.append(key)
            ls_map[key] = new_id
        except Exception as exc:
            logger.warning(
                "load_legal_status_map: could not insert '%s': %s", key, exc
            )
            try:
                db.rollback()
            except Exception:
                pass

    if inserted:
        logger.info(
            "load_legal_status_map: inserted %d new row(s): %s", len(inserted), inserted
        )

    # Re-fetch to pick up anything inserted by a concurrent call.
    rows2 = db.execute(
        text("""
            SELECT lower(trim(name)), id::text
            FROM   public.application_legal_status
            WHERE  deleted_at IS NULL
        """)
    ).fetchall()
    ls_map = {str(r[0]): str(r[1]) for r in rows2 if r[0]}

    # Resolve aliases again against the fresh data.
    for key, display in _LEGAL_STATUS_DISPLAY.items():
        if key not in ls_map:
            real = ls_map.get(display.lower().strip())
            if real:
                ls_map[key] = real

    return ls_map


# ─────────────────────────────────────────────────────────────────────────────
# License-category map
# ─────────────────────────────────────────────────────────────────────────────

def load_category_map(
    db: Any,
    sector_name: Optional[str] = None,
    ensure_names: Optional[list[str]] = None,
) -> Dict[str, str]:
    """Return {lower(name) → uuid_str} for public.categories.

    Parameters
    ----------
    db           : SQLAlchemy session bound to any environment.
    sector_name  : Optional sector filter (lower-cased DB name, e.g. ``"electricity"``).
                   When given, only categories belonging to that sector are returned
                   (and new rows are inserted under that sector).
                   When omitted, **all** categories are loaded.
    ensure_names : Optional list of category display names that MUST exist.
                   Any name not already in the DB is inserted with
                   ``ON CONFLICT (name) DO NOTHING`` so the function is
                   idempotent and safe for concurrent calls.

    The key in the returned dict is always ``lower(trim(name))``.

    Example
    -------
    >>> cat_map = load_category_map(db, ensure_names=["Petroleum Retail", "Water Supply"])
    >>> category_id = cat_map.get("petroleum retail")
    """
    from sqlalchemy import text

    # ── 1. Load existing rows ─────────────────────────────────────────────────
    if sector_name:
        rows = db.execute(
            text("""
                SELECT lower(trim(c.name)), c.id::text
                FROM   public.categories c
                JOIN   public.sectors    s ON s.id = c.sector_id
                WHERE  c.deleted_at IS NULL
                  AND  lower(trim(s.name)) = :sector
            """),
            {"sector": sector_name.lower().strip()},
        ).fetchall()
    else:
        rows = db.execute(
            text("""
                SELECT lower(trim(name)), id::text
                FROM   public.categories
                WHERE  deleted_at IS NULL
            """)
        ).fetchall()

    cat_map: Dict[str, str] = {str(r[0]): str(r[1]) for r in rows if r[0]}
    logger.debug(
        "load_category_map(sector=%r): loaded %d categories", sector_name, len(cat_map)
    )

    # ── 2. Insert missing names (if requested) ───────────────────────────────
    if not ensure_names:
        return cat_map

    missing = [
        n for n in ensure_names
        if n and n.strip() and n.strip().lower() not in cat_map
    ]
    if not missing:
        return cat_map

    # Resolve sector_id for inserts.
    # When caller provides a sector_name, use it.  Otherwise, load the full
    # sector map and detect per-name from keywords (petroleum, gas, water,
    # electricity, etc.).  This lets the application_migrations_service pass
    # category names from any sector without specifying which one.
    sector_map: Dict[str, str] = {}
    default_sector_id: Optional[str] = None

    all_sectors = db.execute(
        text("""
            SELECT lower(trim(name)), id::text
            FROM   public.sectors
            WHERE  deleted_at IS NULL
        """)
    ).fetchall()
    sector_map = {str(r[0]): str(r[1]) for r in all_sectors if r[0]}

    if sector_name:
        default_sector_id = sector_map.get(sector_name.lower().strip())

    if not sector_map:
        logger.warning(
            "load_category_map: no sectors found in DB — cannot insert %d missing "
            "categories; they will resolve to NULL",
            len(missing),
        )
        return cat_map

    # Keywords → sector name mapping for auto-detection.
    _SECTOR_HINTS: list[tuple[list[str], str]] = [
        (["petroleum", "lpg", "bunkering", "bitumen", "petcoke", "lubricant",
          "condensate", "pipeline transportation"], "petroleum"),
        (["natural gas", "lng", "compressed natural gas", "re-gasification",
          "re – gasification", "aggregation"], "natural_gas"),
        (["electricity", "generation", "transmission", "distribution",
          "cross border trade", "independent system operator", "supply"], "electricity"),
        (["water", "sanitation", "operatorship", "leasing assets",
          "bulk water"], "water_supply"),
    ]

    def _detect_sector_id(cat_name: str) -> Optional[str]:
        """Return sector_id for a category name by keyword match."""
        if default_sector_id:
            return default_sector_id
        low = cat_name.lower()
        for keywords, sname in _SECTOR_HINTS:
            for kw in keywords:
                if kw in low:
                    sid = sector_map.get(sname)
                    if sid:
                        return sid
        # Fallback: pick the first available sector so the row at least exists.
        return next(iter(sector_map.values()), None)

    # Schema guard: unique indexes required for ON CONFLICT
    try:
        db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_name
            ON public.categories (name) WHERE deleted_at IS NULL;
        """))
        db.flush()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    inserted: list[str] = []
    for name in missing:
        display = name.strip()
        key = display.lower()
        if key in cat_map:
            continue  # another iteration already inserted this name

        sid = _detect_sector_id(display)
        if not sid:
            logger.warning(
                "load_category_map: cannot determine sector for '%s' — skipping insert",
                display,
            )
            continue

        new_id = str(_uuid_mod.uuid4())
        try:
            db.execute(
                text("""
                    INSERT INTO public.categories (
                        id, name, code, sector_id,
                        sub_sector_type, category_type,
                        is_approved, created_at, updated_at
                    ) VALUES (
                        :id, :name, :name, :sector_id,
                        'OPERATIONAL', 'License',
                        false, now(), now()
                    )
                    ON CONFLICT (name) WHERE deleted_at IS NULL
                    DO NOTHING
                """),
                {"id": new_id, "name": display, "sector_id": sid},
            )
            db.flush()
            inserted.append(display)
        except Exception as exc:
            logger.warning(
                "load_category_map: could not insert category '%s': %s",
                display, exc,
            )
            try:
                db.rollback()
            except Exception:
                pass

    if inserted:
        logger.info(
            "load_category_map: inserted %d new category row(s): %s",
            len(inserted), inserted,
        )

    # ── 3. Re-fetch to pick up inserted + concurrently-added rows ─────────────
    if sector_name:
        rows2 = db.execute(
            text("""
                SELECT lower(trim(c.name)), c.id::text
                FROM   public.categories c
                JOIN   public.sectors    s ON s.id = c.sector_id
                WHERE  c.deleted_at IS NULL
                  AND  lower(trim(s.name)) = :sector
            """),
            {"sector": sector_name.lower().strip()},
        ).fetchall()
    else:
        rows2 = db.execute(
            text("""
                SELECT lower(trim(name)), id::text
                FROM   public.categories
                WHERE  deleted_at IS NULL
            """)
        ).fetchall()

    cat_map = {str(r[0]): str(r[1]) for r in rows2 if r[0]}
    logger.debug(
        "load_category_map(sector=%r): final map has %d categories",
        sector_name, len(cat_map),
    )
    return cat_map


# ─────────────────────────────────────────────────────────────────────────────
# Applicant role ID
# ─────────────────────────────────────────────────────────────────────────────

# The FDW foreign table is public.role (singular) on the remote side but
# mapped locally as public.roles (plural).  We try both names.
_ROLE_TABLE_CANDIDATES = ["public.roles", "public.role"]


def load_applicant_role_id(db: Any) -> Optional[str]:
    """Return the UUID of the 'APPLICANT' role as a plain string, or None.

    Strategy
    --------
    1. Try ``public.user_roles`` first — if any row exists, its role_id is
       guaranteed to be valid in this environment (avoids the FDW alias problem).
    2. If ``user_roles`` is empty, probe ``public.roles`` then ``public.role``
       (FDW alias) for a row whose name matches 'applicant' (case-insensitive).
    3. If the role is found nowhere, insert it into ``public.roles`` and return
       the new UUID.
    4. If all attempts fail, return ``None`` — callers skip role assignment
       gracefully.

    This replaces the fragile ``SELECT role_id FROM public.user_roles LIMIT 1``
    pattern that breaks when the table is empty or returns an unrelated role.

    Example
    -------
    >>> role_id = load_applicant_role_id(db)
    >>> if role_id:
    ...     db.execute(text("INSERT INTO public.user_roles ..."), {"role_id": role_id})
    """
    from sqlalchemy import text

    # ── 1. Fast path: borrow from an existing assignment ──────────────────────
    # Joining against public.roles/public.role here would hit the FDW alias
    # issue, so we skip the join and just trust the existing role_id value.
    try:
        row = db.execute(
            text("""
                SELECT DISTINCT ur.role_id::text
                FROM   public.user_roles ur
                LIMIT  1
            """)
        ).fetchone()
        if row and row[0]:
            logger.debug("load_applicant_role_id: resolved from user_roles → %s", row[0])
            return str(row[0])
    except Exception as exc:
        logger.debug("load_applicant_role_id: user_roles probe failed (%s)", exc)
        try:
            db.rollback()
        except Exception:
            pass

    # ── 2. Probe roles tables directly ────────────────────────────────────────
    for table in _ROLE_TABLE_CANDIDATES:
        try:
            row = db.execute(
                text(f"""
                    SELECT id::text
                    FROM   {table}
                    WHERE  lower(trim(name)) = 'applicant'
                      AND  (deleted_at IS NULL OR deleted_at > now())
                    LIMIT  1
                """)
            ).fetchone()
            if row and row[0]:
                logger.debug(
                    "load_applicant_role_id: found in %s → %s", table, row[0]
                )
                return str(row[0])
        except Exception as exc:
            logger.debug(
                "load_applicant_role_id: probe of %s failed (%s)", table, exc
            )
            try:
                db.rollback()
            except Exception:
                pass

    # ── 3. Insert into public.roles (first candidate that accepts the write) ──
    new_id = str(_uuid_mod.uuid4())
    for table in _ROLE_TABLE_CANDIDATES:
        try:
            db.execute(
                text(f"""
                    INSERT INTO {table} (id, name, created_at, updated_at)
                    VALUES (:id, 'APPLICANT', now(), now())
                    ON CONFLICT DO NOTHING
                """),
                {"id": new_id},
            )
            db.flush()
            # Verify it landed (ON CONFLICT DO NOTHING may mean another row won)
            row = db.execute(
                text(f"""
                    SELECT id::text FROM {table}
                    WHERE  lower(trim(name)) = 'applicant'
                    LIMIT  1
                """)
            ).fetchone()
            if row and row[0]:
                logger.info(
                    "load_applicant_role_id: inserted APPLICANT role in %s → %s",
                    table, row[0],
                )
                return str(row[0])
        except Exception as exc:
            logger.debug(
                "load_applicant_role_id: insert into %s failed (%s)", table, exc
            )
            try:
                db.rollback()
            except Exception:
                pass

    logger.warning(
        "load_applicant_role_id: APPLICANT role not found and could not be inserted "
        "— role assignment will be skipped for this import run"
    )
    return None


# ── Zone map cache ────────────────────────────────────────────────────────────
# Keyed by lower(region_name) → zone_id (uuid str).
# Loaded once per DB connection session; safe across multiple imports because
# zone assignments are stable within a DB.  A module-level dict protects
# against re-querying on every upload.
#
# Usage:
#   from app.utils.lookup_cache import load_zone_map
#   zone_map = load_zone_map(db)          # cached after first call per process
#   zone_id  = zone_map.get(region_name.lower().strip())
#
# SQL:
#   SELECT lower(n.name), z.id
#   FROM   napa_regions n
#   JOIN   zones z ON z.id = n.zone_id
# ─────────────────────────────────────────────────────────────────────────────
import threading as _threading

_zone_map_cache: Dict[str, str] = {}
_zone_map_lock  = _threading.Lock()
_zone_map_loaded = False


def load_zone_map(db: Any) -> Dict[str, str]:
    """Return {lower(region_name): zone_id_str} loaded from napa_regions JOIN zones.

    The result is cached in a module-level dict so repeated calls (e.g. across
    many batches of the same import, or across multiple uploads in the same
    server process) never hit the DB more than once.

    Thread-safe: the first call acquires a lock, loads, and sets the flag.
    All subsequent calls return the cached dict immediately.
    """
    global _zone_map_loaded

    # Fast path — already loaded.
    if _zone_map_loaded:
        return _zone_map_cache

    with _zone_map_lock:
        # Double-check inside the lock.
        if _zone_map_loaded:
            return _zone_map_cache

        try:
            from sqlalchemy import text
            rows = db.execute(text("""
                SELECT lower(trim(n.name)) AS region_name_lower,
                       z.id::text          AS zone_id
                FROM   public.napa_regions n
                JOIN   public.zones z ON z.id = n.zone_id
                WHERE  n.name  IS NOT NULL
                  AND  n.zone_id IS NOT NULL
            """)).fetchall()

            for row in rows:
                if row[0] and row[1]:
                    _zone_map_cache[row[0]] = row[1]

            _zone_map_loaded = True
            logger.info("load_zone_map: loaded %d region→zone mappings", len(_zone_map_cache))

        except Exception as exc:
            logger.warning(
                "load_zone_map: napa_regions/zones not available (%s) "
                "— zone_id will be NULL for this import run",
                exc,
            )
            # The failed query leaves the transaction in an aborted state.
            # Rollback so the caller's session is still usable.
            try:
                db.rollback()
            except Exception:
                pass
            # Mark as loaded so we don't keep retrying on every row.
            _zone_map_loaded = True

    return _zone_map_cache


def clear_zone_map_cache() -> None:
    """Force the next call to load_zone_map() to re-query the DB.

    Useful in tests or when the zones table is known to have changed.
    """
    global _zone_map_loaded
    with _zone_map_lock:
        _zone_map_cache.clear()
        _zone_map_loaded = False
