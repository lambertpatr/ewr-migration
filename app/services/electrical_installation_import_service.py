from __future__ import annotations

"""
Electrical Installation (Electricity sector – individual applicant) import service.

Excel columns expected
──────────────────────
application_number, application_type, district, email, facility_name,
mobile_no, plot_no, po_box, region, ward,
license_category_id, company_name, license_type, licensecategoryclass,
completed_at, effective_date, expire_date, approval_no, approvedclass,
parent_application_id, created_at, created_by,
dateofbirth, nationality, employmentstatus, gender, userid,
iftanzanian, iftanzanianfilename, workpermitno, cpphoto, cpphotofilename

Target tables (one row per Excel row, all linked by application_id)
────────────────────────────────────────────────────────────────────
1. applications                         – upsert by application_number
2. application_electrical_installation  – 1:1 with application
3. contact_details                      – email/mobile/address (region/district/ward mapped)
4. personal_details                     – dob/nationality/gender/work_permit
5. attachments                          – iftanzanian, cpphoto  (filename + object_id pairs)
6. work_experience                      – employmentstatus → experience_type
7. self_employed                        – only when experience_type = SELF_EMPLOYED
8. certificate_verifications            – approvedclass / license category info
"""

from typing import Any, Callable, Dict, Optional
import uuid
import pandas as pd
import logging
import io

from app.utils.lookup_cache import (
    load_elec_category_map,
    push_category_map_temp_table,
    load_applicant_role_id,
    load_default_role_id,
    ELEC_CAT_MAP_TEMP,
    load_zone_map,
)

logger = logging.getLogger(__name__)

_EMPTY = {"", "nan", "none", "null", "nat"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_conflict_set(insert_cols: list[str], table: str, *,
                        skip: tuple[str, ...] = ("id", "created_at")) -> str:
    """Return the SET clause for ON CONFLICT (id) DO UPDATE SET.

    For every column that was inserted (except the PK and created_at), emit:
        col = COALESCE(EXCLUDED.col, public.<table>.col)
    plus a trailing ``updated_at = now()``.

    This means adding a new column to an INSERT automatically appears in the
    conflict-update without any manual edits here.
    """
    parts = [
        f"{c} = COALESCE(EXCLUDED.{c}, public.{table}.{c})"
        for c in insert_cols
        if c not in skip
    ]
    parts.append("updated_at = now()")
    return ",\n                ".join(parts)


def _c(v) -> str:
    """Clean: strip whitespace, return '' for null/nan."""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in _EMPTY else s


def _n(v) -> str:
    """Return '' for empty, else clean integer string (handles scientific notation)."""
    s = _c(v)
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, OverflowError):
        return s


def _d(v) -> str:
    """Normalise to ISO date string YYYY-MM-DD, '' if unparseable."""
    import datetime as dt
    if isinstance(v, (dt.datetime, dt.date)):
        return str(v.date()) if isinstance(v, dt.datetime) else str(v)
    s = _c(v)
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Excel serial float
    try:
        import datetime as _dt
        f = float(s)
        if f > 1000:
            from datetime import timedelta
            base = _dt.datetime(1899, 12, 30)
            return (base + timedelta(days=f)).strftime("%Y-%m-%d")
    except Exception:
        pass
    return s


def _map_location(raw: str, lookup: Dict[str, str]) -> str:
    """Map a numeric ID string → name using a preloaded CSV dict.
    Handles plain integer strings, float strings ('1234.0'), and scientific notation.
    Falls back to the original value when not found in the map.
    """
    s = _c(raw)
    if not s:
        return ""
    if s in lookup:
        return lookup[s]
    # Try integer normalisation (strips '.0', handles scientific notation)
    try:
        nk = str(int(float(s)))
        if nk in lookup:
            return lookup[nk]
    except (ValueError, OverflowError):
        pass
    return s  # keep raw if not in map


EXPERIENCE_TYPE_MAP = {
    "employed":           "EMPLOYED",
    "fresh from college": "FRESH_FROM_COLLEGE",
    "self-employed":      "SELF_EMPLOYED",
    "self employed":      "SELF_EMPLOYED",
}


def _map_experience(v: str) -> str:
    s = _c(v).lower().strip()
    return EXPERIENCE_TYPE_MAP.get(s, "EMPLOYED")


APPLICATION_TYPE_MAP = {
    "new":     "NEW",
    "renew":   "RENEW",
    "renewal": "RENEW",
    "upgrade": "UPGRADE",
    "appeal":  "APPEAL",
}


def _map_application_type(v: str) -> str:
    s = _c(v).lower().strip()
    return APPLICATION_TYPE_MAP.get(s, _c(v).upper() if _c(v) else "NEW")


# ---------------------------------------------------------------------------
# Required / optional Excel columns
# ---------------------------------------------------------------------------
REQUIRED_COLS = {"application_number"}

OPTIONAL_COLS = {
    "application_type", "district", "email", "facility_name",
    "mobile_no", "plot_no", "po_box", "region", "ward",
    "license_category_id", "company_name", "license_type", "licensecategoryclass",
    "completed_at", "effective_date", "expire_date", "approval_no", "approvedclass",
    "parent_application_id", "created_at", "created_by",
    "dateofbirth", "nationality", "employmentstatus", "gender", "userid",
    "iftanzanian", "iftanzanianfilename", "workpermitno", "cpphoto", "cpphotofilename",
}


# ---------------------------------------------------------------------------
# Main import function
# ---------------------------------------------------------------------------

def import_electrical_installation_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
    include_rows: bool = False,
    limit_rows: int = 50,
) -> dict:
    """Import electricity-sector individual-applicant records.

    One Excel row inserts into up to 8 tables:
        applications  →  application_electrical_installation
          ├─ contact_details
          ├─ personal_details
          ├─ attachments
          ├─ work_experience
          │    └─ (if SELF_EMPLOYED) self_employed
          │         └─ costumer_details  (sic – matches DB column name)
          └─ certificate_verifications

    Region / district / ward are resolved from CSV maps (data/regions.csv etc.)
    using the same approach as the main applications importer.
    """
    from sqlalchemy import text

    def _progress(msg: str):
        logger.info("[electrical_installation_import] %s", msg)
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # ── 1. Normalise column names ──────────────────────────────────────
    df2 = df.copy()
    df2.columns = (
        df2.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    import sys
    _progress(f"columns ({len(df2.columns)}): {list(df2.columns)}")
    if len(df2) > 0:
        _progress(f"first row: {df2.iloc[0].to_dict()}")

    # ── 2. Validate required columns ──────────────────────────────────
    missing = REQUIRED_COLS - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    total_rows = len(df2)
    if total_rows == 0:
        return {"total_rows_in_file": 0, "inserted": {}}

    for col in OPTIONAL_COLS:
        if col not in df2.columns:
            df2[col] = ""

    # ── 3. Load region / district / ward maps ─────────────────────────
    def _load_map(attr: str) -> Dict[str, str]:
        try:
            from app.services.application_migrations_service import _normalize_numeric_string
            from app.services import application_migrations_service as _ams
            src: Dict[str, str] = getattr(_ams, attr, {}) or {}
            out: Dict[str, str] = {}
            for k, v in src.items():
                ks = str(k).strip()
                out[ks] = v
                try:
                    nk = _normalize_numeric_string(ks)
                    if nk and nk != ks:
                        out[nk] = v
                except Exception:
                    pass
                try:
                    out[f"{float(ks):.1f}"] = v
                    out[str(int(float(ks)))] = v
                except (ValueError, OverflowError):
                    pass
            _progress(f"{attr} loaded: {len(out)} entries")
            return out
        except Exception as e:
            _progress(f"WARNING: {attr} failed to load: {e}")
            return {}

    region_map   = _load_map("region_map_csv")
    district_map = _load_map("district_map_csv")
    ward_map     = _load_map("ward_map_csv")

    # Zone mapping — region name → zone_id (text UUID) via napa_regions JOIN zones.
    zone_map_elec = load_zone_map(db)

    # ── 4. Build per-row Python frame ─────────────────────────────────
    _progress(f"prepare: building export frame rows={total_rows}")

    n = total_rows

    # Generate stable UUIDs for every child record up front
    app_ids          = [str(uuid.uuid4()) for _ in range(n)]
    cert_ids         = [str(uuid.uuid4()) for _ in range(n)]
    aei_ids          = [str(uuid.uuid4()) for _ in range(n)]
    pd_ids           = [str(uuid.uuid4()) for _ in range(n)]
    cd_ids           = [str(uuid.uuid4()) for _ in range(n)]
    att_ids          = [str(uuid.uuid4()) for _ in range(n)]
    we_ids           = [str(uuid.uuid4()) for _ in range(n)]
    se_ids           = [str(uuid.uuid4()) for _ in range(n)]
    cv_ids           = [str(uuid.uuid4()) for _ in range(n)]

    rows = df2.to_dict(orient="records")

    # ── 5. DROP + CREATE staging table ────────────────────────────────
    _progress("staging:create")
    # Ensure the session is clean before DDL (a failed query upstream, e.g.
    # load_zone_map on a DB without public.zones, can leave the txn aborted).
    try:
        db.rollback()
    except Exception:
        pass
    db.execute(text("DROP TABLE IF EXISTS public.stage_elec_install_raw"))
    db.execute(text("""
        CREATE TABLE public.stage_elec_install_raw (
            row_no                    bigint PRIMARY KEY,
            app_id                    uuid NOT NULL,
            cert_id                   uuid NOT NULL,
            aei_id                    uuid NOT NULL,
            pd_id                     uuid NOT NULL,
            cd_id                     uuid NOT NULL,
            att_id                    uuid NOT NULL,
            we_id                     uuid NOT NULL,
            se_id                     uuid NOT NULL,
            cv_id                     uuid NOT NULL,
            -- application columns
            application_number        text,
            application_type          text,
            approval_no               text,
            license_type              text,
            licensecategoryclass      text,
            effective_date            text,
            expire_date               text,
            completed_at              text,
            approvedclass             text,
            parent_application_id     text,
            license_category_id       text,
            company_name              text,
            userid                    text,
            created_by                text,
            created_at_raw            text,
            -- certificate columns
            certificate_owner         text,
            -- personal details
            date_of_birth             text,
            nationality               text,
            gender                    text,
            work_permit_no            text,
            -- contact details (region/district/ward already mapped to names)
            region                    text,
            district                  text,
            ward                      text,
            zone_id                   text,
            email                     text,
            mobile_no                 text,
            plot_no                   text,
            po_box                    text,
            -- attachments
            identification            text,
            identification_name       text,
            passport                  text,
            passport_name             text,
            -- work experience
            experience_type           text,
            facility_name             text,
            -- certificate_verifications
            approved_class            text
        )
    """))

    # Build rows
    staging_rows = []
    for i, row in enumerate(rows):
        exp_raw   = _c(row.get("employmentstatus", ""))
        exp_type  = _map_experience(exp_raw)
        app_type  = _map_application_type(row.get("application_type", ""))

        staging_rows.append((
            i + 1,
            app_ids[i],
            cert_ids[i],
            aei_ids[i],
            pd_ids[i],
            cd_ids[i],
            att_ids[i],
            we_ids[i],
            se_ids[i],
            cv_ids[i],
            # application
            _c(row.get("application_number", "")),
            app_type,                                    # normalized application_type
            _c(row.get("approval_no", "")),
            _c(row.get("license_type", "")),
            _c(row.get("licensecategoryclass", "")),
            _d(row.get("effective_date", "")),
            _d(row.get("expire_date", "")),
            _d(row.get("completed_at", "")),
            _c(row.get("approvedclass", "")),
            _c(row.get("parent_application_id", "")),
            _c(row.get("license_category_id", "")),
            _c(row.get("company_name", "")),
            _c(row.get("userid", "")),
            _c(row.get("created_by", "")),
            _d(row.get("created_at", "")),
            # certificate_owner: facility_name first, fall back to company_name
            _c(row.get("facility_name", "")) or _c(row.get("company_name", "")),
            # personal_details
            _d(row.get("dateofbirth", "")),
            _c(row.get("nationality", "")),
            _c(row.get("gender", "")),
            _n(row.get("workpermitno", "")),
            # contact_details — map IDs → names using CSV
            _map_location(row.get("region",   ""), region_map),
            _map_location(row.get("district", ""), district_map),
            _map_location(row.get("ward",     ""), ward_map),
            # zone_id: resolved from region name via napa_regions→zones cache
            zone_map_elec.get((_map_location(row.get("region", ""), region_map) or "").lower().strip()) or "",
            _c(row.get("email", "")),
            _n(row.get("mobile_no", "")),
            _c(row.get("plot_no", "")),
            _c(row.get("po_box", "")),
            # attachments
            _n(row.get("iftanzanian", "")),
            _c(row.get("iftanzanianfilename", "")),
            _n(row.get("cpphoto", "")),
            _c(row.get("cpphotofilename", "")),
            # work_experience
            exp_type,
            _c(row.get("facility_name", "")),
            # certificate_verifications
            _c(row.get("approvedclass", "")),
        ))

    # COPY into staging
    sio = io.StringIO()
    for r in staging_rows:
        fields = []
        for v in r:
            if v is None or v == "":
                fields.append("")
            else:
                fields.append('"' + str(v).replace('"', '""') + '"')
        sio.write(",".join(fields) + "\n")
    sio.seek(0)

    sa_conn = db.connection()
    raw_conn = sa_conn.connection
    cur = raw_conn.cursor()
    try:
        cur.copy_expert("""
            COPY public.stage_elec_install_raw (
                row_no, app_id, cert_id, aei_id, pd_id, cd_id, att_id, we_id, se_id, cv_id,
                application_number, application_type, approval_no,
                license_type, licensecategoryclass,
                effective_date, expire_date, completed_at,
                approvedclass, parent_application_id,
                license_category_id, company_name,
                userid, created_by, created_at_raw,
                certificate_owner,
                date_of_birth, nationality, gender, work_permit_no,
                region, district, ward, zone_id, email, mobile_no, plot_no, po_box,
                identification, identification_name, passport, passport_name,
                experience_type, facility_name, approved_class
            ) FROM STDIN WITH CSV NULL ''
        """, sio)
    finally:
        cur.close()

    staged = int(db.execute(text("SELECT COUNT(*) FROM public.stage_elec_install_raw")).scalar() or 0)
    _progress(f"staging:done rows={staged}")
    # Commit staging so DDL in the next phase sees it and locks are released early
    db.commit()

    # ── 6. Schema guard — add migration columns if missing ────────────
    _progress("schema:guard")
    db.execute(text("""
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS applicant_name   character varying(255);
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS employer_name    character varying(255);
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS experience_type  character varying(255) DEFAULT 'EMPLOYED';
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ALTER COLUMN experience_type SET DEFAULT 'EMPLOYED';
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS approved_class_id uuid;
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS application_id   uuid;
        ALTER TABLE IF EXISTS public.application_electrical_installation
            ADD COLUMN IF NOT EXISTS is_from_lois     boolean DEFAULT false;

        ALTER TABLE IF EXISTS public.personal_details
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.personal_details
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

        ALTER TABLE IF EXISTS public.contact_details
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.contact_details
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;
        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS passport_photo                        character varying(255);
        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS passport_photo_name                   character varying(255);
        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS permit_document                       character varying(255);
        ALTER TABLE IF EXISTS public.attachments
            ADD COLUMN IF NOT EXISTS permit_document_name                  character varying(255);

        ALTER TABLE IF EXISTS public.work_experience
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.work_experience
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

        ALTER TABLE IF EXISTS public.self_employed
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.self_employed
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;

        ALTER TABLE IF EXISTS public.supervisor_details
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.supervisor_details
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;
        ALTER TABLE IF EXISTS public.supervisor_details
            ADD COLUMN IF NOT EXISTS work_experience_id                    uuid;

        ALTER TABLE IF EXISTS public.costumer_details
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.costumer_details
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;
        ALTER TABLE IF EXISTS public.costumer_details
            ADD COLUMN IF NOT EXISTS self_employed_id                      uuid;

        ALTER TABLE IF EXISTS public.certificate_verifications
            ADD COLUMN IF NOT EXISTS application_id                        uuid;
        ALTER TABLE IF EXISTS public.certificate_verifications
            ADD COLUMN IF NOT EXISTS application_electrical_installation_id uuid;
    """))
    # Cast application_sector_details.latitude/longitude to numeric so the
    # DB trigger trg_sync_approved_licenses (which copies them into
    # approved_licenses.latitude/longitude numeric columns) doesn't fail.
    for _col in ("latitude", "longitude"):
        try:
            db.execute(text(f"""
                ALTER TABLE public.application_sector_details
                    ALTER COLUMN {_col} TYPE numeric
                    USING CASE
                        WHEN NULLIF(TRIM({_col}), '') IS NULL THEN NULL
                        ELSE TRIM({_col})::numeric
                    END
            """))
            db.commit()
        except Exception as _cast_err:
            logger.debug("schema:guard: %s cast skipped (%s)", _col, _cast_err)
            try:
                db.rollback()
            except Exception:
                pass
    # Commit DDL immediately — ALTER TABLE must not share a transaction with DML
    db.commit()
    _progress("schema:guard:done")

    # ── 6½. Ensure uq_certificates_application_number exists ──────────
    #
    # Business rule: one certificate row per application_number.
    # All import pipelines now use ON CONFLICT (application_number) DO UPDATE.
    # We must:
    #   1. Drop any existing unique on certificates that includes approval_no
    #      or application_number (could conflict with the new constraint).
    #   2. Dedup any duplicate application_number rows, then
    #   3. Add UNIQUE (application_number) if not already present.
    #
    # Applies to 'public' and 'align_live' schemas if they exist.
    _progress("schema:relax-certificates-constraints")
    db.execute(text("""
        DO $$
        DECLARE
            _rec               RECORD;
            _schema            text;
            _app_attnum        smallint;
            _cert_apprv_attnum  smallint;
            _cert_appnum_attnum smallint;
        BEGIN
            FOR _schema IN
                SELECT nspname FROM pg_namespace
                WHERE  nspname IN ('public', 'align_live')
                ORDER BY nspname
            LOOP

                -- ── A. Drop ANY unique on applications.approval_no ────────────
                IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                           WHERE n.nspname=_schema AND c.relname='applications') THEN
                    SELECT attnum INTO _app_attnum
                    FROM pg_attribute
                    WHERE attrelid = (
                              SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='applications')
                      AND attname = 'approval_no';

                    IF _app_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname
                            FROM   pg_constraint con
                            JOIN   pg_class cls ON cls.oid = con.conrelid
                            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE  con.contype = 'u'
                              AND  nsp.nspname = _schema
                              AND  cls.relname = 'applications'
                              AND  _app_attnum = ANY(con.conkey)
                        LOOP
                            EXECUTE format('ALTER TABLE %I.applications DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped applications unique on approval_no: %.%', _schema, _rec.conname;
                        END LOOP;
                    END IF;
                END IF;

                -- ── B & C. Drop ALL unique constraints on certificates that include
                --          approval_no OR application_number (single or composite) ──
                IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                           WHERE n.nspname=_schema AND c.relname='certificates') THEN

                    SELECT attnum INTO _cert_apprv_attnum
                    FROM pg_attribute
                    WHERE attrelid = (
                              SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'approval_no';

                    SELECT attnum INTO _cert_appnum_attnum
                    FROM pg_attribute
                    WHERE attrelid = (
                              SELECT c.oid FROM pg_class c
                              JOIN pg_namespace n ON n.oid=c.relnamespace
                              WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'application_number';

                    -- Drop any unique that mentions approval_no (single OR composite)
                    IF _cert_apprv_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname
                            FROM   pg_constraint con
                            JOIN   pg_class cls ON cls.oid = con.conrelid
                            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE  con.contype = 'u'
                              AND  nsp.nspname = _schema
                              AND  cls.relname = 'certificates'
                              AND  _cert_apprv_attnum = ANY(con.conkey)
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped certificates unique on approval_no: %.%', _schema, _rec.conname;
                        END LOOP;
                    END IF;

                    -- Drop any existing unique that mentions application_number
                    -- (we are about to re-add a clean single-column one below)
                    IF _cert_appnum_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname
                            FROM   pg_constraint con
                            JOIN   pg_class cls ON cls.oid = con.conrelid
                            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE  con.contype = 'u'
                              AND  nsp.nspname = _schema
                              AND  cls.relname = 'certificates'
                              AND  _cert_appnum_attnum = ANY(con.conkey)
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped certificates unique on application_number: %.%', _schema, _rec.conname;
                        END LOOP;
                    END IF;

                    -- ── D. Add UNIQUE (application_number) ── the upsert conflict key ──
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint con
                        JOIN pg_class     cls ON cls.oid = con.conrelid
                        JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                        WHERE  con.contype = 'u'
                          AND  nsp.nspname = _schema
                          AND  cls.relname = 'certificates'
                          AND  con.conname = 'uq_certificates_application_number'
                    ) THEN
                        -- Dedup first so the constraint can be created cleanly
                        EXECUTE format(
                            'DELETE FROM %I.certificates
                             WHERE id IN (
                                 SELECT id FROM (
                                     SELECT id,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY application_number
                                                ORDER BY updated_at DESC NULLS LAST,
                                                         created_at  DESC NULLS LAST,
                                                         id
                                            ) AS rn
                                     FROM %I.certificates
                                     WHERE application_number IS NOT NULL
                                 ) ranked
                                 WHERE rn > 1
                             )',
                            _schema, _schema
                        );
                        EXECUTE format(
                            'ALTER TABLE %I.certificates
                             ADD CONSTRAINT uq_certificates_application_number
                             UNIQUE (application_number)',
                            _schema
                        );
                        RAISE NOTICE 'Added uq_certificates_application_number on %.certificates', _schema;
                    END IF;

                END IF; -- certificates table exists

            END LOOP; -- schemas
        END;
        $$;
    """))
    _progress("schema:relax-certificates-constraints:done")
    # Commit constraint changes before data inserts
    db.commit()

    # ── STEP A0: ensure users exist for userid values in the Excel ─────
    # Done FIRST — before inserting applications — so that created_by can
    # be back-filled from users.id immediately after applications are inserted.
    _progress("transform:users_from_userid")
    db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
    # Normalize userids to lowercase in staging table so mixed-case duplicates collapse
    db.execute(text("""
        UPDATE public.stage_elec_install_raw
        SET userid = lower(trim(userid))
        WHERE userid IS NOT NULL
    """))
    _elec_users_result = db.execute(text("""
        WITH u AS (
            SELECT DISTINCT lower(trim(s.userid)) AS username
            FROM public.stage_elec_install_raw s
            WHERE NULLIF(trim(s.userid), '') IS NOT NULL
        )
        INSERT INTO public.users (
            id, full_name, username, password_hash, status,
            phone_number, email_address, user_category,
            account_type, auth_mode, failed_attempts,
            is_first_login, deleted, created_at, updated_at
        )
        SELECT
            gen_random_uuid(), u.username, u.username, '',
            'ACTIVE', NULL, NULL, 'EXTERNAL', 'INDIVIDUAL', 'DB',
            0, false, false, now(), now()
        FROM u
        WHERE NOT EXISTS (
            SELECT 1 FROM public.users eu
            WHERE lower(trim(eu.username)) = u.username
        );
    """))
    _inserted_users = _elec_users_result.rowcount or 0
    _total_elec_usernames = db.execute(text(
        "SELECT COUNT(DISTINCT lower(trim(userid))) FROM public.stage_elec_install_raw "
        "WHERE NULLIF(trim(userid), '') IS NOT NULL"
    )).scalar() or 0
    _skipped_users = max(0, int(_total_elec_usernames) - int(_inserted_users))
    _progress(f"transform:users_from_userid: inserted={_inserted_users}, already_existed={_skipped_users}")

    # Resolve DEFAULT role_id — assigned to all migrated users.
    _elec_role_id = load_default_role_id(db)
    _inserted_user_roles = 0
    _skipped_user_roles = 0
    if not _elec_role_id:
        logger.info("DEFAULT role not resolved; role assignment skipped for this run")
        _skipped_user_roles = int(_total_elec_usernames)

    # Assign DEFAULT role to every staged user that doesn't have it yet.
    # FDW sends all locally-defined columns (user_id, role_id, deleted,
    # created_at) to the remote — supply explicit values to avoid NOT-NULL
    # violations on the remote.
    if _elec_role_id:
        try:
            _rr = db.execute(text("""
                INSERT INTO public.user_roles (user_id, role_id, deleted, created_at)
                SELECT u.id, :role_id, false, now()
                FROM public.users u
                WHERE EXISTS (
                    SELECT 1 FROM public.stage_elec_install_raw s
                    WHERE NULLIF(trim(s.userid), '') IS NOT NULL
                      AND lower(trim(s.userid)) = lower(trim(u.username))
                )
                AND NOT EXISTS (
                    SELECT 1 FROM public.user_roles ex
                    WHERE ex.user_id = u.id AND ex.role_id = :role_id
                )
            """), {"role_id": _elec_role_id})
            db.commit()
            _inserted_user_roles = _rr.rowcount or 0
            _skipped_user_roles = max(0, int(_total_elec_usernames) - int(_inserted_user_roles))
            _progress(f"transform:role_assignment: inserted={_inserted_user_roles}, already_had_role={_skipped_user_roles}")
        except Exception as _ure:
            logger.warning("Role assignment skipped (non-fatal): %s", _ure)
            _skipped_user_roles = int(_total_elec_usernames)
            try:
                db.rollback()
            except Exception:
                pass
    _progress("transform:users_from_userid:done")

    # ── 7a. Load category map from DB (dynamic — insert if missing) ───────────
    # Replaces the old hard-coded UUID CASE expression.
    # Falls back gracefully: if the DB/FDW is unreachable for categories the
    # temp table will be empty and the SQL ELSE branch returns NULL (same as before).
    _progress("transform:category_map:load")
    _cat_map: Dict[str, str] = {}
    try:
        _cat_map = load_elec_category_map(db)
        push_category_map_temp_table(db, _cat_map)
        db.flush()
        _progress(
            f"transform:category_map:loaded codes={list(_cat_map.keys())}"
        )
    except Exception as _cme:
        logger.warning(
            "transform:category_map: failed to load/push (%s) — "
            "SQL will fall back to hard-coded CASE uuids",
            _cme,
        )
        # Ensure the temp table exists but is empty so the SQL still runs cleanly.
        try:
            db.execute(text(f"""
                DROP TABLE IF EXISTS {ELEC_CAT_MAP_TEMP};
                CREATE TEMP TABLE {ELEC_CAT_MAP_TEMP} (
                    code        text PRIMARY KEY,
                    class_label text NOT NULL,
                    category_id uuid NOT NULL
                );
            """))
        except Exception:
            pass

    # ── 7. Set-based SQL transform across all 8 tables ────────────────
    _progress("transform:sql:start")

    transform_sql = text("""
        -- ── STEP A: Upsert into applications ──────────────────────────────
        WITH eligible_apps AS (
            -- Keep only first occurrence per application_number within the file
            -- AND skip any application_number already in the DB
            SELECT s.*
            FROM public.stage_elec_install_raw s
            WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM public.applications a
                  WHERE a.application_number = trim(s.application_number)
              )
              AND s.row_no = (
                  SELECT MIN(s2.row_no)
                  FROM public.stage_elec_install_raw s2
                  WHERE trim(s2.application_number) = trim(s.application_number)
              )
        ),
        -- Deduplicate approval_no: only the first occurrence per approval_no value
        -- keeps its approval_no; duplicates get NULL to avoid the unique constraint.
        deduped AS (
            SELECT
                s.*,
                NULLIF(trim(s.approval_no), '') AS safe_approval_no
            FROM eligible_apps s
        ),
        ins_apps AS (
            INSERT INTO public.applications (
                id, application_number, application_type,
                approval_no, approval_date,
                license_type, category_license_type,
                category_id,
                effective_date, expire_date,
                old_parent_application_id,
                username, old_created_by,
                status, is_from_lois,
                zone_id,
                created_at, updated_at
            )
            SELECT
                ea.app_id,
                trim(ea.application_number),
                NULLIF(trim(ea.application_type), ''),
                ea.safe_approval_no,             -- NULL when duplicate or already exists
                -- completed_at → approval_date
                CASE WHEN NULLIF(trim(ea.completed_at),'') IS NOT NULL
                     THEN trim(ea.completed_at)::date END,
                'LICENSE_ELECTRICITY_INSTALLATION',  -- license_type: fixed per check constraint
                'OPERATIONAL',                       -- category_license_type: fixed per check constraint
                -- category_id: resolved dynamically from stage_elec_category_map temp table
                -- (loaded from public.categories at import time; missing codes inserted on-demand).
                -- The JOIN normalises both "CLASS A" and bare "A" formats.
                -- Fallback ELSE: raw UUID string pass-through, then NULL.
                COALESCE(
                    cm_lc.category_id,
                    -- OLD hard-coded fallback (kept for safety; active when temp table is empty):
                    -- CASE UPPER(REGEXP_REPLACE(TRIM(ea.licensecategoryclass), '\\s+', ' '))
                    --     WHEN 'CLASS A'  THEN '6dd52222-0eb2-471e-830d-1ee943177f93'::uuid ...
                    CASE WHEN NULLIF(TRIM(ea.licensecategoryclass),'') ~ '^[0-9a-fA-F-]{36}$'
                         THEN TRIM(ea.licensecategoryclass)::uuid
                         ELSE NULL END
                ),
                CASE WHEN NULLIF(trim(ea.effective_date),'') IS NOT NULL
                     THEN trim(ea.effective_date)::date END,
                CASE WHEN NULLIF(trim(ea.expire_date),'') IS NOT NULL
                     THEN trim(ea.expire_date)::date END,
                NULLIF(trim(ea.parent_application_id), ''),
                NULLIF(trim(ea.userid), ''),
                NULLIF(trim(ea.created_by), ''),
                'APPROVED',
                true,
                NULLIF(trim(ea.zone_id), '')::uuid,
                COALESCE(
                    CASE WHEN NULLIF(trim(ea.created_at_raw),'') IS NOT NULL
                         THEN trim(ea.created_at_raw)::timestamp END,
                    now()
                ),
                now()
            FROM deduped ea
            -- Dynamic category map: JOIN normalises both "CLASS A" and bare "A".
            LEFT JOIN stage_elec_category_map cm_lc
                   ON cm_lc.code = UPPER(REGEXP_REPLACE(TRIM(ea.licensecategoryclass), '\\s+', ' '))
                   OR cm_lc.class_label = UPPER(REGEXP_REPLACE(TRIM(ea.licensecategoryclass), '\\s+', ' '))
            ON CONFLICT (application_number) DO UPDATE SET
                -- Only fill NULLs — never overwrite existing non-null values (COALESCE pattern).
                application_type          = COALESCE(public.applications.application_type,          EXCLUDED.application_type),
                approval_no               = COALESCE(public.applications.approval_no,               EXCLUDED.approval_no),
                approval_date             = COALESCE(public.applications.approval_date,             EXCLUDED.approval_date),
                category_id               = COALESCE(public.applications.category_id,               EXCLUDED.category_id),
                effective_date            = COALESCE(public.applications.effective_date,            EXCLUDED.effective_date),
                expire_date               = COALESCE(public.applications.expire_date,               EXCLUDED.expire_date),
                old_parent_application_id = COALESCE(public.applications.old_parent_application_id, EXCLUDED.old_parent_application_id),
                username                  = COALESCE(public.applications.username,                  EXCLUDED.username),
                old_created_by            = COALESCE(public.applications.old_created_by,            EXCLUDED.old_created_by),
                zone_id                   = COALESCE(public.applications.zone_id,                   EXCLUDED.zone_id),
                -- These two are always enforced for electrical installation imports.
                status                    = 'APPROVED',
                is_from_lois              = true,
                updated_at                = now()
            RETURNING id
        )
        SELECT COUNT(*) AS inserted_apps FROM ins_apps;
    """)
    r = db.execute(transform_sql).mappings().first() or {}
    ins_apps = int(r.get("inserted_apps", 0) or 0)
    _progress(f"transform:applications inserted={ins_apps}")

    # Ensure all matching rows (including pre-existing) have status=APPROVED and is_from_lois=true,
    # and back-fill category_id where it was previously NULL (e.g. rows inserted before the
    # licensecategoryclass mapping was added).
    # Uses the same stage_elec_category_map temp table loaded above.
    db.execute(text(f"""
        UPDATE public.applications a
        SET    status       = 'APPROVED',
               is_from_lois = true,
               category_id  = COALESCE(
                   a.category_id,
                   -- Dynamic lookup from temp table (code or class_label match):
                   cm.category_id
                   -- OLD hard-coded fallback (uncomment to revert):
                   -- CASE UPPER(REGEXP_REPLACE(TRIM(s.licensecategoryclass), '\\s+', ' '))
                   --     WHEN 'CLASS A'  THEN '6dd52222-0eb2-471e-830d-1ee943177f93'::uuid ... ELSE NULL END
               ),
               updated_at   = now()
        FROM   public.stage_elec_install_raw s
        LEFT JOIN {ELEC_CAT_MAP_TEMP} cm
               ON cm.code        = UPPER(REGEXP_REPLACE(TRIM(s.licensecategoryclass), '\\s+', ' '))
               OR cm.class_label = UPPER(REGEXP_REPLACE(TRIM(s.licensecategoryclass), '\\s+', ' '))
        WHERE  a.application_number = trim(s.application_number)
          AND  (
               a.status != 'APPROVED'
            OR a.is_from_lois IS DISTINCT FROM true
            OR a.category_id IS NULL
          );
    """))
    db.commit()
    _progress("transform:applications status+is_from_lois+category_id enforced")

    # ── STEP A½: certificates ──────────────────────────────────────────
    # Business rule: one certificate row per application_number.
    # Different application_numbers may share the same approval_no — that is fine,
    # each application owns its own certificate row keyed by application_number.
    # Re-uploads update the existing row rather than fail on approval_no conflicts.
    #
    # license_type and category_license_type are copied directly from the already-inserted
    # applications row (joined via application_id) so both tables always share the same values.
    r1b = db.execute(text("""
        WITH eligible AS (
            -- One row per application_number (first occurrence in the file).
            SELECT DISTINCT ON (trim(s.application_number))
                   s.*,
                   a.id                    AS resolved_app_id,
                   a.category_id           AS resolved_category_id,
                   a.license_type          AS resolved_license_type,
                   a.category_license_type AS resolved_category_license_type
            FROM public.stage_elec_install_raw s
            JOIN public.applications a ON a.application_number = trim(s.application_number)
            ORDER BY trim(s.application_number), s.row_no
        ),
        ins AS (
            INSERT INTO public.certificates (
                id,
                application_id,
                application_number,
                certificate_owner,
                approval_no,
                approval_date,
                category_license_type,
                license_type,
                effective_date,
                expire_date,
                application_certificate_type,
                owner_id,
                created_at, updated_at
            )
            SELECT
                -- id is just a surrogate PK; conflict key is application_number (unique).
                gen_random_uuid(),
                e.resolved_app_id,
                trim(e.application_number),
                NULLIF(trim(e.certificate_owner), ''),
                NULLIF(trim(e.approval_no), ''),
                CASE WHEN NULLIF(trim(e.completed_at),'') IS NOT NULL
                     THEN trim(e.completed_at)::date END,
                e.resolved_category_license_type,
                e.resolved_license_type,
                CASE WHEN NULLIF(trim(e.effective_date),'') IS NOT NULL
                     THEN trim(e.effective_date)::date END,
                CASE WHEN NULLIF(trim(e.expire_date),'') IS NOT NULL
                     THEN trim(e.expire_date)::date END,
                COALESCE(NULLIF(trim(e.application_type), ''), 'NEW'),
                -- owner_id: the user who created the application (created_by = users.id)
                a_owner.id,
                now(), now()
            FROM eligible e
            LEFT JOIN public.applications a_src ON a_src.id = e.resolved_app_id
            LEFT JOIN public.users a_owner ON a_owner.id = a_src.created_by
            ON CONFLICT (application_number) DO UPDATE
            SET
                approval_no               = COALESCE(EXCLUDED.approval_no,               public.certificates.approval_no),
                approval_date             = COALESCE(EXCLUDED.approval_date,             public.certificates.approval_date),
                license_type              = EXCLUDED.license_type,
                category_license_type     = EXCLUDED.category_license_type,
                application_id            = EXCLUDED.application_id,
                effective_date            = COALESCE(EXCLUDED.effective_date,            public.certificates.effective_date),
                expire_date               = COALESCE(EXCLUDED.expire_date,               public.certificates.expire_date),
                certificate_owner         = COALESCE(EXCLUDED.certificate_owner,         public.certificates.certificate_owner),
                application_certificate_type = COALESCE(EXCLUDED.application_certificate_type, public.certificates.application_certificate_type),
                -- Always fill owner_id when we now have a value and the stored one is still NULL
                owner_id                  = COALESCE(public.certificates.owner_id, EXCLUDED.owner_id),
                updated_at                = now()
            RETURNING id
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_certs = int(r1b.get("cnt", 0) or 0)
    _progress(f"transform:certificates upserted={ins_certs}")

    # Back-fill applications.certificate_id where still NULL
    db.execute(text("""
        UPDATE public.applications a
        SET    certificate_id = c.id,
               updated_at     = now()
        FROM   public.certificates c
        WHERE  c.application_id = a.id
          AND  a.certificate_id IS NULL
          AND  a.is_from_lois = true;
    """))
    db.commit()
    _progress("transform:applications.certificate_id back-filled")

    # ── STEP B: application_electrical_installation ────────────────────
    # approved_class_id is resolved via stage_elec_category_map temp table loaded above.
    r2 = db.execute(text(f"""
        WITH eligible AS (
            SELECT DISTINCT ON (a.id)
                   s.*,
                   a.id AS resolved_app_id,
                   -- approved_class_id: dynamic lookup from stage_elec_category_map
                   -- (matches both "CLASS A" and bare "A" via class_label / code columns).
                   -- OLD hard-coded fallback (uncomment to revert):
                   -- CASE UPPER(REGEXP_REPLACE(TRIM(s.approvedclass), '\\s+', ' '))
                   --     WHEN 'CLASS A'  THEN '6dd52222-...'::uuid ... ELSE NULL END
                   COALESCE(
                       cm_ac.category_id,
                       NULL  -- stays NULL when code not in temp table (rare / unknown class)
                   ) AS resolved_approved_class_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a ON a.application_number = trim(s.application_number)
            LEFT JOIN {ELEC_CAT_MAP_TEMP} cm_ac
                   ON cm_ac.code        = UPPER(REGEXP_REPLACE(TRIM(s.approvedclass), '\\s+', ' '))
                   OR cm_ac.class_label = UPPER(REGEXP_REPLACE(TRIM(s.approvedclass), '\\s+', ' '))
            ORDER BY a.id, s.row_no
        ),
        ins AS (
            INSERT INTO public.application_electrical_installation (
                id, application_id,
                applicant_name, experience_type,
                approved_class_id,
                is_from_lois, created_at, updated_at
            )
            SELECT
                -- Stable id: md5(application_id || '|aei') so reruns are idempotent.
                md5(e.resolved_app_id::text || '|aei')::uuid,
                e.resolved_app_id,
                -- applicant_name from company_name per mapping spec
                NULLIF(trim(e.company_name), ''),
                COALESCE(NULLIF(trim(e.experience_type), ''), 'EMPLOYED'),
                e.resolved_approved_class_id,
                true, now(), now()
            FROM eligible e
            ON CONFLICT (id) DO UPDATE SET
                {_build_conflict_set(
                    ['application_id', 'applicant_name', 'experience_type', 'approved_class_id'],
                    'application_electrical_installation',
                    skip=('id', 'created_at')
                )},
                is_from_lois = true
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_aei = int(r2.get("cnt", 0) or 0)
    _progress(f"transform:application_electrical_installation inserted={ins_aei}")
    db.commit()

    # ── STEP C: contact_details ────────────────────────────────────────
    _cd_cols = [
        'application_id', 'application_electrical_installation_id',
        'region', 'district', 'ward', 'email', 'mobile_no', 'plot_no', 'po_box',
    ]
    r3 = db.execute(text(f"""
        WITH eligible AS (
            SELECT DISTINCT ON (aei.id)
                   s.*,
                   a.id   AS resolved_app_id,
                   aei.id AS resolved_aei_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a
                ON a.application_number = trim(s.application_number)
            JOIN public.application_electrical_installation aei
                ON aei.application_id = a.id
            WHERE NOT EXISTS (
                SELECT 1 FROM public.contact_details cd
                WHERE cd.application_electrical_installation_id = aei.id
            )
            ORDER BY aei.id, s.row_no
        ),
        ins AS (
            INSERT INTO public.contact_details (
                id,
                application_id,
                application_electrical_installation_id,
                region, district, ward,
                email, mobile_no, plot_no, po_box,
                created_at, updated_at
            )
            SELECT
                -- Stable id derived from aei_id so re-uploads always produce the
                -- same UUID and ON CONFLICT (id) deduplicates correctly.
                md5(e.resolved_aei_id::text || '|cd')::uuid,
                e.resolved_app_id,
                e.resolved_aei_id,
                NULLIF(trim(e.region), ''),
                NULLIF(trim(e.district), ''),
                NULLIF(trim(e.ward), ''),
                NULLIF(trim(e.email), ''),
                NULLIF(trim(e.mobile_no), ''),
                NULLIF(trim(e.plot_no), ''),
                NULLIF(trim(e.po_box), ''),
                now(), now()
            FROM eligible e
            ON CONFLICT (id) DO UPDATE SET
                {_build_conflict_set(_cd_cols, 'contact_details')}
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_cd = int(r3.get("cnt", 0) or 0)
    _progress(f"transform:contact_details inserted={ins_cd}")
    db.commit()

    # ── STEP D: personal_details ───────────────────────────────────────
    _pd_cols = [
        'application_id', 'application_electrical_installation_id',
        'date_of_birth', 'nationality', 'gender', 'work_permit_no',
    ]
    r4 = db.execute(text(f"""
        WITH eligible AS (
            SELECT DISTINCT ON (aei.id)
                   s.*,
                   a.id   AS resolved_app_id,
                   aei.id AS resolved_aei_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a
                ON a.application_number = trim(s.application_number)
            JOIN public.application_electrical_installation aei
                ON aei.application_id = a.id
            WHERE NOT EXISTS (
                SELECT 1 FROM public.personal_details pd
                WHERE pd.application_electrical_installation_id = aei.id
            )
            ORDER BY aei.id, s.row_no
        ),
        ins AS (
            INSERT INTO public.personal_details (
                id,
                application_id,
                application_electrical_installation_id,
                date_of_birth, nationality, gender, work_permit_no,
                created_at, updated_at
            )
            SELECT
                -- Stable id derived from aei_id so re-uploads always produce the
                -- same UUID and ON CONFLICT (id) deduplicates correctly.
                md5(e.resolved_aei_id::text || '|pd')::uuid,
                e.resolved_app_id,
                e.resolved_aei_id,
                CASE WHEN NULLIF(trim(e.date_of_birth),'') IS NOT NULL
                     THEN trim(e.date_of_birth)::date END,
                -- Normalize nationality to enum: TANZANIAN | NON_TANZANIAN | NULL
                CASE
                    WHEN LOWER(TRIM(e.nationality)) = 'tanzanian'          THEN 'TANZANIAN'
                    WHEN LOWER(TRIM(e.nationality)) LIKE '%non%tanzanian%' THEN 'NON_TANZANIAN'
                    WHEN LOWER(TRIM(e.nationality)) = 'non-tanzanian'      THEN 'NON_TANZANIAN'
                    ELSE NULL
                END,
                NULLIF(trim(e.gender), ''),
                NULLIF(trim(e.work_permit_no), ''),
                now(), now()
            FROM eligible e
            ON CONFLICT (id) DO UPDATE SET
                {_build_conflict_set(_pd_cols, 'personal_details')}
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_pd = int(r4.get("cnt", 0) or 0)
    _progress(f"transform:personal_details inserted={ins_pd}")
    db.commit()

    # ── STEP E: attachments ────────────────────────────────────────────
    _att_cols = [
        'application_id', 'application_electrical_installation_id',
        'identification', 'identification_name', 'passport', 'passport_name',
    ]
    r5 = db.execute(text(f"""
        WITH eligible AS (
            SELECT DISTINCT ON (aei.id)
                   s.*,
                   a.id   AS resolved_app_id,
                   aei.id AS resolved_aei_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a
                ON a.application_number = trim(s.application_number)
            JOIN public.application_electrical_installation aei
                ON aei.application_id = a.id
            WHERE NOT EXISTS (
                SELECT 1 FROM public.attachments att
                WHERE att.application_electrical_installation_id = aei.id
            )
            ORDER BY aei.id, s.row_no
        ),
        ins AS (
            INSERT INTO public.attachments (
                id,
                application_id,
                application_electrical_installation_id,
                identification,
                identification_name,
                passport,
                passport_name,
                created_at, updated_at
            )
            SELECT
                -- Stable id derived from aei_id so re-uploads always produce the
                -- same UUID and ON CONFLICT (id) deduplicates correctly.
                md5(e.resolved_aei_id::text || '|att')::uuid,
                e.resolved_app_id,
                e.resolved_aei_id,
                NULLIF(trim(e.identification), ''),
                NULLIF(trim(e.identification_name), ''),
                NULLIF(trim(e.passport), ''),
                NULLIF(trim(e.passport_name), ''),
                now(), now()
            FROM eligible e
            ON CONFLICT (id) DO UPDATE SET
                {_build_conflict_set(_att_cols, 'attachments')}
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_att = int(r5.get("cnt", 0) or 0)
    _progress(f"transform:attachments inserted={ins_att}")
    db.commit()

    # ── STEP F: work_experience — SKIPPED on this import ──────────────
    # work_experience.voltage_level is NOT NULL and is not available in this
    # Excel file. A separate import (with from_date / to_date / voltage_level
    # / work_description etc.) will populate this table.
    ins_we = 0
    _progress("transform:work_experience skipped (voltage_level data not in this file)")

    # ── STEP G: self_employed — insert for SELF_EMPLOYED applicants ──────────
    # The main file has employmentstatus → experience_type = 'SELF_EMPLOYED'
    # already staged in stage_elec_install_raw.experience_type.
    # We insert one self_employed row per application whose experience_type
    # is SELF_EMPLOYED. work_experience / voltage_level fields are not in this
    # file; use NULL / 'NONE' placeholders — they will be enriched by the
    # supervisors/work-experience upload later.
    _se_cols = [
        'application_id', 'application_electrical_installation_id',
        'from_date', 'to_date', 'project_performed', 'voltage_level', 'voltage',
    ]
    r6 = db.execute(text(f"""
        WITH eligible AS (
            SELECT DISTINCT ON (aei.id)
                -- Use aei.id as the stable seed so supervisors upload can find
                -- and enrich the same row using md5(aei.id::text || '|se')::uuid
                md5(aei.id::text || '|se')::uuid AS se_id,
                a.id   AS resolved_app_id,
                aei.id AS resolved_aei_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a
                ON a.application_number = trim(s.application_number)
            JOIN public.application_electrical_installation aei
                ON aei.application_id = a.id
            WHERE UPPER(TRIM(s.experience_type)) = 'SELF_EMPLOYED'
              AND NULLIF(trim(s.application_number), '') IS NOT NULL
            ORDER BY aei.id, s.row_no
        ),
        ins AS (
            INSERT INTO public.self_employed (
                id,
                application_id,
                application_electrical_installation_id,
                from_date,
                to_date,
                project_performed,
                voltage_level,
                voltage,
                created_at,
                updated_at
            )
            SELECT
                e.se_id,
                e.resolved_app_id,
                e.resolved_aei_id,
                NULL,       -- from_date: populated by supervisors upload
                NULL,       -- to_date:   populated by supervisors upload
                NULL,       -- project_performed: populated by supervisors upload
                'NONE',     -- voltage_level: satisfies NOT NULL / CHECK if any
                NULL,       -- voltage: populated by supervisors upload
                now(),
                now()
            FROM eligible e
            ON CONFLICT (id) DO UPDATE SET
                {_build_conflict_set(_se_cols, 'self_employed')}
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_se = int(r6.get("cnt", 0) or 0)
    _progress(f"transform:self_employed inserted={ins_se}")
    db.commit()

    # ── STEP H: certificate_verifications — SKIPPED on this import ───
    # The table does not exist yet; it will be populated from a separate
    # certificates Excel file in a follow-up import.
    ins_cv = 0
    _progress("transform:certificate_verifications skipped (table not yet created; deferred to certificates import)")

    # ── STEP I: collect inserted rows for response ─────────────────────
    # Join only through the staging table's application_number to avoid
    # cartesian products. Use a temp CTE anchored on staging app_ids.
    # ── STEP I: collect inserted rows for response (optional) ─────────
    include_rows = bool(include_rows)
    try:
        limit_rows = int(limit_rows)
    except Exception:
        limit_rows = 50
    # Hard cap to keep responses small and Swagger stable
    limit_rows = max(0, min(limit_rows, 200))

    inserted_applications: list = []
    inserted_certificates: list = []
    inserted_aei: list = []
    inserted_personal_details: list = []
    inserted_contact_details: list = []
    inserted_attachments: list = []

    if include_rows and limit_rows > 0:
        _progress(f"summary:collecting inserted rows (limit={limit_rows})")

        def _rows(sql: str, *, params: Optional[dict] = None) -> list:
            result = db.execute(text(sql), params or {})
            return [dict(row) for row in result.mappings()]

        inserted_applications = _rows("""
            SELECT DISTINCT
                a.id,
                a.application_number,
                a.application_type,
                a.approval_no,
                a.license_type,
                a.category_license_type,
                a.status,
                a.effective_date::text,
                a.expire_date::text,
                a.created_at::text
            FROM public.applications a
            WHERE a.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})

        inserted_certificates = _rows("""
            SELECT DISTINCT
                c.id,
                c.application_id,
                c.application_number,
                c.approval_no,
                c.application_certificate_type,
                c.license_type,
                c.category_license_type,
                c.certificate_owner,
                c.effective_date::text,
                c.expire_date::text,
                c.created_at::text
            FROM public.certificates c
            WHERE c.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})

        inserted_aei = _rows("""
            SELECT DISTINCT
                aei.id,
                aei.application_id,
                aei.applicant_name,
                aei.experience_type,
                aei.approved_class_id,
                aei.is_from_lois,
                aei.created_at::text
            FROM public.application_electrical_installation aei
            JOIN public.applications a ON a.id = aei.application_id
            WHERE a.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})

        inserted_personal_details = _rows("""
            SELECT DISTINCT
                pd.id,
                pd.application_id,
                pd.application_electrical_installation_id,
                pd.date_of_birth::text,
                pd.nationality,
                pd.gender,
                pd.work_permit_no,
                pd.created_at::text
            FROM public.personal_details pd
            JOIN public.application_electrical_installation aei
                ON aei.id = pd.application_electrical_installation_id
            JOIN public.applications a ON a.id = aei.application_id
            WHERE a.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})

        inserted_contact_details = _rows("""
            SELECT DISTINCT
                cd.id,
                cd.application_id,
                cd.application_electrical_installation_id,
                cd.region,
                cd.district,
                cd.ward,
                cd.email,
                cd.mobile_no,
                cd.plot_no,
                cd.po_box,
                cd.created_at::text
            FROM public.contact_details cd
            JOIN public.application_electrical_installation aei
                ON aei.id = cd.application_electrical_installation_id
            JOIN public.applications a ON a.id = aei.application_id
            WHERE a.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})

        inserted_attachments = _rows("""
            SELECT DISTINCT
                att.id,
                att.application_id,
                att.application_electrical_installation_id,
                att.identification,
                att.identification_name,
                att.passport,
                att.passport_name,
                att.created_at::text
            FROM public.attachments att
            JOIN public.application_electrical_installation aei
                ON aei.id = att.application_electrical_installation_id
            JOIN public.applications a ON a.id = aei.application_id
            WHERE a.application_number IN (
                SELECT DISTINCT trim(s.application_number)
                FROM public.stage_elec_install_raw s
                WHERE NULLIF(trim(s.application_number), '') IS NOT NULL
            )
            LIMIT :limit
        """, params={"limit": limit_rows})
    else:
        _progress("summary:skipped row collection (include_rows=false)")

    _progress("done")

    # ...existing code...

    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
        "inserted_users": int(_inserted_users),
        "skipped_users": int(_skipped_users),
        "inserted_user_roles": int(_inserted_user_roles),
        "skipped_user_roles": int(_skipped_user_roles),
        "inserted": {
            "applications": {
                "count": ins_apps,
                "rows": inserted_applications,
            },
            "certificates": {
                "count": ins_certs,
                "rows": inserted_certificates,
            },
            "application_electrical_installation": {
                "count": ins_aei,
                "rows": inserted_aei,
            },
            "personal_details": {
                "count": ins_pd,
                "rows": inserted_personal_details,
            },
            "contact_details": {
                "count": ins_cd,
                "rows": inserted_contact_details,
            },
            "attachments": {
                "count": ins_att,
                "rows": inserted_attachments,
            },
            "work_experience": {
                "count": ins_we,
                "rows": [],
                "note": "skipped — voltage_level (NOT NULL) not available in this Excel file",
            },
            "self_employed": {
                "count": ins_se,
                "rows": [],
                "note": "inserted for all applications where employmentstatus = Self-Employed; work details enriched later by supervisors upload",
            },
            "certificate_verifications": {
                "count": ins_cv,
                "rows": [],
                "note": "skipped — table not yet created; deferred to certificates import",
            },
        },
    }


# Alias so the router can import either spelling
import_electrical_installations_via_staging_copy = import_electrical_installation_via_staging_copy
