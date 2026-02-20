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

logger = logging.getLogger(__name__)

_EMPTY = {"", "nan", "none", "null", "nat"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
                region, district, ward, email, mobile_no, plot_no, po_box,
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
    # Commit DDL immediately — ALTER TABLE must not share a transaction with DML
    db.commit()
    _progress("schema:guard:done")

    # ── 6½. Relax unique constraints on certificates to allow LOIS data model ──
    #
    # Business rule: the same approval_no can appear in certificates with different
    # application_certificate_type values (NEW vs RENEW vs UPGRADE share a licence number).
    # The same application_number can also have multiple certificates — one per type.
    #
    # For EVERY schema that actually exists on this DB ('public' and 'align_live' if present):
    #   REMOVE  single-column UNIQUE on applications.approval_no
    #   REMOVE  single-column UNIQUE on certificates.approval_no
    #   REMOVE  single-column UNIQUE on certificates.application_number
    #   ADD     composite UNIQUE (approval_no, application_certificate_type)
    #
    # All constraint names are Hibernate-generated hashes that differ per environment,
    # so they are looked up dynamically — nothing is hardcoded.
    _progress("schema:relax-certificates-constraints")
    db.execute(text("""
        DO $$
        DECLARE
            _rec        RECORD;
            _schema     text;
            _app_attnum smallint;
            _cert_attnum smallint;
            _cert_type_attnum smallint;
        BEGIN
            -- Iterate only over schemas that actually exist on this database
            FOR _schema IN
                SELECT nspname FROM pg_namespace
                WHERE  nspname IN ('public', 'align_live')
                ORDER BY nspname
            LOOP

                -- ── A. Drop single-column UNIQUE on applications.approval_no ──
                IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                           WHERE n.nspname=_schema AND c.relname='applications') THEN
                    SELECT attnum INTO _app_attnum
                    FROM pg_attribute
                    WHERE attrelid = (SELECT c.oid FROM pg_class c
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
                              AND  con.conkey = ARRAY[_app_attnum]
                        LOOP
                            EXECUTE format('ALTER TABLE %I.applications DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped %.applications.%', _schema, _rec.conname;
                        END LOOP;
                    END IF;
                END IF;

                -- ── B & C. Drop single-column UNIQUEs on certificates ─────────
                IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                           WHERE n.nspname=_schema AND c.relname='certificates') THEN

                    -- Get column numbers for approval_no and application_number
                    SELECT attnum INTO _cert_attnum
                    FROM pg_attribute
                    WHERE attrelid = (SELECT c.oid FROM pg_class c
                                      JOIN pg_namespace n ON n.oid=c.relnamespace
                                      WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'approval_no';

                    SELECT attnum INTO _cert_type_attnum
                    FROM pg_attribute
                    WHERE attrelid = (SELECT c.oid FROM pg_class c
                                      JOIN pg_namespace n ON n.oid=c.relnamespace
                                      WHERE n.nspname=_schema AND c.relname='certificates')
                      AND attname = 'application_number';

                    -- Drop UNIQUE(approval_no)
                    IF _cert_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname
                            FROM   pg_constraint con
                            JOIN   pg_class cls ON cls.oid = con.conrelid
                            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE  con.contype = 'u'
                              AND  nsp.nspname = _schema
                              AND  cls.relname = 'certificates'
                              AND  con.conkey = ARRAY[_cert_attnum]
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped %.certificates.% (approval_no)', _schema, _rec.conname;
                        END LOOP;
                    END IF;

                    -- Drop UNIQUE(application_number)  — same app can have NEW + RENEW + UPGRADE
                    IF _cert_type_attnum IS NOT NULL THEN
                        FOR _rec IN
                            SELECT con.conname
                            FROM   pg_constraint con
                            JOIN   pg_class cls ON cls.oid = con.conrelid
                            JOIN   pg_namespace nsp ON nsp.oid = cls.relnamespace
                            WHERE  con.contype = 'u'
                              AND  nsp.nspname = _schema
                              AND  cls.relname = 'certificates'
                              AND  con.conkey = ARRAY[_cert_type_attnum]
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                            RAISE NOTICE 'Dropped %.certificates.% (application_number)', _schema, _rec.conname;
                        END LOOP;
                    END IF;

                    -- ── D. Add composite UNIQUE (approval_no, application_certificate_type) ──
                    --    Real dedup key: same approval_no is fine across different cert types.
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint con
                        JOIN pg_class     cls ON cls.oid = con.conrelid
                        JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                        WHERE  con.contype = 'u'
                          AND  nsp.nspname = _schema
                          AND  cls.relname = 'certificates'
                          AND  con.conname = 'certificates_approval_no_cert_type_uq'
                    ) THEN
                        EXECUTE format(
                            'ALTER TABLE %I.certificates
                             ADD CONSTRAINT certificates_approval_no_cert_type_uq
                             UNIQUE (approval_no, application_certificate_type)',
                            _schema
                        );
                        RAISE NOTICE 'Added certificates_approval_no_cert_type_uq on %.certificates', _schema;
                    END IF;

                END IF; -- certificates table exists

            END LOOP; -- schemas
        END;
        $$;
    """))
    _progress("schema:relax-certificates-constraints:done")
    # Commit constraint changes before data inserts
    db.commit()

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
                -- license_category_id: try as UUID directly, else NULL
                CASE WHEN NULLIF(trim(ea.license_category_id),'') IS NOT NULL
                     THEN (CASE WHEN trim(ea.license_category_id) ~ '^[0-9a-fA-F-]{36}$'
                                THEN trim(ea.license_category_id)::uuid
                                ELSE NULL END)
                END,
                CASE WHEN NULLIF(trim(ea.effective_date),'') IS NOT NULL
                     THEN trim(ea.effective_date)::date END,
                CASE WHEN NULLIF(trim(ea.expire_date),'') IS NOT NULL
                     THEN trim(ea.expire_date)::date END,
                NULLIF(trim(ea.parent_application_id), ''),
                NULLIF(trim(ea.userid), ''),
                NULLIF(trim(ea.created_by), ''),
                'APPROVED',
                true,
                COALESCE(
                    CASE WHEN NULLIF(trim(ea.created_at_raw),'') IS NOT NULL
                         THEN trim(ea.created_at_raw)::timestamp END,
                    now()
                ),
                now()
            FROM deduped ea
            ON CONFLICT (id) DO NOTHING
            RETURNING id
        )
        SELECT COUNT(*) AS inserted_apps FROM ins_apps;
    """)
    r = db.execute(transform_sql).mappings().first() or {}
    ins_apps = int(r.get("inserted_apps", 0) or 0)
    _progress(f"transform:applications inserted={ins_apps}")

    # Ensure all matching rows (including pre-existing) have status=APPROVED and is_from_lois=true
    db.execute(text("""
        UPDATE public.applications a
        SET    status       = 'APPROVED',
               is_from_lois = true,
               updated_at   = now()
        FROM   public.stage_elec_install_raw s
        WHERE  a.application_number = trim(s.application_number)
          AND  (a.status != 'APPROVED' OR a.is_from_lois IS DISTINCT FROM true);
    """))
    db.commit()
    _progress("transform:applications status+is_from_lois enforced")

    # ── STEP A½: certificates ──────────────────────────────────────────
    # Business rule: one certificate row per (application_number, application_certificate_type).
    # Same approval_no can appear with different types (NEW, RENEW, UPGRADE etc.)
    # — the composite unique (approval_no, application_certificate_type) guards real duplicates.
    #
    # license_type and category_license_type are copied directly from the already-inserted
    # applications row (joined via application_id) so both tables always share the same values.
    r1b = db.execute(text("""
        WITH eligible AS (
            -- One row per (application_number, application_certificate_type) pair.
            -- This allows multiple certificate types for the same application_number
            -- (e.g. a NEW and a subsequent RENEW), while still deduplicating within
            -- the same type.
            SELECT DISTINCT ON (trim(s.application_number), COALESCE(NULLIF(trim(s.application_type),''), 'NEW'))
                   s.*,
                   a.id                    AS resolved_app_id,
                   a.license_type          AS resolved_license_type,
                   a.category_license_type AS resolved_category_license_type
            FROM public.stage_elec_install_raw s
            JOIN public.applications a ON a.application_number = trim(s.application_number)
            WHERE NOT EXISTS (
                SELECT 1 FROM public.certificates c
                WHERE c.application_id = a.id
                  AND c.application_certificate_type = COALESCE(NULLIF(trim(s.application_type),''), 'NEW')
            )
            ORDER BY trim(s.application_number),
                     COALESCE(NULLIF(trim(s.application_type),''), 'NEW'),
                     s.row_no
        ),
        -- Second dedup layer: within the INSERT batch, two different application_numbers
        -- may share the same approval_no. ON CONFLICT DO UPDATE cannot hit the same
        -- (approval_no, application_certificate_type) target twice in one command,
        -- so keep only the first row per conflict key.
        deduped AS (
            SELECT DISTINCT ON (
                NULLIF(trim(e.approval_no), ''),
                COALESCE(NULLIF(trim(e.application_type), ''), 'NEW')
            )
                e.*
            FROM eligible e
            ORDER BY
                NULLIF(trim(e.approval_no), ''),
                COALESCE(NULLIF(trim(e.application_type), ''), 'NEW'),
                e.row_no
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
                created_at, updated_at
            )
            SELECT
                e.cert_id,
                e.resolved_app_id,
                trim(e.application_number),
                NULLIF(trim(e.certificate_owner), ''),
                NULLIF(trim(e.approval_no), ''),
                -- completed_at → approval_date
                CASE WHEN NULLIF(trim(e.completed_at),'') IS NOT NULL
                     THEN trim(e.completed_at)::date END,
                -- mirror the application's own values — single source of truth
                e.resolved_category_license_type,
                e.resolved_license_type,
                CASE WHEN NULLIF(trim(e.effective_date),'') IS NOT NULL
                     THEN trim(e.effective_date)::date END,
                CASE WHEN NULLIF(trim(e.expire_date),'') IS NOT NULL
                     THEN trim(e.expire_date)::date END,
                -- application_certificate_type: use the already-normalised application_type
                -- (NEW/RENEW/UPGRADE/APPEAL) — same allowed values, defaulting to 'NEW'
                COALESCE(NULLIF(trim(e.application_type), ''), 'NEW'),
                now(), now()
            FROM deduped e
            ON CONFLICT (approval_no, application_certificate_type) DO UPDATE
            SET
                license_type          = EXCLUDED.license_type,
                category_license_type = EXCLUDED.category_license_type,
                application_id        = EXCLUDED.application_id,
                effective_date        = EXCLUDED.effective_date,
                expire_date           = EXCLUDED.expire_date,
                approval_date         = EXCLUDED.approval_date,
                certificate_owner     = COALESCE(EXCLUDED.certificate_owner, public.certificates.certificate_owner),
                updated_at            = now()
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
    # approved_class_id is resolved via licence_categories.name lookup using
    # the Excel column `approvedclass`.
    r2 = db.execute(text("""
        WITH eligible AS (
            SELECT s.*,
                   a.id AS resolved_app_id,
                   (
                       SELECT lc.id
                       FROM public.categories lc
                       WHERE lc.deleted_at IS NULL
                         AND LOWER(TRIM(lc.name)) = LOWER(TRIM(s.approvedclass))
                       LIMIT 1
                   ) AS resolved_approved_class_id
            FROM public.stage_elec_install_raw s
            JOIN public.applications a ON a.application_number = trim(s.application_number)
            WHERE NOT EXISTS (
                SELECT 1 FROM public.application_electrical_installation x
                WHERE x.application_id = a.id
            )
        ),
        ins AS (
            INSERT INTO public.application_electrical_installation (
                id, application_id,
                applicant_name, experience_type,
                approved_class_id,
                is_from_lois, created_at, updated_at
            )
            SELECT
                e.aei_id,
                e.resolved_app_id,
                -- applicant_name from company_name per mapping spec
                NULLIF(trim(e.company_name), ''),
                COALESCE(NULLIF(trim(e.experience_type), ''), 'EMPLOYED'),
                e.resolved_approved_class_id,
                true, now(), now()
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_aei = int(r2.get("cnt", 0) or 0)
    _progress(f"transform:application_electrical_installation inserted={ins_aei}")
    db.commit()

    # ── STEP C: contact_details ────────────────────────────────────────
    r3 = db.execute(text("""
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
                e.cd_id,
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
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_cd = int(r3.get("cnt", 0) or 0)
    _progress(f"transform:contact_details inserted={ins_cd}")
    db.commit()

    # ── STEP D: personal_details ───────────────────────────────────────
    r4 = db.execute(text("""
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
                e.pd_id,
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
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_pd = int(r4.get("cnt", 0) or 0)
    _progress(f"transform:personal_details inserted={ins_pd}")
    db.commit()

    # ── STEP E: attachments ────────────────────────────────────────────
    r5 = db.execute(text("""
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
                e.att_id,
                e.resolved_app_id,
                e.resolved_aei_id,
                NULLIF(trim(e.identification), ''),
                NULLIF(trim(e.identification_name), ''),
                NULLIF(trim(e.passport), ''),
                NULLIF(trim(e.passport_name), ''),
                now(), now()
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM ins;
    """)).mappings().first() or {}
    ins_att = int(r5.get("cnt", 0) or 0)
    _progress(f"transform:attachments inserted={ins_att}")
    db.commit()

    # ── STEP E½: back-fill FK columns on application_electrical_installation ─
    # Wire contact_details_id, personal_details_id, attachments_id now that
    # all three child rows exist.
    db.execute(text("""
        UPDATE public.application_electrical_installation aei
        SET
            contact_details_id  = cd.id,
            personal_details_id = pd.id,
            attachments_id      = att.id,
            updated_at          = now()
        FROM public.application_electrical_installation aei2
        JOIN public.applications a
            ON a.id = aei2.application_id
        JOIN public.stage_elec_install_raw s
            ON trim(s.application_number) = trim(a.application_number)
        LEFT JOIN public.contact_details cd
            ON cd.application_electrical_installation_id = aei2.id
        LEFT JOIN public.personal_details pd
            ON pd.application_electrical_installation_id = aei2.id
        LEFT JOIN public.attachments att
            ON att.application_electrical_installation_id = aei2.id
        WHERE aei.id = aei2.id
          AND (
              aei.contact_details_id  IS DISTINCT FROM cd.id  OR
              aei.personal_details_id IS DISTINCT FROM pd.id  OR
              aei.attachments_id      IS DISTINCT FROM att.id
          );
    """))
    _progress("transform:application_electrical_installation FKs back-filled")
    db.commit()

    # ── STEP F: work_experience — SKIPPED on this import ──────────────
    # work_experience.voltage_level is NOT NULL and is not available in this
    # Excel file. A separate import (with from_date / to_date / voltage_level
    # / work_description etc.) will populate this table.
    ins_we = 0
    _progress("transform:work_experience skipped (voltage_level data not in this file)")

    # ── STEP G: self_employed — SKIPPED on this import ────────────────
    # self_employed is only created for SELF_EMPLOYED experience rows, which
    # require work_experience to exist first. Deferred to the follow-up import.
    ins_se = 0
    _progress("transform:self_employed skipped (depends on work_experience)")

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

    return {
        "total_rows_in_file": total_rows,
        "staged_total": staged,
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
                "note": "skipped — depends on work_experience",
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
