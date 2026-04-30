from __future__ import annotations

"""Water Supply application import service.

Source Excel columns (WATER sector)
------------------------------------
address_code, address_no, application_number, application_type,
block_no, brela_number, gngzon, govtnoteno, district, email,
facility_name, latitude, mobile_no, plot_no, po_box, region, road,
street, tin, ward, license_category_id, brela_registration_type,
company_name, license_type, longitude, website,
certificate_of_incorporation_no, completed_at, effective_date,
expire_date, approval_no, parent_application_id, created_at,
created_by, userid, tinc, tincfilename, vrndoc, vrndocfilename,
abftcoy, abftcoyfilename, ccsc, ccscfilename, sesr, sesrfilename,
eiarcfpui, eiarcfpuifilename, cafsftpy, cafsftpyfilename,
moudqaw, moudqawfilename, moumaw, moumawfilename, cbp, cbpfilename,
caat, caatfilename, rvalue, nofcustomer, dloc, dlocfilename,
fs, amount, currency, ateleno, aposition, aconpername,
blbs, blbsfilename, bposition, bconpername, bname,
todis, todisfilename, tdfwsaaoi, tdfwsaaoifilename,
wap, wapfilename, llc, llcfilename, bp, bpfilename,
ppitsotpdiq, ppitsotpdiqfilename, pfd, pfdfilename,
miaoapstapa, miaoapstapafilename, daabd, daabdfilename,
lup, lupfilename, sm, smfilename, pd, pdfilename,
ar, arfilename, pmp, pmpfilename, qmp, qmpfilename,
vrnno, cmobile_no, title, contact_name

Target tables
-------------
1. public.applications              — core application row
2. public.application_sector_details — company/location/vrn/gov notice details
3. public.referees                  — contact details (cmobile_no→telephone_no, title, contact_name→contact_person_name)
4. public.bank_details_tanzania     — bank info (bname, bposition, bconpername, ateleno)
5. public.financial_information     — application_sector_detail_id + application_id + fs + amount + currency
6. public.documents                 — one row per (id_col, filename_col) attachment pair

Column mapping highlights
--------------------------
  gngzon       → application_sector_details.gn_gazette_on
  govtnoteno   → application_sector_details.government_notice_no
  vrnno        → application_sector_details.vrn
  tinc         → tin (stored as TIN document)
  cmobile_no   → referees.telephone_no
  title        → referees.title
  contact_name → referees.contact_person_name
  bname        → bank_details_tanzania.name_of_banker
  bposition    → bank_details_tanzania.position
  bconpername  → bank_details_tanzania.contact_person_name
  ateleno      → bank_details_tanzania.telephone_no
  fs           → financial_information.fs
  amount       → financial_information.amount
  currency     → financial_information.currency

Notes
-----
- Uses staging table + COPY for performance (same pattern as other importers).
- financial_information drops all FK back-references; only stores
  application_sector_detail_id + application_id.
- All *filename columns create document rows in public.documents.
- vrn (VRN document) also stored in application_sector_details.vrn.
- Idempotent: ON CONFLICT (application_number) DO UPDATE on applications;
  md5-based stable UUIDs on asd / financial_information / documents.
"""

from typing import Callable, Optional
import io
import logging
from decimal import Decimal, InvalidOperation

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.utils.lookup_cache import load_applicant_role_id, load_default_role_id
from app.services.application_migrations_service import (
    region_map_csv,
    district_map_csv,
    ward_map_csv,
    _normalize_numeric_string,
)

logger = logging.getLogger(__name__)

# ── Column alias map: Excel header → staging column name ─────────────────────
# All keys are lowercase (the reader normalises headers).
_EXCEL_TO_STAGE: dict[str, str] = {
    # Application meta
    "application_number":               "application_number",
    "apprefno":                         "application_number",
    "application_type":                 "application_type",
    "approval_no":                      "approval_no",
    "approvalno":                       "approval_no",
    "licenceno":                        "approval_no",
    "effective_date":                   "effective_date",
    "expire_date":                      "expire_date",
    "expiry_date":                      "expire_date",
    "completed_at":                     "completed_at",
    "approval_date":                    "completed_at",
    "license_type":                     "license_type",
    "license_category_id":              "license_category_raw",
    "parent_application_id":            "old_parent_application_id",
    "created_by":                       "old_created_by",
    "userid":                           "username",
    # Address / location
    "address_code":                     "address_code",
    "address_no":                       "address_no",
    "block_no":                         "block_no",
    "plot_no":                          "plot_no",
    "road":                             "road",
    "street":                           "street",
    "region":                           "region",
    "district":                         "district",
    "ward":                             "ward",
    "latitude":                         "latitude",
    "longitude":                        "longitude",
    "po_box":                           "po_box",
    "facility_name":                    "facility_name",
    "company_name":                     "company_name",
    "email":                            "email",
    "mobile_no":                        "mobile_no",
    "cmobile_no":                       "cmobile_no",
    "website":                          "website",
    # TIN / BRELA / legal
    "tin":                              "tin",
    "tin_name":                         "tin_name",
    "brela_number":                     "brela_number",
    "brela_registration_type":          "brela_registration_type",
    "certificate_of_incorporation_no":  "certificate_of_incorporation_no",
    # Water-specific fields
    "vrnno":                            "vrnno",          # → asd.vrn
    "gngzon":                           "gngzon",         # → asd.gn_gazette_on
    "govtnoteco":                       "govtnoteno",     # → asd.government_notice_no
    "govtnoteno":                       "govtnoteno",     # → asd.government_notice_no
    "rvalue":                           "rvalue",
    "nofcustomer":                      "no_of_customer", # → project_description.number_of_customer
    "no_of_customer":                   "no_of_customer",
    # Financial / bank fields
    "amount":                           "amount",
    "currency":                         "currency",
    "fs":                               "fs",             # financial statement reference
    "ateleno":                          "ateleno",        # bank telephone
    "aposition":                        "aposition",      # applicant position
    "aconpername":                      "aconpername",    # applicant contact person
    "bposition":                        "bposition",      # banker position
    "bconpername":                      "bconpername",    # bank contact person name
    "bname":                            "bname",          # name of banker
    "title":                            "title",
    "contact_name":                     "contact_name",
    # Attachments (id + filename pairs)
    "tinc":                             "tinc",
    "tincfilename":                     "tincfilename",
    "vrndoc":                           "vrndoc",
    "vrndocfilename":                   "vrndocfilename",
    "abftcoy":                          "abftcoy",
    "abftcoyfilename":                  "abftcoyfilename",
    "ccsc":                             "ccsc",
    "ccscfilename":                     "ccscfilename",
    "sesr":                             "sesr",
    "sesrfilename":                     "sesrfilename",
    "eiarcfpui":                        "eiarcfpui",
    "eiarcfpuifilename":                "eiarcfpuifilename",
    "cafsftpy":                         "cafsftpy",
    "cafsftpyfilename":                 "cafsftpyfilename",
    "moudqaw":                          "moudqaw",
    "moudqawfilename":                  "moudqawfilename",
    "moumaw":                           "moumaw",
    "moumawfilename":                   "moumawfilename",
    "cbp":                              "cbp",
    "cbpfilename":                      "cbpfilename",
    "caat":                             "caat",
    "caatfilename":                     "caatfilename",
    "dloc":                             "dloc",
    "dlocfilename":                     "dlocfilename",
    "blbs":                             "blbs",
    "blbsfilename":                     "blbsfilename",
    "todis":                            "todis",
    "todisfilename":                    "todisfilename",
    "tdfwsaaoi":                        "tdfwsaaoi",
    "tdfwsaaoifilename":                "tdfwsaaoifilename",
    "wap":                              "wap",
    "wapfilename":                      "wapfilename",
    "llc":                              "llc",
    "llcfilename":                      "llcfilename",
    "bp":                               "bp",
    "bpfilename":                       "bpfilename",
    "ppitsotpdiq":                      "ppitsotpdiq",
    "ppitsotpdiqfilename":              "ppitsotpdiqfilename",
    "pfd":                              "pfd",
    "pfdfilename":                      "pfdfilename",
    "miaoapstapa":                      "miaoapstapa",
    "miaoapstapafilename":              "miaoapstapafilename",
    "daabd":                            "daabd",
    "daabdfilename":                    "daabdfilename",
    "lup":                              "lup",
    "lupfilename":                      "lupfilename",
    "sm":                               "sm",
    "smfilename":                       "smfilename",
    "pd":                               "pd",
    "pdfilename":                       "pdfilename",
    "ar":                               "ar",
    "arfilename":                       "arfilename",
    "pmp":                              "pmp",
    "pmpfilename":                      "pmpfilename",
    "qmp":                              "qmp",
    "qmpfilename":                      "qmpfilename",
}

# Attachment pairs: (id_col, filename_col, document_label)
_ATTACHMENT_PAIRS: list[tuple[str, str, str]] = [
    ("tinc",          "tincfilename",          "TIN_CERTIFICATE"),
    ("vrndoc",        "vrndocfilename",         "VRN_DOCUMENT"),
    ("abftcoy",       "abftcoyfilename",        "ABFTCOY"),
    ("ccsc",          "ccscfilename",           "CCSC"),
    ("sesr",          "sesrfilename",           "SESR"),
    ("eiarcfpui",     "eiarcfpuifilename",      "EIARCFPUI"),
    ("cafsftpy",      "cafsftpyfilename",       "CAFSFTPY"),
    ("moudqaw",       "moudqawfilename",        "MOUDQAW"),
    ("moumaw",        "moumawfilename",         "MOUMAW"),
    ("cbp",           "cbpfilename",            "CBP"),
    ("caat",          "caatfilename",           "CAAT"),
    ("dloc",          "dlocfilename",           "DLOC"),
    ("blbs",          "blbsfilename",           "BLBS"),
    ("todis",         "todisfilename",          "TODIS"),
    ("tdfwsaaoi",     "tdfwsaaoifilename",      "TDFWSAAOI"),
    ("wap",           "wapfilename",            "WAP"),
    ("llc",           "llcfilename",            "LLC"),
    ("bp",            "bpfilename",             "BP"),
    ("ppitsotpdiq",   "ppitsotpdiqfilename",    "PPITSOTPDIQ"),
    ("pfd",           "pfdfilename",            "PFD"),
    ("miaoapstapa",   "miaoapstapafilename",    "MIAOAPSTAPA"),
    ("daabd",         "daabdfilename",          "DAABD"),
    ("lup",           "lupfilename",            "LUP"),
    ("sm",            "smfilename",             "SM"),
    ("pd",            "pdfilename",             "PD"),
    ("ar",            "arfilename",             "AR"),
    ("pmp",           "pmpfilename",            "PMP"),
    ("qmp",           "qmpfilename",            "QMP"),
]

# ── Staging table DDL ─────────────────────────────────────────────────────────
_STAGE = "public.stage_water_supply_raw"

_STAGE_DDL = f"""
DROP TABLE IF EXISTS {_STAGE};
CREATE TABLE {_STAGE} (
    application_number              text,
    application_type                text,
    approval_no                     text,
    effective_date                  text,
    expire_date                     text,
    completed_at                    text,
    license_type                    text,
    license_category_raw            text,
    old_parent_application_id       text,
    old_created_by                  text,
    username                        text,
    -- address / location
    address_code                    text,
    address_no                      text,
    block_no                        text,
    plot_no                         text,
    road                            text,
    street                          text,
    region                          text,
    district                        text,
    ward                            text,
    latitude                        text,
    longitude                       text,
    po_box                          text,
    facility_name                   text,
    company_name                    text,
    email                           text,
    mobile_no                       text,
    cmobile_no                      text,
    website                         text,
    -- legal / identifiers
    tin                             text,
    tin_name                        text,
    brela_number                    text,
    brela_registration_type         text,
    certificate_of_incorporation_no text,
    -- water-specific
    vrnno                           text,
    gngzon                          text,
    govtnoteno                      text,
    rvalue                          text,
    no_of_customer                  text,
    -- financial / bank
    amount                          text,
    currency                        text,
    fs                              text,
    ateleno                         text,
    aposition                       text,
    aconpername                     text,
    bposition                       text,
    bconpername                     text,
    bname                           text,
    title                           text,
    contact_name                    text,
    -- attachment id + filename columns
    tinc                            text,
    tincfilename                    text,
    vrndoc                          text,
    vrndocfilename                  text,
    abftcoy                         text,
    abftcoyfilename                 text,
    ccsc                            text,
    ccscfilename                    text,
    sesr                            text,
    sesrfilename                    text,
    eiarcfpui                       text,
    eiarcfpuifilename               text,
    cafsftpy                        text,
    cafsftpyfilename                text,
    moudqaw                         text,
    moudqawfilename                 text,
    moumaw                          text,
    moumawfilename                  text,
    cbp                             text,
    cbpfilename                     text,
    caat                            text,
    caatfilename                    text,
    dloc                            text,
    dlocfilename                    text,
    blbs                            text,
    blbsfilename                    text,
    todis                           text,
    todisfilename                   text,
    tdfwsaaoi                       text,
    tdfwsaaoifilename               text,
    wap                             text,
    wapfilename                     text,
    llc                             text,
    llcfilename                     text,
    bp                              text,
    bpfilename                      text,
    ppitsotpdiq                     text,
    ppitsotpdiqfilename             text,
    pfd                             text,
    pfdfilename                     text,
    miaoapstapa                     text,
    miaoapstapafilename             text,
    daabd                           text,
    daabdfilename                   text,
    lup                             text,
    lupfilename                     text,
    sm                              text,
    smfilename                      text,
    pd                              text,
    pdfilename                      text,
    ar                              text,
    arfilename                      text,
    pmp                             text,
    pmpfilename                     text,
    qmp                             text,
    qmpfilename                     text
);
"""

_EMPTY = {"", "nan", "none", "null", "nat"}


def _c(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in _EMPTY else s


# ── Location map helpers (region / district / ward) ───────────────────────────
# Excel may supply numeric IDs (e.g. 1554087241462) or name strings.
# We resolve both to names using the same CSV maps as application_migrations.

def _build_norm_map(source: dict[str, str]) -> dict[str, str]:
    """Build a normalised lookup: both raw key and numeric-normalised key → name."""
    nm: dict[str, str] = {}
    for k, v in source.items():
        ks = str(k).strip()
        nm[ks] = v
        try:
            nk = _normalize_numeric_string(ks)
            if nk and nk != ks:
                nm[nk] = v
        except Exception:
            pass
    return nm


_NORM_REGION_MAP   = _build_norm_map(region_map_csv)
_NORM_DISTRICT_MAP = _build_norm_map(district_map_csv)
_NORM_WARD_MAP     = _build_norm_map(ward_map_csv)


def _resolve_location(raw: str, norm_map: dict[str, str]) -> str:
    """Return the name for *raw* (id or name) using *norm_map*, or *raw* itself."""
    if not raw:
        return raw
    sval = raw.strip()
    if sval in norm_map:
        return norm_map[sval]
    try:
        nk = _normalize_numeric_string(sval)
        if nk and nk in norm_map:
            return norm_map[nk]
    except Exception:
        pass
    return sval  # already a name — keep as-is


# ── license_type normalisation ────────────────────────────────────────────────
# Maps free-text Excel values (and known aliases) to the DB CHECK enum values.
# Matching is case-insensitive; unknown values are coerced to NULL so the row
# is accepted without violating applications_license_type_check.
_LICENSE_TYPE_MAP: dict[str, str] = {
    # Water sector aliases
    "water & sanitation licenses":     "LICENSE_WATER",
    "water and sanitation licenses":   "LICENSE_WATER",
    "water & sanitation licence":      "LICENSE_WATER",
    "water and sanitation licence":    "LICENSE_WATER",
    "water supply license":            "LICENSE_WATER",
    "water supply licence":            "LICENSE_WATER",
    "water license":                   "LICENSE_WATER",
    "license_water":                   "LICENSE_WATER",
    # Electricity
    "electricity supply license":      "LICENSE_ELECTRICITY_SUPPLY",
    "electricity supply licence":      "LICENSE_ELECTRICITY_SUPPLY",
    "license_electricity_supply":      "LICENSE_ELECTRICITY_SUPPLY",
    "electricity installation":        "LICENSE_ELECTRICITY_INSTALLATION",
    "license_electricity_installation":"LICENSE_ELECTRICITY_INSTALLATION",
    # Natural Gas
    "natural gas license":             "LICENSE_NATURAL_GAS",
    "license_natural_gas":             "LICENSE_NATURAL_GAS",
    "natural gas construction":        "CONSTRUCTION_NATURAL_GAS",
    "construction_natural_gas":        "CONSTRUCTION_NATURAL_GAS",
    # Petroleum
    "petroleum license":               "LICENSE_PETROLEUM",
    "license_petroleum":               "LICENSE_PETROLEUM",
    "petroleum construction":          "CONSTRUCTION_PETROLEUM",
    "construction_petroleum":          "CONSTRUCTION_PETROLEUM",
}

# Full set of valid DB CHECK values — these pass through unchanged.
_LICENSE_TYPE_VALID = {
    "CONSTRUCTION_NATURAL_GAS",
    "CONSTRUCTION_PETROLEUM",
    "LICENSE_NATURAL_GAS",
    "LICENSE_PETROLEUM",
    "LICENSE_WATER",
    "LICENSE_ELECTRICITY_SUPPLY",
    "LICENSE_ELECTRICITY_INSTALLATION",
}


def _license_type_sql(col: str) -> str:
    """Return a SQL CASE expression that normalises *col* to a valid enum value.

    - If the raw value already matches a valid CHECK enum  → pass through.
    - If it matches a known alias (case-insensitive)       → map to enum.
    - Otherwise                                            → NULL (safe).

    The expression expects *col* to be an unquoted staging column reference
    such as ``s.license_type``.
    """
    # Build the WHEN clauses for known aliases
    when_clauses = "\n".join(
        f"        WHEN lower(btrim({col})) = '{alias}' THEN '{enum}'"
        for alias, enum in _LICENSE_TYPE_MAP.items()
    )
    # Build the WHEN clauses for valid pass-through values (upper-cased already)
    passthrough = "\n".join(
        f"        WHEN upper(btrim({col})) = '{v}' THEN '{v}'"
        for v in sorted(_LICENSE_TYPE_VALID)
    )
    return f"""CASE
{passthrough}
{when_clauses}
        ELSE NULL
    END"""


# Valid application_type CHECK values
_APP_TYPE_VALID = {
    "NEW", "RENEW", "EXTEND", "CHANGE_OF_NAME", "RELOCATION",
    "ALTERATION", "COMPLIANCE", "APPEAL", "UPGRADE", "TRANSFER",
    "MODIFICATION",
}
_APP_TYPE_ALIASES: dict[str, str] = {
    "renewal":      "RENEW",
    "new license":  "NEW",
    "new licence":  "NEW",
    "extension":    "EXTEND",
    "transfer":     "TRANSFER",
    "modification": "MODIFICATION",
    "upgrade":      "UPGRADE",
    "appeal":       "APPEAL",
    "compliance":   "COMPLIANCE",
    "relocation":   "RELOCATION",
    "alteration":   "ALTERATION",
    "change of name": "CHANGE_OF_NAME",
}


def _app_type_sql(col: str) -> str:
    """Normalise application_type to valid CHECK enum, or NULL if unrecognised."""
    when_clauses = "\n".join(
        f"        WHEN lower(btrim({col})) = '{alias}' THEN '{enum}'"
        for alias, enum in _APP_TYPE_ALIASES.items()
    )
    passthrough = "\n".join(
        f"        WHEN upper(btrim({col})) = '{v}' THEN '{v}'"
        for v in sorted(_APP_TYPE_VALID)
    )
    return f"""CASE
{passthrough}
{when_clauses}
        ELSE NULL
    END"""


def import_water_supply_via_staging(
    db: Session,
    df,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """Import Water Supply applications via staging table + COPY.

    Steps
    -----
    1. Normalise DataFrame headers.
    2. Drop + recreate staging table.
    3. COPY rows into staging via psycopg2 cursor.
    4. Schema-guard: ensure all target columns exist.
    5. Transform CTE:
       a. Upsert public.applications
       b. Upsert public.application_sector_details
       c. Insert public.referees          (cmobile_no→telephone_no, title, contact_name→contact_person_name)
       d. Insert public.bank_details_tanzania  (bname, bposition, bconpername, ateleno)
       e. Upsert public.financial_information  (application_sector_detail_id + application_id + fs + amount + currency)
       f. Insert public.documents              (one per attachment pair that has a filename)
    """

    def _progress(msg: str):
        logger.info("[water-supply-import] %s", msg)
        if progress_cb:
            progress_cb(msg)

    import pandas as pd

    _progress("Normalising column headers …")
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Build the list of stage columns that are actually present in the DataFrame.
    stage_cols = list(_EXCEL_TO_STAGE.values())
    # Deduplicate while preserving order
    seen: set[str] = set()
    stage_cols_ordered: list[str] = []
    for v in _EXCEL_TO_STAGE.values():
        if v not in seen:
            seen.add(v)
            stage_cols_ordered.append(v)

    _progress("Creating staging table …")
    db.execute(text(_STAGE_DDL))
    db.commit()

    # Build rows for staging
    _progress("Preparing rows for COPY …")
    rows: list[list[str]] = []
    for _, row in df.iterrows():
        stage_row: dict[str, str] = {col: "" for col in stage_cols_ordered}
        for excel_col, stage_col in _EXCEL_TO_STAGE.items():
            if excel_col in df.columns:
                stage_row[stage_col] = _c(row.get(excel_col, ""))

        # Resolve region / district / ward IDs → names using CSV maps
        stage_row["region"]   = _resolve_location(stage_row.get("region",   ""), _NORM_REGION_MAP)
        stage_row["district"] = _resolve_location(stage_row.get("district", ""), _NORM_DISTRICT_MAP)
        stage_row["ward"]     = _resolve_location(stage_row.get("ward",     ""), _NORM_WARD_MAP)

        rows.append([stage_row[c] for c in stage_cols_ordered])

    staged_rows = len(rows)
    _progress(f"COPYing {staged_rows} rows into staging …")

    # COPY via psycopg2 cursor
    buf = io.StringIO()
    for r in rows:
        buf.write("\t".join(
            v.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")
            for v in r
        ) + "\n")
    buf.seek(0)

    raw_conn = db.connection().connection
    cur = raw_conn.cursor()
    cols_sql = ", ".join(stage_cols_ordered)
    try:
        cur.copy_expert(
            f"COPY {_STAGE} ({cols_sql}) FROM STDIN WITH (FORMAT TEXT, NULL '')",
            buf,
        )
    finally:
        cur.close()
    raw_conn.commit()

    # ── Schema guard ─────────────────────────────────────────────────────────
    # Each ALTER TABLE runs in its own try/commit so that one pre-existing
    # column (or missing table) never blocks the remaining guards.
    _progress("Running schema guard …")

    _add_column_guards: list[str] = [
        # application_sector_details: water-specific regulatory fields
        "ALTER TABLE public.application_sector_details ADD COLUMN IF NOT EXISTS vrn text NULL",
        "ALTER TABLE public.application_sector_details ADD COLUMN IF NOT EXISTS gn_gazette_on text NULL",
        "ALTER TABLE public.application_sector_details ADD COLUMN IF NOT EXISTS government_notice_no text NULL",
        # financial_information: direct application link + financial fields
        "ALTER TABLE public.financial_information ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        "ALTER TABLE public.financial_information ADD COLUMN IF NOT EXISTS fs text NULL",
        "ALTER TABLE public.financial_information ADD COLUMN IF NOT EXISTS amount text NULL",
        "ALTER TABLE public.financial_information ADD COLUMN IF NOT EXISTS currency text NULL",
        # documents: migration extra columns
        "ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        "ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL",
        "ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS logic_doc_id bigint NULL",
        "ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS documents_order integer NULL",
        # applications: created_by FK
        "ALTER TABLE public.applications ADD COLUMN IF NOT EXISTS created_by uuid NULL",
        # applicant_proposed_investment: migration link columns
        "ALTER TABLE public.applicant_proposed_investment ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL",
        "ALTER TABLE public.applicant_proposed_investment ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        # project_description: migration link columns
        "ALTER TABLE public.project_description ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        "ALTER TABLE public.project_description ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL",
        # referees: migration link columns
        "ALTER TABLE public.referees ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        "ALTER TABLE public.referees ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL",
        # bank_details_tanzania: migration link columns
        "ALTER TABLE public.bank_details_tanzania ADD COLUMN IF NOT EXISTS application_id uuid NULL",
        "ALTER TABLE public.bank_details_tanzania ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid NULL",
    ]

    for _ddl in _add_column_guards:
        try:
            db.execute(text(_ddl))
            db.commit()
        except Exception as _ge:
            logger.warning("[schema-guard] skipped (%s): %s", _ddl[:60], _ge)
            try:
                db.rollback()
            except Exception:
                pass

    # ── Schema guard: add FK constraints to child tables ─────────────────────
    # ADD CONSTRAINT IF NOT EXISTS is NOT valid PostgreSQL syntax.
    # Instead, each FK is wrapped in a DO $$ block that checks pg_constraint
    # first — so re-runs are safe and no error is raised if it already exists.
    _fk_guards: list[tuple[str, str, str, str, str]] = [
        # (table, constraint_name, fk_col, ref_table, ref_col)
        ("applicant_proposed_investment", "fk_api_application_id",  "application_id",              "applications",              "id"),
        ("applicant_proposed_investment", "fk_api_asd_id",          "application_sector_detail_id","application_sector_details","id"),
        ("project_description",           "fk_pd_application_id",   "application_id",              "applications",              "id"),
        # NOTE: fk_pd_asd_id intentionally skipped —
        # project_description.application_sector_detail_id already has
        # the system-generated FK fk3gq9vloe8sd42cassh4whgf57 pointing to
        # application_sector_details(id). Adding a second named FK on the
        # same column causes a constraint conflict on re-runs.
        ("referees",                      "fk_ref_application_id",  "application_id",              "applications",              "id"),
        ("referees",                      "fk_ref_asd_id",          "application_sector_detail_id","application_sector_details","id"),
        ("bank_details_tanzania",         "fk_bdt_application_id",  "application_id",              "applications",              "id"),
        ("bank_details_tanzania",         "fk_bdt_asd_id",          "application_sector_detail_id","application_sector_details","id"),
    ]

    for _tbl, _cname, _fk_col, _ref_tbl, _ref_col in _fk_guards:
        _fk_sql = f"""
            DO $$
            BEGIN
                -- Drop the constraint first in case it exists with a different ON DELETE rule,
                -- then re-create it with ON DELETE CASCADE.
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.{_tbl}'::regclass
                      AND conname  = '{_cname}'
                ) THEN
                    ALTER TABLE public.{_tbl} DROP CONSTRAINT {_cname};
                END IF;
                ALTER TABLE public.{_tbl}
                    ADD CONSTRAINT {_cname}
                    FOREIGN KEY ({_fk_col})
                    REFERENCES public.{_ref_tbl} ({_ref_col})
                    ON DELETE CASCADE;
            END $$;
        """
        try:
            db.execute(text(_fk_sql))
            db.commit()
        except Exception as _fe:
            logger.warning("[schema-guard:fk] skipped %s.%s: %s", _tbl, _cname, _fe)
            try:
                db.rollback()
            except Exception:
                pass

    # ── Schema guard: drop blocking unique + FK constraints on financial_information ──
    # Drop all 13 constraints in ONE transaction inside a DO $$ block so that
    # no interleaved AccessShareLock / AccessExclusiveLock deadlock can occur
    # between individual commit cycles.
    _progress("Schema guard: dropping financial_information blocking constraints …")
    try:
        db.execute(text("""
            DO $$
            DECLARE
                _cname text;
                _constraints text[] := ARRAY[
                    -- UNIQUE constraints (drop first so FK drops don't stall)
                    'uk3330k9q1sw66jlduqpsrhj0p',
                    'uk3bdd17w6dmarkybpxwdqyig3e',
                    'ukel2wimvd2s27v1yv6piue4gdt',
                    'ukfa6pmxx6fmxjml7o4tnpiqnk4',
                    'ukh0i89cf4bjf1w09xb5phl8shm',
                    'uksj2p6a47sf5y8u6uyui21sk8v',
                    -- FK constraints
                    'fk1fyiwcuau1xor0biuf5hiy8cf',
                    'fk4hpnlnjnfofc7khnkxp0emfqn',
                    'fk5uk0tmchnlb1ystu0jpqq42sl',
                    'fk66wid3r5eqs7mukb826wj7f3s',
                    'fkbp7gwdkep3f5gkn44t1ms7p30',
                    'fkjmhltcnyhhewyo6hmn8q6hbno',
                    'fksmkj5b9iapr68uiw8wrcj5s2m'
                ];
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_class
                    WHERE oid = 'public.financial_information'::regclass
                ) THEN
                    FOREACH _cname IN ARRAY _constraints
                    LOOP
                        IF EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conrelid = 'public.financial_information'::regclass
                              AND conname  = _cname
                        ) THEN
                            EXECUTE format(
                                'ALTER TABLE public.financial_information DROP CONSTRAINT %I',
                                _cname
                            );
                            RAISE NOTICE 'Dropped financial_information constraint: %', _cname;
                        END IF;
                    END LOOP;
                END IF;
            END $$;
        """))
        db.commit()
    except Exception as _ce:
        logger.warning("[schema-guard:fi-constraints] block skipped: %s", _ce)
        try:
            db.rollback()
        except Exception:
            pass
    _progress("Schema guard: financial_information constraints done")

    # ── Schema guard: relax certificates constraints (same as elec importer) ──
    _progress("Schema guard: relax certificates constraints …")
    try:
        db.execute(text("""
            DO $$
            DECLARE
                _schema text;
                _rec    record;
            BEGIN
                FOR _schema IN SELECT nspname FROM pg_namespace
                               WHERE nspname NOT IN ('pg_catalog','information_schema')
                LOOP
                    IF EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = _schema AND c.relname = 'certificates'
                    ) THEN
                        -- Drop any unique constraint on certificates that covers approval_no
                        FOR _rec IN
                            SELECT con.conname
                            FROM pg_constraint con
                            JOIN pg_class cls ON cls.oid = con.conrelid
                            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                            JOIN pg_attribute att ON att.attrelid = cls.oid
                                                  AND att.attnum = ANY(con.conkey)
                            WHERE con.contype IN ('u','p')
                              AND ns.nspname  = _schema
                              AND cls.relname = 'certificates'
                              AND att.attname = 'approval_no'
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                        END LOOP;

                        -- Drop any unique constraint on certificates that covers application_number
                        FOR _rec IN
                            SELECT con.conname
                            FROM pg_constraint con
                            JOIN pg_class cls ON cls.oid = con.conrelid
                            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                            JOIN pg_attribute att ON att.attrelid = cls.oid
                                                  AND att.attnum = ANY(con.conkey)
                            WHERE con.contype = 'u'
                              AND ns.nspname  = _schema
                              AND cls.relname = 'certificates'
                              AND att.attname = 'application_number'
                              AND con.conname <> 'uq_certificates_application_number'
                        LOOP
                            EXECUTE format('ALTER TABLE %I.certificates DROP CONSTRAINT IF EXISTS %I',
                                           _schema, _rec.conname);
                        END LOOP;

                        -- Ensure canonical unique index exists
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint con
                            JOIN pg_class cls ON cls.oid = con.conrelid
                            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                            WHERE ns.nspname  = _schema
                              AND cls.relname = 'certificates'
                              AND con.conname = 'uq_certificates_application_number'
                        ) THEN
                            EXECUTE format(
                                'ALTER TABLE %I.certificates
                                 ADD CONSTRAINT uq_certificates_application_number
                                 UNIQUE (application_number)',
                                _schema
                            );
                        END IF;

                    END IF;
                END LOOP;
            END;
            $$;
        """))
        db.commit()
    except Exception as _ce:
        logger.warning("[schema-guard:certificates] skipped: %s", _ce)
        try:
            db.rollback()
        except Exception:
            pass
    _progress("Schema guard: certificates constraints done")

    # ── Step 0: Ensure users exist from username (userid) column ─────────────
    # Mirrors the electrical importer pattern: create users rows for every
    # distinct userid/username value so that applications.created_by can be
    # back-filled by username → users.id after the applications upsert.
    _progress("Ensuring users from username column …")
    try:
        db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        db.commit()
    except Exception:
        pass

    # Normalise username to lowercase in staging so mixed-case duplicates collapse.
    db.execute(text(f"""
        UPDATE {_STAGE}
        SET username = lower(trim(username))
        WHERE username IS NOT NULL AND username <> ''
    """))
    db.commit()

    _r_users = db.execute(text(f"""
        WITH u AS (
            SELECT DISTINCT lower(trim(s.username)) AS uname
            FROM {_STAGE} s
            WHERE NULLIF(trim(s.username), '') IS NOT NULL
        )
        INSERT INTO public.users (
            id, full_name, username, password_hash, status,
            phone_number, email_address, user_category,
            account_type, auth_mode, failed_attempts,
            is_first_login, deleted, created_at, updated_at
        )
        SELECT
            gen_random_uuid(), u.uname, u.uname, '',
            'ACTIVE', NULL, NULL, 'EXTERNAL', 'INDIVIDUAL', 'DB',
            0, false, false, now(), now()
        FROM u
        WHERE NOT EXISTS (
            SELECT 1 FROM public.users eu
            WHERE lower(trim(eu.username)) = u.uname
        )
    """))
    inserted_users = _r_users.rowcount or 0
    total_usernames = db.execute(text(
        f"SELECT COUNT(DISTINCT lower(trim(username))) FROM {_STAGE} "
        "WHERE NULLIF(trim(username), '') IS NOT NULL"
    )).scalar() or 0
    skipped_users = max(0, int(total_usernames) - int(inserted_users))
    db.commit()
    _progress(f"users: inserted={inserted_users}, already_existed={skipped_users}")

    # Assign DEFAULT role to every staged user that doesn't have it yet.
    _water_role_id = load_default_role_id(db)
    inserted_user_roles = 0
    skipped_user_roles = 0
    if not _water_role_id:
        logger.info("[water-supply] DEFAULT role not resolved; role assignment skipped")
        skipped_user_roles = int(total_usernames)
    else:
        try:
            _rr = db.execute(text(f"""
                INSERT INTO public.user_roles (user_id, role_id, deleted, created_at)
                SELECT u.id, :role_id, false, now()
                FROM public.users u
                WHERE EXISTS (
                    SELECT 1 FROM {_STAGE} s
                    WHERE NULLIF(trim(s.username), '') IS NOT NULL
                      AND lower(trim(s.username)) = lower(trim(u.username))
                )
                AND NOT EXISTS (
                    SELECT 1 FROM public.user_roles ex
                    WHERE ex.user_id = u.id AND ex.role_id = :role_id
                )
            """), {"role_id": _water_role_id})
            db.commit()
            inserted_user_roles = _rr.rowcount or 0
            skipped_user_roles = max(0, int(total_usernames) - int(inserted_user_roles))
            _progress(f"user_roles: inserted={inserted_user_roles}, already_had_role={skipped_user_roles}")
        except Exception as _ure:
            logger.warning("[water-supply] role assignment skipped (non-fatal): %s", _ure)
            skipped_user_roles = int(total_usernames)
            try:
                db.rollback()
            except Exception:
                pass

    # ── Step 1: Upsert applications ───────────────────────────────────────────
    _progress("Upserting applications …")
    _lt_sql  = _license_type_sql("s.license_type")
    _at_sql  = _app_type_sql("s.application_type")
    r_apps = db.execute(text(f"""
        INSERT INTO public.applications (
            id,
            application_number,
            application_type,
            approval_no,
            effective_date,
            expire_date,
            completed_at,
            license_type,
            category_license_type,
            username,
            created_by,
            is_from_lois,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_APP:' || lower(btrim(s.application_number)))::uuid,
            btrim(s.application_number),
            {_at_sql},
            NULLIF(btrim(s.approval_no), ''),
            CASE WHEN btrim(s.effective_date) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN btrim(s.effective_date)::date ELSE NULL END,
            CASE WHEN btrim(s.expire_date)    ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN btrim(s.expire_date)::date    ELSE NULL END,
            CASE WHEN btrim(s.completed_at)   ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN btrim(s.completed_at)::timestamp ELSE NULL END,
            {_lt_sql},
            'OPERATIONAL',
            NULLIF(lower(btrim(s.username)), ''),
            u.id,
            true,
            now(),
            now()
        FROM {_STAGE} s
        LEFT JOIN public.users u
            ON lower(trim(u.username)) = lower(trim(s.username))
           AND NULLIF(trim(s.username), '') IS NOT NULL
        WHERE btrim(s.application_number) <> ''
        ON CONFLICT (application_number) DO UPDATE SET
            application_type      = COALESCE(EXCLUDED.application_type,  public.applications.application_type),
            approval_no           = COALESCE(EXCLUDED.approval_no,       public.applications.approval_no),
            effective_date        = COALESCE(EXCLUDED.effective_date,    public.applications.effective_date),
            expire_date           = COALESCE(EXCLUDED.expire_date,       public.applications.expire_date),
            completed_at          = COALESCE(EXCLUDED.completed_at,      public.applications.completed_at),
            license_type          = COALESCE(EXCLUDED.license_type,      public.applications.license_type),
            created_by            = COALESCE(public.applications.created_by, EXCLUDED.created_by),
            updated_at            = now()
    """))
    upserted_apps = r_apps.rowcount or 0
    db.commit()
    _progress(f"applications upserted: {upserted_apps}")

    # Back-fill created_by for any application where it is still NULL
    # (handles cases where the user was created in a prior run).
    db.execute(text(f"""
        UPDATE public.applications a
        SET    created_by = u.id,
               updated_at = now()
        FROM   {_STAGE} s
        JOIN   public.users u
            ON lower(trim(u.username)) = lower(trim(s.username))
        WHERE  a.application_number = btrim(s.application_number)
          AND  a.created_by IS NULL
          AND  NULLIF(trim(s.username), '') IS NOT NULL
          AND  a.is_from_lois = true
    """))
    db.commit()
    _progress("applications.created_by back-filled")

    # ── Step 1b: Upsert certificates ─────────────────────────────────────────
    _progress("Upserting certificates …")
    r_certs = db.execute(text(f"""
        INSERT INTO public.certificates (
            id,
            application_id,
            application_number,
            approval_no,
            approval_date,
            license_type,
            category_license_type,
            effective_date,
            expire_date,
            application_certificate_type,
            sector,
            certificate_type,
            created_at,
            updated_at
        )
        SELECT
            gen_random_uuid(),
            a.id,
            btrim(s.application_number),
            NULLIF(btrim(s.approval_no), ''),
            CASE WHEN NULLIF(btrim(s.completed_at), '') IS NOT NULL
                 THEN btrim(s.completed_at)::date END,
            {_lt_sql},
            'OPERATIONAL',
            CASE WHEN NULLIF(btrim(s.effective_date), '') IS NOT NULL
                 THEN btrim(s.effective_date)::date END,
            CASE WHEN NULLIF(btrim(s.expire_date), '') IS NOT NULL
                 THEN btrim(s.expire_date)::date END,
            COALESCE(({_at_sql}), 'NEW'),
            'WATER_SUPPLY',
            'License',
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        WHERE btrim(s.application_number) <> ''
        ON CONFLICT (application_number) DO UPDATE SET
            approval_no               = COALESCE(EXCLUDED.approval_no,               public.certificates.approval_no),
            approval_date             = COALESCE(EXCLUDED.approval_date,             public.certificates.approval_date),
            license_type              = COALESCE(EXCLUDED.license_type,              public.certificates.license_type),
            category_license_type     = COALESCE(EXCLUDED.category_license_type,     public.certificates.category_license_type),
            application_id            = COALESCE(EXCLUDED.application_id,            public.certificates.application_id),
            effective_date            = COALESCE(EXCLUDED.effective_date,            public.certificates.effective_date),
            expire_date               = COALESCE(EXCLUDED.expire_date,               public.certificates.expire_date),
            application_certificate_type = COALESCE(EXCLUDED.application_certificate_type, public.certificates.application_certificate_type),
            sector                    = 'WATER_SUPPLY',
            certificate_type          = 'License',
            updated_at                = now()
    """))
    upserted_certs = r_certs.rowcount or 0
    db.commit()
    _progress(f"certificates upserted: {upserted_certs}")

    # Back-fill applications.certificate_id where still NULL
    db.execute(text("""
        UPDATE public.applications a
        SET    certificate_id = c.id,
               updated_at     = now()
        FROM   public.certificates c
        WHERE  c.application_id = a.id
          AND  a.certificate_id IS NULL
          AND  a.is_from_lois = true
    """))
    db.commit()
    _progress("applications.certificate_id back-filled")

    # ── Step 2: Upsert application_sector_details ─────────────────────────────
    _progress("Upserting application_sector_details …")
    r_asd = db.execute(text(f"""
        INSERT INTO public.application_sector_details (
            id,
            application_id,
            company_name,
            address_code,
            address_no,
            block_no,
            plot_no,
            road,
            street,
            region,
            district,
            ward,
            latitude,
            longitude,
            po_box,
            facility_name,
            email,
            mobile_no,
            website,
            tin,
            tin_name,
            brela_number,
            brela_registration_type,
            certificate_of_incorporation_no,
            vrn,
            gn_gazette_on,
            government_notice_no,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid,
            a.id,
            NULLIF(btrim(s.company_name), ''),
            NULLIF(btrim(s.address_code), ''),
            NULLIF(btrim(s.address_no), ''),
            NULLIF(btrim(s.block_no), ''),
            NULLIF(btrim(s.plot_no), ''),
            NULLIF(btrim(s.road), ''),
            NULLIF(btrim(s.street), ''),
            NULLIF(btrim(s.region), ''),
            NULLIF(btrim(s.district), ''),
            NULLIF(btrim(s.ward), ''),
            NULLIF(btrim(s.latitude), ''),
            NULLIF(btrim(s.longitude), ''),
            NULLIF(btrim(s.po_box), ''),
            NULLIF(btrim(s.facility_name), ''),
            NULLIF(btrim(s.email), ''),
            NULLIF(btrim(s.mobile_no), ''),
            NULLIF(btrim(s.website), ''),
            NULLIF(btrim(s.tin), ''),
            NULLIF(btrim(s.tin_name), ''),
            NULLIF(btrim(s.brela_number), ''),
            NULLIF(btrim(s.brela_registration_type), ''),
            NULLIF(btrim(s.certificate_of_incorporation_no), ''),
            NULLIF(btrim(s.vrnno), ''),
            NULLIF(btrim(s.gngzon), ''),
            NULLIF(btrim(s.govtnoteno), ''),
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a ON a.application_number = btrim(s.application_number)
        WHERE btrim(s.application_number) <> ''
        ON CONFLICT (id) DO UPDATE SET
            company_name                    = COALESCE(EXCLUDED.company_name,                    public.application_sector_details.company_name),
            vrn                             = COALESCE(EXCLUDED.vrn,                             public.application_sector_details.vrn),
            gn_gazette_on                   = COALESCE(EXCLUDED.gn_gazette_on,                   public.application_sector_details.gn_gazette_on),
            government_notice_no            = COALESCE(EXCLUDED.government_notice_no,            public.application_sector_details.government_notice_no),
            certificate_of_incorporation_no = COALESCE(EXCLUDED.certificate_of_incorporation_no, public.application_sector_details.certificate_of_incorporation_no),
            updated_at                      = now()
    """))
    upserted_asd = r_asd.rowcount or 0
    db.commit()
    _progress(f"application_sector_details upserted: {upserted_asd}")

    # ── Step 3: Upsert referees (contact details) ─────────────────────────────
    _progress("Upserting referees …")
    r_ref = db.execute(text(f"""
        INSERT INTO public.referees (
            id,
            telephone_no,
            title,
            contact_person_name,
            application_id,
            application_sector_detail_id,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_REFEREE:' || lower(btrim(s.application_number)))::uuid,
            NULLIF(btrim(s.cmobile_no), ''),
            NULLIF(btrim(s.title), ''),
            NULLIF(btrim(s.contact_name), ''),
            a.id,
            asd.id,
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        JOIN public.application_sector_details asd
            ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
        WHERE btrim(s.application_number) <> ''
          AND (
              btrim(s.cmobile_no)    <> '' OR
              btrim(s.title)         <> '' OR
              btrim(s.contact_name)  <> ''
          )
        ON CONFLICT (id) DO UPDATE SET
            telephone_no                 = COALESCE(EXCLUDED.telephone_no,                 public.referees.telephone_no),
            title                        = COALESCE(EXCLUDED.title,                        public.referees.title),
            contact_person_name          = COALESCE(EXCLUDED.contact_person_name,          public.referees.contact_person_name),
            application_id               = EXCLUDED.application_id,
            application_sector_detail_id = EXCLUDED.application_sector_detail_id,
            updated_at                   = now()
    """))
    upserted_ref = r_ref.rowcount or 0
    db.commit()
    _progress(f"referees upserted: {upserted_ref}")

    # ── Step 4: Insert bank_details_tanzania ──────────────────────────────────
    _progress("Inserting bank_details_tanzania …")
    r_bank = db.execute(text(f"""
        INSERT INTO public.bank_details_tanzania (
            id,
            name_of_banker,
            position,
            contact_person_name,
            telephone_no,
            email,
            po_box,
            application_id,
            application_sector_detail_id,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_BANK_TZ:' || lower(btrim(s.application_number)))::uuid,
            NULLIF(btrim(s.bname), ''),
            NULLIF(btrim(s.bposition), ''),
            NULLIF(btrim(s.bconpername), ''),
            NULLIF(btrim(s.ateleno), ''),
            NULLIF(btrim(s.email), ''),
            NULLIF(btrim(s.po_box), ''),
            a.id,
            asd.id,
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        JOIN public.application_sector_details asd
            ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
        WHERE btrim(s.application_number) <> ''
          AND (
              btrim(s.bname)       <> '' OR
              btrim(s.bposition)   <> '' OR
              btrim(s.bconpername) <> '' OR
              btrim(s.ateleno)     <> ''
          )
        ON CONFLICT (id) DO UPDATE SET
            name_of_banker               = COALESCE(EXCLUDED.name_of_banker,               public.bank_details_tanzania.name_of_banker),
            position                     = COALESCE(EXCLUDED.position,                     public.bank_details_tanzania.position),
            contact_person_name          = COALESCE(EXCLUDED.contact_person_name,          public.bank_details_tanzania.contact_person_name),
            telephone_no                 = COALESCE(EXCLUDED.telephone_no,                 public.bank_details_tanzania.telephone_no),
            application_id               = EXCLUDED.application_id,
            application_sector_detail_id = EXCLUDED.application_sector_detail_id,
            updated_at                   = now()
    """))
    upserted_bank = r_bank.rowcount or 0
    db.commit()
    _progress(f"bank_details_tanzania upserted: {upserted_bank}")

    # ── Step 5: Upsert financial_information ──────────────────────────────────
    _progress("Upserting financial_information …")
    r_fi = db.execute(text(f"""
        INSERT INTO public.financial_information (
            id,
            application_sector_detail_id,
            application_id,
            bank_details_tz_id,
            referee_id,
            fs,
            amount,
            currency,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_FI:' || lower(btrim(s.application_number)))::uuid,
            asd.id,
            a.id,
            -- bank_details_tz_id: reference the bank row we inserted in Step 4
            -- (only set when bank data was present for this application)
            CASE WHEN (
                btrim(s.bname)       <> '' OR
                btrim(s.bposition)   <> '' OR
                btrim(s.bconpername) <> '' OR
                btrim(s.ateleno)     <> ''
            ) THEN md5('WATER_BANK_TZ:' || lower(btrim(s.application_number)))::uuid
            ELSE NULL END,
            -- referee_id: reference the referee row we inserted in Step 3
            -- (only set when contact data was present for this application)
            CASE WHEN (
                btrim(s.cmobile_no)   <> '' OR
                btrim(s.title)        <> '' OR
                btrim(s.contact_name) <> ''
            ) THEN md5('WATER_REFEREE:' || lower(btrim(s.application_number)))::uuid
            ELSE NULL END,
            NULLIF(btrim(s.fs), ''),
            NULLIF(btrim(s.amount), ''),
            NULLIF(btrim(s.currency), ''),
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        JOIN public.application_sector_details asd
            ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
        WHERE btrim(s.application_number) <> ''
        ON CONFLICT (id) DO UPDATE SET
            application_sector_detail_id = EXCLUDED.application_sector_detail_id,
            application_id               = EXCLUDED.application_id,
            bank_details_tz_id           = COALESCE(EXCLUDED.bank_details_tz_id,  public.financial_information.bank_details_tz_id),
            referee_id                   = COALESCE(EXCLUDED.referee_id,          public.financial_information.referee_id),
            fs                           = COALESCE(EXCLUDED.fs,                  public.financial_information.fs),
            amount                       = COALESCE(EXCLUDED.amount,              public.financial_information.amount),
            currency                     = COALESCE(EXCLUDED.currency,            public.financial_information.currency),
            updated_at                   = now()
    """))
    upserted_fi = r_fi.rowcount or 0
    db.commit()
    _progress(f"financial_information upserted: {upserted_fi}")

    # ── Step 5b: Upsert applicant_proposed_investment ─────────────────────────
    # Stores amount + currency from the Excel financial columns.
    # Linked to both application_sector_detail_id and application_id.
    _progress("Upserting applicant_proposed_investment …")
    r_api = db.execute(text(f"""
        INSERT INTO public.applicant_proposed_investment (
            id,
            amount,
            currency,
            application_sector_detail_id,
            application_id,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_API:' || lower(btrim(s.application_number)))::uuid,
            NULLIF(btrim(s.amount), ''),
            NULLIF(btrim(s.currency), ''),
            asd.id,
            a.id,
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        JOIN public.application_sector_details asd
            ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
        WHERE btrim(s.application_number) <> ''
          AND (
              NULLIF(btrim(s.amount), '')   IS NOT NULL OR
              NULLIF(btrim(s.currency), '') IS NOT NULL
          )
        ON CONFLICT (id) DO UPDATE SET
            amount                       = COALESCE(EXCLUDED.amount,    public.applicant_proposed_investment.amount),
            currency                     = COALESCE(EXCLUDED.currency,  public.applicant_proposed_investment.currency),
            application_sector_detail_id = EXCLUDED.application_sector_detail_id,
            application_id               = EXCLUDED.application_id,
            updated_at                   = now()
    """))
    upserted_api = r_api.rowcount or 0
    db.commit()
    _progress(f"applicant_proposed_investment upserted: {upserted_api}")

    # ── Step 5c: Upsert project_description ───────────────────────────────────
    # Stores number_of_customer (from nofcustomer Excel column).
    # Linked to application_sector_detail_id (required FK) + application_id.
    # Only inserts when at least one content column is non-empty/non-null.
    _progress("Upserting project_description …")
    r_pd = db.execute(text(f"""
        INSERT INTO public.project_description (
            id,
            number_of_customer,
            application_sector_detail_id,
            application_id,
            created_at,
            updated_at
        )
        SELECT
            md5('WATER_PD:' || lower(btrim(s.application_number)))::uuid,
            NULLIF(btrim(s.no_of_customer), ''),
            asd.id,
            a.id,
            now(),
            now()
        FROM {_STAGE} s
        JOIN public.applications a
            ON a.application_number = btrim(s.application_number)
        JOIN public.application_sector_details asd
            ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
        WHERE btrim(s.application_number) <> ''
          AND (
              NULLIF(btrim(s.no_of_customer), '') IS NOT NULL
          )
        ON CONFLICT (id) DO UPDATE SET
            number_of_customer           = COALESCE(EXCLUDED.number_of_customer, public.project_description.number_of_customer),
            application_sector_detail_id = EXCLUDED.application_sector_detail_id,
            application_id               = EXCLUDED.application_id,
            updated_at                   = now()
    """))
    upserted_pd = r_pd.rowcount or 0
    db.commit()
    _progress(f"project_description upserted: {upserted_pd}")

    # ── Step 6: Insert documents (one per attachment pair with a filename) ─────
    _progress("Inserting documents …")
    total_docs = 0
    for order, (id_col, filename_col, label) in enumerate(_ATTACHMENT_PAIRS, start=1):
        r_doc = db.execute(text(f"""
            INSERT INTO public.documents (
                id,
                document_name,
                document_url,
                file_name,
                application_id,
                application_sector_detail_id,
                documents_order,
                created_at,
                updated_at
            )
            SELECT
                md5('WATER_DOC:{label}:' || lower(btrim(s.application_number)))::uuid,
                '{label}',
                NULLIF(btrim(s.{id_col}), ''),
                NULLIF(btrim(s.{filename_col}), ''),
                a.id,
                asd.id,
                {order},
                now(),
                now()
            FROM {_STAGE} s
            JOIN public.applications a
                ON a.application_number = btrim(s.application_number)
            JOIN public.application_sector_details asd
                ON asd.id = md5('WATER_ASD:' || lower(btrim(s.application_number)))::uuid
            WHERE btrim(s.application_number) <> ''
              AND btrim(s.{filename_col}) <> ''
            ON CONFLICT (id) DO UPDATE SET
                document_url  = COALESCE(EXCLUDED.document_url,  public.documents.document_url),
                file_name     = COALESCE(EXCLUDED.file_name,     public.documents.file_name),
                updated_at    = now()
        """))
        total_docs += r_doc.rowcount or 0
        db.commit()

    _progress(f"documents upserted: {total_docs}")

    return {
        "status": "OK",
        "staged_rows": staged_rows,
        "inserted_users": inserted_users,
        "skipped_users": skipped_users,
        "inserted_user_roles": inserted_user_roles,
        "skipped_user_roles": skipped_user_roles,
        "upserted_applications": upserted_apps,
        "upserted_certificates": upserted_certs,
        "upserted_application_sector_details": upserted_asd,
        "upserted_referees": upserted_ref,
        "upserted_bank_details_tanzania": upserted_bank,
        "upserted_financial_information": upserted_fi,
        "upserted_applicant_proposed_investment": upserted_api,
        "upserted_project_description": upserted_pd,
        "upserted_documents": total_docs,
    }
