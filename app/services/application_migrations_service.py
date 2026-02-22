from typing import List, Tuple, Dict, Any
import uuid
import re
import os
import csv
import logging
from decimal import Decimal, InvalidOperation
import io
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

# Setup logger
logger = logging.getLogger(__name__)

# Normalization cache to avoid repeated Decimal conversions
_normalize_cache: Dict[str, str] = {}


def _convert_excel_date(val: Any) -> Any:
    """Convert Excel serial date (e.g. 43979.0) to YYYY-MM-DD string."""
    if val is None:
        return None
    sval = str(val).strip()
    if not sval:
        return None
    
    # If it is already a date-like string (YYYY-MM-DD), leave it
    if re.match(r'^\d{4}-\d{2}-\d{2}', sval):
        return sval

    # Check for Excel serial number
    try:
        # Remove .0 if present for parsing logic (though float handles it)
        d = float(sval)
        # Sanity check: Excel dates between 1900 and 2200 roughly 1 to 109573
        # 43979 is ~2020.
        # Let's say we only convert valid-looking modern dates to avoid false positives
        # on small integers if specific columns aren't targeted.
        # However, we will target specific columns.
        if d > 0:
            # Excel base date: December 30, 1899
            dt = datetime(1899, 12, 30) + timedelta(days=d)
            # Return ISO format date
            return dt.strftime('%Y-%m-%d')
    except (ValueError, OverflowError):
        pass
    
    return val


def _normalize_numeric_string(val: str) -> str:
    """Normalize numeric-like strings to an integer string without losing precision.

    Examples:
    - '1554087241462.0' -> '1554087241462'
    - '1.554087241462e+12' -> '1554087241462'
    - if not numeric, returns original stripped string
    """
    if val is None:
        return val
    sval = str(val).strip()
    if not sval:
        return sval
    if sval in _normalize_cache:
        return _normalize_cache[sval]
    # quick check: if there are non-numeric characters other than . and e/E and +- then bail
    try:
        d = Decimal(sval)
        # if it's an integer value, return without exponent/decimal
        if d == d.to_integral_value():
            out = format(d.quantize(Decimal(1)), 'f')
            # remove any fractional .0
            if out.endswith('.0'):
                out = out[:-2]
            _normalize_cache[sval] = out
            return out
    except InvalidOperation:
        # not a decimal number; keep original
        _normalize_cache[sval] = sval
        return sval
    # fallback: return original
    _normalize_cache[sval] = sval
    return sval


def _extract_db_error_detail(exc: Exception) -> str:
    """Extract detailed error information from a database exception.
    
    Parses PostgreSQL/SQLAlchemy errors to provide human-readable details
    about constraint violations, type mismatches, etc.
    """
    err_str = str(exc)
    details = []
    
    # Try to get the original database error
    orig = getattr(exc, 'orig', None)
    if orig:
        # psycopg2 errors have pgcode and pgerror attributes
        pgcode = getattr(orig, 'pgcode', None)
        pgerror = getattr(orig, 'pgerror', None)
        if pgerror:
            err_str = str(pgerror)
        if pgcode:
            details.append(f"code={pgcode}")
    
    # Extract constraint name if present
    constraint_match = re.search(r'constraint "([^"]+)"', err_str, re.IGNORECASE)
    if constraint_match:
        details.append(f"constraint={constraint_match.group(1)}")
    
    # Extract column name if present
    column_match = re.search(r'column "([^"]+)"', err_str, re.IGNORECASE)
    if column_match:
        details.append(f"column={column_match.group(1)}")
    
    # Extract table name if present
    table_match = re.search(r'(?:table|relation) "([^"]+)"', err_str, re.IGNORECASE)
    if table_match:
        details.append(f"table={table_match.group(1)}")
    
    # Extract value if present (for constraint violations)
    value_match = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', err_str)
    if value_match:
        details.append(f"key={value_match.group(1)}, value={value_match.group(2)}")
    
    # Extract the invalid value from the error message
    invalid_value_match = re.search(r'invalid input syntax for type \w+: "([^"]+)"', err_str)
    if invalid_value_match:
        bad_value = invalid_value_match.group(1)
        details.append(f"bad_value=\"{bad_value}\"")
        # Try to guess the column based on context
        if bad_value.lower() in ('petroleum', 'gas', 'electricity', 'water'):
            details.append("hint=value looks like a sector/category, check if it's in wrong column (maybe application_legal_status_id?)")
    
    # Extract data type info
    type_match = re.search(r'(?:invalid input syntax for (?:type )?(\w+)|cannot cast .* to (\w+))', err_str, re.IGNORECASE)
    if type_match:
        dtype = type_match.group(1) or type_match.group(2)
        details.append(f"expected_type={dtype}")
    
    # Common error type descriptions
    error_type = "Unknown error"
    if '23505' in err_str or 'unique' in err_str.lower() or 'duplicate' in err_str.lower():
        error_type = "DUPLICATE_KEY"
    elif '23503' in err_str or 'foreign key' in err_str.lower():
        error_type = "FOREIGN_KEY_VIOLATION"
    elif '23502' in err_str or 'not-null' in err_str.lower() or 'null value' in err_str.lower():
        error_type = "NOT_NULL_VIOLATION"
    elif '22P02' in err_str or 'invalid input syntax' in err_str.lower():
        error_type = "INVALID_DATA_TYPE"
    elif '22001' in err_str or 'value too long' in err_str.lower():
        error_type = "VALUE_TOO_LONG"
    elif '23514' in err_str or 'check constraint' in err_str.lower():
        error_type = "CHECK_CONSTRAINT_VIOLATION"
    
    # Build the final message
    if details:
        return f"{error_type}: {', '.join(details)} - {err_str[:200]}"
    return f"{error_type}: {err_str[:300]}"


def _load_map_from_csv_module(rel_path: str) -> Dict[str, str]:
    """Load a simple two-column CSV mapping file into a dict.

    Returns a dict mapping the first column (as a string) to the second column.
    Also populates normalized numeric keys for each entry (if applicable).
    """
    out: Dict[str, str] = {}
    base_dir = os.path.dirname(__file__)
    csv_path = os.path.join(base_dir, '..', rel_path)
    csv_path = os.path.normpath(csv_path)
    if not os.path.exists(csv_path):
        return out
    try:
        with open(csv_path, 'r', encoding='utf-8') as fh:
            reader = csv.reader(fh)
            for r in reader:
                if not r:
                    continue
                key = str(r[0]).strip()
                val = r[1].strip() if len(r) > 1 else ''
                if not key:
                    continue
                out[key] = val
                # add normalized numeric key too if possible
                try:
                    nk = _normalize_numeric_string(key)
                    if nk and nk != key:
                        out[nk] = val
                except Exception:
                    pass
    except Exception as e:
        logger.exception("Failed loading mapping CSV %s: %s", csv_path, e)
        return {}
    return out


def _load_id_name_map(rel_path: str) -> Dict[str, str]:
    """Load an (id,name) CSV from app/data into a lookup dict.

    - Works with quoted or unquoted CSV.
    - Skips a header row like: id,name
    - Adds normalized numeric keys to handle scientific notation strings.
    """

    out: Dict[str, str] = {}
    base_dir = os.path.dirname(__file__)
    csv_path = os.path.normpath(os.path.join(base_dir, '..', rel_path))
    if not os.path.exists(csv_path):
        return out

    try:
        with open(csv_path, 'r', encoding='utf-8') as fh:
            reader = csv.reader(fh)
            for r in reader:
                if not r or all(not str(x).strip() for x in r):
                    continue

                key = str(r[0]).strip().strip('"')
                val = (str(r[1]).strip().strip('"') if len(r) > 1 else '')

                # Skip header row
                if key.lower() in ('id', 'ward_id', 'district_id', 'region_id') and val.lower() in ('name',):
                    continue

                if not key:
                    continue

                out[key] = val

                # Add normalized numeric key too (handles things like 1.554087241741e+12)
                try:
                    nk = _normalize_numeric_string(key)
                    if nk and nk != key:
                        out[nk] = val
                except Exception:
                    pass
    except Exception as e:
        logger.exception("Failed loading id->name mapping CSV %s: %s", csv_path, e)
        return {}

    return out


# Preload lookup maps at module import time (fast, done once)
region_map_csv = _load_id_name_map('data/regions.csv')
district_map_csv = _load_id_name_map('data/districts.csv')
ward_map_csv = _load_id_name_map('data/wards.csv')
# Alias for backward compatibility
ward_map = ward_map_csv


def _detect_attachment_pairs_from_cols(cols: List[str]) -> List[Tuple[str, str, str]]:
    """Detect (id_col, filename_col, base) triples from a list of column names.

    Heuristics: find columns ending with 'filename' and pair them with plausible
    id columns (base or base + '_id').
    """
    pairs = []
    filename_cols = [c for c in cols if c.lower().endswith('filename')]
    for fname in filename_cols:
        base = fname[:-len('filename')]
        id_col = None
        if base in cols:
            id_col = base
        elif (base + '_id') in cols:
            id_col = base + '_id'
        elif base.endswith('_') and base.rstrip('_') in cols:
            id_col = base.rstrip('_')
        else:
            id_col = base if base in cols else None
        pairs.append((id_col, fname, base))
    return pairs


def _build_default_mappings():
    """Return the explicit mappings requested by the user.

    This maps Excel column names to staging column names and
    returns the fixed attachments specification.

    MASTER MAPPING TABLE (Excel column -> staging/DB column):
    ─────────────────────────────────────────────────────────
    APPLICATION / ASD columns (direct):
      address_code                -> address_code
      address_no                  -> address_no
      block_no                    -> block_no
      district                    -> district   (mapped ID->name via districts.csv)
      region                      -> region     (mapped ID->name via regions.csv)
      ward                        -> ward       (mapped ID->name via wards.csv)
      street                      -> street
      road                        -> road
      plot_no                     -> plot_no
      facility_name               -> facility_name
      company_name                -> company_name
      latitude                    -> latitude
      longitude                   -> longitude
      mobile_no                   -> mobile_no
      po_box                      -> po_box
      website                     -> website
      tin                         -> tin
      tin_name                    -> tin_name
      brela_number                -> brela_number
      brela_registration_type     -> brela_registration_type
      certificate_of_incorporation_no -> certificate_of_incorporation_no
      email                       -> email

    APPLICATION meta columns:
      application_number          -> application_number
      application_type            -> application_type  (uppercased)
      approval_no                 -> approval_no
      effective_date              -> effective_date    (ISO date)
      expire_date                 -> expire_date       (ISO date)
      completed_at                -> completed_at
      license_type                -> license_type
      license_category_id         -> license_category_raw (staging)
      application_legal_status_id -> application_legal_status_raw (staging)
      userid                      -> username
      created_by                  -> old_created_by
      parent_application_id       -> old_parent_application_id

    FIRE certificate columns (Excel f... -> staging fire_*):
      fcontrolno                  -> fire_certificate_control_number
      fpremisename                -> fire_premise_name
      fregion                     -> fire_region
      fdistrict                   -> fire_district
      fadministrativearea         -> fire_administrative_area
      fward                       -> fire_ward
      fstreet                     -> fire_street
      fvalidfrom                  -> fire_valid_from
      fvalidto                    -> fire_valid_to

    INSURANCE / TIRA columns (Excel tira... -> staging cover_note_*/insurance_*):
      covernoterefno              -> cover_note_ref_no
      tiracovernotenumber         -> cover_note_number
      tiracovernotereferencenumber -> insurance_ref_no
      tirapolicyholdername        -> policy_holder_name
      tirainsurercompanyname      -> insurer_company_name
      tiracovernotestartdate      -> cover_note_start_date
      tiracovernotenddate         -> cover_note_end_date
      tirariskname                -> risk_name
      tirasubjectmatterdesc       -> subject_matter_desc
    """
    # ── APPLICATION / ASD fields ──────────────────────────────────────────────
    excel_to_app = {
        # address / location (region/district/ward IDs mapped to names via CSV)
        'address_code':                    'address_code',
        'address_no':                      'address_no',
        'block_no':                        'block_no',
        'plot_no':                         'plot_no',
        'road':                            'road',
        'street':                          'street',
        'region':                          'region',
        'district':                        'district',
        'ward':                            'ward',
        # contact / business
        'mobile_no':                       'mobile_no',
        'email':                           'email',
        'website':                         'website',
        'latitude':                        'latitude',
        'longitude':                       'longitude',
        'po_box':                          'po_box',
        'facility_name':                   'facility_name',
        'company_name':                    'company_name',
        'tin':                             'tin',
        'tin_name':                        'tin_name',
        'brela_number':                    'brela_number',
        'brela_registration_type':         'brela_registration_type',
        'certificate_of_incorporation_no': 'certificate_of_incorporation_no',
        # application meta
        'application_number':              'application_number',
        'appno':                           'application_number',
        'app_number':                      'application_number',
        'apprefno':                        'application_number',
        'application_type':                'application_type',
        'approval_no':                     'approval_no',
        'approvalno':                      'approval_no',
        'approval_number':                 'approval_no',
        'licenceno':                       'approval_no',
        'license_no':                      'approval_no',
        'effective_date':                  'effective_date',
        'effectivedate':                   'effective_date',
        'date_effective':                  'effective_date',
        'start_date':                      'effective_date',
        'startdate':                       'effective_date',
        'expire_date':                     'expire_date',
        'expiry_date':                     'expire_date',
        'expiredate':                      'expire_date',
        'expired_date':                    'expire_date',
        'expirydate':                      'expire_date',
        'expdate':                         'expire_date',
        'date_expire':                     'expire_date',
        'date_expiry':                     'expire_date',
        'end_date':                        'expire_date',
        'enddate':                         'expire_date',
        'completed_at':                    'completed_at',
        'license_type':                    'license_type',
        'license_category_id':             'license_category_id',
        'application_legal_status_id':     'application_legal_status_id',
        'userid':                          'username',
        'created_by':                      'old_created_by',
        'parent_application_id':           'old_parent_application_id',
    }

    # ── FIRE certificate fields ───────────────────────────────────────────────
    # Keys are normalized (lowercase, no spaces) Excel headers.
    # Values are the staging column names (fire_* prefix).
    fire_map = {
        'fcontrolno':        'fire_certificate_control_number',
        'fpremisename':      'fire_premise_name',
        'fregion':           'fire_region',
        'fdistrict':         'fire_district',
        'fadministrativearea': 'fire_administrative_area',
        'fward':             'fire_ward',
        'fstreet':           'fire_street',
        'fvalidfrom':        'fire_valid_from',
        'fvalidto':          'fire_valid_to',
    }
    excel_to_app.update(fire_map)

    # ── INSURANCE / TIRA fields ───────────────────────────────────────────────
    tira_map = {
        'covernoterefno':             'cover_note_ref_no',
        'tiracovernotenumber':        'cover_note_number',
        'tiracovernotereferencenumber': 'insurance_ref_no',
        'tirapolicyholdername':       'policy_holder_name',
        'tirainsurercompanyname':     'insurer_company_name',
        'tiracovernotestartdate':     'cover_note_start_date',
        'tiracovernotenddate':        'cover_note_end_date',
        'tirariskname':               'risk_name',
        'tirasubjectmatterdesc':      'subject_matter_desc',
    }
    excel_to_app.update(tira_map)

    # Attachment specification: tuples of (id_col, filename_col, label)
    attachments_spec = [
        ('tinc', 'tincfilename', 'TINC'),
        ('coc', 'cocfilename', 'COC'),
        ('upop', 'upopfilename', 'UPOP'),
        ('ucl', 'uclfilename', 'UCL'),
        ('uplr', 'uplrfilename', 'UPLR'),
        ('spdoc1', 'spdoc1filename', 'SPDOC1'),
        ('spdoc2', 'spdoc2filename', 'SPDOC2'),
        ('spdoc3', 'spdoc3filename', 'SPDOC3'),
        ('downloadlicense', 'downloadlicensefilename', 'DOWNLOADLICENSE'),
        ('peclabelnew', None, 'PECLABELNEW'),
    ]
    # The user provided a long list of other possible attachment columns
    # (many Excel sheets may include some of these). We dynamically extend
    # the attachments_spec with those names so the importer will handle
    # them if present.
    extra_names = [
        'afr', 'dpana', 'wp', 'ccocoooaoaf', 'laictaontootl', 'ccobp', 'apobpdtsotpba',
        'roalopapotaq', 'aelpdsbareoaloftbc', 'epmtbetpheirf', 'pprpm', 'adsip',
        'poavcocoami', 'poalalcbaraoa', 'poasp', 'poaeiacibra', 'polbptcwgpi', 'blbs',
        'pofcftptol', 'soppcau', 'cloybu', 'pooolotlp', 'fsc', 'txinc', 'aelpdsbarefpi',
        'areffletter', 'bgonltobafhms', 'adonltobafhsaabofi', 'aulocfafioabtcttb',
        'powolsfafpoaha', 'popoaqp', 'lofapoaosftstpb',
        'apwrttpog', 'pfspes', 'jvc', 'pfsramotdtfc', 'cmu', 'eotcaafs',
        'daabd', 'fs', 'ccfc', 'cctic', 'commissioningreport', 'cngplan',
        'commission', 'decommission', 'imfng', 'tcam', 'dptos', 'sotcam',
        'adsotarrpfffy', 'tpiitffy', 'trror', 'trrorfilename', 'coi',
        'apopoaaf', 'ccoavtcc', 'lwllabel1', 'lwllabel2', 'lwllabel3', 'lwllabel4',
        'lwllabel8', 'lwllabel9', 'bpotra', 'pnclabel1', 'pnclabel2', 'frd',
        'loptarl', 'pfsapes', 'bust', 'pipelinemeasuresdoc', 'pacra',
        'pipelineengplandoc', 'peclabel1filename',
        'tpdfcacocotf', 'pnclabel3', 'tmpiwfsdtf', 'businessplan',
        'pipelinedetaildoc', 'alofaposftstpb', 'fssa', 'pofctmaotf', 'accotinc',
        'adotpttbattf', 'tcc', 'poaqpp', 'coteasiac', 'pipelinecvdoc', 'ptrl',
        'openarea', 'consume', 'eoperator', 'peclable6', 'peclable7', 'peclable8',
        'cccf', 'lfc', 'cbplga', 'lst', 'regdoc', 'latpa', 'fcotf', 'padsofpf',
        'aoiasbrbta', 'vrnc', 'qmp', 'pmp', 'cpphoto', 'certificatetestimonial',
        'iftanzanian', 'projectdrawingfilename', 'commissioningplanfilename',
        'pitsfilename', 'eoarcaibrafilename', 'memo', 'cor', 'brelac', 'lssd',
        'owner', 'lease', 'assessmentcertificate', 'nemc', 'registeredengineer',
        'sitemaps', 'mru', 'tteadhbdiawas', 'cvattach', 'company', 'healthothers',
        'daabdisoaanpofs', 'polur', 'permits', 'edadof', 'comou', 'gsa', 'gsas',
        'erp', 'inpl', 'lcp', 'oshacer', 'dsptp', 'eiasraabtnem', 'vatc',
        'cafsaaftypta', 'lftbbs', 'capexp', 'ar', 'projectdrawing', 'commissioningplan',
        'pits', 'eoarcaibra', 'dsystemplan', 'cngsysplan', 'tsysplan', 'psysplan',
        'dateofiss', 'ngpflowdia', 'engdrawngp', 'entryexit', 'ngdisclabel1',
        'ngdisclabel2', 'ngdisclabel3', 'ngdisclabel4', 'ngdisclabel5',
        'ngdisclabel6', 'ngtranslabel1', 'ngtranslabel2', 'ngtransabel3',
        'ngtranslabel4', 'ngtranslabel5', 'tcompany', 'particular', 'stackhoilder',
        'dsysplan', 'poneosoppwcna', 'eiac', 'adfipf', 'coponi', 'pro',
        'dawtlwtctfi', 'bpdtsoala', 'loildttsdhdawfsol',
        'lofapoaosftstp', 'adfip', 'ccooc', 'popolpg', 'loldttsdhdawfsol',
        'maaa', 'cvoal',
        'lirwplt', 'popoasd', 'popoasdlpg', 'rfaoalcdcam', 'lwllabel5', 'lwllabel6',
        'lwllabel7', 'prtraatcotfo', 'cpana', 'ccovttcc', 'pd', 'sm', 'lup',
        'workpermit', 'ccobrd', 'wpoooloalwtc', 'pooalhpfara', 'dawtlw',
        'lolr', 'elpdsbarewss', 'aswdp', 'popoavcocoamiftca', 'spcaatp',
        'oshc', 'alopapotaq', 'doaaa', 'popoasfohawael', 'sacb',
        'pcoassowsorb', 'mtrdiclo', 'popoaefmsfpc', 'ppwllabel1', 'sc', 'ppwllabel3',
        'smtcelabel', 'aloptarl', 'ppsa', 'peclabel', 'ppwllabel2',
        'pfdpu', 'tnotfsfapp', 'tlladotl', 'tladbbsafae', 'eae', 'tpwapb', 'dotfsctlp',
        'poloarouotlt', 'ccoabtico', 'ccopoc', 'ccowamacop', 'cvarccoa', 'popoalitc',
        'sop', 'popoaf', 'pimsrlabel1', 'pimsrlabel2', 'pimsrlabel3', 'pimsrlabel4',
        'ppsawomc', 'tasac', 'ccobrdp', 'poootpw', 'pbotpitutwtpwbp', 'alofa',
        'tregistration', 'townership', 'tlease', 'tpermit', 'tfire', 'teiac',
        'telayout', 'tdetail', 'taccess', 'aeiacibra',
        'ccoavfcftfd', 'adotmapiwco', 'pipelinefclosuredoc',
        'pipelinelandowndoc', 'pelabel5', 'tconsume', 'teoperator', 'tform',
        'lopapotaq', 'cvoaltkp', 'ccoha', 'corrosion', 'peclabel1', 'peclabel2',
        'peclabel3', 'peclabel4', 'mnblicense', 'mnusage',
        'ccovtcc', 'miaoapstapa', 'pfd', 'ppitsotpdiq', 'bp', 'llc', 'wap',
        'tdfwsaaoi', 'todis', 'coa', 'releventcon', 'stsatwngwbs', 'mougctorgas',
        'comt', 'os', 'caat', 'cbp', 'moudqaw', 'cafsftpy', 'eiarcfpui', 'ccsc',
        'abftcoy', 'moumaw', 'sesr', 'wsdpfruabtua', 'sv', 'cafsaafpupta', 'ppirwplt',
        'pwllabel1', 'pwllabel2', 'pwllabel3', 'pwllabel4', 'refltr',
        'pwllabel5', 'pwllabel6', 'accotsawalw', 'ccicftf', 'atgproof', 'pwllabel7',
        'pwllabel8', 'pwllabel9', 'avccotcc', 'prodoc', 'commreport', 'ppapotapsapea',
        'losflg', 'losfmrfe'
    ]

    for name in extra_names:
        # If the name already looks like a filename column, derive the base id
        if name.lower().endswith('filename'):
            base = name[:-len('filename')]
            fname = name
        else:
            base = name
            fname = f"{name}filename"
        label = name.upper()
        # only append if not already present
        if not any((base == t[0] and fname == t[1]) for t in attachments_spec):
            attachments_spec.append((base, fname, label))

    return excel_to_app, attachments_spec


def _build_stage_mappings():
    """Build the authoritative Excel->staging column mapping for the COPY pipeline.

    This is the SINGLE SOURCE OF TRUTH for what lands in stage_ca_applications_raw.

    Rules:
      - All keys must be lowercase (no spaces). The staging importer normalizes
        Excel headers to lowercase+strip before matching, so these will always match.
      - region/district/ward values are numeric IDs in Excel; the staging importer
        maps them to names via CSV dicts AFTER copying them into stage columns.
      - Fire fields: fregion/fdistrict/fward go into fire_region/fire_district/fire_ward
        (NOT region/district/ward — those are application-level columns).
      - insurance/tira fields go into cover_note_* and insurance_ref_no columns.
      - license_category_id and application_legal_status_id are stored as raw text
        so the SQL transform can resolve them by name later.
    """
    excel_to_stage = {
        # ── ADDRESS / LOCATION (application-level) ────────────────────────────
        'address_code':                    'address_code',
        'address_no':                      'address_no',
        'block_no':                        'block_no',
        'plot_no':                         'plot_no',
        'road':                            'road',
        'street':                          'street',
        'region':                          'region',       # ID->name mapped via regions.csv
        'district':                        'district',     # ID->name mapped via districts.csv
        'ward':                            'ward',         # ID->name mapped via wards.csv
        # ── CONTACT / BUSINESS ────────────────────────────────────────────────
        'mobile_no':                       'mobile_no',
        'email':                           'email',
        'website':                         'website',
        'latitude':                        'latitude',
        'longitude':                       'longitude',
        'po_box':                          'po_box',
        'facility_name':                   'facility_name',
        'company_name':                    'company_name',
        'tin':                             'tin',
        'tin_name':                        'tin_name',
        'brela_number':                    'brela_number',
        'brela_registration_type':         'brela_registration_type',
        'certificate_of_incorporation_no': 'certificate_of_incorporation_no',
        # ── APPLICATION META ─────────────────────────────────────────────────
        'application_number':              'application_number',
        'appno':                           'application_number',
        'app_number':                      'application_number',
        'apprefno':                        'application_number',
        'application_type':                'application_type',
        'approval_no':                     'approval_no',
        'approvalno':                      'approval_no',
        'approval_number':                 'approval_no',
        'licenceno':                       'approval_no',
        'license_no':                      'approval_no',
        'effective_date':                  'effective_date',
        'effectivedate':                   'effective_date',
        'date_effective':                  'effective_date',
        'start_date':                      'effective_date',
        'startdate':                       'effective_date',
        'expire_date':                     'expire_date',
        'expiry_date':                     'expire_date',
        'expiredate':                      'expire_date',
        'expired_date':                    'expire_date',
        'expirydate':                      'expire_date',
        'expdate':                         'expire_date',
        'date_expire':                     'expire_date',
        'date_expiry':                     'expire_date',
        'end_date':                        'expire_date',
        'enddate':                         'expire_date',
        'completed_at':                    'completed_at',
        'license_type':                    'license_type',
        'license_category_id':             'license_category_raw',        # raw text -> resolved in transform
        'application_legal_status_id':     'application_legal_status_raw', # raw text -> resolved in transform
        'userid':                          'username',
        'created_by':                      'old_created_by',
        'parent_application_id':           'old_parent_application_id',
        # ── FIRE CERTIFICATE (fcontrolno etc -> fire_* staging columns) ───────
        'fcontrolno':                      'fire_certificate_control_number',
        'fpremisename':                    'fire_premise_name',
        'fregion':                         'fire_region',           # NOT mapped via CSV (saved as-is)
        'fdistrict':                       'fire_district',         # NOT mapped via CSV (saved as-is)
        'fadministrativearea':             'fire_administrative_area',
        'fward':                           'fire_ward',             # NOT mapped via CSV (saved as-is)
        'fstreet':                         'fire_street',
        'fvalidfrom':                      'fire_valid_from',
        'fvalidto':                        'fire_valid_to',
        # ── INSURANCE / TIRA ─────────────────────────────────────────────────
        'covernoterefno':                  'cover_note_ref_no',
        'tiracovernotenumber':             'cover_note_number',
        'tiracovernotereferencenumber':    'insurance_ref_no',
        'tirapolicyholdername':            'policy_holder_name',
        'tirainsurercompanyname':          'insurer_company_name',
        'tiracovernotestartdate':          'cover_note_start_date',
        'tiracovernotenddate':             'cover_note_end_date',
        'tirariskname':                    'risk_name',
        'tirasubjectmatterdesc':           'subject_matter_desc',
    }

    # Attachments spec is the same as the default mappings.
    _, attachments_spec = _build_default_mappings()
    return excel_to_stage, attachments_spec


def import_applications_via_staging_copy(
    db: Any,
    df,
    *,
    chunk_rows: int = 50000,
    truncate_first: bool = True,
    progress_cb=None,
    sector_name: str = "PETROLEUM"
):
    """High-volume import (recommended for 500k+ remote DB): staging + COPY + SQL transform."""
    # Local import to avoid adding import-time dependencies for users that don't use this path.
    from scripts.stage_and_copy_import import stage_and_copy_import

    excel_to_stage, attachments_spec = _build_stage_mappings()
    return stage_and_copy_import(
        db,
        df,
        attachments_spec=attachments_spec,
        excel_to_stage=excel_to_stage,
        progress_cb=progress_cb,
        chunk_rows=chunk_rows,
        truncate_first=truncate_first,
        sector_name=sector_name
    )


def import_applications_from_df(db: Any, df, preserve_source_id: bool = False, batch_size: int = 1000):
    """Import application rows and their attachments into applications and
    documents using the provided SQLAlchemy Session-like `db`.

    This mirrors the previous attachments migration logic but lives under the
    `application_migrations` name as requested.
    """
    logger.info("Starting import_applications_from_df with %d rows, batch_size=%d", len(df), batch_size)
    errors: List[str] = []
    inserted_apps = 0
    inserted_docs = 0
    skipped_docs_total = 0
    failed_app_rows = 0
    inserted_users = 0
    skipped_users = 0
    inserted_user_roles = 0
    skipped_user_roles = 0

    # Build explicit mappings and attachment spec per requirements
    excel_to_app, attachments_spec = _build_default_mappings()
    logger.info("Built mappings: %d excel_to_app, %d attachment specs", len(excel_to_app), len(attachments_spec))

    # Region/District/Ward values may arrive in Excel as bigint-like ids.
    # The DB columns expect the *names*, so we map ids -> names using CSV lookups.
    # (Single source of truth: app/data/{regions,districts,wards}.csv)
    region_map = region_map_csv
    district_map = district_map_csv

    # Build normalized region/district maps to ensure lookups use string-of-number keys
    def _build_normalized_map(source: Dict[str, str]) -> Dict[str, str]:
        nm: Dict[str, str] = {}
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

    norm_region_map = _build_normalized_map(region_map)
    norm_district_map = _build_normalized_map(district_map)

    # Application legal status mapping: map human-readable names to UUID ids
    application_legal_status_map = {
        'co-operative society': '1047cb8f-0919-410f-888c-7688bf7f0711',
        'government agency': '3012d756-9582-42ef-bff0-04f5a79de2f2',
        'joint venture': '66e097e7-c712-4b53-bdda-821a4197cea8',
        'others': '7153f4dd-d7e4-4b1b-bae9-e16187dcb264',
        'parastatal organization': '66149389-3eb1-4146-938f-d704c7ea89fb',
        'partnership': 'f84abd71-9d8b-4f38-8ed6-1a212e0f7b83',
        'private limited liability company': '79fa4906-7861-4413-bb18-253a508f0981',
        'public limited liability company': '7233cdfe-4cc8-4324-a8b0-90e896961f24',
        'sole proprietor': '650d5f5b-75a2-46a1-b52a-02c3fd6eec84',
        'sole proprietor in a column': '650d5f5b-75a2-46a1-b52a-02c3fd6eec84',
    }

    # License category mapping: map license category names to UUID ids
    license_category_map = {
        'petroleum station': '515addf4-d984-4de2-bf39-1e3cc900bde8',
        'village petroleum station': '6979a7b2-f4fd-4f83-b960-8bee565b616f',
        'pipeline transportation (above 100 km)': '3e1c85ef-e1cf-42ea-9d7a-5bef66ffa264',
        'petroleum petcoke construction approval': 'ba56d45a-1794-4d85-b5d6-8b4f209b044a',
        'petroleum bitumen manufacturing plant construction approval': '25414332-6af1-409f-ab54-13f5a00d1f82',
        'petroleum waste oil recycling plant construction approval': '09ab7f80-b6d3-48f4-a2ba-9d2e07cd5a93',
        'petroleum marine loading and offloading facility construction approval': '8f92caed-5a26-4674-bda5-07f3e1a4669c',
        'petroleum refinery plant construction approval': 'ca38cca2-5124-4b8c-bae7-11cd8adff416',
        'lpg wholesalers business': '0f840ba7-7026-4153-8478-5361bef40116',
        'petroleum bitumen wholesale licence': 'b56149bd-3311-4e90-914c-8be9edd1a872',
        'condensate dealership licence': '698742db-7e61-47b6-b5b1-c3a9862e6149',
        'natural gas compression licence': '247fbcbb-0fd0-4191-a4d6-7140c6f145d7',
        'petroleum consumer installation (mining)': '55491e17-b2e8-4808-84d4-c562f3ee018e',
        'lpg distribution (super dealer)': 'a7196b7a-54f5-466b-a7ed-99115ea23c90',
        'bunkering licence': 'bfbab183-c707-4096-86b8-9a29b2c54170',
        'compressed natural gas supply licence': '09f147ac-d2e3-4f45-aa4a-30ea968b721a',
        'pipeline transportation (up to 100 km)': '29471cf6-eaf0-4332-a8bd-d36a6fd06f12',
        'consumer  installation  (transporter)': '0358e5c2-21e8-423a-a59c-39ad00664541',
        'consumer installation (transporter)': '0358e5c2-21e8-423a-a59c-39ad00664541',
        'petroleum retail': '141d4c39-9074-4b03-aab4-123544f2d6c2',
        'petroleum storage business': 'b57047bd-40c8-4bf4-bd6e-7087be1b1052',
        'petroleum wholesales': '7e96b73f-0939-40d0-be55-3b3b856e6e95',
        'township and village petroleum retail': '7397f78a-db27-4c44-a7b2-273450ce7969',
        'petroleum petcoke wholesale licence': '97e01662-8460-4562-8dae-db13e8cc1ad2',
        'waste recycling operations licence': '48a36120-0ae4-4ed2-b1e0-98992294b19d',
        'petroleum marine loading and offloading operations licence': '73718924-f79f-4484-a0d6-36bc9671a5de',
        'petroleum independent marine surveyor registration': 'b2257345-eebc-4aa6-85d6-c9467ef17143',
        'petroleum refinery operation licence': '7a1c13aa-179b-4e90-810a-114fdecd0557',
        'pipeline transportation licence up to 100 km': '91c77552-1358-4df1-8060-c53821ae3c74',
        'pipeline transportation licence above to 100 km': 'fd70a40c-faf7-4cd5-a480-b0ac78d84b8c',
        'petroleum consumer installation (transporter)': '4d13db31-5f63-4e43-b4ce-e10a9fb325cb',
        'bulk water supply': 'b990c603-bcf3-4faa-8081-4c876253104d',
        'leasing assets': '17c8ac8f-56ee-47d1-be43-e091205e2920',
        'operatorship': '24019a94-7ca2-4e87-99bd-7d90e12e54b5',
        'sanitation services': 'a54c1852-df5c-4d25-9f34-fec6be6d96d9',
        'water supply': '2ad69837-1352-4089-944c-85bd9b1d0298',
        'water supply and sanitation services': '18d8eef0-9bd0-42c0-b45e-9179d0e90ba5',
        'natural gas processing licence': 'c572ecb4-8691-4556-b492-22120ef06011',
        'natural gas transmission licence': '20f72572-7ec8-4385-a906-dc0f606c212e',
        'natural gas storage licence': 'afbf0275-7b4e-454d-b9cb-2cd9af122fac',
        'natural gas distribution licence': '8c191527-6b6d-4745-a7c0-db8bc674ba9d',
        'condensate storage construction approval': 'd6b95f40-99f6-44e7-a687-64f947e0267f',
        'lubricant blending plant': 'af3c1cdd-45e7-4d4a-b217-63e3ac537697',
        'lpg storage and filling plant': '0c428af8-4952-43b5-ae16-c62172d51923',
        'consumer  installation (agriculture)': '8a50348f-7ad3-4a4e-a75a-b87192b10556',
        'consumer installation (agriculture)': '8a50348f-7ad3-4a4e-a75a-b87192b10556',
        'natural gas re – gasification construction approval': '55b16e2e-ce0a-4eb9-9f65-19fc929702ce',
        'natural gas re-gasification construction approval': '55b16e2e-ce0a-4eb9-9f65-19fc929702ce',
        'compressed natural gas (own use) construction approval': '06f3c79c-e625-4cfd-8c2e-ab200271d09e',
        'compressed natural gas filling station construction approval': '57c7779c-a7cb-473f-8b6a-9595a013f2e0',
        'natural gas compression construction approval': '39e38ad3-973f-4a39-8b90-7f18e48504f3',
        'natural gas distribution – construction approval (up to 10 km)': 'a0849bc9-f907-4df2-8b4b-4cf0f9102e81',
        'natural gas distribution - construction approval (up to 10 km)': 'a0849bc9-f907-4df2-8b4b-4cf0f9102e81',
        'natural gas transmission construction approval (above 100 km)': '2cfd589a-f1c0-41db-a1dc-86e3d98be29d',
        'natural gas distribution – construction approval (above 10 km)': '7e3c08e6-6cc0-45c0-880f-bce39f3f98d5',
        'natural gas distribution - construction approval (above 10 km)': '7e3c08e6-6cc0-45c0-880f-bce39f3f98d5',
        'natural gas processing construction approval': '18c27158-28e5-4337-9d78-4d591099eb9f',
        'natural gas aggregation licence': '4d91d10a-b57f-49cc-bd02-d316ae55d538',
        'compressed natural gas (own use) licence': '93acb1b5-a186-4594-8398-021ebdbec10d',
        'compressed natural gas filling station licence': '1b5eb856-c3f0-49e1-b770-4d909d14c8e8',
        'natural gas re-gasification licence': 'd039e1b5-b06a-4fe2-b466-4a44f81c0f30',
        'lng transportation (local) licence': '6dad8f8a-a63d-4e31-a713-1596e1f01944',
        'natural gas supply and marketing licence': '87a57b7c-05a6-4d1d-8fcc-2842193f29bd',
        'consumer  installation  (mining)': '1313b25f-153e-4cb4-94dc-ac073f40694e',
        'consumer installation (mining)': '1313b25f-153e-4cb4-94dc-ac073f40694e',
        'cross border trade in electricity': '2eea9d2f-5060-40d0-8c02-2bfef0459955',
        'distribution': 'bdbe1c0f-d80c-46cf-a24c-c4c2d08fa1eb',
        'generation': 'd0afb437-7bdf-4d49-b0c6-6c2341455470',
        'independent system operator': 'e3d78c31-0272-4ac7-ab59-08a64ec3cf3c',
        'physical and financial trade in electricity': 'c9b6c1b4-2643-453b-a7ab-53b8a4fade79',
        'supply': 'fd5aa885-89e6-49a1-91c0-6b8aee91b173',
        'transmission': '1232d724-0fb7-4b4a-8c99-0d91921ecb2d',
        'lubricant distribution business': '3e61eb70-1292-4bc7-bbfe-42c466eff47a',
        'lubricant wholesales business': 'fed36103-f180-4708-aaa5-6ea56434dd92',
        'export abroad': '624b062d-9bd3-42e4-b6b5-5c5a943ac650',
        'natural gas transmission construction approval (up to 100 km)': 'c07308c5-c245-4c97-80bd-45fd5535d999',
        'consumer installation': '275f64c7-6e02-4f33-97f1-092ae49b33bf',
    }

    # columns in the incoming dataframe
    df_columns = [str(c).strip() for c in df.columns]

    # Use the explicit attachments_spec but only keep pairs that exist in df
    attachment_pairs = []
    for id_col, fname_col, label in attachments_spec:
        if (id_col is None or id_col in df_columns) or (fname_col is not None and fname_col in df_columns):
            attachment_pairs.append((id_col, fname_col, label))

    # Determine which application columns we can copy into applications
    # by intersecting excel_to_app keys with the destination table columns.
    ca_cols = [r[0] for r in db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='applications' ")).fetchall()]
    # Build list of target app columns we will set (exclude id and created_at since we add created_at=now() manually)
    copy_cols = []
    for excel_col, app_col in excel_to_app.items():
        if app_col in ca_cols and app_col not in ('id', 'created_at'):
            copy_cols.append((excel_col, app_col))

    # Defensive: ensure copy_cols contains only 2-tuples. If any malformed
    # entry shows up (which caused a "not enough values to unpack" in runtime),
    # coerce/skip and log the problem so operator can inspect.
    safe_copy_cols: List[Tuple[str, str]] = []
    for item in copy_cols:
        try:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                safe_copy_cols.append((str(item[0]), str(item[1])))
            else:
                logger.warning("Skipping malformed copy_cols entry: %r", item)
        except Exception:
            logger.exception("Error while normalizing copy_cols entry: %r", item)
    copy_cols = safe_copy_cols

    # Mapping statistics and unmapped samples for operator feedback
    mapping_stats = {
        'region_mapped': 0,
        'region_unmapped': 0,
        'district_mapped': 0,
        'district_unmapped': 0,
        'ward_mapped': 0,
        'ward_unmapped': 0,
        'legal_status_mapped': 0,
        'legal_status_unmapped': 0,
        'license_category_mapped': 0,
        'license_category_unmapped': 0,
        'region_unmapped_samples': set(),
        'district_unmapped_samples': set(),
        'ward_unmapped_samples': set(),
        'legal_status_unmapped_samples': set(),
        'license_category_unmapped_samples': set(),
    }

    total_rows = len(df)
    idx = 0
    while idx < total_rows:
        batch_start = idx  # Track start index for error reporting
        batch = df.iloc[idx: idx + batch_size]
        idx += batch_size
        # Provide a reusable no-op context manager when an outer transaction
        # is already active on the Session.
        class _NoopCM:
            def __enter__(self):
                return None
            def __exit__(self, exc_type, exc, tb):
                return False

        try:
            app_inserts: List[Dict[str, Any]] = []
            doc_inserts: List[Dict[str, Any]] = []

            # Collect usernames (Excel userid) for this batch so we can ensure
            # corresponding users exist and have the APPLICANT ROLE.
            batch_usernames: set[str] = set()

            for _, row in batch.iterrows():
                    # If the user wants DB to assign IDs, don't provide 'id' here.
                    # We keep a temporary row index to map returned DB ids back to
                    # document rows after the INSERT ... RETURNING id step.
                    tmp_idx = len(app_inserts)
                    app_row = {}
                    if preserve_source_id and 'id' in row and row['id']:
                        try:
                            app_row['id'] = str(row['id'])
                        except Exception:
                            app_row['id'] = str(uuid.uuid4())
                    # else: leave 'id' out so DB default applies
                    # mark temporary index for later mapping
                    app_row['_row_idx'] = tmp_idx
                    for excel_col, app_col in copy_cols:
                        # read source value (if column exists in the DataFrame)
                        val = row.get(excel_col) if excel_col in row else None
                        
                        # Handle NaN/Nat string values globally (convert to None)
                        if isinstance(val, str) and val.strip().lower() in ('nan', 'nat', 'none', 'null'):
                             val = None
                        elif val is not None and str(val).lower() == 'nan': # Handle pandas nan float/object
                             val = None

                        # conversion: normalize application_type to uppercase (NEW, RENEW, EXTEND)
                        # NOTE: keep username/userid exactly as provided (no uppercasing).
                        if excel_col == 'application_type' and isinstance(val, str):
                            val = val.strip().upper()

                        # Capture userid/username values (for users table creation)
                        if app_col == 'username' and val not in (None, ''):
                            try:
                                s_un = str(val).strip().lower()
                                if s_un and s_un not in ('nan', 'nat', 'none', 'null'):
                                    batch_usernames.add(s_un)
                                    val = s_un  # also store lowercased in app_row
                            except Exception:
                                pass
                        # If region column contains an id, map it to the name.
                        # IMPORTANT: only map for the application-level "region" column,
                        # not fire-prefixed delimitations (fregion/fdistrict/fward).
                        if excel_col not in ('fregion', 'fdistrict', 'fward') and app_col == 'region' and val not in (None, ''):
                            sval = str(val).strip()
                            nk = _normalize_numeric_string(sval)
                            if sval in norm_region_map:
                                val = norm_region_map[sval]
                                mapping_stats['region_mapped'] += 1
                            elif nk in norm_region_map:
                                val = norm_region_map[nk]
                                mapping_stats['region_mapped'] += 1
                            else:
                                mapping_stats['region_unmapped'] += 1
                                if len(mapping_stats['region_unmapped_samples']) < 20:
                                    mapping_stats['region_unmapped_samples'].add(sval)
                        # Map district ids to names if necessary (application-level only)
                        if excel_col not in ('fregion', 'fdistrict', 'fward') and app_col == 'district' and val not in (None, ''):
                            sval = str(val).strip()
                            nk = _normalize_numeric_string(sval)
                            if sval in norm_district_map:
                                val = norm_district_map[sval]
                                mapping_stats['district_mapped'] += 1
                            elif nk in norm_district_map:
                                val = norm_district_map[nk]
                                mapping_stats['district_mapped'] += 1
                            else:
                                mapping_stats['district_unmapped'] += 1
                                if len(mapping_stats['district_unmapped_samples']) < 20:
                                    mapping_stats['district_unmapped_samples'].add(sval)
                        # Map ward ids to names if a ward_map is provided (application-level only)
                        if excel_col not in ('fregion', 'fdistrict', 'fward') and app_col == 'ward' and val not in (None, ''):
                            sval = str(val).strip()
                            nk = _normalize_numeric_string(sval)
                            if sval in ward_map:
                                val = ward_map[sval]
                                mapping_stats['ward_mapped'] += 1
                            elif nk in ward_map:
                                val = ward_map[nk]
                                mapping_stats['ward_mapped'] += 1
                            else:
                                mapping_stats['ward_unmapped'] += 1
                                if len(mapping_stats['ward_unmapped_samples']) < 20:
                                    mapping_stats['ward_unmapped_samples'].add(sval)
                        # Map application legal status text to UUID id
                        if app_col == 'application_legal_status_id' and val not in (None, ''):
                            sval = str(val).strip()
                            # if the value already is a valid UUID, keep it
                            try:
                                uuid.UUID(sval)
                                # valid UUID -> keep
                            except Exception:
                                # normalize: lower-case, remove punctuation, and
                                # convert CamelCase/PascalCase to space-separated words
                                key = sval.lower()
                                if key in application_legal_status_map:
                                    val = application_legal_status_map[key]
                                    mapping_stats['legal_status_mapped'] += 1
                                else:
                                    # convert camel/pascal -> spaced (e.g. PrivateLimited -> Private Limited)
                                    spaced = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', sval)
                                    spaced = re.sub(r'[_\-]+', ' ', spaced)
                                    spaced = ' '.join(spaced.split())
                                    key2 = spaced.lower().replace('.', '').replace(',', '')
                                    if key2 in application_legal_status_map:
                                        val = application_legal_status_map[key2]
                                        mapping_stats['legal_status_mapped'] += 1
                                    else:
                                        # Value not in map - set to NULL to avoid UUID type error
                                        val = None
                                        mapping_stats['legal_status_unmapped'] += 1
                                        if len(mapping_stats['legal_status_unmapped_samples']) < 20:
                                            mapping_stats['legal_status_unmapped_samples'].add(sval)
                        # Map license_category_id text (category name) to UUID
                        if app_col == 'license_category_id' and val not in (None, ''):
                            sval = str(val).strip()
                            # if the value already is a valid UUID, keep it
                            try:
                                uuid.UUID(sval)
                                # valid UUID -> keep
                            except Exception:
                                # normalize: lower-case
                                key = sval.lower()
                                if key in license_category_map:
                                    val = license_category_map[key]
                                    mapping_stats['license_category_mapped'] += 1
                                else:
                                    # try alternate normalization: remove punctuation, multiple spaces
                                    key2 = re.sub(r'[_\-]+', ' ', sval)
                                    key2 = ' '.join(key2.split()).lower()
                                    if key2 in license_category_map:
                                        val = license_category_map[key2]
                                        mapping_stats['license_category_mapped'] += 1
                                    else:
                                        # Value not in map - set to NULL to avoid UUID type error
                                        val = None
                                        mapping_stats['license_category_unmapped'] += 1
                                        if len(mapping_stats['license_category_unmapped_samples']) < 20:
                                            mapping_stats['license_category_unmapped_samples'].add(sval)
                        
                        # Fix Excel serial dates for date columns
                        if app_col in ('effective_date', 'expire_date', 'completed_at') and val not in (None, ''):
                            val = _convert_excel_date(val)

                        app_row[app_col] = val

                    # ── Migration defaults ──────────────────────────────────
                    # category_license_type is always OPERATIONAL for imported applications.
                    if 'category_license_type' in ca_cols:
                        app_row['category_license_type'] = 'OPERATIONAL'
                    # All migrated records are flagged as originating from LOIS.
                    if 'is_from_lois' in ca_cols:
                        app_row['is_from_lois'] = True

                    app_inserts.append(app_row)

                    order = 1
                    for id_col, filename_col, label in attachment_pairs:
                        logic_val = None
                        if id_col and id_col in row:
                            logic_val = row.get(id_col)
                        
                        # Process filename first - strict requirement: must exist and not be empty
                        fname_val = None
                        raw_fname = row.get(filename_col) if (filename_col and filename_col in row) else None
                        if raw_fname is not None:
                            s_fname = str(raw_fname).strip()
                            # Check against common empty/Excel-null indicators
                            if s_fname and s_fname.lower() not in ('nan', 'nat', 'none', 'null', ''):
                                fname_val = s_fname
                        
                        # If filename is invalid/empty, user request: "no need to add rows of empty attachments"
                        if not fname_val:
                            continue

                        # Process logic_doc_id - strict requirement: must exist and be valid integer
                        valid_logic_id = None
                        if logic_val not in (None, ''):
                            s_logic = str(logic_val).strip()
                            if s_logic.lower() in ('nan', 'nat', 'none', 'null', ''):
                                continue # Invalid ID means skip attachment
                            try:
                                # Parse float string "123.0" -> 123
                                valid_logic_id = int(float(s_logic))
                            except Exception:
                                continue # Parse failure (e.g. malformed string) means skip attachment
                        else:
                            continue # Missing logic_id means skip attachment

                        doc = {
                            'id': str(uuid.uuid4()),
                            'document_name': label,
                            'document_url': None,
                            # reference the application by temporary row index for now
                            'application_row_idx': tmp_idx,
                            'file_name': fname_val,
                            'documents_order': order,
                            'logic_doc_id': valid_logic_id,
                        }
                        
                        order += 1
                        doc_inserts.append(doc)

            # ── Ensure users exist and have APPLICANT ROLE ─────────────────
            # For every userid collected from Excel in this batch, create a
            # users row if it doesn't exist yet, then ensure the role
            # "APPLICANT ROLE" exists and is assigned to each user.
            if batch_usernames:
                # Ensure pgcrypto is available for gen_random_uuid()
                try:
                    db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
                    db.commit()
                except Exception:
                    pass

                # Insert each user individually so one bad row never blocks others.
                for _uname in batch_usernames:
                    try:
                        _ur = db.execute(text("""
                            INSERT INTO public.users (
                                id, full_name, username, password_hash, status,
                                phone_number, email_address, user_category,
                                account_type, auth_mode, failed_attempts,
                                is_first_login, deleted, created_at, updated_at
                            )
                            SELECT
                                gen_random_uuid(), :uname, :uname, '',
                                'ACTIVE', NULL, NULL, 'EXTERNAL', 'INDIVIDUAL', 'DB',
                                0, false, false, now(), now()
                            WHERE NOT EXISTS (
                                SELECT 1 FROM public.users eu
                                WHERE lower(trim(eu.username)) = :uname
                            )
                        """), {"uname": _uname.lower().strip()})
                        db.commit()
                        if (_ur.rowcount or 0) > 0:
                            inserted_users += 1
                        else:
                            skipped_users += 1
                    except Exception as _ue:
                        logger.error("Failed to insert user '%s': %s", _uname, _ue)
                        skipped_users += 1
                        try:
                            db.rollback()
                        except Exception:
                            pass

                # Look up APPLICANT ROLE id — read from an existing user_roles row.
                # Never scan public.roles: it is a FDW foreign table whose remote name
                # differs (public.role), causing every query to fail.
                # If no user_roles row exists yet, skip role assignment gracefully.
                _applicant_role_id = None
                try:
                    _ur_row = db.execute(text(
                        "SELECT role_id FROM public.user_roles LIMIT 1"
                    )).fetchone()
                    if _ur_row:
                        _applicant_role_id = str(_ur_row[0])
                except Exception:
                    try:
                        db.rollback()
                    except Exception:
                        pass

                if not _applicant_role_id:
                    # public.roles FDW always fails (remote table is named public.role).
                    # Cannot safely scan roles — skip role assignment silently.
                    logger.info("APPLICANT ROLE id not found from user_roles; role assignment skipped (FDW limitation)")
                    skipped_user_roles += len(batch_usernames)

                # Assign APPLICANT ROLE to each user individually.
                # Remote user_roles FDW table only exposes (user_id, role_id).
                if _applicant_role_id:
                    for _uname in batch_usernames:
                        try:
                            _rr = db.execute(text("""
                                INSERT INTO public.user_roles (user_id, role_id)
                                SELECT u.id, :role_id
                                FROM public.users u
                                WHERE lower(trim(u.username)) = :uname
                                AND NOT EXISTS (
                                    SELECT 1 FROM public.user_roles ex
                                    WHERE ex.user_id = u.id AND ex.role_id = :role_id
                                )
                            """), {"uname": _uname.lower().strip(), "role_id": _applicant_role_id})
                            db.commit()
                            if (_rr.rowcount or 0) > 0:
                                inserted_user_roles += 1
                            else:
                                skipped_user_roles += 1
                        except Exception as _ure:
                            logger.warning("Failed to assign role for user '%s': %s", _uname, _ure)
                            skipped_user_roles += 1
                            try:
                                db.rollback()
                            except Exception:
                                pass

        except Exception as _batch_exc:
            # Log, rollback session to leave it in a clean state, and re-raise
            logger.exception("Batch import failed: %s", _batch_exc)
            try:
                db.rollback()
            except Exception:
                pass
            raise

        inserted_app_ids = []
        if app_inserts:
                # determine real columns to insert (exclude temporary keys)
                sample = app_inserts[0]
                col_names = [c for c in sample.keys() if not c.startswith('_')]

                # ── Deduplicate within this batch on application_number ──────
                # If the same application_number appears multiple times in the
                # Excel file keep only the first occurrence.
                _seen_app_nums: set = set()
                deduped_inserts = []
                for _a in app_inserts:
                    _anum = str(_a.get('application_number') or '').strip()
                    if _anum and _anum in _seen_app_nums:
                        logger.debug("Skipping duplicate application_number in batch: %s", _anum)
                        continue
                    if _anum:
                        _seen_app_nums.add(_anum)
                    deduped_inserts.append(_a)
                app_inserts = deduped_inserts

                # ── Skip rows already in the DB by application_number ─────────
                # Collect existing application_numbers from the DB for this batch.
                batch_app_nums = [str(a.get('application_number') or '').strip()
                                  for a in app_inserts
                                  if a.get('application_number')]
                existing_app_nums: set = set()
                if batch_app_nums:
                    try:
                        chunk_size = 500
                        for _i in range(0, len(batch_app_nums), chunk_size):
                            _chunk = batch_app_nums[_i:_i + chunk_size]
                            _placeholders = ','.join([f':p{j}' for j in range(len(_chunk))])
                            _params = {f'p{j}': v for j, v in enumerate(_chunk)}
                            _rows = db.execute(
                                text(f"SELECT application_number FROM public.applications WHERE application_number IN ({_placeholders})"),
                                _params
                            ).fetchall()
                            existing_app_nums.update(r[0] for r in _rows if r[0])
                    except Exception as _lookup_err:
                        logger.warning("Could not pre-check existing application_numbers: %s", _lookup_err)

                new_inserts = []
                update_rows = []
                for _a in app_inserts:
                    _anum = str(_a.get('application_number') or '').strip()
                    if _anum and _anum in existing_app_nums:
                        update_rows.append(_a)
                    else:
                        new_inserts.append(_a)

                # ── UPDATE existing applications (non-null columns only) ───────
                if update_rows and col_names:
                    _update_cols = [c for c in col_names
                                    if c not in ('id', 'application_number', 'created_at')]
                    if _update_cols:
                        _set_sql = ', '.join([f"{c} = COALESCE(:{c}, {c})" for c in _update_cols])
                        _upd_sql = text(
                            f"UPDATE public.applications SET {_set_sql}, updated_at = now() "
                            f"WHERE application_number = :application_number "
                            f"RETURNING id"
                        )
                        for _upd in update_rows:
                            try:
                                _res = db.execute(_upd_sql, _upd)
                                _row = _res.fetchone()
                                if _row:
                                    inserted_app_ids.append(str(_row[0]))
                                else:
                                    # fallback: look up existing id
                                    _eid = db.execute(
                                        text("SELECT id FROM public.applications WHERE application_number = :n"),
                                        {"n": str(_upd.get('application_number') or '').strip()}
                                    ).scalar()
                                    inserted_app_ids.append(str(_eid) if _eid else None)
                            except Exception as _ue:
                                logger.warning("Update skipped for %s: %s",
                                               _upd.get('application_number'), _ue)
                                inserted_app_ids.append(None)

                # replace app_inserts with only the genuinely new rows
                app_inserts = new_inserts

                cols_sql = ','.join(col_names)
                vals_sql = ','.join([f":{c}" for c in col_names])
                # Build ON CONFLICT DO UPDATE — fill any column that is currently
                # NULL in the DB with the incoming value (COALESCE keeps existing
                # non-null values, so reruns are safe and additive).
                _conflict_update_cols = [
                    c for c in col_names
                    if c not in ('id', 'application_number', 'created_at')
                ]
                _set_conflict_sql = ', '.join(
                    [f"{c} = COALESCE(EXCLUDED.{c}, public.applications.{c})"
                     for c in _conflict_update_cols]
                ) + ", updated_at = now()"
                # ask DB to RETURNING id so we can map document rows to real ids
                insert_sql = text(
                    f"INSERT INTO public.applications ({cols_sql}) VALUES ({vals_sql}) "
                    f"ON CONFLICT (application_number) DO UPDATE SET {_set_conflict_sql} "
                    f"RETURNING id"
                )
                logger.debug("Insert columns: %s", col_names)

                # Attempt fastest path: use PostgreSQL COPY FROM STDIN when
                # available. This requires that we provide 'id' for every
                # application row (we generate UUIDs when missing). We build
                # CSVs in-memory and call copy_expert on the raw connection.
                copy_ok = False
                try:
                    # prepare all app rows with ids
                    for a in app_inserts:
                        if 'id' not in a or not a.get('id'):
                            a['id'] = str(uuid.uuid4())

                    # try COPY path only for psycopg2 (postgres)
                    raw_conn = None
                    try:
                        # SQLAlchemy Session -> Connection -> DBAPI connection
                        raw_conn = db.connection().connection
                    except Exception:
                        raw_conn = None

                    if raw_conn is not None and hasattr(raw_conn, 'cursor'):
                        try:
                            cur = raw_conn.cursor()
                            # Build CSV for applications
                            sio = io.StringIO()
                            now_str = datetime.utcnow().isoformat()
                            cols = [c for c in sample.keys() if not c.startswith('_')]
                            # include created_at as last column
                            for params in app_inserts:
                                row_vals = []
                                for c in cols:
                                    v = params.get(c)
                                    if v is None:
                                        row_vals.append('')
                                    else:
                                        # Escape double quotes by doubling them
                                        sval = str(v).replace('"', '""')
                                        row_vals.append(sval)
                                # append created_at - SKIP
                                # row_vals.append(now_str)
                                sio.write(','.join(f'"{x}"' for x in row_vals) + '\n')
                            sio.seek(0)
                            copy_cols_sql = ','.join(cols)
                            # copy_cols_sql = ','.join(cols) + ', created_at'
                            copy_sql = f"COPY public.applications ({copy_cols_sql}) FROM STDIN WITH CSV"
                            cur.copy_expert(copy_sql, sio)
                            raw_conn.commit()
                            # On success, collect the ids we assigned
                            inserted_app_ids = [str(a['id']) for a in app_inserts]
                            inserted_apps += len(app_inserts)
                            copy_ok = True
                        except Exception as e:
                            logger.debug("COPY path failed, falling back: %s", e)
                            try:
                                raw_conn.rollback()
                            except Exception:
                                pass
                            copy_ok = False
                    else:
                        copy_ok = False
                except Exception:
                    copy_ok = False

                if not copy_ok:
                    # Bulk insert fallback (existing fast path)
                    try:
                        result = db.execute(insert_sql, app_inserts)
                        try:
                            returned = result.fetchall()
                            if returned and len(returned) == len(app_inserts):
                                inserted_app_ids = [str(r[0]) for r in returned]
                                inserted_apps += len(app_inserts)
                            else:
                                # treat as failure to get complete ids and fall back
                                raise RuntimeError("bulk insert did not return complete ids")
                        except Exception:
                            raise
                    except Exception as bulk_err:
                        logger.debug("Bulk insert failed or incomplete, falling back to per-row inserts: %s", bulk_err)
                        try:
                            db.rollback()
                        except Exception:
                            pass
                        for row_idx, params in enumerate(app_inserts):
                            # Calculate the actual Excel row number (1-indexed, +1 for header)
                            excel_row = (batch_start + row_idx + 2)  # +2: 1 for 0-index, 1 for header
                            try:
                                if hasattr(db, 'begin_nested'):
                                    with db.begin_nested():
                                        res = db.execute(insert_sql, params)
                                        row = res.fetchone()
                                else:
                                    res = db.execute(insert_sql, params)
                                    row = res.fetchone()
                                if row and row[0] is not None:
                                    inserted_app_ids.append(str(row[0]))
                                else:
                                    inserted_app_ids.append(None)
                                    failed_app_rows += 1
                            except Exception as e:
                                # Abort if this is a severe connectivity error
                                if isinstance(e, OperationalError) or "Operation timed out" in str(e):
                                    logger.error("Aborting batch due to connectivity error at row %d: %s", excel_row, e)
                                    raise

                                # Extract detailed error info
                                err_msg = str(e)
                                err_detail = _extract_db_error_detail(e)
                                # Get identifying info from the row
                                row_id = params.get('id', 'N/A')
                                app_num = params.get('application_number', 'N/A')
                                legal_status = params.get('application_legal_status_id', 'N/A')
                                # If it's a UUID error, show the legal_status value
                                extra_info = ""
                                if 'uuid' in err_detail.lower() and legal_status != 'N/A':
                                    extra_info = f", legal_status_value={legal_status}"
                                # If it's a date error, try to identify the column
                                bad_value_match = re.search(
                                    r'invalid input syntax for type date: "([^"]+)"',
                                    err_msg,
                                    re.IGNORECASE,
                                )
                                if bad_value_match:
                                    bad_value = bad_value_match.group(1)
                                    bad_value_norm = _normalize_numeric_string(bad_value)
                                    matching_cols = []
                                    for k, v in params.items():
                                        if v is None:
                                            continue
                                        if str(v) == bad_value:
                                            matching_cols.append(k)
                                            continue
                                        v_norm = _normalize_numeric_string(str(v))
                                        if v_norm == bad_value_norm:
                                            matching_cols.append(k)
                                    if matching_cols:
                                        extra_info += f", date_column={matching_cols}"
                                errors.append(
                                    f"Row {excel_row} (app_number={app_num}, id={row_id}{extra_info}): {err_detail}"
                                )
                                logger.warning("Failed to insert row %d: %s", excel_row, err_detail)
                                inserted_app_ids.append(None)
                                failed_app_rows += 1
                        inserted_apps += len(app_inserts)

        # Safety: we must have the real application ids to correctly reference
        # documents. If the DB didn't return ids, only proceed to insert documents
        # if every app_inserts provided its own 'id' (preserve_source_id flow).
        if not inserted_app_ids:
            # check whether all app_inserts have an explicit 'id'
            all_have_ids = all(('id' in a and a['id']) for a in app_inserts)
            if not all_have_ids:
                # abort to avoid inserting documents with simulated ids
                raise RuntimeError(
                    "Database did not return inserted application IDs; cannot safely insert documents referencing them."
                )

        # If DB didn't return ids, collect ids from app_inserts
        app_ids_in_order: List[str] = []
        if inserted_app_ids:
            app_ids_in_order = inserted_app_ids
        else:
            for a in app_inserts:
                    if 'id' in a:
                        app_ids_in_order.append(str(a['id']))
                    else:
                        # For missing returned ids, simulate DB ids
                        app_ids_in_order.append(str(uuid.uuid4()))

            # update doc_inserts to replace application_row_idx with actual application_id
            docs_to_insert: List[Dict[str, Any]] = []
            skipped_doc_count = 0
            for doc in doc_inserts:
                if 'application_row_idx' in doc:
                    idx_ref = doc.pop('application_row_idx')
                    try:
                        appid = app_ids_in_order[idx_ref]
                    except Exception:
                        appid = None
                    # Skip documents whose application failed to insert
                    if appid is None:
                        skipped_doc_count += 1
                        # Don't add individual messages for each doc - summarize at end
                        continue
                    doc['application_id'] = appid
                    docs_to_insert.append(doc)
            
            # Track skipped docs across batches
            skipped_docs_total += skipped_doc_count

            # Insert documents into DB.
            if docs_to_insert:
                doc_cols = ['id', 'document_name', 'document_url', 'application_id', 'file_name', 'documents_order', 'logic_doc_id']
                cols_sql = ','.join(doc_cols) + ', created_at'
                vals_sql = ','.join([f':{c}' for c in doc_cols]) + ', now()'
                insert_docs_sql = text(
                    f"INSERT INTO public.documents ({cols_sql}) VALUES ({vals_sql}) "
                    f"ON CONFLICT (id) DO NOTHING"
                )
                
                # OPTIMIZATION: Try bulk insert first (much faster)
                # We chunk by 1000 to be safe with parameter binding limits
                chunk_size = 1000
                docs_chunked = [docs_to_insert[i:i + chunk_size] for i in range(0, len(docs_to_insert), chunk_size)]
                
                for chunk in docs_chunked:
                    try:
                        # Fast path: Bulk insert the chunk using SQLAlchemy's executemany support
                        db.execute(insert_docs_sql, chunk)
                        inserted_docs += len(chunk)
                    except Exception as bulk_err:
                        # Fallback path: If bulk fails (e.g. one constraint violation), try row-by-row
                        logger.warning("Bulk document insert failed for chunk (size %d), falling back to row-by-row: %s", len(chunk), bulk_err)
                        for d in chunk:
                            try:
                                if hasattr(db, 'begin_nested'):
                                    with db.begin_nested():
                                        db.execute(insert_docs_sql, d)
                                else:
                                    db.execute(insert_docs_sql, d)
                                inserted_docs += 1
                            except Exception as e:
                                errors.append(f"Failed to insert document {d.get('id')}: {e}")

        # ── Certificates insert ────────────────────────────────────────────
        # Every application must end up with a certificates row. We insert one
        # row per application that was loaded in this batch (deduplicated on
        # application_number). The conflict target is application_number so
        # reruns are idempotent — we UPDATE key fields on conflict.
        inserted_certs = 0
        try:
            # Guard: ON CONFLICT (application_number) requires a UNIQUE/EXCLUDE
            # constraint. Ensure it exists (and dedup first so creation succeeds).
            db.execute(text("""
                DO $$
                BEGIN
                    -- Deduplicate by application_number (keep newest row).
                    WITH ranked AS (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY application_number
                                ORDER BY COALESCE(updated_at, created_at) DESC, created_at DESC, id
                            ) AS rn
                        FROM public.certificates
                        WHERE application_number IS NOT NULL
                          AND NULLIF(TRIM(application_number), '') IS NOT NULL
                    )
                    DELETE FROM public.certificates c
                    USING ranked r
                    WHERE c.id = r.id
                      AND r.rn > 1;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM   pg_constraint con
                        JOIN   pg_class rel ON rel.oid = con.conrelid
                        JOIN   pg_namespace nsp ON nsp.oid = rel.relnamespace
                        WHERE  nsp.nspname = 'public'
                          AND  rel.relname = 'certificates'
                          AND  con.conname = 'uq_certificates_application_number'
                    ) THEN
                        ALTER TABLE public.certificates
                            ADD CONSTRAINT uq_certificates_application_number
                            UNIQUE (application_number);
                    END IF;
                END $$;
            """))
            db.commit()

            r_certs = db.execute(text("""
                WITH src AS (
                    SELECT DISTINCT ON (a.application_number)
                        a.id                  AS application_id,
                        a.application_number,
                        a.approval_no,
                        a.effective_date,
                        a.expire_date,
                        a.license_type,
                        a.category_license_type,
                        a.zone_id,
                        a.zone_name,
                        COALESCE(NULLIF(UPPER(TRIM(a.application_type)), ''), 'NEW') AS application_certificate_type
                    FROM public.applications a
                    WHERE a.is_from_lois = true
                      AND a.application_number IS NOT NULL
                    ORDER BY a.application_number, a.created_at DESC
                )
                INSERT INTO public.certificates (
                    id,
                    created_at,
                    updated_at,
                    application_id,
                    application_number,
                    approval_no,
                    effective_date,
                    expire_date,
                    license_type,
                    category_license_type,
                    zone_id,
                    zone_name,
                    application_certificate_type
                )
                SELECT
                    -- id is a surrogate PK; the unique conflict key is application_number.
                    gen_random_uuid(),
                    now(),
                    now(),
                    src.application_id,
                    src.application_number,
                    src.approval_no,
                    src.effective_date,
                    src.expire_date,
                    src.license_type,
                    src.category_license_type,
                    src.zone_id,
                    src.zone_name,
                    src.application_certificate_type
                FROM src
                ON CONFLICT (application_number) DO UPDATE
                SET
                    approval_no               = COALESCE(EXCLUDED.approval_no,               public.certificates.approval_no),
                    application_id            = EXCLUDED.application_id,
                    effective_date            = COALESCE(EXCLUDED.effective_date,            public.certificates.effective_date),
                    expire_date               = COALESCE(EXCLUDED.expire_date,               public.certificates.expire_date),
                    license_type              = COALESCE(EXCLUDED.license_type,              public.certificates.license_type),
                    category_license_type     = COALESCE(EXCLUDED.category_license_type,     public.certificates.category_license_type),
                    application_certificate_type = COALESCE(EXCLUDED.application_certificate_type, public.certificates.application_certificate_type),
                    zone_id                   = COALESCE(EXCLUDED.zone_id,                   public.certificates.zone_id),
                    zone_name                 = COALESCE(EXCLUDED.zone_name,                 public.certificates.zone_name),
                    updated_at                = now()
            """))
            inserted_certs = r_certs.rowcount or 0
            db.commit()
            logger.info("Certificates upserted: %d", inserted_certs)

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
            logger.info("Back-filled applications.certificate_id")
        except Exception as _ce:
            logger.warning("Certificates insert failed (non-fatal): %s", _ce)
            try:
                db.rollback()
            except Exception:
                pass

        result = {
            'total_rows': total_rows,
            'inserted_applications': inserted_apps,
            'inserted_certificates': inserted_certs,
            'failed_applications': failed_app_rows,
            'inserted_documents': inserted_docs,
            'skipped_documents': skipped_docs_total,
            'inserted_users': inserted_users,
            'skipped_users': skipped_users,
            'inserted_user_roles': inserted_user_roles,
            'skipped_user_roles': skipped_user_roles,
        }

        # Add summary message if there were failures
        if failed_app_rows > 0:
            result['failure_summary'] = (
                f"{failed_app_rows} application rows failed to insert. "
                f"{skipped_docs_total} documents were skipped because their parent applications failed. "
                f"See 'errors' array for details on each failed row."
            )

        # Convert sample sets to lists for serialization and include mapping stats
        # (limit unmapped samples to small lists)
        for k in ('region_unmapped_samples', 'district_unmapped_samples', 'ward_unmapped_samples', 'legal_status_unmapped_samples', 'license_category_unmapped_samples'):
            if k in mapping_stats and isinstance(mapping_stats[k], set):
                mapping_stats[k] = list(mapping_stats[k])
        result['mapping_stats'] = mapping_stats
        if errors:
            result['errors'] = errors

        # ── Auto backfill created_by from username ─────────────────────────
        # Run after all inserts so every user that was provisioned above gets
        # their UUID reflected on applications + all child tables.
        try:
            _bf = backfill_created_by_from_username(db)
            logger.info("Auto backfill created_by: %s", _bf)
            result['backfill_created_by'] = _bf
        except Exception as _bfe:
            logger.warning("Auto backfill created_by failed (non-fatal): %s", _bfe)

        return result


def backfill_created_by_from_username(db: Any) -> Dict[str, int]:
    """Backfill created_by UUIDs from users.username.

    Match rule:
        Case-insensitive match on normalized usernames:
        lower(trim(applications.username)) == lower(trim(users.username))

    Update rule:
        Only set created_by when it is currently NULL (idempotent / safe to re-run).

    Tables updated
    ──────────────
    Group A – direct application_id FK:
        applications, application_sector_details, certificates,
        application_electrical_installation, task_assignments,
        batch_application_advertisements, transfer_applications,
        app_evaluation_checklist, application_additional_conditions,
        application_reviews

    Group B – via application_sector_details (application_sector_detail_id FK):
        documents, shareholders, managing_directors, fire,
        insurance_cover_details, ardhi_information

    Special FK name:
        contact_persons uses  app_sector_detail_id  (not application_sector_detail_id)
    """

    # ------------------------------------------------------------------
    # Reusable CTE SQL fragment (Group A — direct application_id FK)
    # ------------------------------------------------------------------
    _APP_CTE = """
        WITH u AS (
            SELECT a.id AS application_id, usr.id AS user_id
            FROM public.applications a
            JOIN public.users usr
              ON lower(trim(usr.username)) = lower(trim(a.username))
            WHERE a.username IS NOT NULL AND lower(trim(a.username)) <> ''
        )
    """

    # ------------------------------------------------------------------
    # Reusable CTE SQL fragment (Group B — via application_sector_details)
    # ------------------------------------------------------------------
    _ASD_CTE = """
        WITH u AS (
            SELECT asd.id AS asd_id, usr.id AS user_id
            FROM public.application_sector_details asd
            JOIN public.applications a ON a.id = asd.application_id
            JOIN public.users usr
              ON lower(trim(usr.username)) = lower(trim(a.username))
            WHERE a.username IS NOT NULL AND lower(trim(a.username)) <> ''
        )
    """

    counts: Dict[str, int] = {}

    def _run(sql: str, key: str) -> None:
        try:
            counts[key] = db.execute(text(sql)).rowcount or 0
        except Exception as exc:
            logger.warning("backfill_created_by: skipped %s — %s", key, exc)
            db.rollback()
            counts[key] = -1

    # ── Group A ──────────────────────────────────────────────────────
    _run(_APP_CTE + """
        UPDATE public.applications a
        SET created_by = u.user_id
        FROM u
        WHERE a.id = u.application_id AND a.created_by IS NULL
    """, "applications")

    _run(_APP_CTE + """
        UPDATE public.application_sector_details asd
        SET created_by = u.user_id
        FROM u
        WHERE asd.application_id = u.application_id AND asd.created_by IS NULL
    """, "application_sector_details")

    _run(_APP_CTE + """
        UPDATE public.certificates c
        SET created_by = u.user_id
        FROM u
        WHERE c.application_id = u.application_id AND c.created_by IS NULL
    """, "certificates")

    _run(_APP_CTE + """
        UPDATE public.application_electrical_installation aei
        SET created_by = u.user_id
        FROM u
        WHERE aei.application_id = u.application_id AND aei.created_by IS NULL
    """, "application_electrical_installation")

    _run(_APP_CTE + """
        UPDATE public.task_assignments ta
        SET created_by = u.user_id
        FROM u
        WHERE ta.application_id = u.application_id AND ta.created_by IS NULL
    """, "task_assignments")

    _run(_APP_CTE + """
        UPDATE public.batch_application_advertisements baa
        SET created_by = u.user_id
        FROM u
        WHERE baa.application_id = u.application_id AND baa.created_by IS NULL
    """, "batch_application_advertisements")

    _run(_APP_CTE + """
        UPDATE public.transfer_applications tra
        SET created_by = u.user_id
        FROM u
        WHERE tra.application_id = u.application_id AND tra.created_by IS NULL
    """, "transfer_applications")

    _run(_APP_CTE + """
        UPDATE public.app_evaluation_checklist aec
        SET created_by = u.user_id
        FROM u
        WHERE aec.application_id = u.application_id AND aec.created_by IS NULL
    """, "app_evaluation_checklist")

    _run(_APP_CTE + """
        UPDATE public.application_additional_conditions aac
        SET created_by = u.user_id
        FROM u
        WHERE aac.application_id = u.application_id AND aac.created_by IS NULL
    """, "application_additional_conditions")

    _run(_APP_CTE + """
        UPDATE public.application_reviews ar
        SET created_by = u.user_id
        FROM u
        WHERE ar.application_id = u.application_id AND ar.created_by IS NULL
    """, "application_reviews")

    # ── Group B ──────────────────────────────────────────────────────
    _run(_ASD_CTE + """
        UPDATE public.documents d
        SET created_by = u.user_id
        FROM u
        WHERE d.application_sector_detail_id = u.asd_id AND d.created_by IS NULL
    """, "documents")

    # contact_persons uses app_sector_detail_id (different FK column name)
    _run(_ASD_CTE + """
        UPDATE public.contact_persons cp
        SET created_by = u.user_id
        FROM u
        WHERE cp.app_sector_detail_id = u.asd_id AND cp.created_by IS NULL
    """, "contact_persons")

    _run(_ASD_CTE + """
        UPDATE public.shareholders s
        SET created_by = u.user_id
        FROM u
        WHERE s.application_sector_detail_id = u.asd_id AND s.created_by IS NULL
    """, "shareholders")

    _run(_ASD_CTE + """
        UPDATE public.managing_directors md
        SET created_by = u.user_id
        FROM u
        WHERE md.application_sector_detail_id = u.asd_id AND md.created_by IS NULL
    """, "managing_directors")

    _run(_ASD_CTE + """
        UPDATE public.fire f
        SET created_by = u.user_id
        FROM u
        WHERE f.application_sector_detail_id = u.asd_id AND f.created_by IS NULL
    """, "fire")

    _run(_ASD_CTE + """
        UPDATE public.insurance_cover_details icd
        SET created_by = u.user_id
        FROM u
        WHERE icd.application_sector_detail_id = u.asd_id AND icd.created_by IS NULL
    """, "insurance_cover_details")

    _run(_ASD_CTE + """
        UPDATE public.ardhi_information ai
        SET created_by = u.user_id
        FROM u
        WHERE ai.application_sector_detail_id = u.asd_id AND ai.created_by IS NULL
    """, "ardhi_information")

    db.commit()
    return counts

