from __future__ import annotations

from typing import Any, Callable, Optional
import uuid
import pandas as pd


# Countries mapping (countriesid -> countryname)
# Source: provided LOIS countries list. Unknown IDs should map to NULL.
COUNTRY_ID_TO_NAME: dict[int, str] = {
    1544693140391: "Angola",
    1544693140399: "Tanzania",
    1544693140401: "India",
    1544693140722: "England",
    1544693140724: "Australia",
    1546529420453: "UNITED STATES OF AMERICA",
    1567508949695: "CHINA",
    1567508954132: "Zambia",
    1567508961150: "Afghanistan",
    1567508961151: "Albania",
    1567508961152: "Algeria",
    1567508961153: "American Samoa",
    1567508961154: "Andorra",
    1567508961155: "Anguilla",
    1567508961156: "Antarctica",
    1567508961157: "Antigua and Barbuda",
    1567508961158: "Argentina",
    1567508961159: "Armenia",
    1567508961160: "Aruba",
    1567508961161: "Austria",
    1567508961162: "Azerbaijan",
    1567508961163: "Bahamas",
    1567508961164: "Bahamas",
    1567508961165: "Bangladesh",
    1567508961166: "Barbados",
    1567508961167: "Belarus",
    1567508961168: "Belgium",
    1567508961169: "Belize",
    1567508961170: "Benin",
    1567508961171: "Bermuda",
    1567508961172: "Bhutan",
    1567508961173: "Bolivia",
    1567508961174: "Bosnia and Herzegovina",
    1567508961175: "Botswana",
    1567508961176: "Brazil",
    1567508961177: "British Indian Ocean Territory",
    1567508961178: "British Virgin Islands",
    1567508961179: "Brunei",
    1567508961180: "Bulgaria",
    1567508961181: "Burkina Faso",
    1567508961182: "Burundi",
    1567508961183: "Cambodia",
    1567508961184: "Cameroon",
    1567508961185: "Canada",
    1567508961186: "Cape Verde",
    1567508961187: "Cayman Islands",
    1567508961188: "Central African Republic",
    1567508961189: "Chad",
    1567508961190: "Chile",
    1567508961191: "Christmas Island",
    1567508961192: "Cocos Islands",
    1567508961193: "Colombia",
    1567508961194: "Comoros",
    1567508961195: "Cook Islands",
    1567508961196: "Costa Rica",
    1567508961197: "Croatia",
    1567508961198: "Cuba",
    1567508961199: "Curacao",
    1567508961200: "Cyprus",
    1567508961201: "Czech Republic",
    1567508961202: "Democratic Republic of the Congo",
    1567508961203: "Denmark",
    1567508961204: "Djibouti",
    1567508961205: "Dominica",
    1567508961206: "Dominican Republic",
    1567508961207: "East Timor",
    1567508961208: "Ecuador",
    1567508961209: "Egypt",
    1567508961210: "El Salvador",
    1567508961211: "Equatorial Guinea",
    1567508961212: "Eritrea",
    1567508961213: "Estonia",
    1567508961214: "Ethiopia",
    1567508961215: "Falkland Islands",
    1567508961216: "Faroe Islands",
    1567508961217: "Fiji",
    1567508961218: "Finland",
    1567508961219: "France",
    1567508961220: "French Polynesia",
    1567508961221: "Gabon",
    1567508961222: "Gambia",
    1567508961223: "Georgia",
    1567508961224: "Germany",
    1567508961225: "Ghana",
    1567508961226: "Gibraltar",
    1567508961227: "Greece",
    1567508961228: "Greenland",
    1567508961229: "Grenada",
    1567508961230: "Guam",
    1567508961231: "Guatemala",
    1567508961232: "Guernsey",
    1567508961233: "Guinea",
    1567508961234: "Guinea-Bissau",
    1567508961235: "Guyana",
    1567508961236: "Haiti",
    1567508961237: "Honduras",
    1567508961238: "Hong Kong",
    1567508961239: "Hungary",
    1567508961240: "Iceland",
    1567508961241: "Indonesia",
    1567508961242: "Iran",
    1567508961243: "Iraq",
    1567508961244: "Ireland",
    1567508961245: "Isle of Man",
    1567508961246: "Israel",
    1567508961247: "Italy",
    1567508961248: "Ivory Coast",
    1567508961249: "Jamaica",
    1567508961250: "Japan",
    1567508961251: "Jersey",
    1567508961252: "Jordan",
    1567508961253: "Kazakhstan",
    1567508961254: "Kenya",
    1567508961255: "Kiribati",
    1567508961256: "Kosovo",
    1567508961257: "Kuwait",
    1567508961258: "Kyrgyzstan",
    1567508961259: "Laos",
    1567508961260: "Latvia",
    1567508961261: "Lebanon",
    1567508961262: "Lesotho",
    1567508961263: "Liberia",
    1567508961264: "Libya",
    1567508961265: "Liechtenstein",
    1567508961266: "Lithuania",
    1567508961267: "Luxembourg",
    1567508961268: "Macau",
    1567508961269: "Macedonia",
    1567508961270: "Madagascar",
    1567508961271: "Malawi",
    1567508961272: "Malaysia",
    1567508961273: "Maldives",
    1567508961274: "Mali",
    1567508961275: "Malta",
    1567508961276: "Marshall Islands",
    1567508961277: "Mauritania",
    1567508961278: "Mauritius",
    1567508961279: "Mayotte",
    1567508961280: "Mexico",
    1567508961281: "Micronesia",
    1567508961282: "Moldova",
    1567508961283: "Monaco",
    1567508961284: "Mongolia",
    1567508961285: "Montenegro",
    1567508961286: "Montserrat",
    1567508961287: "Morocco",
    1567508961288: "Mozambique",
    1567508961289: "Myanmar",
    1567508961290: "Namibia",
    1567508961291: "Nauru",
    1567508961292: "Nepal",
    1567508961293: "Netherlands",
    1567508961294: "Netherlands Antilles",
    1567508961295: "New Caledonia",
    1567508961296: "New Zealand",
    1567508961297: "Nicaragua",
    1567508961298: "Niger",
    1567508961299: "Nigeria",
    1567508961300: "Niue",
    1567508961301: "North Korea",
    1567508961302: "Northern Mariana Islands",
    1567508961303: "Norway",
    1567508961304: "Oman",
    1567508961305: "Pakistan",
    1567508961306: "Palau",
    1567508961307: "Palestine",
    1567508961308: "Panama",
    1567508961309: "Papua New Guinea",
    1567508961310: "Paraguay",
    1567508961311: "Peru",
    1567508961312: "Philippines",
    1567508961313: "Pitcairn",
    1567508961314: "Poland",
    1567508961315: "Portugal",
    1567508961316: "Puerto Rico",
    1567508961317: "Qatar",
    1567508961318: "Republic of the Congo",
    1567508961319: "Reunion",
    1567508961320: "Romania",
    1567508961321: "Russia",
    1567508961322: "Rwanda",
    1567508961323: "Saint Barthelemy",
    1567508961324: "Saint Helena",
    1567508961325: "Saint Kitts and Nevis",
    1567508961326: "Saint Lucia",
    1567508961327: "Saint Martin",
    1567508961328: "Saint Pierre and Miquelon",
    1567508961329: "Saint Vincent and the Grenadines",
    1567508961330: "Samoa",
    1567508961331: "San Marino",
    1567508961332: "Sao Tome and Principe",
    1567508961333: "Saudi Arabia",
    1567508961334: "Senegal",
    1567508961335: "Serbia",
    1567508961336: "Seychelles",
    1567508961337: "Sierra Leone",
    1567508961338: "Singapore",
    1567508961339: "Sint Maarten",
    1567508961340: "Slovakia",
    1567508961341: "Slovenia",
    1567508961342: "Solomon Islands",
    1567508961343: "Somalia",
    1567508961344: "South Africa",
    1567508961345: "South Korea",
    1567508961346: "South Sudan",
    1567508961347: "Spain",
    1567508961348: "Sri Lanka",
    1567508961349: "Sudan",
    1567508961350: "Suriname",
    1567508961351: "Svalbard and Jan Mayen",
    1567508961352: "Swaziland",
    1567508961353: "Sweden",
    1567508961354: "Switzerland",
    1567508961355: "Syria",
    1567508961356: "Taiwan",
    1567508961357: "Tajikistan",
    1567508961358: "Thailand",
    1567508961359: "Togo",
    1567508961360: "Tokelau",
    1567508961361: "Tonga",
    1567508961362: "Trinidad and Tobago",
    1567508961363: "Tunisia",
    1567508961364: "Turkey",
    1567508961365: "Turkmenistan",
    1567508961366: "Turks and Caicos Islands",
    1567508961367: "Tuvalu",
    1567508961368: "U.S. Virgin Islands",
    1567508961369: "Uganda",
    1567508961370: "Ukraine",
    1567508961371: "United Arab Emirates",
    1567508961372: "United Kingdom",
    1567508961373: "Uruguay",
    1567508961374: "Uzbekistan",
    1567508961375: "Vanuatu",
    1567508961376: "Vatican",
    1567508961377: "Venezuela",
    1567508961378: "Vietnam",
    1567508961379: "Wallis and Futuna",
    1567508961380: "Western Sahara",
    1567508961381: "Yemen",
    1567508961382: "Zimbabwe",
}


def import_shareholders_via_staging_copy(
    db: Any,
    df,
    *,
    source_file_name: Optional[str] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """High-volume import of shareholders using staging + COPY + SQL transform.

    Contract
    - Input df: dataframe containing (at least) the Excel columns used by the mapping:
        apprefno, countryname, amountofshare, objectid, nationality, indcomp,
        sconadd, shname, rowid
    - Output: dict of counts.

    Notes
    - Stages into public.stage_ca_shareholders_raw.
    - Transforms into public.shareholders by joining:
        stage.application_number (apprefno) -> applications.application_number -> application_sector_details.id
    """

    from sqlalchemy import text
    import io

    def _progress(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # Normalize columns
    df2 = df.copy()
    df2.columns = (
        df2.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    required = {
        "apprefno",
        "shname",
    }
    missing = required - set(df2.columns)
    if missing:
        raise ValueError(f"Missing required columns for shareholders import: {sorted(missing)}")

    # ── DEBUG: log actual column names received from the file reader ──────────
    import sys
    print(
        f"[shareholders_import] columns received ({len(df2.columns)}): {list(df2.columns)}",
        file=sys.stderr,
        flush=True,
    )
    if len(df2) > 0:
        print(
            f"[shareholders_import] first data row: {df2.iloc[0].to_dict()}",
            file=sys.stderr,
            flush=True,
        )
    # ─────────────────────────────────────────────────────────────────────────

    # Optional columns — fill with empty string if absent so downstream code
    # doesn't need to branch on their existence.
    for _opt in ("countryname", "amountofshare", "objectid", "nationality", "indcomp", "sconadd", "filename"):
        if _opt not in df2.columns:
            df2[_opt] = ""

    total_rows_in_file = int(len(df2))
    if total_rows_in_file == 0:
        return {
            "total_rows_in_file": 0,
            "staged_total": 0,
            "inserted_shareholders": 0,
            "skipped_total": 0,
            "skipped_breakdown": {
                "missing_application": 0,
                "missing_shareholder_name": 0,
                "already_exists": 0,
            },
            "diagnostics": {
                "invalid_objectid": 0,
            },
        }

    _progress("shareholders:staging:create")

    # Always DROP and recreate the staging table so column ORDER is guaranteed to
    # match the COPY CSV exactly.  "CREATE TABLE IF NOT EXISTS" + ALTER TABLE
    # patchwork causes silent column misalignment when the table was created by
    # an older version of this code.
    db.execute(text("DROP TABLE IF EXISTS public.stage_ca_shareholders_raw"))
    db.execute(
        text(
            """
            CREATE TABLE public.stage_ca_shareholders_raw (
                id                 uuid PRIMARY KEY,
                application_number text,
                shname             text,
                amountofshare      text,
                file_name          text,
                objectid           text,
                nationality        text,
                indcomp            text,
                sconadd            text,
                countryname        text,
                source_row_no      bigint
            )
            """
        )
    )

    _progress("shareholders:prepare:export")

    # Build export frame — one column per Excel column, in the same order as
    # the staging CREATE TABLE above.
    # Excel columns: apprefno | countryname | amountofshare | filename | objectid | nationality | indcomp | sconadd | shname
    export = pd.DataFrame()
    export["shname"]        = df2["shname"].astype(str).str.strip()
    export["amountofshare"] = df2["amountofshare"].astype(str).str.strip()
    export["file_name"]     = df2["filename"].astype(str).str.strip()   # Excel "filename" → file_name
    export["objectid"]      = df2["objectid"].astype(str).str.strip()   # Excel "objectid" → logic_doc_id
    export["nationality"]   = df2["nationality"].astype(str).str.strip()
    export["indcomp"]       = df2["indcomp"].astype(str).str.strip()
    export["sconadd"]       = df2["sconadd"].astype(str).str.strip()
    export["countryname"]   = df2["countryname"].astype(str).str.strip()

    # Nationality mapping: Excel provides a numeric `countriesid` (bigint, may arrive as
    # scientific notation like "1.54469E+12"). Look it up in COUNTRY_ID_TO_NAME and store
    # the country NAME.  Anything unresolvable becomes an empty string (→ NULL in DB).
    def _map_nationality_to_name(v) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return ""
        # float() parses both plain integers and scientific notation correctly
        try:
            key = int(float(s))
        except (ValueError, OverflowError):
            return ""
        return COUNTRY_ID_TO_NAME.get(key, "")

    export["nationality"] = export["nationality"].map(_map_nationality_to_name).fillna("")

    # Assemble final staging frame in the exact column order the staging table expects.
    n = len(export)
    export.insert(0, "id",                 [str(uuid.uuid4()) for _ in range(n)])
    export.insert(1, "application_number", df2["apprefno"].astype(str).str.strip())
    export["source_row_no"] = range(1, n + 1)

    _progress(f"shareholders:prepare:done rows={n}")

    # Final column order MUST match CREATE TABLE column order above.
    export = export[[
        "id",
        "application_number",
        "shname",
        "amountofshare",
        "file_name",
        "objectid",
        "nationality",
        "indcomp",
        "sconadd",
        "countryname",
        "source_row_no",
    ]]

    # Stream COPY in chunks
    def _iter_csv_chunks(frame, chunk_rows: int = 50000):
        header_written = False
        for start in range(0, len(frame), chunk_rows):
            chunk = frame.iloc[start : start + chunk_rows]
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=not header_written)
            header_written = True
            buf.seek(0)
            yield buf, min(len(frame), start + len(chunk))

    sa_conn = db.connection()
    raw_conn = sa_conn.connection
    cur = raw_conn.cursor()
    try:
        total = len(export)
        copied = 0
        _progress(f"shareholders:copy:start total_rows={total}")
        for buf, copied_now in _iter_csv_chunks(export, chunk_rows=50000):
            cur.copy_expert(
                """COPY public.stage_ca_shareholders_raw (
                    id, application_number, shname, amountofshare,
                    file_name, objectid, nationality, indcomp, sconadd,
                    countryname, source_row_no
                ) FROM STDIN WITH CSV HEADER""",
                buf,
            )
            copied = copied_now
            pct = round((copied / total) * 100, 2) if total else 100.0
            _progress(f"shareholders:copy {copied}/{total} ({pct}%)")
    finally:
        cur.close()

    staged_total = int(db.execute(text("SELECT COUNT(*) FROM public.stage_ca_shareholders_raw")).scalar() or 0)
    _progress(f"shareholders:staged rows={staged_total}")

    # Transform into final tables.
    _progress("shareholders:transform:start")

    # Run transform and compute detailed stats.
    transform_sql = text(
        """
        -- Ensure public.shareholders has the expected columns (safe additive).
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS application_sector_detail_id uuid;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS file_name text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS logic_doc_id bigint;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS street_address text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS amount_of_shares numeric;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS birth_date date;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS country_of_incorporation text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS country_of_residence text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS email text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS first_name text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS gender text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS individual_company text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS last_name text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS middle_name text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS mobile_no text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS nationality text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS passport_or_nationalid text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS shareholder_name text;
        ALTER TABLE IF EXISTS public.shareholders
            ADD COLUMN IF NOT EXISTS amount_of_share_percent numeric;

        -- Normalize staging and compute eligibility/skips.
        WITH s_norm AS (
            SELECT
                s.id,
                NULLIF(trim(s.application_number), '') AS application_number,
                NULLIF(trim(s.shname), '')             AS shareholder_name,
                NULLIF(trim(s.amountofshare), '')      AS amountofshare_raw,
                NULLIF(trim(s.sconadd), '')            AS street_address,
                NULLIF(trim(s.countryname), '')        AS countryname,
                NULLIF(trim(s.indcomp), '')            AS individual_company,
                -- nationality was already resolved to a country name in Python via COUNTRY_ID_TO_NAME
                -- Normalize to enum: TANZANIAN | NON_TANZANIAN | NULL
                CASE
                    WHEN LOWER(TRIM(s.nationality)) = 'tanzanian'          THEN 'TANZANIAN'
                    WHEN LOWER(TRIM(s.nationality)) LIKE '%non%tanzanian%' THEN 'NON_TANZANIAN'
                    WHEN LOWER(TRIM(s.nationality)) = 'non-tanzanian'      THEN 'NON_TANZANIAN'
                    ELSE NULL
                END                                    AS nationality_name,
                -- objectid from Excel maps to logic_doc_id (integer, e.g. 121858)
                NULLIF(trim(s.objectid), '')           AS objectid_raw,
                NULLIF(trim(s.file_name), '')          AS file_name
            FROM public.stage_ca_shareholders_raw s
        ),
        joined AS (
            SELECT
                sn.*,
                a.id   AS application_id,
                asd.id AS application_sector_detail_id,
                -- objectid_raw is a plain integer (e.g. 121858); flag if non-numeric
                (sn.objectid_raw IS NOT NULL AND sn.objectid_raw !~ '^[0-9]+(\.[0-9]+)?$') AS invalid_objectid
            FROM s_norm sn
            LEFT JOIN public.applications a
                ON a.application_number IS NOT DISTINCT FROM sn.application_number
            LEFT JOIN public.application_sector_details asd
                ON asd.application_id = a.id
        ),
        eligible AS (
            SELECT j.*
            FROM joined j
            WHERE j.application_id IS NOT NULL
              AND j.application_sector_detail_id IS NOT NULL
              AND j.shareholder_name IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.shareholders x
                  WHERE x.application_sector_detail_id = j.application_sector_detail_id
                    AND x.shareholder_name IS NOT DISTINCT FROM j.shareholder_name
              )
        ),
        ins AS (
            INSERT INTO public.shareholders (
                id,
                created_at,
                created_by,
                deleted_at,
                deleted_by,
                updated_at,
                updated_by,
                shareholder_name,
                amount_of_shares,
                country_of_residence,
                country_of_incorporation,
                nationality,
                individual_company,
                passport_or_nationalid,
                application_sector_detail_id,
                file_name,
                logic_doc_id,
                amount_of_share_percent,
                birth_date,
                email,
                first_name,
                gender,
                last_name,
                middle_name,
                mobile_no,
                street_address
            )
            SELECT
                e.id,
                now() AS created_at,
                NULL AS created_by,
                NULL AS deleted_at,
                NULL AS deleted_by,
                now() AS updated_at,
                NULL AS updated_by,
                e.shareholder_name,
                NULL AS amount_of_shares,
                e.countryname AS country_of_residence,
                e.countryname AS country_of_incorporation,
                e.nationality_name AS nationality,
                e.individual_company,
                NULL AS passport_or_nationalid,
                e.application_sector_detail_id,
                NULLIF(trim(e.file_name), '') AS file_name,
                -- objectid from Excel is a plain integer (e.g. 121858) → logic_doc_id
                CASE
                    WHEN e.objectid_raw ~ '^[0-9]+$'        THEN e.objectid_raw::bigint
                    WHEN e.objectid_raw ~ '^[0-9]+\.[0-9]+$' THEN trunc(e.objectid_raw::numeric)::bigint
                    ELSE NULL
                END AS logic_doc_id,
                CASE
                    WHEN e.amountofshare_raw ~ '^[0-9]+(\\.[0-9]+)?$' THEN e.amountofshare_raw::numeric
                    ELSE NULL
                END AS amount_of_share_percent,
                NULL::date AS birth_date,
                NULL AS email,
                CASE
                    WHEN e.shareholder_name IS NULL THEN NULL
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 1 THEN split_part(trim(e.shareholder_name), ' ', 1)
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 2 THEN split_part(trim(e.shareholder_name), ' ', 1)
                    ELSE split_part(trim(e.shareholder_name), ' ', 1)
                END AS first_name,
                NULL AS gender,
                CASE
                    WHEN e.shareholder_name IS NULL THEN NULL
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 1 THEN NULL
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 2 THEN NULL
                    ELSE regexp_replace(trim(e.shareholder_name), '^\\S+\\s+(.+)\\s+\\S+$', '\\1')
                END AS middle_name,
                CASE
                    WHEN e.shareholder_name IS NULL THEN NULL
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 1 THEN NULL
                    WHEN array_length(regexp_split_to_array(trim(e.shareholder_name), '\\s+'), 1) = 2 THEN split_part(trim(e.shareholder_name), ' ', 2)
                    ELSE regexp_replace(trim(e.shareholder_name), '^.*\\s+(\\S+)$', '\\1')
                END AS last_name,
                NULL AS mobile_no,
                e.street_address AS street_address
            FROM eligible e
            ON CONFLICT (id) DO NOTHING
            RETURNING 1
        ),
        stats AS (
            SELECT
                (SELECT COUNT(*) FROM joined) AS processed_rows,
                (SELECT COUNT(*) FROM ins) AS inserted_rows,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NULL OR application_sector_detail_id IS NULL) AS skipped_missing_application,
                (SELECT COUNT(*) FROM joined WHERE application_id IS NOT NULL AND shareholder_name IS NULL) AS skipped_missing_shareholder_name,
                (SELECT COUNT(*) FROM joined j
                    WHERE j.application_id IS NOT NULL
                      AND j.application_sector_detail_id IS NOT NULL
                      AND j.shareholder_name IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM public.shareholders x
                          WHERE x.application_sector_detail_id = j.application_sector_detail_id
                            AND x.shareholder_name IS NOT DISTINCT FROM j.shareholder_name
                      )
                ) AS skipped_already_exists,
                (SELECT COUNT(*) FROM joined WHERE invalid_objectid) AS invalid_objectid
        )
        SELECT
            processed_rows,
            inserted_rows,
            (processed_rows - inserted_rows) AS skipped_total,
            skipped_missing_application,
            skipped_missing_shareholder_name,
            skipped_already_exists,
            invalid_objectid
        FROM stats;
        """
    )

    _progress("shareholders:transform:sql")
    row = db.execute(transform_sql).first()
    if not row:
        # Shouldn't happen, but keep API response stable.
        row = (staged_total, 0, staged_total, 0, 0, 0, 0)

    processed_rows = int(row[0] or 0)
    inserted_rows = int(row[1] or 0)
    skipped_total = int(row[2] or 0)
    skipped_missing_application = int(row[3] or 0)
    skipped_missing_shareholder_name = int(row[4] or 0)
    skipped_already_exists = int(row[5] or 0)
    invalid_objectid = int(row[6] or 0)

    _progress(
        "shareholders:transform:done "
        f"processed={processed_rows} inserted={inserted_rows} skipped={skipped_total} "
        f"missing_app={skipped_missing_application} missing_name={skipped_missing_shareholder_name} already_exists={skipped_already_exists}"
    )

    result = {
        "total_rows_in_file": total_rows_in_file,
        "staged_total": staged_total,
        "processed_rows": processed_rows,
        "inserted_rows": inserted_rows,
        "skipped_total": skipped_total,
        "skipped_breakdown": {
            "missing_application": skipped_missing_application,
            "missing_shareholder_name": skipped_missing_shareholder_name,
            "already_exists": skipped_already_exists,
        },
        "diagnostics": {
            "invalid_objectid": invalid_objectid,
            "note": "Rows are skipped when application_number doesn't match ca_applications, shareholder_name is blank, or (application_id + shareholder_order) already exists.",
        },
    }

    # If nothing inserted, attach a small sample of rows for each skip reason
    # so it's easy to see what's wrong without querying the DB manually.
    if inserted_rows == 0 and staged_total > 0:
        try:
            result["skip_samples"] = _fetch_skip_samples(db, limit=10)
        except Exception as e:
            result["skip_samples"] = {"error": str(e)}

    return result


def _fetch_skip_samples(db: Any, *, limit: int = 10) -> dict:
    """Return small sample rows from staging to explain why inserts might be 0."""
    from sqlalchemy import text

    reg = db.execute(text("select to_regclass('public.stage_ca_shareholders_raw')")).scalar()
    if not reg:
        return {"note": "staging table public.stage_ca_shareholders_raw does not exist in this DB/session"}

    # Note: we keep this lightweight: only return a few rows for each skip reason.
    samples = {}

    samples["missing_application"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.shname
                FROM public.stage_ca_shareholders_raw s
                LEFT JOIN public.applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                LEFT JOIN public.application_sector_details asd
                  ON asd.application_id = a.id
                WHERE a.id IS NULL OR asd.id IS NULL
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    samples["missing_shareholder_name"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.shname
                FROM public.stage_ca_shareholders_raw s
                JOIN public.applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                JOIN public.application_sector_details asd
                  ON asd.application_id = a.id
                WHERE NULLIF(trim(s.shname), '') IS NULL
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    samples["already_exists"] = [
        dict(r)
        for r in db.execute(
            text(
                """
                SELECT s.source_row_no, s.application_number, s.shname
                FROM public.stage_ca_shareholders_raw s
                JOIN public.applications a
                  ON a.application_number IS NOT DISTINCT FROM NULLIF(trim(s.application_number), '')
                JOIN public.application_sector_details asd
                  ON asd.application_id = a.id
                JOIN public.shareholders x
                  ON x.application_sector_detail_id = asd.id
                 AND x.shareholder_name IS NOT DISTINCT FROM NULLIF(trim(s.shname), '')
                ORDER BY s.source_row_no
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    ]

    return samples
