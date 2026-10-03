# EWURA Migration API: Multi-Sector Master Data Import Guide

This guide details the end-to-end architecture, API endpoints, scripting workflows, and cross-sector database rules for migrating data into the EWURA e-Services system (`eservice_applications`).

---

## 1. Multi-Sector Architecture & Database Matrix

All sectors store core applications in `public.applications` and generated permits/licenses in `public.certificates`, but license types, child table associations, and certificate ownership logic vary by sector:

| Sector | Sector ID (`public.sectors`) | Valid `license_type` Values | Default `certificate_type` | Primary Child Table | Certificate Owner Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`PETROLEUM`** | `a080b0a2-b9f6-46ec-b433-291f006992a4` | `LICENSE_PETROLEUM`, `CONSTRUCTION_PETROLEUM` | `License` / `Construction` | `application_sector_details` | Facility/Company Name + PoBox |
| **`NATURAL_GAS`** | `9e1ab27f-da1a-45c6-9238-6dec767dd525` | `LICENSE_NATURAL_GAS`, `CONSTRUCTION_NATURAL_GAS` | `License` / `Construction` | `application_sector_details` | Facility/Company Name + PoBox |
| **`WATER_SUPPLY`** | `857dee45-d27a-4c46-833e-ce9d07c39955` | `LICENSE_WATER` | `License` | `application_sector_details` | Facility/Company Name + PoBox |
| **`ELECTRICITY` (Supply)** | `383b172c-d3f1-4913-9653-895f5ebdd747` | `LICENSE_ELECTRICITY_SUPPLY` | `License` | `application_sector_details` | Facility/Company Name + PoBox |
| **`ELECTRICITY` (Installation)**| `383b172c-d3f1-4913-9653-895f5ebdd747` | `LICENSE_ELECTRICITY_INSTALLATION` | `License` | `application_electrical_installation` | `aei.applicant_name` (Person) |

---

## 2. Critical Database Constraints & Validation Rules

PostgreSQL enforces strict CHECK constraints on `certificates` and `applications`. Any violation aborts the transaction.

### A. Certificate Type (`certificates_certificate_type_check`)
- **Permitted values**: Strictly `'Construction'`, `'License'`, `'Registration'`.
- **Known Pitfall**: In `public.categories`, legacy rows have composite strings like `'Construction,License'` (e.g. `'Consumer Installation (Agriculture, Transportation & Small Scale Industries)'`).
- **Standard Mapping**:
  ```sql
  CASE 
      WHEN cat.category_type IN ('Construction', 'License', 'Registration') THEN cat.category_type
      WHEN a.license_type LIKE 'CONSTRUCTION%' OR cat.name ~* 'Construction' THEN 'Construction'
      WHEN cat.category_type = 'Registration' THEN 'Registration'
      ELSE 'License'
  END
  ```

### B. Application Certificate Type (`certificates_application_certificate_type_check`)
- **Permitted values**: `'NEW'`, `'RENEW'`, `'EXTEND'`, `'CHANGE_OF_NAME'`, `'RELOCATION'`, `'ALTERATION'`, `'COMPLIANCE'`, `'APPEAL'`, `'UPGRADE'`, `'TRANSFER'`, `'MODIFICATION'`.
- **Mapping**: Always map input strings:
  - `EXTENSION` $\rightarrow$ `EXTEND`
  - `RENEWAL` $\rightarrow$ `RENEW`
  - `CHANGE_NAME` / `CHANGES` $\rightarrow$ `CHANGE_OF_NAME`
  - Default $\rightarrow$ `NEW`

### C. Category License Type (`certificates_category_license_type_check`)
- **Permitted values**: Strictly `'OPERATIONAL'`, `'PROVISIONAL'`.

---

## 3. High-Risk Pitfalls & Solutions

### A. Duplicate Category Names (Cardinality Violation)
- `public.categories` contains multiple rows with identical names:
  - `Distribution` (in `ELECTRICITY`: one `Registration`, one `License`)
  - `Generation` (in `ELECTRICITY`: one `License`, one `Registration`)
  - `LPG Storage and Filling Plant` (in `PETROLEUM`: one `License`, one `Construction`)
- **Solution**: Always join categories using a LATERAL subquery with `LIMIT 1`:
  ```sql
  LEFT JOIN LATERAL (
      SELECT c.id, c.name, c.category_type, c.sector_id
      FROM public.categories c
      WHERE LOWER(TRIM(c.name)) = LOWER(TRIM(s.license_type))
      ORDER BY c.created_at ASC
      LIMIT 1
  ) cat ON true
  ```

### B. Approval Number Index Collision (`uq_certificates_approval_no`)
- Bulk loading multiple historical applications that share an `approval_no` fails if `uq_certificates_approval_no` is present during initial load.
- **Solution**: The staging pipeline drops this index in Section 0, loads certificates keyed by `application_number`, deduplicates certificates by `approval_no` in Section 2b, and then re-creates the index.

### C. Intimation Periods
- **Electricity Installation**: Operators and electricians renew every 2–3 years; intimation is **90 days** prior to expiration:
  `intimate_date = expire_date - INTERVAL '90 days'`
- **All Other Sectors**: Intimations occur **180 days** prior to expiration:
  `intimate_date = expire_date - INTERVAL '180 days'`

---

## 4. Idempotent Deduplication Patterns

### Shareholders & Managing Directors
To prevent duplicating existing records while properly inserting new ones:
- **Shareholder ID Formula**:
  `md5(e.application_number || '|' || lower(trim(e.shareholder_name)))::uuid`
- **Managing Director ID Formula**:
  `md5(e.application_number || '|' || lower(trim(e.md_name)))::uuid`
- **Upsert Rule**: `INSERT INTO ... ON CONFLICT (id) DO UPDATE SET ...`

### Applications & Staging Priority
- Primary key / conflict target is `application_number`:
  `ON CONFLICT (application_number) DO UPDATE SET ...`
- Geographic data priority: Uploaded file (`EXCLUDED`) takes precedence:
  `region = COALESCE(EXCLUDED.region, public.applications.region)`
- Dynamic Zone resolution: Join `napa_regions` and `zones` dynamically by text region name.

---

## 5. API Endpoints Reference

### Server Startup
```sh
./use-env.sh prod
uvicorn app.main:app --reload
```
Swagger UI available at: `http://localhost:8000/docs`

### A. Users
- `POST /api/v1/users_upload`: Upload main LOIS users file before running sector imports so user IDs link correctly.
- `POST /api/v1/backfill_users`: Backfill `created_by` / `owner_id` from usernames.

### B. Sector Applications
- `POST /api/v1/petroleum_upload`: Petroleum sector applications & certificates.
- `POST /api/v1/natural_gas_upload`: Natural Gas sector applications & certificates.
- `POST /api/v1/electricity_upload`: Electricity Supply applications & certificates.
- `POST /api/v1/water_supply_upload`: Water Supply applications & certificates.

### C. Electrical Installation (Personnel Licensing)
- `POST /api/v1/electrical_installations_upload`: Main electrical installation applications.
- `POST /api/v1/electrical_installations_employed_upload`: Employed electricians.
- `POST /api/v1/electrical_installations_self_employed_upload`: Self-employed electricians.
- `POST /api/v1/electrical_certificate_verifications_upload`: Academic/professional certificate verifications.
- `POST /api/v1/electrical_installations_supervisors_upload`: Supervisor details.

### D. Corporate Officers & Ownership
- `POST /api/v1/petroleum_managing_directors_upload`
- `POST /api/v1/electricity_managing_directors_upload`
- `POST /api/v1/natural_gas_managing_directors_upload`
- `POST /api/v1/petroleum_shareholders_upload`
- `POST /api/v1/electricity_shareholders_upload`
- `POST /api/v1/natural_gas_shareholders_upload`

---

## 6. Pre-Flight & Post-Import Safeguards

1. **Pre-Flight OpenVPN Check**:
   Ensure `tun0` is active and ping to `10.1.90.90` and the target database (`10.1.8.166` / `10.1.8.157`) succeeds.
2. **Timestamped Table Backup**:
   Always back up key tables before running migrations:
   ```sql
   CREATE TABLE applications_backup_YYYYMMDD_HHMMSS AS SELECT * FROM applications;
   CREATE TABLE certificates_backup_YYYYMMDD_HHMMSS AS SELECT * FROM certificates;
   CREATE TABLE shareholders_backup_YYYYMMDD_HHMMSS AS SELECT * FROM shareholders;
   CREATE TABLE managing_directors_backup_YYYYMMDD_HHMMSS AS SELECT * FROM managing_directors;
   ```
3. **Post-Import Hooks**:
   Always execute `run_post_import_hooks(db)` after any data import to synchronize foreign keys, backfill missing user IDs, and align certificate sectors and owners.
