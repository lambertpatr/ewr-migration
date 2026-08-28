# EWURA Migration API — Architecture & Data Flow Guide

Welcome to the **EWURA Migration System**. This document clearly explains how data flows from uploaded Excel/CSV files into the PostgreSQL database, how different sector migrations work, and how the core table relationships (Applications, Certificates, Sector Details) operate.

---

## 🏗️ 1. Core Architecture & Data Flow Pattern

The migration engine uses a high-performance **Two-Stage Pattern** (Staging $\rightarrow$ Final Production Tables):

```
┌────────────────────────────────┐
│   Excel / CSV Source File      │
│  (LOIS Legacy or eService File)│
└───────────────┬────────────────┘
                │
                ▼ 1. Bulk Upload (Python / COPY)
┌────────────────────────────────┐
│        STAGING TABLES          │
│ - stage_ca_applications_raw    │
│ - stage_elec_install_raw       │
│ - stage_water_supply_raw       │
└───────────────┬────────────────┘
                │
                ▼ 2. Transform & Upsert (SQL Engine)
┌─────────────────────────────────────────────────────────────┐
│                       FINAL PRODUCTION TABLES                │
│                                                             │
│  ┌──────────────────────┐        1:1        ┌────────────┐  │
│  │     applications     ├──────────────────►│categories  │  │
│  │ (NEW, RENEW, etc.)   │                   └────────────┘  │
│  └──────────┬───────────┘                                   │
│             │                                               │
│             │ N:1 (Many applications share 1 license)       │
│             ▼                                               │
│  ┌──────────────────────┐        1:1        ┌────────────┐  │
│  │     certificates     ├──────────────────►│   zones    │  │
│  │(1 Unique Approval No)│                   └────────────┘  │
│  └──────────────────────┘                                   │
│             ▲                                               │
│             │ 1:1 Child Details                             │
│             ├───────────────────┬──────────────────────┐    │
│             │                   │                      │    │
│             ▼                   ▼                      ▼    │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────┐  │
│  │application_sector│  │   application_   │  │ documents │  │
│  │     _details     │  │   electrical_    │  │ contacts  │  │
│  │(Petroleum/Water) │  │   installation   │  │   etc.    │  │
│  └──────────────────┘  └──────────────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────┘
                │
                ▼ 3. Materialized View Refresh
┌─────────────────────────────────────────────────────────────┐
│                 API & PUBLIC MATERIALIZED VIEWS             │
│ - website_api_mv  (Public EWURA Portal)                     │
│ - tanesco_api_mv  (Electricity Sector Integration)           │
│ - npgis_api_mv    (Petroleum & Natural Gas GIS System)      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 2. Relationship Rules (Applications & Certificates)

Understanding the relationship between `applications` and `certificates` is essential:

### A. One Unique Certificate per License (`approval_no`)
- A physical EWURA license has a unique **`approval_no`** (e.g. `PWL-2016-006`, `PRL-2021-087`, `ELD-2022-107`).
- The `certificates` table stores **strictly 1 row per `approval_no`**. Duplicate certificate rows are not allowed.

### B. Applications (`NEW` vs `RENEW`)
- Over time, a license goes through multiple applications:
  1. Initial **`NEW`** application (e.g. created in 2016).
  2. First **`RENEW`** application (e.g. created in 2021).
  3. Second **`RENEW`** application (e.g. created in 2026).
- **All applications** sharing the same `approval_no` point to the **same single `certificate_id`** (`applications.certificate_id = certificates.id`).

### C. Latest Application Synchronization
- The single `certificates` record **always displays the latest active status**:
  - `certificates.application_id` $\rightarrow$ points to the **latest `RENEW` (or approved) application**.
  - `certificates.effective_date` & `approval_date` $\rightarrow$ updated to the latest application's approval date.
  - `certificates.expire_date` $\rightarrow$ updated to the latest application's expiration date.
  - `certificates.intimate_date` $\rightarrow$ updated to the latest application's reminder trigger date.

### D. Certificate Owner Rules (`certificate_owner`)
- **Electrical Installation Sector (`LICENSE_ELECTRICITY_INSTALLATION`)**:
  - `certificates.certificate_owner` is populated from `application_electrical_installation.applicant_name`.
- **All Other Sectors (Petroleum, Natural Gas, Electricity Supply, Water Supply)**:
  - `certificates.certificate_owner` is populated from `application_sector_details.facility_name` (formatted with `po_box` if present: `facility_name, po_box`), falling back to `company_name` or `tin_name`.
- **All `is_from_lois = true` applications** must have `certificate_owner` populated in `certificates`.

---

## ⏳ 3. License Durations & Reminder Thresholds

| Sector / License Type | Standard Duration | Reminder Threshold (`intimate_date`) |
| :--- | :---: | :--- |
| **Electrical Installation** | **5 Years** (60 months) | **90 days** before `expire_date` (`expire_date - 90 days`) |
| **Petroleum Operational** | **5 Years** (60 months) | **180 days** before `expire_date` (`expire_date - 180 days`) |
| **Petroleum Construction** | **2 Years** (24 months) | **180 days** before `expire_date` (`expire_date - 180 days`) |
| **Electricity Supply** | **5 Years** (60 months) | **180 days** before `expire_date` (`expire_date - 180 days`) |
| **Natural Gas** | **5 Years** (60 months) | **180 days** before `expire_date` (`expire_date - 180 days`) |
| **Water Supply** | **5 Years** (60 months) | **180 days** before `expire_date` (`expire_date - 180 days`) |

---

## 🌐 4. Sector Migration Pipelines

The migration system handles 5 distinct regulated sectors:

### 1. Petroleum Sector
- **Source**: Excel/CSV files uploaded to `POST /api/v1/application-migrations/upload` with `sector_name = PETROLEUM`.
- **Staging Table**: `stage_ca_applications_raw`.
- **Target Tables**: `applications` $\rightarrow$ `certificates` $\rightarrow$ `application_sector_details`.
- **License Types**: `LICENSE_PETROLEUM`, `CONSTRUCTION_PETROLEUM`.

### 2. Natural Gas Sector
- **Source**: Uploaded under `PETROLEUM` or specialized Natural Gas files.
- **Target Tables**: `applications` $\rightarrow$ `certificates` $\rightarrow$ `application_sector_details`.
- **License Types**: `LICENSE_NATURAL_GAS` (Operational categories like CNG filling stations) and `CONSTRUCTION_NATURAL_GAS` (Pipelines, processing facilities).

### 3. Electricity Supply Sector
- **Source**: Uploaded to `POST /api/v1/application-migrations/upload` with `sector_name = ELECTRICITY`.
- **Staging Table**: `stage_ca_applications_raw`.
- **Target Tables**: `applications` $\rightarrow$ `certificates`.
- **License Type**: `LICENSE_ELECTRICITY_SUPPLY`.

### 4. Electrical Installation Sector
- **Source**: Uploaded to `POST /api/v1/electrical-installations/upload`.
- **Staging Table**: `stage_elec_install_raw`.
- **Target Tables**: `applications` $\rightarrow$ `certificates` $\rightarrow$ `application_electrical_installation`.
- **License Type**: `LICENSE_ELECTRICITY_INSTALLATION` (Classes A, B, C, D, Wireman, Contractor).

### 5. Water Supply Sector
- **Source**: Uploaded to `POST /api/v1/water-supply/upload`.
- **Staging Table**: `stage_water_supply_raw`.
- **Target Tables**: `applications` $\rightarrow$ `certificates` $\rightarrow$ `application_sector_details`.
- **License Type**: `LICENSE_WATER`.

---

## 🌍 5. Geographical & Dynamic Zone Mapping Rules

- **Precedence**: When importing geographical columns (`region`, `district`, `ward`, `zone_name`), the uploaded string (`EXCLUDED`) strictly overwrites null or legacy database entries.
- **Dynamic Zone Mapping**: Instead of hardcoding static UUIDs, `zone_id` and `zone_name` are mapped dynamically during transformation by looking up incoming `region` names against `napa_regions` joined with `zones`.

---

## 🛡️ 6. eService Data Protection Rule

- Applications originating from the live eService system have `is_from_lois = false`.
- Legacy LOIS migration re-uploads (`is_from_lois = true`) **will NOT overwrite** `effective_date`, `expire_date`, or `is_from_lois` on native eService applications (`is_from_lois = false`).

---

## 🛠️ 7. Key Maintenance Scripts & Endpoints

| Maintenance Task | Command / Endpoint | Purpose |
| :--- | :--- | :--- |
| **Certificate Deduplication & Sync** | `POST /api/v1/admin-tools/fix-certificates`<br>or `python3 scripts/fix_certificates_unique_approval.py --apply` | Cleans up duplicate `approval_no` certificate rows, syncs `certificates` with latest application dates, and links all applications to single `certificate_id`. |
| **Natural Gas License Type Sync** | `python3 scripts/fix_natural_gas_license_types.py` | Ensures operational CNG filling stations use `LICENSE_NATURAL_GAS`. |
| **Electrical Expiration Alignment** | `python3 scripts/fix_electrical_license_dates.py --apply` | Aligns all Electrical Installation licenses to 5-year durations. |
| **Server Deployment** | `python3 scripts/deploy_remote.py` | Bundles and deploys codebase updates to server `10.1.8.144`. |

---

## 🖥️ 8. Environment & Server Details

- **Production API Server**: `10.1.8.144` (Path: `/var/lib/pgsql/scripts/ewura-migration`, Service: `ewura-migration-api`)
- **Production Database**: `10.1.8.166:5432/eservice_applications`
- **Swagger Documentation**: `http://10.1.8.144:8000/docs`
