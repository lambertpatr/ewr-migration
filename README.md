# EWURA Migration API — Upload Checklist & Reference

> **Quick rule:** Always run steps in the numbered order below. Each step depends on rows
> created in the previous one (e.g. users must exist before applications link to them).

---

## Prerequisites

Before touching any endpoint:

| # | Check | Command / action |
|---|-------|-----------------|
| 1 | Confirm target database | `./use-env.sh show` |
| 2 | Switch environment if needed | `./use-env.sh test` / `staging` / `production` |
| 3 | Start the API (restart after env switch) | `uvicorn app.main:app --reload` |
| 4 | Open Swagger UI | http://127.0.0.1:8000/docs |

---

## Upload Checklist — Step by Step

### Step 1 — Sync schema *(run once per environment)*

Aligns live DB tables/columns with the migration schema (adds missing columns, drops stale UNIQUE constraints). **Always run before any data upload.**

| | |
|---|---|
| **Endpoint** | `POST /api/v1/admin-tools/sync-schemas` |
| **Swagger tag** | `01 - Schema Sync` |
| **File required** | None |
| **Dry-run first** | `?dry_run=true` — preview SQL with no changes |
| **Apply** | `?dry_run=false` — executes ALTER TABLE / DROP CONSTRAINT |

> ⚠️ Schema sync **drops** certain UNIQUE constraints on `ca_applications` (except the one on `application_number`). Always dry-run first on production.

---

### Step 2 — Upload LOIS users

Creates user accounts in `public.users`. Must run **before** applications so FK links resolve.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/lois-users/upload` |
| **Swagger tag** | `02 - LOIS Users Migration` |
| **File field** | `file` — Excel/CSV with at least a `userid` / `username` column |
| **Key response fields** | `inserted_users`, `skipped_users` (skipped = already existed) |

> Re-runs are safe. Duplicate usernames are deduped within the batch before the bulk COPY.

---

### Step 3 — Upload Applications (Petroleum **or** Electricity)

Imports applications + documents + contacts via staging COPY. This is a **background job** — the endpoint returns a `job_id` immediately.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/application-migrations/upload` |
| **Swagger tag** | `03 - Applications Migration` |
| **File field** | `file` |
| **Form field** | `sector_name` — `PETROLEUM` (default) or `ELECTRICITY` |
| **Returns** | `job_id` — use it in Step 3a |

#### Step 3a — Poll job status

```
GET /api/v1/application-migrations/jobs/{job_id}
```

Response fields to verify:

| Field | Meaning |
|---|---|
| `status` | `RUNNING` / `COMPLETED` / `FAILED` |
| `result.inserted_applications` | Newly inserted rows |
| `result.inserted_users` | New user accounts created |
| `result.skipped_users` | Users that already existed |
| `result.inserted_user_roles` | New role assignments |
| `result.skipped_user_roles` | Role assignments already present |

> A `skipped_*` count is **normal on re-uploads** — it means those rows already existed.

---

### Step 4 — Upload Shareholders

| | |
|---|---|
| **Endpoint** | `POST /api/v1/shareholders/upload` |
| **Swagger tag** | `04 - Shareholders Migration` |
| **File field** | `file` — Excel/CSV with `apprefno` / `application_number` column |
| **Depends on** | Step 3 (applications must exist first) |

---

### Step 5 — Upload Managing Directors

| | |
|---|---|
| **Endpoint** | `POST /api/v1/managing-directors/upload` |
| **Swagger tag** | `05 - Managing Directors Migration` |
| **File field** | `file` |
| **Depends on** | Step 3 |

---

### Step 6 — Sync License Types

Syncs `license_type` values between source and target databases. No file needed.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/sync-license-type` |
| **Swagger tag** | `05 - Sync License Type` |
| **File required** | None — reads from DB directly |

---

### Step 7 — Upload Electrical Installations (main)

The largest import — fans out into **8 tables**: `applications`, `application_electrical_installation`, `personal_details`, `contact_details`, `attachments`, `work_experience`, `self_employed`, `certificate_verifications`. Background job.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/electrical-installations/upload` |
| **Swagger tag** | `06 - Electrical Installations Migration` |
| **File field** | `file` |
| **Key Excel columns** | `apprefno`, `licensecategoryclass` (→ `category_id`), `approvedclass` (→ `approved_class_id`) |
| **Depends on** | Step 1, Step 2 |

#### Category class UUID mapping (auto-applied)

The importer accepts both `CLASS A` style and bare codes (`A`, `S1`, …):

| Excel value | Code | Written to |
|---|---|---|
| CLASS A / A | A | `applications.category_id`, `application_electrical_installation.approved_class_id` |
| CLASS B / B | B | same |
| CLASS C / C | C | same |
| CLASS D / D | D | same |
| CLASS W / W | W | same |
| CLASS S1 / S1 | S1 | same |
| CLASS S2 / S2 | S2 | same |
| CLASS S3 / S3 | S3 | same |

> On re-upload, `category_id` and `approved_class_id` are **back-filled** on existing rows that previously had NULL in those columns.

#### Step 7a — Poll job status

```
GET /api/v1/electrical-installations/status/{job_id}
GET /api/v1/electrical-installations/jobs          ← list all recent jobs
```

Response fields to verify:

| Field | Meaning |
|---|---|
| `result.inserted_applications` | New application rows |
| `result.inserted_certificates` | New certificate rows |
| `result.inserted_electrical_installations` | New AEI rows |
| `result.inserted_users` / `result.skipped_users` | User creation stats |
| `result.inserted_user_roles` / `result.skipped_user_roles` | Role assignment stats |

---

### Step 8 — Upload Electrical Supervisors / Work Experience

| | |
|---|---|
| **Endpoint** | `POST /api/v1/electrical-installations/upload-supervisors-work-experience` |
| **Swagger tag** | `06 - Electrical Installations Migration` |
| **File field** | `file` |
| **Depends on** | Step 7 (electrical installations must exist) |

Poll: `GET /api/v1/electrical-installations/supervisors-status/{job_id}`

---

### Step 9 — Upload Supervisor Details

| | |
|---|---|
| **Endpoint** | `POST /api/v1/electrical-installations/upload-supervisor-details` |
| **Swagger tag** | `06 - Electrical Installations Migration` |
| **File field** | `file` |
| **Depends on** | Step 8 |

Poll: `GET /api/v1/electrical-installations/supervisor-details-status/{job_id}`

---

### Step 10 — Upload Self-Employed Records

| | |
|---|---|
| **Endpoint** | `POST /api/v1/electrical-installations/upload-self-employed` |
| **Swagger tag** | `06 - Electrical Installations Migration` |
| **File field** | `file` |
| **Depends on** | Step 7 |

Poll: `GET /api/v1/electrical-installations/self-employed-status/{job_id}`

---

### Step 11 — Upload Certificate Verifications

| | |
|---|---|
| **Endpoint** | `POST /api/v1/electrical-installations/upload-certificate-verifications` |
| **Swagger tag** | `06 - Electrical Installations Migration` |
| **File field** | `file` |
| **Depends on** | Step 7 (needs `application_id`; links `application_electrical_installation_id` best-effort) |

Poll: `GET /api/v1/electrical-installations/cert-verifications-status/{job_id}`

---

### Step 12 — Upload Categories

Upserts records into `categories` / `license_types` lookup tables.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/categories/upload` |
| **Swagger tag** | `07 - Categories Upload` |
| **File field** | `file` |

---

### Step 13 — Upload License Category Fees

| | |
|---|---|
| **Endpoint** | `POST /api/v1/license-categories/upload` |
| **Swagger tag** | `06 - License Category Fees Migration` |
| **File field** | `file` |

Poll: `GET /api/v1/license-categories/status/{job_id}`

---

### Step 14 — Backfill `created_by` *(optional, run after all uploads)*

Patches `created_by` on applications by matching `username` columns case-insensitively.

| | |
|---|---|
| **Endpoint** | `POST /api/v1/admin-tools/repair-and-backfill` |
| **Swagger tag** | `99 - Backfill created_by` |
| **File required** | None |

---

## Re-upload / Idempotency Notes

- **All importers are idempotent.** Re-uploading the same file is safe — no duplicates will be created.
- `inserted_* = 0` and `skipped_* > 0` on a re-upload is the **expected** result.
- `category_id` and `approved_class_id` are back-filled on existing rows — re-upload the electrical file to patch rows that previously had NULL in those columns.
- If FDW role tables (`public.users`, `public.roles`, `public.user_roles`) are unreachable, role assignment is skipped automatically and logged. This is **non-fatal**.

---

## Environment Reference

| Command | Use for |
|---|---|
| `./use-env.sh show` | Print the current active `.env` without switching |
| `./use-env.sh test` | Safe sandbox — run all experiments here first |
| `./use-env.sh staging` | Pre-production validation |
| `./use-env.sh production` | Live DB — use with caution |

> **Always restart `uvicorn` after switching environments.**

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ON CONFLICT DO UPDATE command cannot affect row a second time` | Two rows in the Excel share the same unique key | Source Excel has exact duplicates — deduplicate the file before uploading |
| `category_id` / `approved_class_id` still NULL | Rows existed before the UUID mapping was added | Re-upload the electrical file; `DO UPDATE` + post-insert UPDATE will back-fill it |
| `inserted_users = 0` | All usernames already exist | Normal on re-runs — check `skipped_users` count |
| Role assignment skipped entirely | FDW foreign tables unreachable | Non-fatal — re-run after FDW connection is restored |
| Job stuck at `RUNNING` after server restart | In-memory `_job_status` dict is lost on restart | Re-upload safely — upsert pattern prevents duplicate rows |
| Job status returns 404 | Wrong `job_id` or server was restarted | Check `GET .../jobs` to list recent jobs |
| No rows imported, no error | Header row not detected | Check `apprefno` column exists in the Excel; `file_reader.py` scans for it as an anchor |

---

## SQL Migration Scripts (`app/migrations/`)

Run these manually in your SQL client when the API endpoints are not applicable.

| File | Purpose |
|---|---|
| `align_live_schema_eservice_construction.sql` | Adds missing columns, drops stale UNIQUE constraints on live DB |
| `staging_schema.sql` | Creates `public.stage_*_raw` staging tables |
| `transform_into_final.sql` | INSERT from staging into final tables |
| `backfill_created_by_from_username.sql` | Patch `created_by` by username match |
| `20260219_add_is_from_lois.sql` | Adds `is_from_lois` column |
| `20260221_certificates_unique_application_number.sql` | Unique constraint on certificates |
| `extend_stage_schema_shareholders.sql` | Extends staging schema for shareholders |
