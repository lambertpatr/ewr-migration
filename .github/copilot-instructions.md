# Copilot instructions (ewura-migration)

## Big picture
- This repo is a **FastAPI “migration API”** that loads Excel/CSV into Postgres using a **staging table + `COPY` + set-based SQL transform** pattern for speed and idempotency.
- API entrypoint is `app/main.py` which mounts routers under `app/api/v1/*` (Swagger tags are intentionally ordered like `01 - ...`, `06 - ...`).
- Each upload endpoint reads a file into a pandas `DataFrame` via `app/utils/file_reader.py`, then calls a service in `app/services/*_import_service.py`.
- Services use SQLAlchemy `text()` + raw `COPY` via psycopg2 cursor for bulk load, then run CTE-heavy SQL to upsert into final tables.

## How imports are structured (copy this pattern)
- Endpoint pattern (sync or background job): see `app/api/v1/electrical_installations_upload.py` and `app/api/v1/electrical_installations_supervisors_upload.py`.
  - `_get_new_session()` initializes the lazy engine (`app/core/database.py`) and creates a session.
  - Background jobs store progress in a module-level `_job_status` dict and update it via a `progress_cb` callback.
- Service pattern: see `app/services/electrical_supervisors_import_service.py`.
  1) Normalize header names (`strip/lower/space->_`) and rename aliases.
  2) Drop + create a `public.stage_*_raw` table.
  3) Stream rows into staging using `cursor.copy_expert(COPY ... FROM STDIN WITH CSV)`.
  4) “Schema guard”: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` (and sometimes drop/replace FKs) so imports can run even on partially-aligned DBs.
  5) Transform via SQL CTEs; keep imports **idempotent** using stable UUIDs derived from `md5(...)::uuid` and `ON CONFLICT (id) DO UPDATE`.
  6) Deduplicate within the same upload using `SELECT DISTINCT ON (...)` to avoid Postgres error: `ON CONFLICT DO UPDATE command cannot affect row a second time`.

## Database conventions specific to this repo
- Primary join key from Excel is usually `apprefno` / `application_number` → `public.applications.application_number`.
- Child tables often link by both:
  - `application_id` (required, via inner join)
  - `application_electrical_installation_id` (best-effort, via left join)
  Example: certificate verifications importer (`app/services/electrical_certificate_verifications_import_service.py`).
- Electrical installation individual import is a multi-table fan-out: `applications` → `application_electrical_installation` + contact/personal/attachments/work_experience/self_employed/certificate_verifications (see header comment in `app/services/electrical_installation_import_service.py`).

## Running locally (macOS/zsh)
- Configure DB via `.env` (only `DATABASE_URL` is required). Use `./use-env.sh {test|staging|production|show}`; **restart uvicorn after switching** (see `use-env.sh`).
- Start API: `uvicorn app.main:app --reload` (see `README.md`).
- Open Swagger at `/docs` and use the upload endpoints.

## Repo-specific helper behavior
- `app/utils/file_reader.py` tries to auto-detect the real Excel header row by scanning for anchor columns like `apprefno`.
- When touching DB connection logic, preserve URL sanitization in `app/core/database.py` (percent-encodes credentials so `@` in passwords doesn’t break parsing).

## When adding/adjusting an importer
- Prefer adding a new `public.stage_*_raw` table rather than inserting row-by-row.
- Keep response payloads small by default; row previews are optional (`include_rows`, `limit_rows`) and capped.
- If you need location mapping, reuse the preloaded CSV maps in `app/services/application_migrations_service.py` (`region_map_csv`, `district_map_csv`, `ward_map_csv`).
