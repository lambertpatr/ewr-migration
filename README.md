# Ewura Migration (FastAPI)

This repo contains a FastAPI service and SQL scripts used to migrate d## Troubleshooting

### "ON CONFLICT DO UPDATE command cannot affect row a second time"
This happens when a single bulk insert statement tries to upsert the same unique key more than once.

Fix implemented: the LOIS users import deduplicates staging rows per `username` before the bulk upsert.

### "No data / looks stuck"
Use the job status endpoint(s) and server logs. The high-volume import path logs progress during:

- staging table creation
- COPY streaming
- SQL transform

---

## Switching between environments (Test / Staging / Production)

The project ships with three named environment files and a helper script so you can safely switch target databases without editing `.env` by hand.

### Environment files

| File | Environment | Status |
|---|---|---|
| `.env.test` | Local / test database | ✅ Active development |
| `.env.staging` | Staging database | ✅ Pre-production verification |
| `.env.production` | Production database | ⚠️ Live — use with extreme caution |

All three files are listed in `.gitignore` — credentials are never committed.

### `use-env.sh` — the switch script

```bash
# Switch to staging
./use-env.sh staging

# Switch to production
./use-env.sh production

# Switch back to test
./use-env.sh test

# Print which DB is currently active (password masked)
./use-env.sh show
```

After switching, **restart the server** so the new `.env` is loaded:

```bash
uvicorn app.main:app --reload
```

### Current connections

| Environment | Host | Database |
|---|---|---|
| `test` | `10.1.8.144:5432` | `auth_migration_v2` |
| `staging` | `10.1.8.144:5432` | `eservice_applications` |
| `production` | `<PRODUCTION_HOST>:5432` | `<PRODUCTION_DB>` |

> Fill in `<PRODUCTION_HOST>` and `<PRODUCTION_DB>` inside `.env.production` when the production server is ready.

> ⚠️ **Always run `./use-env.sh show` before executing a schema sync or bulk migration** to confirm you are targeting the intended database.

---

## Safety notes

- Always verify which database you're connected to before running schema sync.
- Schema sync is intentionally additive for missing columns, but **does drop unique constraints** as requested.
- Prefer running large migrations in a maintenance window.
- **Never point the server at production without a tested, verified migration run on staging first.**ce database using a **fast staging + Postgres COPY + SQL transform** approach.

## What’s included

### Migration APIs (Swagger)
Open the API docs:

- `GET /docs`

The docs are grouped using ordered tags:

1. `01 - Schema Sync`
2. `02 - LOIS Users Migration`
3. `03 - Applications Migration`
4. `04 - Shareholders Migration`
5. `99 - Backfill created_by`

### SQL migration scripts
Located in `app/migrations/`.

- `align_live_schema_eservice_construction.sql` – Aligns live schema for migration (adds missing columns and drops unwanted UNIQUE constraints).
- `staging_schema.sql` – Creates staging tables used for high-volume loads.
- `transform_into_final.sql` – Inserts from staging tables into final tables.
- `backfill_created_by_from_username.sql` – Backfills `created_by` based on username matching.

## How to run

### 1) Start the API

Create and activate a virtualenv, install deps, and run the server. Your exact command may differ depending on how your environment is set up, but the typical workflow is:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# start server (common)
uvicorn app.main:app --reload
```

Then open:

- `http://127.0.0.1:8000/docs`

### 2) Sync schema (live DB alignment)

This is intended to run against the **live** DB so the live tables match the working migration schema.

#### Option A: via API (recommended)

- Preview SQL (no changes):
  - `POST /api/v1/application-migrations/sync-schemas?dry_run=true`

- Apply SQL (executes ALTER TABLE / DROP CONSTRAINT):
  - `POST /api/v1/application-migrations/sync-schemas?dry_run=false`

> Warning: The schema sync drops UNIQUE constraints on `ca_applications` except the one on `application_number`.

#### Option B: run SQL manually

Run `app/migrations/align_live_schema_eservice_construction.sql` in your SQL client connected to the live DB.

### 3) Migrate LOIS users

Use the LOIS users upload endpoint (see Swagger for the exact request shape):

- `POST /api/v1/lois-users/upload`

Notes:

- The import uses TEMP staging + chunked COPY for speed.
- The importer deduplicates usernames within the uploaded batch.
- You can run in a mode that skips existing users (so re-runs are safe).
- Progress events are emitted when the API wires `progress_cb` into job status.

### 4) Migrate applications + documents + contacts

Use the applications migration upload endpoint:

- `POST /api/v1/application-migrations/upload`

Notes:

- Uses staging tables + COPY + set-based SQL transform for performance.
- Attachments are inserted only when `file_name` is present and `logic_doc_id` is a valid integer.
- Applications are deduped by `application_number`.

### 5) Backfill `created_by`

After applications are migrated, you can backfill `created_by` values by matching usernames between `ca_applications.username` and `users.username`.

- `POST /api/v1/application-migrations/backfill-created-by`

The matching is case-insensitive:

- `lower(trim(users.username)) = lower(trim(ca_applications.username))`

## Troubleshooting

### “ON CONFLICT DO UPDATE command cannot affect row a second time”
This happens when a single bulk insert statement tries to upsert the same unique key more than once.

Fix implemented: the LOIS users import deduplicates staging rows per `username` before the bulk upsert.

### “No data / looks stuck”
Use the job status endpoint(s) and server logs. The high-volume import path logs progress during:

- staging table creation
- COPY streaming
- SQL transform

## Safety notes

- Always verify which database you’re connected to before running schema sync.
- Schema sync is intentionally additive for missing columns, but **does drop unique constraints** as requested.
- Prefer running large migrations in a maintenance window.
