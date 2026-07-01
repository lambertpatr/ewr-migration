# EWURA Migration Workspace Guidelines

These are automatically generated rules and patterns learned from Google Antigravity regarding the EWURA Migration project.

## Database & Analytics Architecture
- **Staging vs Final Tables:** The migration process relies heavily on temporary staging tables (e.g. `stage_elec_install_raw`, `stage_ca_applications_raw`, `stage_water_supply_raw`). Data is imported from Excel/CSV into these staging tables first.
- **Applications Upsert Logic:** The core `applications` table is the source of truth for all sector applications. The UPSERTs heavily rely on Postgres `ON CONFLICT (application_number) DO UPDATE SET`.
- **Geographical Data Priority:** When inserting geographical data (region, district, ward, zone_id, zone_name), the uploaded file (`EXCLUDED`) takes strict precedence over any pre-existing database records. The `COALESCE(EXCLUDED.column, public.applications.column)` pattern should always be followed.
- **Dynamic Zone Mapping:** Instead of hardcoding UUIDs, geographic `zone_id` and `zone_name` are dynamically mapped via an in-memory SQL lookup (`napa_regions` joined with `zones`) based on the incoming text string `region`.

## FastAPI Best Practices
- **Deployment Script:** There is a custom `./deploy.sh` script used to bundle and push changes to the production server (`10.1.8.144` and `10.1.8.166`). Always run this after verifying changes.
- **Python SQLAlchemy:** Core logic uses SQLAlchemy's `.insert()` and `.mappings()` bulk patterns rather than ORM instantiation for performance.

## Historical Backfilling
- When schema changes are made to the parsing engine, historical database records will NOT automatically update. SQL scripts (`UPDATE ... FROM ...`) must be run directly on the database to backfill older records where necessary.
