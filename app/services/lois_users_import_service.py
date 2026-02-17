# app/services/lois_users_import_service.py
from typing import Any
import contextlib
import uuid

REQUIRED_COLS = {
    "firstname",
    "lastname",
    "username",
    "password",
    "status",
    "mobilenumber",
    "emailid",
    "user_category",
    "account_type",
    "auth_mode",
    "role",
}

def clean_value(v):
    if v is None:
        return None
    v = str(v).strip()
    if v == "" or v.lower() == "nan":
        return None
    return v

def upper_value(v):
    v = clean_value(v)
    return v.upper() if v else None

class LoisUsersImportService:

    @staticmethod
    def import_users(db: Any, df, *, progress_cb=None, job_id: str | None = None, skip_existing: bool = False):
        # import sqlalchemy symbols lazily so importing this module doesn't
        # fail in environments where SQLAlchemy isn't installed. A clear
        # ImportError will be raised when this function is executed if the
        # dependency is missing.
        from sqlalchemy import text
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        inserted_users = 0
        updated_users = 0
        inserted_roles = 0
        inserted_user_roles = 0
        skipped = 0

        def _progress(step: str, **extra):
            if progress_cb:
                try:
                    progress_cb(job_id, step, **extra)
                except Exception:
                    # Progress should never break the import.
                    pass

        # 1) UPSERT USER (by username)
        upsert_user_sql = text("""
            INSERT INTO users (
                full_name,
                username,
                password_hash,
                status,
                phone_number,
                email_address,
                user_category,
                account_type,
                auth_mode,
                created_at,
                updated_at
            )
            VALUES (
                :full_name,
                :username,
                :password_hash,
                :status,
                :phone_number,
                :email_address,
                :user_category,
                :account_type,
                :auth_mode,
                now(),
                now()
            )
            ON CONFLICT (username)
            DO UPDATE SET
                full_name = EXCLUDED.full_name,
                password_hash = EXCLUDED.password_hash,
                status = EXCLUDED.status,
                phone_number = EXCLUDED.phone_number,
                email_address = EXCLUDED.email_address,
                user_category = EXCLUDED.user_category,
                account_type = EXCLUDED.account_type,
                auth_mode = EXCLUDED.auth_mode,
                updated_at = now()
            RETURNING id, (xmax = 0) AS inserted;
        """)

        # 2) Detect role table name (some schemas use 'role' while others use
        # 'roles') and build the upsert SQL accordingly. We use Postgres
        # to_regclass to check for table existence.
        def _detect_table(*candidates):
            for cand in candidates:
                # to_regclass returns the regclass if the table exists, else NULL
                r = db.execute(text(f"select to_regclass('public.{cand}')")).scalar()
                if r is not None:
                    return cand
            return None

        role_table = _detect_table('roles', 'role')
        if not role_table:
            raise ValueError("No role table found (looked for 'roles' and 'role')")

        upsert_role_sql = text(f"""
            INSERT INTO {role_table} (name, created_at, updated_at)
            VALUES (:name, now(), now())
            ON CONFLICT (name)
            DO UPDATE SET updated_at = now()
            RETURNING id, (xmax = 0) AS inserted;
        """)

        # If the role table doesn't have a UNIQUE constraint on `name`,
        # the ON CONFLICT clause above will raise an error. In that case
        # fall back to a safe manual upsert using SELECT -> INSERT/UPDATE
        def _manual_upsert_role(name_val: str):
            # Try to find existing role by name
            row = db.execute(text(f"SELECT id FROM {role_table} WHERE name = :name LIMIT 1"), {"name": name_val}).mappings().first()
            if row:
                role_id = row["id"]
                # update updated_at
                db.execute(text(f"UPDATE {role_table} SET updated_at = now() WHERE id = :id"), {"id": role_id})
                return role_id, False
            # insert new role with generated uuid
            new_id = str(uuid.uuid4())
            db.execute(text(f"INSERT INTO {role_table} (id, name, created_at, updated_at) VALUES (:id, :name, now(), now())"), {"id": new_id, "name": name_val})
            return new_id, True

        # 3) MAP USER_ROLE
        mapping_candidates = ('users_roles', 'user_roles', 'users_role', 'user_role')
        mapping_table = _detect_table(*mapping_candidates)
        if mapping_table:
            insert_user_role_sql = text(f"""
                INSERT INTO {mapping_table} (user_id, role_id, created_at)
                VALUES (:user_id, :role_id, now())
                ON CONFLICT (user_id, role_id) DO NOTHING;
            """)
        else:
            insert_user_role_sql = None

        errors = []
        tx_ctx = db.begin() if not getattr(db, 'in_transaction', lambda: False)() else contextlib.nullcontext()

        with tx_ctx:
            # 1) Create temp staging table
            _progress("staging:create-temp-table")
            db.execute(text("""
                CREATE TEMP TABLE IF NOT EXISTS staging_lois (
                    firstname text,
                    lastname text,
                    username text,
                    password_hash text,
                    status text,
                    mobilenumber text,
                    emailid text,
                    user_category text,
                    account_type text,
                    auth_mode text,
                    role text
                ) ON COMMIT DROP;
            """))

            # 2) COPY dataframe into staging using the same DBAPI connection
            # For large files, stream COPY in chunks to avoid building a huge CSV in memory.
            import io

            # Ensure columns match staging order and normalize values similarly
            df_copy = df.copy()
            # Normalize headers to the expected names used earlier
            df_copy.columns = (
                df_copy.columns.astype(str)
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('/', '_')
            )
            # Ensure all staging columns exist in dataframe (fill missing with empty)
            staging_cols = [
                'firstname','lastname','username','password','status','mobilenumber',
                'emailid','user_category','account_type','auth_mode','role'
            ]
            # Map 'password' -> 'password_hash' when exporting
            df_export = df_copy.reindex(columns=staging_cols).rename(columns={'password': 'password_hash'})
            # Strip whitespace from string columns
            for c in df_export.columns:
                df_export[c] = df_export[c].astype(str).str.strip()

            # Helper to stream chunks as CSV into COPY
            def _iter_csv_chunks(frame, chunk_rows: int = 50000):
                header_written = False
                for start in range(0, len(frame), chunk_rows):
                    chunk = frame.iloc[start : start + chunk_rows]
                    buf = io.StringIO()
                    chunk.to_csv(buf, index=False, header=not header_written)
                    header_written = True
                    buf.seek(0)
                    yield buf

            # Get raw DBAPI connection from the session's connection
            sa_conn = db.connection()
            raw_conn = sa_conn.connection
            cur = raw_conn.cursor()
            try:
                cur.execute("TRUNCATE TABLE staging_lois")
                total_rows = len(df_export)
                copied = 0
                chunk_rows = 50000
                for buf in _iter_csv_chunks(df_export, chunk_rows=chunk_rows):
                    cur.copy_expert("COPY staging_lois FROM STDIN WITH CSV HEADER", buf)
                    copied = min(total_rows, copied + chunk_rows)
                    _progress(
                        "staging:copy",
                        copied_rows=copied,
                        total_rows=total_rows,
                        percent=round((copied / total_rows) * 100, 2) if total_rows else 100.0,
                    )
                # IMPORTANT: do NOT commit the raw DBAPI connection here.
                # The staging table was created with `ON COMMIT DROP`, so an
                # early commit would drop the temp table and subsequent
                # statements (SELECT DISTINCT ... FROM staging_lois) would
                # fail with "relation staging_lois does not exist". We rely
                # on committing the SQLAlchemy session at the end of the
                # import so the temp table remains available for the whole
                # import transaction.
            finally:
                cur.close()

            staged_total = None
            staged_distinct_usernames = None
            try:
                staged_total = int(db.execute(text("SELECT COUNT(*) FROM staging_lois")).scalar() or 0)
                staged_distinct_usernames = int(db.execute(text("SELECT COUNT(DISTINCT TRIM(username)) FROM staging_lois WHERE username IS NOT NULL AND TRIM(username) <> ''")).scalar() or 0)
            except Exception:
                pass
            _progress(
                "staging:ready",
                staged_total=staged_total,
                staged_distinct_usernames=staged_distinct_usernames,
            )

            # 3) Upsert distinct roles in bulk
            _progress("roles:upsert:start")
            distinct_roles_sql = text(f"""
                WITH distinct_roles AS (
                    SELECT DISTINCT TRIM(UPPER(role)) AS role_name
                    FROM staging_lois WHERE role IS NOT NULL AND TRIM(role) <> ''
                )
                INSERT INTO {role_table} (id, name, created_at, updated_at)
                SELECT gen_random_uuid(), role_name, now(), now()
                FROM distinct_roles
                ON CONFLICT (name) DO UPDATE SET updated_at = now()
                RETURNING id, name;
            """)

            # Attempt the bulk insert inside a nested transaction (savepoint).
            # If the ON CONFLICT bulk INSERT fails (for example because the
            # role table lacks a UNIQUE constraint on `name`) the nested
            # transaction will roll back to the savepoint without aborting
            # the outer transaction; we can then run a fallback path using
            # the same DB connection so the temp table `staging_lois` stays
            # visible.
            try:
                with db.begin_nested():
                    role_rows = db.execute(distinct_roles_sql).mappings().all()
                    # count inserted/updated (approx): rows returned contain both
                    inserted_roles += len(role_rows)
            except Exception:
                # Fallback: table may not have UNIQUE constraint on name.
                # Insert missing roles one-by-one (still faster than per-user loop).
                # We stay on the same DB connection so the TEMP table remains visible.
                distinct_roles = db.execute(text("SELECT DISTINCT TRIM(UPPER(role)) AS role_name FROM staging_lois WHERE role IS NOT NULL AND TRIM(role) <> ''")).scalars().all()
                for rname in distinct_roles:
                    row = db.execute(text(f"SELECT id FROM {role_table} WHERE name = :name LIMIT 1"), {"name": rname}).mappings().first()
                    if row:
                        db.execute(text(f"UPDATE {role_table} SET updated_at = now() WHERE id = :id"), {"id": row['id']})
                    else:
                        new_id = str(uuid.uuid4())
                        db.execute(text(f"INSERT INTO {role_table} (id, name, created_at, updated_at) VALUES (:id, :name, now(), now())"), {"id": new_id, "name": rname})
                        inserted_roles += 1

            _progress("roles:upsert:done", inserted_roles=inserted_roles)

            # 4) Upsert users in bulk
            _progress("users:upsert:start", skip_existing=bool(skip_existing))

            conflict_clause = "DO NOTHING" if skip_existing else "DO UPDATE SET\n                        full_name = EXCLUDED.full_name,\n                        password_hash = EXCLUDED.password_hash,\n                        status = EXCLUDED.status,\n                        phone_number = EXCLUDED.phone_number,\n                        email_address = EXCLUDED.email_address,\n                        user_category = EXCLUDED.user_category,\n                        account_type = EXCLUDED.account_type,\n                        auth_mode = EXCLUDED.auth_mode,\n                        updated_at = now()"

            users_upsert_sql = text(f"""
                WITH user_rows_raw AS (
                    SELECT
                        TRIM(firstname) AS firstname,
                        TRIM(lastname) AS lastname,
                        TRIM(username) AS username,
                        password_hash,
                        TRIM(UPPER(status)) AS status,
                        TRIM(mobilenumber) as phone_number,
                        TRIM(emailid) AS email_address,
                        TRIM(UPPER(user_category)) AS user_category,
                        TRIM(UPPER(account_type)) AS account_type,
                        TRIM(UPPER(auth_mode)) AS auth_mode,
                        ctid AS _ctid
                    FROM staging_lois
                    WHERE username IS NOT NULL AND TRIM(username) <> ''
                ),
                user_rows AS (
                    -- Deduplicate rows for the same username within this batch.
                    -- Required because Postgres cannot "DO UPDATE" the same target row twice
                    -- within a single INSERT statement.
                    SELECT DISTINCT ON (username)
                        firstname,
                        lastname,
                        username,
                        password_hash,
                        status,
                        phone_number,
                        email_address,
                        user_category,
                        account_type,
                        auth_mode
                    FROM user_rows_raw
                    ORDER BY username, _ctid DESC
                ),
                ins_users AS (
                    INSERT INTO users (full_name, username, password_hash, status, phone_number, email_address, user_category, account_type, auth_mode, created_at, updated_at)
                    SELECT
                        (COALESCE(firstname,'') || ' ' || COALESCE(lastname,''))::text,
                        username,
                        password_hash,
                        COALESCE(status, 'ACTIVE'),
                        phone_number,
                        email_address,
                        COALESCE(user_category, 'EXTERNAL'),
                        COALESCE(account_type, 'INDIVIDUAL'),
                        COALESCE(auth_mode, 'DB'),
                        now(), now()
                    FROM user_rows
                    ON CONFLICT (username)
                    {conflict_clause}
                    RETURNING id, username
                )
                SELECT * FROM ins_users;
            """)

            user_rows = db.execute(users_upsert_sql).mappings().all()
            # Count inserted/updated approximately: we can't tell easily which
            # were inserts via RETURNING without tracking xmax; we'll set
            # inserted_users to number of returned rows for now.
            inserted_users += len(user_rows)
            # If skip_existing=True, RETURNING only includes inserted rows, so
            # we can estimate skipped as distinct_usernames - inserted.
            if skip_existing and staged_distinct_usernames is not None:
                skipped += max(0, int(staged_distinct_usernames) - int(inserted_users))

            _progress(
                "users:upsert:done",
                inserted_users=inserted_users,
                skipped_users=skipped,
                staged_distinct_usernames=staged_distinct_usernames,
            )

            errors = []
            # 5) Create mapping rows in bulk
            if mapping_table:
                _progress("user_roles:map:start", mapping_table=mapping_table)
                # Detect whether the mapping table has a `created_at` column.
                try:
                    has_created_at = bool(db.execute(
                        text(
                            "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :t AND column_name = 'created_at'"
                        ), {"t": mapping_table}
                    ).scalar())
                except Exception as e:
                    # If the detection fails for any reason, assume the safer
                    # path of not using created_at.
                    has_created_at = False
                    errors.append(f"column-detect-error: {e}")

                cols = "user_id, role_id, created_at" if has_created_at else "user_id, role_id"
                select_now = ", now()" if has_created_at else ""

                mapping_sql_onconflict = text(f"""
                    INSERT INTO {mapping_table} ({cols})
                    SELECT DISTINCT u.id, r.id{select_now}
                    FROM staging_lois s
                    JOIN users u ON u.username = TRIM(s.username)
                    JOIN {role_table} r ON UPPER(r.name) = TRIM(UPPER(s.role))
                    ON CONFLICT (user_id, role_id) DO NOTHING;
                """)

                # Try the fast ON CONFLICT path inside a savepoint so failures
                # don't abort the outer transaction (which would drop the temp
                # table). If that fails (for example because the mapping table
                # lacks a UNIQUE constraint), fall back to an INSERT ... WHERE
                # NOT EXISTS pattern which doesn't require a unique constraint.
                try:
                    with db.begin_nested():
                        res = db.execute(mapping_sql_onconflict)
                        try:
                            rc = int(res.rowcount)
                            if rc and rc > 0:
                                inserted_user_roles += rc
                        except Exception:
                            pass
                except Exception as e:
                    errors.append(f"mapping-onconflict-failed: {e}")
                    # Fallback path that works without a UNIQUE constraint.
                    mapping_sql_fallback = text(f"""
                        INSERT INTO {mapping_table} ({cols})
                        SELECT DISTINCT u.id, r.id{select_now}
                        FROM staging_lois s
                        JOIN users u ON u.username = TRIM(s.username)
                        JOIN {role_table} r ON UPPER(r.name) = TRIM(UPPER(s.role))
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {mapping_table} m WHERE m.user_id = u.id AND m.role_id = r.id
                        );
                    """)
                    try:
                        res2 = db.execute(mapping_sql_fallback)
                        try:
                            rc2 = int(res2.rowcount)
                            if rc2 and rc2 > 0:
                                inserted_user_roles += rc2
                        except Exception:
                            pass
                    except Exception as e2:
                        errors.append(f"mapping-fallback-failed: {e2}")
                        # Don't re-raise: we want to collect errors and continue.

                _progress("user_roles:map:done", inserted_user_roles=inserted_user_roles)

            # Done: temp table will be dropped on commit

        # Ensure the session is committed so changes persist.
        committed = False
        try:
            if hasattr(db, 'commit'):
                _progress("commit:start")
                db.commit()
                committed = True
                _progress("commit:done")
        except Exception as e:
            try:
                if hasattr(db, 'rollback'):
                    db.rollback()
            except Exception:
                pass
            errors.append(f"commit-failed: {e}")

        # Diagnostic: read back the total users count after commit so the
        # caller can see whether rows are visible outside the transaction.
        users_total_after = None
        try:
            users_total_after = db.execute(text("SELECT COUNT(*) FROM users")).scalar()
        except Exception as e:
            errors.append(f"post-commit-count-failed: {e}")

        # Additional diagnostics: report which database, user and host we
        # connected to so the caller can verify the target DB.
        db_diag = {}
        try:
            row = db.execute(text("SELECT current_database(), current_schema(), current_user, inet_server_addr(), inet_server_port()") ).first()
            if row is not None:
                db_diag = {
                    "current_database": row[0],
                    "current_schema": row[1],
                    "current_user": row[2],
                    "server_addr": str(row[3]) if row[3] is not None else None,
                    "server_port": int(row[4]) if row[4] is not None else None,
                }
        except Exception as e:
            errors.append(f"db-diag-failed: {e}")

        result = {
            "total_rows": len(df),
            "staged_total": staged_total,
            "staged_distinct_usernames": staged_distinct_usernames,
            "inserted_users": inserted_users,
            "updated_users": updated_users,
            "inserted_roles": inserted_roles,
            "inserted_user_roles": inserted_user_roles,
            "skipped": skipped,
            "committed": committed,
            "users_total_after": users_total_after,
        }
        if errors:
            result["errors"] = errors
        if db_diag:
            result["db_diag"] = db_diag
        return result
