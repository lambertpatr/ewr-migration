# app/core/database.py
from typing import Generator, Optional
from app.core.config import settings

# Lazily create SQLAlchemy engine/sessionmaker so importing this module
# doesn't fail at import-time in environments where SQLAlchemy isn't
# installed. The actual engine is created the first time `get_db` is used.
_engine = None
_SessionLocal = None

def _init_engine():
    global _engine, _SessionLocal
    if _engine is None or _SessionLocal is None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        # Ensure username/password are percent-encoded so special
        # characters (for example '@' in a password) don't break URL
        # parsing. We only rewrite the credentials portion (before the
        # last '@') to preserve host/path text.
        def _sanitize_database_url(url: str) -> str:
            try:
                scheme, rest = url.split("://", 1)
            except ValueError:
                return url

            # If there's no '@' then nothing to do
            last_at = rest.rfind("@")
            if last_at == -1:
                return url

            credentials = rest[:last_at]
            host_and_path = rest[last_at + 1 :]

            # credentials may be 'user' or 'user:password'
            if ":" in credentials:
                user, password = credentials.split(":", 1)
            else:
                user, password = credentials, None

            from urllib.parse import quote_plus, unquote_plus

            # If the credentials were already percent-encoded (e.g.
            # password contains '%40'), unquote them first so we don't
            # double-encode. Then quote them properly.
            try:
                user = unquote_plus(user) if user is not None else user
            except Exception:
                pass
            if password is not None:
                try:
                    password = unquote_plus(password)
                except Exception:
                    pass

            user_q = quote_plus(user) if user is not None else ""
            if password is not None:
                password_q = quote_plus(password)
                new_rest = f"{user_q}:{password_q}@{host_and_path}"
            else:
                new_rest = f"{user_q}@{host_and_path}"

            return f"{scheme}://{new_rest}"

        db_url = _sanitize_database_url(settings.DATABASE_URL)
        _engine = create_engine(db_url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

def get_db() -> Generator:
    _init_engine()
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
