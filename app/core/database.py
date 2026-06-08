# app/core/database.py
from contextvars import ContextVar
from typing import Dict, Generator
from app.core.config import settings

# ---------------------------------------------------------------------------
# Per-request environment selection.
# Set via the X-DB-Env header (see app/main.py global dependency).
# Values: "dev" | "staging" | "prod"  (default: "prod")
# ---------------------------------------------------------------------------
_db_env_var: ContextVar[str] = ContextVar("db_env", default="dev")

# One engine + session factory cached per unique DB URL.
_engines: Dict[str, object] = {}
_session_factories: Dict[str, object] = {}


def _sanitize_database_url(url: str) -> str:
    """Percent-encode credentials so special chars (e.g. '@' in password) don't break URL parsing."""
    try:
        scheme, rest = url.split("://", 1)
    except ValueError:
        return url

    last_at = rest.rfind("@")
    if last_at == -1:
        return url

    credentials = rest[:last_at]
    host_and_path = rest[last_at + 1:]

    if ":" in credentials:
        user, password = credentials.split(":", 1)
    else:
        user, password = credentials, None

    from urllib.parse import quote_plus, unquote_plus

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


def _url_for_env(env: str) -> str:
    """Return the DATABASE_URL for the given environment label."""
    env = (env or "prod").strip().lower()
    mapping = {
        "dev": settings.DATABASE_URL_DEV,
        "staging": settings.DATABASE_URL_STAGING,
        "prod": settings.DATABASE_URL_PROD,
    }
    return mapping.get(env) or settings.DATABASE_URL


def _get_session_factory(url: str):
    """Return (and lazily create) the session factory for the given DB URL."""
    if url not in _session_factories:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        clean = _sanitize_database_url(url)
        engine = create_engine(clean, pool_pre_ping=True)
        _engines[url] = engine
        _session_factories[url] = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return _session_factories[url]


def new_session():
    """Create a raw DB session for the current request environment.

    Use this in background tasks that need a fresh session after the
    request has already yielded (replaces the old _get_new_session pattern).
    The environment is inherited from the request's ContextVar copy.
    """
    url = _url_for_env(_db_env_var.get())
    return _get_session_factory(url)()


def get_db() -> Generator:
    """FastAPI dependency: yields a DB session for the current request env."""
    url = _url_for_env(_db_env_var.get())
    factory = _get_session_factory(url)
    db = factory()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Legacy shim kept for backward-compat with routers that still call
#   db_module._init_engine(); db_module._SessionLocal()
# They are safe to migrate to db_module.new_session() at any time.
# ---------------------------------------------------------------------------
_engine = None
_SessionLocal = None


def _init_engine():
    """Legacy init — routers should prefer new_session() instead."""
    global _engine, _SessionLocal
    url = _url_for_env(_db_env_var.get())
    factory = _get_session_factory(url)
    _engine = _engines[url]
    _SessionLocal = factory
