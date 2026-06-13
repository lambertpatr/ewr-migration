# app/core/database.py
import logging
from contextvars import ContextVar
from pathlib import Path
from typing import Dict, Generator, Optional

from app.core.config import settings

# ---------------------------------------------------------------------------
# Per-request environment selection.
# Set via the X-DB-Env header (see app/main.py global dependency).
# Values: "dev" | "staging" | "prod"  (default: "prod")
# ---------------------------------------------------------------------------
from enum import Enum

class DBEnv(str, Enum):
    """Allowed database environment labels."""
    dev     = "dev"
    staging = "staging"
    prod    = "prod"

_db_env_var: ContextVar[str] = ContextVar("db_env", default="dev")

# One engine + session factory cached per unique DB URL.
_engines: Dict[str, object] = {}
_session_factories: Dict[str, object] = {}
# Cache resolved URLs per environment so we only hit the filesystem once.
_env_url_cache: Dict[str, str] = {}

logger = logging.getLogger(__name__)


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

def _mask_url(url: str) -> str:
    """Return the URL with credentials masked for logging."""

    if "@" not in url:
        return url
    try:
        prefix, rest = url.split("@", 1)
        scheme, creds = prefix.split("//", 1)
        return f"{scheme}//***:***@{rest}"
    except ValueError:
        return "***masked***"


def _load_url_from_env_file(env: str) -> Optional[str]:
    """Attempt to load DATABASE_URL from .env.<env> (if present)."""

    project_root = Path(__file__).resolve().parents[2]
    candidate = project_root / f".env.{env}"
    if not candidate.exists():
        return None

    try:
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    except Exception:
        return None
    return None


def _url_for_env(env: str) -> str:
    """Return the DATABASE_URL for the given environment label.

    Resolution order:
      1. Cached value for the environment (from a previous lookup).
      2. Explicit environment-specific setting (DATABASE_URL_<ENV>).
      3. Value stored in .env.<env> file (if present).
      4. Fallback to the main DATABASE_URL (dev).
    """

    env = (env or "prod").strip().lower()

    if env in _env_url_cache:
        return _env_url_cache[env]

    # 1) Explicit environment-specific Settings attribute
    attr_name = f"DATABASE_URL_{env.upper()}"
    url_from_settings = getattr(settings, attr_name, None)
    if url_from_settings:
        _env_url_cache[env] = url_from_settings
        logger.info("[db-env] %s -> settings.%s (%s)", env, attr_name, _mask_url(url_from_settings))
        return url_from_settings
    # 2) Try reading .env.<env>
    file_url = _load_url_from_env_file(env)
    if file_url:
        _env_url_cache[env] = file_url
        logger.info("[db-env] %s -> .env.%s (%s)", env, env, _mask_url(file_url))
        return file_url
    # 3) Fallback to default DATABASE_URL (typically dev)
    _env_url_cache[env] = settings.DATABASE_URL
    logger.info("[db-env] %s -> default DATABASE_URL (%s)", env, _mask_url(settings.DATABASE_URL))
    return settings.DATABASE_URL

def _get_session_factory(url: str):
    """Return (and lazily create) the session factory for the given DB URL."""
    if url not in _session_factories:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        clean = _sanitize_database_url(url)
        logger.info(
            "[db-connect] 🔌 Creating NEW engine for: %s",
            _mask_url(url),
        )
        engine = create_engine(clean, pool_pre_ping=True)
        _engines[url] = engine
        _session_factories[url] = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        logger.info(
            "[db-connect] ✅ Engine ready — pool_pre_ping=True | url=%s",
            _mask_url(url),
        )
    return _session_factories[url]


def new_session():
    """Create a raw DB session for the current request environment.

    Use this in background tasks that need a fresh session after the
    request has already yielded (replaces the old _get_new_session pattern).
    The environment is inherited from the request's ContextVar copy.
    """
    env = _db_env_var.get()
    url = _url_for_env(env)
    logger.info(
        "[db-connect] 🗄  Opening session | env=%-8s | url=%s",
        env,
        _mask_url(url),
    )
    return _get_session_factory(url)()


def get_db() -> Generator:
    """FastAPI dependency: yields a DB session for the current request env."""
    env = _db_env_var.get()
    url = _url_for_env(env)
    logger.info(
        "[db-connect] 🗄  Opening session | env=%-8s | url=%s",
        env,
        _mask_url(url),
    )
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
