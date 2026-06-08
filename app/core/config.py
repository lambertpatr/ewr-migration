# app/core/config.py
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str  # default / fallback
    DATABASE_URL_DEV: Optional[str] = None
    DATABASE_URL_STAGING: Optional[str] = None
    DATABASE_URL_PROD: Optional[str] = None

    class Config:
        env_file = ".env"

settings = Settings()
