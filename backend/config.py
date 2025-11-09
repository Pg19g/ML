"""Configuration management for the quant platform."""

import os
from functools import lru_cache
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # Database
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./quant_platform.db")

    # API Keys
    eodhd_api_key: str = os.getenv("EODHD_API_KEY", "")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")

    # Application
    app_name: str = "Quant Platform"
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"

    # CORS
    cors_origins: list = ["*"]

    # Cache settings
    cache_enabled: bool = True
    cache_ttl_seconds: int = 86400  # 24 hours

    # Backtesting
    default_initial_cash: float = 10000.0
    default_commission: float = 0.001  # 0.1%

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
