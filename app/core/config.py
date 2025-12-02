"""
Application configuration management
"""
import os
from typing import Optional, List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "Real Estate Scraper API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = os.getenv(
        'DATABASE_URL',
        'postgresql://postgres:postgres@localhost:5432/real_estate_db'
    )

    # Scraper Settings
    JIJI_EMAIL: Optional[str] = None
    JIJI_PASSWORD: Optional[str] = None
    SCRAPER_HEADLESS: bool = True
    SCRAPER_MAX_PAGES: int = 5
    SCRAPER_MAX_LISTINGS: int = 50

    # Browser Profiles
    JIJI_PROFILE_DIR: str = "./jiji_browser_profile"
    KUPATANA_PROFILE_DIR: str = "./kupatana_browser_profile"
    MAKAZIMAPYA_PROFILE_DIR: str = "./makazimapya_browser_profile"
    RUAHA_PROFILE_DIR: str = "./ruaha_browser_profile"
    SEVENESTATE_PROFILE_DIR: str = "./sevenestate_browser_profile"
    BEFORWARD_PROFILE_DIR: str = "./beforward_browser_profile"
    IPH_PROFILE_DIR: str = "./iph_browser_profile"

    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8002
    API_PREFIX: str = "/api/v1"

    # CORS Settings
    CORS_ORIGINS: List[str] = [
        "http://localhost:5175",  # Scraper admin frontend
        "http://localhost:5173",   # Alternative frontend port
        "http://localhost:3000",   # Alternative frontend port
        "http://localhost:3006",   # Admin frontend
    ]

    # Security
    SECRET_KEY: str = os.getenv(
        'SECRET_KEY', 'your-secret-key-change-in-production')
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra environment variables (e.g., POSTGRES_USER for docker-compose)


# Global settings instance
settings = Settings()
