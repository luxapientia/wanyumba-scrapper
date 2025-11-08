"""
Application configuration management
"""
import os
from typing import Optional
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
    SCRAPER_HEADLESS: bool = False
    SCRAPER_MAX_PAGES: int = 5
    SCRAPER_MAX_LISTINGS: int = 50
    
    # Browser Profiles
    JIJI_PROFILE_DIR: str = "./jiji_browser_profile"
    KUPATANA_PROFILE_DIR: str = "./kupatana_browser_profile"
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_PREFIX: str = "/api/v1"
    
    # Security
    SECRET_KEY: str = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    
    # CORS
    CORS_ORIGINS: list = [
        "http://localhost:3000",
        "http://localhost:5173",  # Vite dev server
        "http://localhost:8000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ]
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()

