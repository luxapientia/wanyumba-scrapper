"""
FastAPI application instance
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.core.database import init_db
from app.api import api_router
from app.services.jiji_service import JijiService
from app.services.kupatana_service import KupatanaService
from app.services.makazimapya_service import MakaziMapyaService
from app.services.ruaha_service import RuahaService
from app.services.sevenestate_service import SevenEstateService
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_application() -> FastAPI:
    """Create and configure FastAPI application"""

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="API for scraping and managing real estate listings",
        docs_url="/docs",
        redoc_url="/redoc"
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API router with prefix
    app.include_router(api_router, prefix=settings.API_PREFIX)

    # Startup event
    @app.on_event("startup")
    async def startup_event():
        logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
        logger.info(f"API documentation available at: /docs")

        # Initialize database tables if they don't exist
        try:
            init_db()
            logger.info("✓ Database initialized successfully")
        except Exception as e:
            logger.error(f"✗ Database initialization failed: {e}")

        # Initialize scrapers with delay between each to avoid conflicts
        logger.info("Initializing scrapers...")

        try:
            JijiService.get_instance()
        except Exception as e:
            logger.error(f"✗ Failed to initialize Jiji scraper: {e}")

        # Small delay before starting second browser to avoid conflicts
        import asyncio
        await asyncio.sleep(3)

        try:
            KupatanaService.get_instance()
        except Exception as e:
            logger.error(f"✗ Failed to initialize Kupatana scraper: {e}")

        # Small delay before starting third browser to avoid conflicts
        await asyncio.sleep(3)

        try:
            MakaziMapyaService.get_instance()
        except Exception as e:
            logger.error(f"✗ Failed to initialize MakaziMapya scraper: {e}")

        # Small delay before starting fourth browser to avoid conflicts
        await asyncio.sleep(3)

        try:
            RuahaService.get_instance()
        except Exception as e:
            logger.error(f"✗ Failed to initialize Ruaha scraper: {e}")

        # Small delay before starting fifth browser to avoid conflicts
        await asyncio.sleep(3)

        try:
            SevenEstateService.get_instance()
        except Exception as e:
            logger.error(f"✗ Failed to initialize SevenEstate scraper: {e}")

        # Log scraper status
        jiji_status = "ready" if JijiService.is_ready() else "not initialized"
        kupatana_status = "ready" if KupatanaService.is_ready() else "not initialized"
        makazimapya_status = "ready" if MakaziMapyaService.is_ready() else "not initialized"
        ruaha_status = "ready" if RuahaService.is_ready() else "not initialized"
        sevenestate_status = "ready" if SevenEstateService.is_ready() else "not initialized"
        logger.info(
            f"✓ Scraper status: jiji={jiji_status}, kupatana={kupatana_status}, makazimapya={makazimapya_status}, ruaha={ruaha_status}, sevenestate={sevenestate_status}")

    # Shutdown event
    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("Shutting down application...")
        JijiService.close_instance()
        KupatanaService.close_instance()
        MakaziMapyaService.close_instance()
        RuahaService.close_instance()
        SevenEstateService.close_instance()
        logger.info("✓ Shutdown complete")

    # Root endpoint
    @app.get("/")
    async def root():
        return {
            "app": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "docs": "/docs",
            "health": f"{settings.API_PREFIX}/health",
            "scrapers": {
                "jiji": "ready" if JijiService.is_ready() else "not initialized",
                "kupatana": "ready" if KupatanaService.is_ready() else "not initialized",
                "makazimapya": "ready" if MakaziMapyaService.is_ready() else "not initialized",
                "ruaha": "ready" if RuahaService.is_ready() else "not initialized",
                "sevenestate": "ready" if SevenEstateService.is_ready() else "not initialized"
            }
        }

    return app


# Create application instance
app = create_application()
