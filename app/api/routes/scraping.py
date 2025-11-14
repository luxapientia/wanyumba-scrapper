"""
Scraping endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
from app.core.database import get_db
from app.services.database_service import DatabaseService
from app.services.jiji_service import JijiService
from app.services.kupatana_service import KupatanaService
from app.services.makazimapya_service import MakaziMapyaService
from app.services.base_scraper_service import BaseScraperService
from app.api.schemas.scraping import (
    ScrapeAllRequest,
    ScrapeSelectedRequest,
    ScrapeResponse,
    ScrapeDetailedResponse,
    StopScrapingRequest,
    ScrapingStatusResponse,
    AutoCycleRequest
)
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Registry of available scraper services
# Each service's site_name will be used to match against target_site
SCRAPER_SERVICES: List[type[BaseScraperService]] = [
    JijiService,
    KupatanaService,
    MakaziMapyaService,
]

def get_scraper_service(target_site: str) -> Optional[BaseScraperService]:
    """
    Get scraper service instance by target_site name.
    Uses the service's site_name attribute to match.
    
    Args:
        target_site: The site name to match (e.g., 'jiji', 'kupatana')
        
    Returns:
        Scraper service instance or None if not found
    """
    target_site_lower = target_site.lower()
    
    for service_class in SCRAPER_SERVICES:
        # Get instance to check site_name
        try:
            instance = service_class.get_instance()
            if instance and instance.site_name.lower() == target_site_lower:
                return instance
        except Exception as e:
            logger.debug(f"Could not get instance for {service_class.__name__}: {e}")
            continue
    
    return None

# Note: Auto cycle status is now managed by each scraper service instance


# Background task functions have been moved to BaseScraperService
# Use scraper.scrape_all_listings_async(), scraper.scrape_detailed_listings_async(), etc.


@router.post("/scrape-listings", response_model=ScrapeResponse)
async def scrape_all_listings(
    request: ScrapeAllRequest,
    db: Session = Depends(get_db)
):
    """
    Scrape all listings from a site (basic info: url, title, price)

    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    - **max_pages**: Maximum number of pages to scrape (optional)
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Get scraper instance dynamically
        scraper = get_scraper_service(request.target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {request.target_site}"
            )

        # Check if scraper is already scraping
        if scraper.is_scraping_now():
            raise HTTPException(
                status_code=409,
                detail=f"{request.target_site.capitalize()} scraper is already scraping. Please wait for the current operation to complete."
            )

        if request.save_to_db:
            # Run scraping in background using service method
            scraper.scrape_all_listings_async(
                max_pages=request.max_pages,
                db_session=None  # Service will create its own session in the thread
            )

            return {
                "status": "started",
                "message": f"Scraping {request.target_site} listings in background",
                "target_site": request.target_site
            }
        else:
            # Run synchronously and return results
            listings = scraper.get_all_listings_basic(
                max_pages=request.max_pages)

            return {
                "status": "completed",
                "message": f"Scraped {len(listings)} listings",
                "target_site": request.target_site,
                "count": len(listings),
                "data": listings
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape-detailed", response_model=ScrapeDetailedResponse)
async def scrape_detailed_listings(
    request: ScrapeSelectedRequest,
    db: Session = Depends(get_db)
):
    """
    Scrape detailed data for selected URLs

    - **urls**: List of listing URLs to scrape
    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Get scraper instance dynamically
        scraper = get_scraper_service(request.target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {request.target_site}"
            )

        # Check if scraper is already scraping
        if scraper.is_scraping_now():
            raise HTTPException(
                status_code=409,
                detail=f"{request.target_site.capitalize()} scraper is already scraping. Please wait for the current operation to complete."
            )

        if request.save_to_db:
            # Run scraping in background using service method
            scraper.scrape_detailed_listings_async(
                urls=request.urls,
                db_session=None  # Service will create its own session in the thread
            )

            return {
                "status": "started",
                "message": f"Scraping {len(request.urls)} detailed listings in background",
                "target_site": request.target_site,
                "urls_count": len(request.urls)
            }
        else:
            # Run synchronously and return results
            detailed_listings = []
            for url in request.urls:
                try:
                    data = scraper.extract_detailed_data(
                        url,
                        db_session=db,
                        target_site=request.target_site
                    )
                    if data and 'error' not in data:
                        detailed_listings.append(data)
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    continue

            return {
                "status": "completed",
                "message": f"Scraped {len(detailed_listings)} detailed listings",
                "target_site": request.target_site,
                "urls_count": len(request.urls),
                "success_count": len(detailed_listings),
                "data": detailed_listings
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape-all-detailed", response_model=ScrapeResponse)
async def scrape_all_detailed(
    request: ScrapeAllRequest,
    db: Session = Depends(get_db)
):
    """
    Scrape all listings and their detailed data from a site

    This is a two-step process:
    1. Scrape all listing URLs (basic info)
    2. Scrape detailed data for each URL

    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    - **max_pages**: Maximum number of pages to scrape (optional)
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Get scraper instance dynamically
        scraper = get_scraper_service(request.target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {request.target_site}"
            )

        # Check if scraper is already scraping
        if scraper.is_scraping_now():
            raise HTTPException(
                status_code=409,
                detail=f"{request.target_site.capitalize()} scraper is already scraping. Please wait for the current operation to complete."
            )

        # This operation can take a long time, so always run in background
        scraper.scrape_all_with_details_async(
            max_pages=request.max_pages,
            db_session=None  # Service will create its own session in the thread
        )

        return {
            "status": "started",
            "message": f"Scraping all {request.target_site} listings with details in background. This may take a while.",
            "target_site": request.target_site
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape-all-details", response_model=ScrapeDetailedResponse)
async def scrape_all_details(
    request: ScrapeAllRequest,
    db: Session = Depends(get_db)
):
    """
    Scrape detailed data for all existing listings in the database

    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Get scraper instance dynamically
        scraper = get_scraper_service(request.target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {request.target_site}"
            )

        # Check if scraper is already scraping
        if scraper.is_scraping_now():
            raise HTTPException(
                status_code=409,
                detail=f"{request.target_site.capitalize()} scraper is already scraping. Please wait for the current operation to complete."
            )

        if request.save_to_db:
            # Get count of listings first
            db_service = DatabaseService(db)
            listings = db_service.get_all_listings(
                lightweight=True, target_site=request.target_site)
            urls_count = len(listings)

            if urls_count == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No listings found in database for {request.target_site}. Please scrape listings first."
                )

            # Run scraping in background using service method
            scraper.scrape_all_details_async(
                db_session=None  # Service will create its own session in the thread
            )

            return {
                "status": "started",
                "message": f"Scraping details for {urls_count} existing {request.target_site} listings in background",
                "target_site": request.target_site,
                "urls_count": urls_count
            }
        else:
            raise HTTPException(
                status_code=400,
                detail="Synchronous scraping of all details is not supported. Please use save_to_db=true."
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop-scraping", response_model=ScrapeResponse)
async def stop_scraping(
    request: StopScrapingRequest
):
    """
    Stop the current scraping operation for a specific site
    This will also stop the auto cycle if it's running

    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    """
    try:
        target_site = request.target_site.lower()
        
        # Get scraper instance dynamically
        scraper = get_scraper_service(target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {target_site}"
            )
        
        stopped_items = []
        
        # Stop regular scraping
        if scraper.is_scraping_now():
            scraper.stop_scraping()
            stopped_items.append(f"{target_site} scraper")
        
        # Stop auto cycle if running
        if scraper.is_auto_cycle_running():
            scraper.stop_auto_cycle()
            stopped_items.append(f"{target_site} auto cycle")
            logger.info(f"Stopping auto cycle for {target_site}")
        
        if not stopped_items:
            raise HTTPException(
                status_code=400,
                detail=f"No scraping operations are currently running for {target_site}"
            )
        
        return {
            "status": "stopped",
            "message": f"Stop signal sent to: {', '.join(stopped_items)}. Will stop after completing the current operation.",
            "target_site": target_site
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error stopping scraping: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start-auto-cycle", response_model=ScrapeResponse)
async def start_auto_cycle(
    request: AutoCycleRequest
):
    """
    Start automatic scraping cycle
    
    The cycle will continuously:
    1. Scrape basic listings
    2. Scrape details for listings without agent info
    3. Wait for specified delay
    4. Repeat
    
    - **target_site**: Site name (e.g., 'jiji', 'kupatana')
    - **max_pages**: Maximum pages to scrape per cycle (optional)
    - **cycle_delay_minutes**: Minutes to wait between cycles (default: 30)
    - **headless**: Run browser in headless mode (default: true)
    """
    try:
        target_site = request.target_site.lower()
        
        # Get scraper instance dynamically
        scraper = get_scraper_service(target_site)
        if not scraper:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {target_site}"
            )
        
        # Check if already running
        if scraper.is_auto_cycle_running():
            raise HTTPException(
                status_code=400,
                detail=f"Auto cycle already running for {target_site}"
            )
        
        # Start auto cycle using the service method
        # Note: db_session is None - the auto cycle will create its own session in the thread
        success = scraper.start_auto_cycle(
            max_pages=request.max_pages,
            cycle_delay_minutes=request.cycle_delay_minutes,
            db_session=None  # Auto cycle creates its own session in the thread
        )
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to start auto cycle for {target_site}"
            )
        
        logger.info(f"Started auto cycle for {target_site}")
        
        return {
            "status": "started",
            "message": f"Auto cycle started for {target_site}. Cycles will run every {request.cycle_delay_minutes} minutes.",
            "target_site": target_site
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error starting auto cycle: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=ScrapingStatusResponse)
async def get_scraping_status():
    """
    Get the current scraping status for all scrapers

    Returns the current status of all registered scrapers,
    including whether they are currently scraping, their progress,
    and auto cycle status.
    """
    try:
        status_dict = {}
        
        # Get status for all registered scrapers
        for service_class in SCRAPER_SERVICES:
            try:
                site_name = None
                status = None
                
                # Always try to get instance first to get site_name
                # This will create the instance if it doesn't exist
                try:
                    instance = service_class.get_instance()
                    if instance:
                        site_name = instance.site_name
                        
                        # Now check if ready and get status
                        if service_class.is_ready():
                            status = service_class.get_status()
                        else:
                            # Check if auto cycle is running even if scraper not ready
                            if instance.is_auto_cycle_running():
                                status = {
                                    'is_scraping': False,
                                    'auto_cycle_running': True
                                }
                            else:
                                # Service exists but not ready
                                status = {
                                    'is_scraping': False,
                                    'auto_cycle_running': False,
                                    'status': 'not_ready'
                                }
                except Exception as inst_error:
                    # If get_instance fails, try to get site_name from _instance attribute
                    logger.debug(f"Could not get instance for {service_class.__name__}: {inst_error}")
                    if hasattr(service_class, '_instance') and service_class._instance is not None:
                        site_name = service_class._instance.site_name
                        status = {
                            'is_scraping': False,
                            'auto_cycle_running': False,
                            'status': 'not_ready'
                        }
                
                # Add to status dict if we have a site_name
                if site_name:
                    status_dict[site_name] = status
                    
            except Exception as e:
                logger.debug(f"Error getting status for {service_class.__name__}: {e}")
                continue

        return status_dict
    except Exception as e:
        logger.error("Error getting scraping status: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
