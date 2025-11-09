"""
Scraping endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
from app.core.database import get_db, SessionLocal
from app.services.database_service import DatabaseService
from app.services.jiji_service import JijiService
from app.services.kupatana_service import KupatanaService
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
import threading
import time

router = APIRouter()
logger = logging.getLogger(__name__)

# Global dictionary to track auto cycle status
auto_cycle_status = {
    'jiji': {'running': False, 'thread': None, 'should_stop': False},
    'kupatana': {'running': False, 'thread': None, 'should_stop': False}
}


def _scrape_all_listings_task(
    target_site: str,
    max_pages: Optional[int]
):
    """Background task to scrape all listings"""
    # Create a new database session for the background task
    db = SessionLocal()
    try:
        # Get the appropriate scraper instance
        if target_site.lower() == 'jiji':
            scraper = JijiService.get_instance()
        elif target_site.lower() == 'kupatana':
            scraper = KupatanaService.get_instance()
        else:
            raise ValueError(f"Unknown target site: {target_site}")

        logger.info(f"Starting to scrape all listings from {target_site}")

        # Get all listings (basic info) and save to database immediately after each page
        listings = scraper.get_all_listings_basic(
            max_pages=max_pages, db_session=db, target_site=target_site)

        logger.info(f"Scraped {len(listings)} listings from {target_site}")

    except Exception as e:
        logger.error(f"Error scraping listings from {target_site}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()


def _scrape_detailed_listings_task(
    urls: List[str],
    target_site: str
):
    """Background task to scrape detailed listings"""
    # Create a new database session for the background task
    db = SessionLocal()
    try:
        # Get the appropriate scraper instance
        if target_site.lower() == 'jiji':
            scraper = JijiService.get_instance()
        elif target_site.lower() == 'kupatana':
            scraper = KupatanaService.get_instance()
        else:
            raise ValueError(f"Unknown target site: {target_site}")

        logger.info(
            f"Starting to scrape {len(urls)} detailed listings from {target_site}")

        # Initialize scraping status for details
        scraper.is_scraping = True
        scraper.should_stop = False
        scraper.scraping_status = {
            'type': 'details',
            'target_site': target_site,
            'current_page': 0,
            'total_pages': None,
            'pages_scraped': 0,
            'listings_found': 0,
            'listings_saved': 0,
            'current_url': None,
            'total_urls': len(urls),
            'urls_scraped': 0,
            'status': 'scraping'
        }
        scraper._broadcast_status()

        total_urls = len(urls)

        for index, url in enumerate(urls, 1):
            # Check if stop flag is set
            if scraper.should_stop:
                logger.info("Stop flag detected. Stopping detailed scraping.")
                # Update status to stopped
                if scraper.scraping_status.get('type') == 'details':
                    scraper.scraping_status['status'] = 'stopped'
                    scraper._broadcast_status()
                break

            try:
                data = scraper.extract_detailed_data(
                    url,
                    total_urls=total_urls,
                    current_index=index,
                    db_session=db,
                    target_site=target_site
                )

                # Update progress after extraction
                scraper.scraping_status['urls_scraped'] = index
                scraper.scraping_status['current_url'] = None
                scraper._broadcast_status()

                # Check if extraction was stopped
                if data and data.get('error') == 'Scraping was stopped':
                    logger.info("Extraction stopped by user request.")
                    # Update status to stopped
                    if scraper.scraping_status.get('type') == 'details':
                        scraper.scraping_status['status'] = 'stopped'
                        scraper._broadcast_status()
                    break

                # Data is already saved in extract_detailed_data if db_session was provided
                if data and 'error' not in data:
                    logger.info(
                        f"Processed listing: {url} ({index}/{total_urls})")
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Update progress even on error
                scraper.scraping_status['urls_scraped'] = index
                scraper.scraping_status['current_url'] = None
                scraper._broadcast_status()
                continue

        # Reset flags after loop completes
        scraper.is_scraping = False
        scraper.should_stop = False

        # Update final status if still in details mode
        if scraper.scraping_status.get('type') == 'details':
            if scraper.scraping_status.get('status') != 'stopped':
                scraper.scraping_status['status'] = 'completed'
            scraper._broadcast_status()

        logger.info(f"Completed scraping detailed listings from {target_site}")

    except Exception as e:
        logger.error(
            f"Error scraping detailed listings from {target_site}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()


def _scrape_all_with_details_task(
    target_site: str,
    max_pages: Optional[int]
):
    """Background task to scrape all listings with details"""
    # Create a new database session for the background task
    db = SessionLocal()
    try:
        logger.info(f"Starting full scrape of {target_site}")

        # Step 1: Scrape all listings
        _scrape_all_listings_task(target_site, max_pages)

        # Step 2: Get URLs from database and scrape details
        db_service = DatabaseService(db)
        listings = db_service.get_all_listings(
            lightweight=True, target_site=target_site)
        urls = [listing['rawUrl']
                for listing in listings if 'rawUrl' in listing]

        # Scrape details
        _scrape_detailed_listings_task(urls, target_site)

        logger.info(f"Completed full scrape of {target_site}")

    except Exception as e:
        logger.error(f"Error in full scrape of {target_site}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()


def _auto_cycle_scraping_task(
    target_site: str,
    max_pages: Optional[int],
    cycle_delay_minutes: int
):
    """
    Background task for automatic scraping cycle
    Continuously scrapes basic listings then details in a loop
    """
    cycle_number = 0
    
    while not auto_cycle_status[target_site]['should_stop']:
        cycle_number += 1
        db = SessionLocal()
        
        try:
            logger.info(f"Starting auto cycle #{cycle_number} for {target_site}")
            
            # Get scraper instance
            if target_site.lower() == 'jiji':
                scraper = JijiService.get_instance()
            elif target_site.lower() == 'kupatana':
                scraper = KupatanaService.get_instance()
            else:
                logger.error(f"Unknown target site: {target_site}")
                break
            
            # Update status - Phase 1: Scraping basic listings
            scraper.scraping_status = {
                'type': 'auto_cycle',
                'cycle_number': cycle_number,
                'phase': 'basic_listings',
                'current_page': 0,
                'total_pages': max_pages or 'unknown',
                'listings_found': 0,
                'status': 'running'
            }
            
            # Phase 1: Scrape basic listings
            logger.info(f"[Cycle #{cycle_number}] Phase 1: Scraping basic listings from {target_site}")
            _scrape_all_listings_task(target_site, max_pages)
            
            if auto_cycle_status[target_site]['should_stop']:
                break
            
            # Phase 2: Scrape details for listings without agent_name
            logger.info(f"[Cycle #{cycle_number}] Phase 2: Scraping details for incomplete listings from {target_site}")
            
            # Update status - Phase 2: Scraping details
            scraper.scraping_status = {
                'type': 'auto_cycle',
                'cycle_number': cycle_number,
                'phase': 'details',
                'status': 'running'
            }
            
            db_service = DatabaseService(db)
            
            # Get all listings from the target site
            all_listings = db_service.get_all_listings(
                lightweight=True, 
                target_site=target_site
            )
            
            # Separate listings: those without details first, then those with details
            listings_without_details = [
                listing for listing in all_listings 
                if not listing.get('agentName')
            ]
            
            logger.info(f"[Cycle #{cycle_number}] Found {len(listings_without_details)} listings without details")
            
            if listings_without_details:
                urls = [listing['rawUrl'] for listing in listings_without_details if 'rawUrl' in listing]
                _scrape_detailed_listings_task(urls, target_site)
            
            if auto_cycle_status[target_site]['should_stop']:
                break
            
            # Phase 3: Wait before next cycle
            logger.info(f"[Cycle #{cycle_number}] Completed. Waiting {cycle_delay_minutes} minutes before next cycle...")
            
            # Update status - Waiting
            scraper.scraping_status = {
                'type': 'auto_cycle',
                'cycle_number': cycle_number,
                'phase': 'waiting',
                'wait_minutes': cycle_delay_minutes,
                'status': 'waiting'
            }
            
            # Sleep in chunks to allow for responsive stopping
            wait_seconds = cycle_delay_minutes * 60
            sleep_interval = 10  # Check every 10 seconds if we should stop
            elapsed = 0
            
            while elapsed < wait_seconds and not auto_cycle_status[target_site]['should_stop']:
                time.sleep(min(sleep_interval, wait_seconds - elapsed))
                elapsed += sleep_interval
            
        except Exception as e:
            logger.error(f"Error in auto cycle #{cycle_number} for {target_site}: %s", e, exc_info=True)
            # Continue to next cycle despite errors
            time.sleep(60)  # Wait 1 minute before retrying on error
            
        finally:
            db.close()
    
    # Cleanup when stopping
    auto_cycle_status[target_site]['running'] = False
    auto_cycle_status[target_site]['thread'] = None
    logger.info(f"Auto cycle stopped for {target_site} after {cycle_number} cycles")


@router.post("/scrape-listings", response_model=ScrapeResponse)
async def scrape_all_listings(
    request: ScrapeAllRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Scrape all listings from a site (basic info: url, title, price)

    - **target_site**: jiji or kupatana
    - **max_pages**: Maximum number of pages to scrape (optional)
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Check if scraper is already scraping
        if request.target_site.lower() == 'jiji':
            if JijiService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Jiji scraper is already scraping. Please wait for the current operation to complete."
                )
        elif request.target_site.lower() == 'kupatana':
            if KupatanaService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Kupatana scraper is already scraping. Please wait for the current operation to complete."
                )
        else:
            raise ValueError(f"Unknown target site: {request.target_site}")

        if request.save_to_db:
            # Run scraping in background and save to DB
            background_tasks.add_task(
                _scrape_all_listings_task,
                target_site=request.target_site,
                max_pages=request.max_pages
            )

            return {
                "status": "started",
                "message": f"Scraping {request.target_site} listings in background",
                "target_site": request.target_site
            }
        else:
            # Run synchronously and return results
            if request.target_site.lower() == 'jiji':
                scraper = JijiService.get_instance()
            elif request.target_site.lower() == 'kupatana':
                scraper = KupatanaService.get_instance()
            else:
                raise ValueError(f"Unknown target site: {request.target_site}")
            listings = scraper.get_all_listings_basic(
                max_pages=request.max_pages)

            return {
                "status": "completed",
                "message": f"Scraped {len(listings)} listings",
                "target_site": request.target_site,
                "count": len(listings),
                "data": listings
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape-detailed", response_model=ScrapeDetailedResponse)
async def scrape_detailed_listings(
    request: ScrapeSelectedRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Scrape detailed data for selected URLs

    - **urls**: List of listing URLs to scrape
    - **target_site**: jiji or kupatana
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Check if scraper is already scraping
        if request.target_site.lower() == 'jiji':
            if JijiService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Jiji scraper is already scraping. Please wait for the current operation to complete."
                )
        elif request.target_site.lower() == 'kupatana':
            if KupatanaService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Kupatana scraper is already scraping. Please wait for the current operation to complete."
                )
        else:
            raise ValueError(f"Unknown target site: {request.target_site}")

        if request.save_to_db:
            # Run scraping in background and save to DB
            background_tasks.add_task(
                _scrape_detailed_listings_task,
                urls=request.urls,
                target_site=request.target_site
            )

            return {
                "status": "started",
                "message": f"Scraping {len(request.urls)} detailed listings in background",
                "target_site": request.target_site,
                "urls_count": len(request.urls)
            }
        else:
            # Run synchronously and return results
            if request.target_site.lower() == 'jiji':
                scraper = JijiService.get_instance()
            elif request.target_site.lower() == 'kupatana':
                scraper = KupatanaService.get_instance()
            else:
                raise ValueError(f"Unknown target site: {request.target_site}")

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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape-all-detailed", response_model=ScrapeResponse)
async def scrape_all_detailed(
    request: ScrapeAllRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Scrape all listings and their detailed data from a site

    This is a two-step process:
    1. Scrape all listing URLs (basic info)
    2. Scrape detailed data for each URL

    - **target_site**: jiji or kupatana
    - **max_pages**: Maximum number of pages to scrape (optional)
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Check if scraper is already scraping
        if request.target_site.lower() == 'jiji':
            if JijiService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Jiji scraper is already scraping. Please wait for the current operation to complete."
                )
        elif request.target_site.lower() == 'kupatana':
            if KupatanaService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Kupatana scraper is already scraping. Please wait for the current operation to complete."
                )
        else:
            raise ValueError(f"Unknown target site: {request.target_site}")

        # This operation can take a long time, so always run in background
        background_tasks.add_task(
            _scrape_all_with_details_task,
            target_site=request.target_site,
            max_pages=request.max_pages
        )

        return {
            "status": "started",
            "message": f"Scraping all {request.target_site} listings with details in background. This may take a while.",
            "target_site": request.target_site
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _scrape_all_details_task(
    target_site: str
):
    """Background task to scrape details for all existing listings in database"""
    # Create a new database session for the background task
    db = SessionLocal()
    try:
        logger.info(
            f"Starting to scrape details for all existing {target_site} listings")

        # Get all listing URLs from database
        db_service = DatabaseService(db)
        listings = db_service.get_all_listings(
            lightweight=True, target_site=target_site)

        if not listings:
            logger.warning(f"No listings found in database for {target_site}")
            return

        # Filter listings: prioritize those without details (no agent_name)
        listings_without_details = []
        listings_with_details = []

        for listing in listings:
            if 'rawUrl' not in listing:
                continue

            # Check if listing has details (agent_name is present and not empty)
            has_details = listing.get(
                'agentName') is not None and listing.get('agentName') != ''

            if has_details:
                listings_with_details.append(listing['rawUrl'])
            else:
                listings_without_details.append(listing['rawUrl'])

        # Prioritize listings without details first
        urls = listings_without_details + listings_with_details

        if not urls:
            logger.warning(
                f"No valid URLs found in database for {target_site}")
            return

        logger.info(
            f"Found {len(urls)} listings in database for {target_site}:")
        logger.info(
            f"  - {len(listings_without_details)} without details (will be scraped first)")
        logger.info(
            f"  - {len(listings_with_details)} with details (will be scraped after)")
        logger.info(f"Starting to scrape details...")

        # Scrape details for all URLs (prioritized order)
        _scrape_detailed_listings_task(urls, target_site)

        logger.info(f"Completed scraping details for {target_site}")

    except Exception as e:
        logger.error(f"Error scraping details for {target_site}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()


@router.post("/scrape-all-details", response_model=ScrapeDetailedResponse)
async def scrape_all_details(
    request: ScrapeAllRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Scrape detailed data for all existing listings in the database

    - **target_site**: jiji or kupatana
    - **save_to_db**: Whether to save results to database (default: true)
    """
    try:
        # Check if scraper is already scraping
        if request.target_site.lower() == 'jiji':
            if JijiService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Jiji scraper is already scraping. Please wait for the current operation to complete."
                )
        elif request.target_site.lower() == 'kupatana':
            if KupatanaService.is_scraping_now():
                raise HTTPException(
                    status_code=409,
                    detail=f"Kupatana scraper is already scraping. Please wait for the current operation to complete."
                )
        else:
            raise ValueError(f"Unknown target site: {request.target_site}")

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

            # Run scraping in background
            background_tasks.add_task(
                _scrape_all_details_task,
                target_site=request.target_site
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

    - **target_site**: jiji or kupatana
    """
    try:
        target_site = request.target_site.lower()
        
        if target_site not in ['jiji', 'kupatana']:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {target_site}. Must be 'jiji' or 'kupatana'."
            )
        
        stopped_items = []
        
        # Stop regular scraping
        if target_site == 'jiji':
            if JijiService.is_scraping_now():
                JijiService.stop_scraping()
                stopped_items.append(f"{target_site} scraper")
        elif target_site == 'kupatana':
            if KupatanaService.is_scraping_now():
                KupatanaService.stop_scraping()
                stopped_items.append(f"{target_site} scraper")
        
        # Stop auto cycle if running
        if auto_cycle_status[target_site]['running']:
            auto_cycle_status[target_site]['should_stop'] = True
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
    
    - **target_site**: jiji or kupatana
    - **max_pages**: Maximum pages to scrape per cycle (optional)
    - **cycle_delay_minutes**: Minutes to wait between cycles (default: 30)
    - **headless**: Run browser in headless mode (default: true)
    """
    try:
        target_site = request.target_site.lower()
        
        if target_site not in ['jiji', 'kupatana']:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown target site: {target_site}. Must be 'jiji' or 'kupatana'."
            )
        
        # Check if already running
        if auto_cycle_status[target_site]['running']:
            raise HTTPException(
                status_code=400,
                detail=f"Auto cycle already running for {target_site}"
            )
        
        # Reset stop flag
        auto_cycle_status[target_site]['should_stop'] = False
        auto_cycle_status[target_site]['running'] = True
        
        # Start the cycle in a separate thread
        thread = threading.Thread(
            target=_auto_cycle_scraping_task,
            args=(target_site, request.max_pages, request.cycle_delay_minutes),
            daemon=True
        )
        thread.start()
        auto_cycle_status[target_site]['thread'] = thread
        
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

    Returns the current status of both Jiji and Kupatana scrapers,
    including whether they are currently scraping, their progress,
    and auto cycle status.
    """
    try:
        jiji_status = None
        kupatana_status = None

        # Get Jiji status if instance exists
        if JijiService.is_ready():
            jiji_status = JijiService.get_status()
            if jiji_status:
                # Add auto cycle status
                jiji_status['auto_cycle_running'] = auto_cycle_status['jiji']['running']
        else:
            # Even if scraper not ready, check if auto cycle is running
            if auto_cycle_status['jiji']['running']:
                jiji_status = {
                    'is_scraping': False,
                    'auto_cycle_running': True
                }

        # Get Kupatana status if instance exists
        if KupatanaService.is_ready():
            kupatana_status = KupatanaService.get_status()
            if kupatana_status:
                # Add auto cycle status
                kupatana_status['auto_cycle_running'] = auto_cycle_status['kupatana']['running']
        else:
            # Even if scraper not ready, check if auto cycle is running
            if auto_cycle_status['kupatana']['running']:
                kupatana_status = {
                    'is_scraping': False,
                    'auto_cycle_running': True
                }

        return {
            "jiji": jiji_status,
            "kupatana": kupatana_status
        }
    except Exception as e:
        logger.error("Error getting scraping status: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
