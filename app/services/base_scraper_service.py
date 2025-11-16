"""
Base scraper service class
Provides common functionality for all scraper services
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging
import os
import threading
import time
import undetected_chromedriver as uc

logger = logging.getLogger(__name__)


class BaseScraperService(ABC):
    """
    Base class for all scraper services.
    Provides common functionality and defines the interface that all scrapers must implement.
    """

    def __init__(
        self,
        base_url: str,
        headless: bool = False,
        profile_dir: str = None,
        site_name: str = None
    ):
        """
        Initialize the base scraper service

        Args:
            base_url: Base URL of the target website
            headless: Run browser in headless mode
            profile_dir: Directory to save browser profile (for persistent sessions)
            site_name: Name of the site (e.g., 'jiji', 'kupatana') - used for status tracking
        """
        self.base_url = base_url
        self.headless = headless
        self.profile_dir = profile_dir
        self.site_name = site_name or self.__class__.__name__.lower().replace('service', '')
        self.driver = None
        
        # Scraping state
        self.is_scraping = False
        self.should_stop = False
        
        # Auto cycle state
        self._auto_cycle_thread: Optional[threading.Thread] = None
        self._auto_cycle_running = False
        self._auto_cycle_should_stop = False
        
        self.scraping_status = {
            "type": None,  # 'listings' or 'details' or 'auto_cycle' or None
            "target_site": None,
            "current_page": 0,
            "total_pages": None,
            "pages_scraped": 0,
            "listings_found": 0,
            "listings_saved": 0,
            "current_url": None,
            "total_urls": 0,
            "urls_scraped": 0,
            "status": "idle",  # 'idle', 'scraping', 'completed', 'error', 'stopped'
            "auto_cycle_running": False,
            "cycle_number": None,
            "phase": None,  # 'basic_listings', 'details', 'waiting'
            "wait_minutes": None,
        }

    @classmethod
    @abstractmethod
    def get_instance(cls):
        """
        Get or create singleton instance of the scraper service.
        Must be implemented by each child class.
        """
        pass

    @classmethod
    @abstractmethod
    def close_instance(cls):
        """
        Close the singleton instance and browser.
        Must be implemented by each child class.
        """
        pass

    @classmethod
    def is_ready(cls) -> bool:
        """
        Check if the scraper instance is ready.
        Child classes should override if they have their own _instance.
        """
        return hasattr(cls, '_instance') and cls._instance is not None and cls._instance.driver is not None

    @classmethod
    def is_scraping_now(cls) -> bool:
        """
        Check if the scraper is currently scraping.
        Child classes should override if they have their own _instance.
        """
        return hasattr(cls, '_instance') and cls._instance is not None and cls._instance.is_scraping

    @classmethod
    def get_status(cls) -> Optional[Dict]:
        """
        Get the current scraping status.
        Child classes should override if they have their own _instance.
        """
        if hasattr(cls, '_instance') and cls._instance:
            return cls._instance.scraping_status.copy()
        return None

    @classmethod
    def stop_scraping(cls):
        """
        Stop the current scraping operation.
        Child classes should override if they have their own _instance.
        """
        if hasattr(cls, '_instance') and cls._instance:
            cls._instance.should_stop = True
            cls._instance.is_scraping = False  # Immediately reset the flag
            logger.info(f"Stop flag set for {cls.__name__} scraper")

    def _check_should_stop(self) -> bool:
        """Check if scraping should be stopped"""
        return self.should_stop

    def _broadcast_status(self):
        """Broadcast scraping status via WebSocket"""
        try:
            from app.core.websocket_manager import manager
            manager.broadcast_sync({
                "type": "scraping_status",
                "target_site": self.scraping_status.get("target_site"),
                "data": self.scraping_status.copy(),
            })
        except Exception:
            logger.debug("Error broadcasting status", exc_info=True)

    def _update_status_field(self, field: str, value, broadcast: bool = True):
        """
        Update a single field in scraping_status and optionally broadcast
        
        Args:
            field: Field name to update
            value: Value to set
            broadcast: Whether to broadcast status after update (default: True)
        """
        self.scraping_status[field] = value
        if broadcast:
            self._broadcast_status()

    def _init_listings_status(self, target_site: str, max_pages: Optional[int] = None):
        """
        Initialize scraping status for listings scraping
        
        Args:
            target_site: Target site name
            max_pages: Maximum number of pages to scrape (optional)
        """
        # Preserve auto cycle fields if they exist
        auto_cycle_running = self.scraping_status.get("auto_cycle_running", False)
        cycle_number = self.scraping_status.get("cycle_number")
        phase = self.scraping_status.get("phase")
        wait_minutes = self.scraping_status.get("wait_minutes")

        # Set scraping flags
        self.is_scraping = True
        self.should_stop = False

        # Initialize scraping status
        self.scraping_status = {
            "type": "listings",
            "target_site": target_site,
            "current_page": 0,
            "total_pages": max_pages,
            "pages_scraped": 0,
            "listings_found": 0,
            "listings_saved": 0,
            "current_url": None,
            "total_urls": 0,
            "urls_scraped": 0,
            "status": "scraping",
            "auto_cycle_running": auto_cycle_running,
            "cycle_number": cycle_number,
            "phase": phase if auto_cycle_running else None,
            "wait_minutes": wait_minutes,
        }
        self._broadcast_status()

    def _init_details_status(self, target_site: str, total_urls: int = 0):
        """
        Initialize scraping status for details scraping
        
        Args:
            target_site: Target site name
            total_urls: Total number of URLs to scrape
        """
        # Preserve auto cycle fields if they exist
        auto_cycle_running = self.scraping_status.get("auto_cycle_running", False)
        cycle_number = self.scraping_status.get("cycle_number")
        phase = self.scraping_status.get("phase")
        wait_minutes = self.scraping_status.get("wait_minutes")

        # Set scraping flags
        self.is_scraping = True
        self.should_stop = False

        # Initialize scraping status
        self.scraping_status = {
            "type": "details",
            "target_site": target_site,
            "current_page": 0,
            "total_pages": None,
            "pages_scraped": 0,
            "listings_found": 0,
            "listings_saved": 0,
            "current_url": None,
            "total_urls": total_urls,
            "urls_scraped": 0,
            "status": "scraping",
            "auto_cycle_running": auto_cycle_running,
            "cycle_number": cycle_number,
            "phase": phase if auto_cycle_running else None,
            "wait_minutes": wait_minutes,
        }
        self._broadcast_status()

    def _update_page_progress(self, page_num: int, listings_count: int, broadcast: bool = True):
        """
        Update page progress in scraping status
        
        Args:
            page_num: Current page number
            listings_count: Total number of listings found so far
            broadcast: Whether to broadcast status after update (default: True)
        """
        self.scraping_status["current_page"] = page_num
        self.scraping_status["pages_scraped"] = page_num
        self.scraping_status["listings_found"] = listings_count
        if broadcast:
            self._broadcast_status()

    def _update_url_progress(
        self, 
        current_url: Optional[str] = None, 
        current_index: int = 0, 
        total_urls: Optional[int] = None,
        broadcast: bool = True
    ):
        """
        Update URL progress in scraping status for details scraping
        
        Args:
            current_url: Current URL being processed (None to clear)
            current_index: Current index in URL list
            total_urls: Total number of URLs (optional, only updates if provided)
            broadcast: Whether to broadcast status after update (default: True)
        """
        if current_url is not None:
            self.scraping_status["current_url"] = current_url
        self.scraping_status["urls_scraped"] = current_index
        if total_urls is not None:
            self.scraping_status["total_urls"] = total_urls
        if broadcast:
            self._broadcast_status()

    def _finalize_status(self, was_stopped: bool = False):
        """
        Finalize scraping status after scraping completes
        
        Args:
            was_stopped: Whether scraping was stopped by user
        """
        # Reset scraping flags
        self.is_scraping = False
        self.should_stop = False

        # Update final status
        if was_stopped:
            self.scraping_status["status"] = "stopped"
        else:
            self.scraping_status["status"] = "completed"
        
        self.scraping_status["current_page"] = 0
        
        # If not part of auto cycle, clear phase
        if not self.scraping_status.get("auto_cycle_running"):
            self.scraping_status["phase"] = None
        
        self._broadcast_status()

    def start_auto_cycle(
        self,
        max_pages: Optional[int] = None,
        cycle_delay_minutes: int = 30,
        db_session=None
    ) -> bool:
        """
        Start automatic scraping cycle
        
        The cycle will continuously:
        1. Scrape basic listings
        2. Scrape details for listings without agent info
        3. Wait for specified delay
        4. Repeat
        
        Args:
            max_pages: Maximum pages to scrape per cycle (optional)
            cycle_delay_minutes: Minutes to wait between cycles (default: 30)
            db_session: Database session for saving listings
            
        Returns:
            True if started successfully, False if already running
        """
        if self._auto_cycle_running:
            logger.warning(f"Auto cycle already running for {self.site_name}")
            return False
        
        # Reset stop flag
        self._auto_cycle_should_stop = False
        self._auto_cycle_running = True
        
        # Start the cycle in a separate thread
        self._auto_cycle_thread = threading.Thread(
            target=self._auto_cycle_task,
            args=(max_pages, cycle_delay_minutes, db_session),
            daemon=True
        )
        self._auto_cycle_thread.start()
        
        logger.info(f"Started auto cycle for {self.site_name}")
        return True

    def stop_auto_cycle(self):
        """
        Stop the automatic scraping cycle
        """
        if not self._auto_cycle_running:
            logger.warning(f"Auto cycle not running for {self.site_name}")
            return
        
        self._auto_cycle_should_stop = True
        logger.info(f"Stop signal sent for auto cycle of {self.site_name}")

    def is_auto_cycle_running(self) -> bool:
        """
        Check if auto cycle is currently running
        
        Returns:
            True if auto cycle is running, False otherwise
        """
        return self._auto_cycle_running

    def _auto_cycle_task(
        self,
        max_pages: Optional[int],
        cycle_delay_minutes: int,
        db_session=None
    ):
        """
        Background task for automatic scraping cycle
        Continuously scrapes basic listings then details in a loop
        
        Args:
            max_pages: Maximum pages to scrape per cycle
            cycle_delay_minutes: Minutes to wait between cycles
            db_session: Database session for saving listings
        """
        from app.core.database import SessionLocal
        from app.services.database_service import DatabaseService
        
        cycle_number = 0
        
        while not self._auto_cycle_should_stop:
            cycle_number += 1
            db = db_session if db_session else SessionLocal()
            
            try:
                logger.info(f"Starting auto cycle #{cycle_number} for {self.site_name}")
                
                # Update status - Phase 1: Scraping basic listings
                self._update_status_field("auto_cycle_running", True, broadcast=False)
                self._update_status_field("cycle_number", cycle_number, broadcast=False)
                self._update_status_field("phase", "basic_listings", broadcast=False)
                self._update_status_field("wait_minutes", None, broadcast=False)
                self._update_status_field("status", "scraping", broadcast=True)
                
                # Phase 1: Scrape basic listings
                logger.info(f"[Cycle #{cycle_number}] Phase 1: Scraping basic listings from {self.site_name}")
                self.get_all_listings_basic(
                    max_pages=max_pages,
                    db_session=db,
                    target_site=self.site_name
                )
                
                if self._auto_cycle_should_stop:
                    break
                
                # Phase 2: Scrape details for listings without agent_name
                logger.info(f"[Cycle #{cycle_number}] Phase 2: Scraping details for incomplete listings from {self.site_name}")
                
                # Update status - Phase 2: Scraping details
                self._update_status_field("phase", "details", broadcast=False)
                self._update_status_field("status", "scraping", broadcast=True)
                
                db_service = DatabaseService(db)
                
                # Get all listings from the target site
                all_listings = db_service.get_all_listings(
                    lightweight=True,
                    target_site=self.site_name
                )
                
                # Separate listings: those without details first
                listings_without_details = [
                    listing for listing in all_listings
                    if not listing.get('agentName')
                ]
                
                logger.info(f"[Cycle #{cycle_number}] Found {len(listings_without_details)} listings without details")
                
                if listings_without_details:
                    urls = [listing['rawUrl'] for listing in listings_without_details if 'rawUrl' in listing]
                    
                    # Initialize details status
                    self._init_details_status(self.site_name, len(urls))
                    
                    # Scrape details for each URL
                    for index, url in enumerate(urls, 1):
                        if self._auto_cycle_should_stop or self.should_stop:
                            logger.info("Stop flag detected. Stopping detailed scraping.")
                            self._update_status_field("status", "stopped", broadcast=True)
                            break
                        
                        try:
                            self.extract_detailed_data(
                                url,
                                total_urls=len(urls),
                                current_index=index,
                                db_session=db,
                                target_site=self.site_name
                            )
                        except Exception as e:
                            logger.error(f"Error scraping {url}: {e}", exc_info=True)
                            continue
                    
                    # Finalize details status
                    if not self._auto_cycle_should_stop and not self.should_stop:
                        self._finalize_status(was_stopped=False)
                
                if self._auto_cycle_should_stop:
                    break
                
                # Phase 3: Wait before next cycle
                logger.info(f"[Cycle #{cycle_number}] Completed. Waiting {cycle_delay_minutes} minutes before next cycle...")
                
                # Update status - Waiting
                self._update_status_field("auto_cycle_running", True, broadcast=False)
                self._update_status_field("cycle_number", cycle_number, broadcast=False)
                self._update_status_field("phase", "waiting", broadcast=False)
                self._update_status_field("wait_minutes", cycle_delay_minutes, broadcast=False)
                self._update_status_field("status", "idle", broadcast=False)
                self._update_status_field("type", None, broadcast=False)
                self._update_status_field("current_page", 0, broadcast=False)
                self._update_status_field("pages_scraped", 0, broadcast=False)
                self._update_status_field("listings_found", 0, broadcast=False)
                self._update_status_field("current_url", None, broadcast=False)
                self._update_status_field("urls_scraped", 0, broadcast=True)
                
                # Sleep in chunks to allow for responsive stopping
                wait_seconds = cycle_delay_minutes * 60
                sleep_interval = 10  # Check every 10 seconds if we should stop
                elapsed = 0
                
                while elapsed < wait_seconds and not self._auto_cycle_should_stop:
                    time.sleep(min(sleep_interval, wait_seconds - elapsed))
                    elapsed += sleep_interval
                
            except Exception as e:
                logger.error(f"Error in auto cycle #{cycle_number} for {self.site_name}: {e}", exc_info=True)
                # Continue to next cycle despite errors
                time.sleep(60)  # Wait 1 minute before retrying on error
            
            finally:
                if not db_session:  # Only close if we created the session
                    db.close()
        
        # Cleanup when stopping
        self._auto_cycle_running = False
        self._auto_cycle_thread = None
        
        # Clear auto cycle fields from scraper status
        self._update_status_field("auto_cycle_running", False, broadcast=False)
        self._update_status_field("cycle_number", None, broadcast=False)
        self._update_status_field("phase", None, broadcast=False)
        self._update_status_field("wait_minutes", None, broadcast=True)
        
        logger.info(f"Auto cycle stopped for {self.site_name} after {cycle_number} cycles")

    def scrape_all_listings_async(
        self,
        max_pages: Optional[int] = None,
        db_session=None
    ) -> threading.Thread:
        """
        Start scraping all listings in a background thread
        
        Args:
            max_pages: Maximum number of pages to scrape (optional)
            db_session: Database session (if None, will create one in the thread)
            
        Returns:
            Thread object running the task
        """
        thread = threading.Thread(
            target=self._scrape_all_listings_task,
            args=(max_pages, db_session),
            daemon=True
        )
        thread.start()
        return thread

    def _scrape_all_listings_task(
        self,
        max_pages: Optional[int],
        db_session=None
    ):
        """
        Background task to scrape all listings
        
        Args:
            max_pages: Maximum number of pages to scrape
            db_session: Database session (if None, creates a new one)
        """
        from app.core.database import SessionLocal
        
        db = db_session if db_session else SessionLocal()
        try:
            logger.info(f"Starting to scrape all listings from {self.site_name}")
            self.get_all_listings_basic(
                max_pages=max_pages,
                db_session=db,
                target_site=self.site_name
            )
            logger.info(f"Scraped listings from {self.site_name}")
        except Exception as e:
            logger.error(f"Error scraping listings from {self.site_name}: {e}", exc_info=True)
            raise
        finally:
            if not db_session:  # Only close if we created the session
                db.close()

    def scrape_detailed_listings_async(
        self,
        urls: List[str],
        db_session=None
    ) -> threading.Thread:
        """
        Start scraping detailed listings in a background thread
        
        Args:
            urls: List of listing URLs to scrape
            db_session: Database session (if None, will create one in the thread)
            
        Returns:
            Thread object running the task
        """
        thread = threading.Thread(
            target=self._scrape_detailed_listings_task,
            args=(urls, db_session),
            daemon=True
        )
        thread.start()
        return thread

    def _scrape_detailed_listings_task(
        self,
        urls: List[str],
        db_session=None
    ):
        """
        Background task to scrape detailed listings
        
        Args:
            urls: List of listing URLs to scrape
            db_session: Database session (if None, creates a new one)
        """
        from app.core.database import SessionLocal
        
        db = db_session if db_session else SessionLocal()
        try:
            logger.info(f"Starting to scrape {len(urls)} detailed listings from {self.site_name}")
            
            # Initialize scraping status for details
            self._init_details_status(self.site_name, len(urls))
            
            total_urls = len(urls)
            
            for index, url in enumerate(urls, 1):
                # Check if stop flag is set
                if self.should_stop:
                    logger.info("Stop flag detected. Stopping detailed scraping.")
                    self._update_status_field("status", "stopped", broadcast=True)
                    break
                
                try:
                    data = self.extract_detailed_data(
                        url,
                        total_urls=total_urls,
                        current_index=index,
                        db_session=db,
                        target_site=self.site_name
                    )
                    
                    # Update progress after extraction
                    self._update_url_progress(
                        current_url=None,
                        current_index=index,
                        broadcast=True
                    )
                    
                    # Check if extraction was stopped
                    if data and data.get('error') == 'Scraping was stopped':
                        logger.info("Extraction stopped by user request.")
                        self._update_status_field("status", "stopped", broadcast=True)
                        break
                    
                    # Data is already saved in extract_detailed_data if db_session was provided
                    if data and 'error' not in data:
                        logger.info(f"Processed listing: {url} ({index}/{total_urls})")
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}", exc_info=True)
                    # Update progress even on error
                    self._update_url_progress(
                        current_url=None,
                        current_index=index,
                        broadcast=True
                    )
                    continue
            
            # Finalize status
            was_stopped = self.should_stop
            self._finalize_status(was_stopped=was_stopped)
            
            logger.info(f"Completed scraping detailed listings from {self.site_name}")
            
        except Exception as e:
            logger.error(f"Error scraping detailed listings from {self.site_name}: {e}", exc_info=True)
            raise
        finally:
            if not db_session:  # Only close if we created the session
                db.close()

    def scrape_all_with_details_async(
        self,
        max_pages: Optional[int] = None,
        db_session=None
    ) -> threading.Thread:
        """
        Start scraping all listings with details in a background thread
        
        This is a two-step process:
        1. Scrape all listing URLs (basic info)
        2. Scrape detailed data for each URL
        
        Args:
            max_pages: Maximum number of pages to scrape (optional)
            db_session: Database session (if None, will create one in the thread)
            
        Returns:
            Thread object running the task
        """
        thread = threading.Thread(
            target=self._scrape_all_with_details_task,
            args=(max_pages, db_session),
            daemon=True
        )
        thread.start()
        return thread

    def _scrape_all_with_details_task(
        self,
        max_pages: Optional[int],
        db_session=None
    ):
        """
        Background task to scrape all listings with details
        
        Args:
            max_pages: Maximum number of pages to scrape
            db_session: Database session (if None, creates a new one)
        """
        from app.core.database import SessionLocal
        from app.services.database_service import DatabaseService
        
        db = db_session if db_session else SessionLocal()
        try:
            logger.info(f"Starting full scrape of {self.site_name}")
            
            # Step 1: Scrape all listings
            self._scrape_all_listings_task(max_pages, db)
            
            # Step 2: Get URLs from database and scrape details
            db_service = DatabaseService(db)
            listings = db_service.get_all_listings(
                lightweight=True,
                target_site=self.site_name
            )
            urls = [listing['rawUrl'] for listing in listings if 'rawUrl' in listing]
            
            # Scrape details
            if urls:
                self._scrape_detailed_listings_task(urls, db)
            
            logger.info(f"Completed full scrape of {self.site_name}")
            
        except Exception as e:
            logger.error(f"Error in full scrape of {self.site_name}: {e}", exc_info=True)
            raise
        finally:
            if not db_session:  # Only close if we created the session
                db.close()

    def scrape_all_details_async(
        self,
        db_session=None
    ) -> threading.Thread:
        """
        Start scraping details for all existing listings in a background thread
        
        Args:
            db_session: Database session (if None, will create one in the thread)
            
        Returns:
            Thread object running the task
        """
        thread = threading.Thread(
            target=self._scrape_all_details_task,
            args=(db_session,),
            daemon=True
        )
        thread.start()
        return thread

    def _scrape_all_details_task(
        self,
        db_session=None
    ):
        """
        Background task to scrape details for all existing listings in database
        
        Args:
            db_session: Database session (if None, creates a new one)
        """
        from app.core.database import SessionLocal
        from app.services.database_service import DatabaseService
        
        db = db_session if db_session else SessionLocal()
        try:
            logger.info(f"Starting to scrape details for all existing {self.site_name} listings")
            
            # Get all listing URLs from database
            db_service = DatabaseService(db)
            listings = db_service.get_all_listings(
                lightweight=True,
                target_site=self.site_name
            )
            
            if not listings:
                logger.warning(f"No listings found in database for {self.site_name}")
                return
            
            # Filter listings: prioritize those without details (no agent_name)
            listings_without_details = []
            listings_with_details = []
            
            for listing in listings:
                if 'rawUrl' not in listing:
                    continue
                
                # Check if listing has details (agent_name is present and not empty)
                has_details = listing.get('agentName') is not None and listing.get('agentName') != ''
                
                if has_details:
                    listings_with_details.append(listing['rawUrl'])
                else:
                    listings_without_details.append(listing['rawUrl'])
            
            # Prioritize listings without details first
            urls = listings_without_details + listings_with_details
            
            if not urls:
                logger.warning(f"No valid URLs found in database for {self.site_name}")
                return
            
            logger.info(f"Found {len(urls)} listings in database for {self.site_name}:")
            logger.info(f"  - {len(listings_without_details)} without details (will be scraped first)")
            logger.info(f"  - {len(listings_with_details)} with details (will be scraped after)")
            logger.info("Starting to scrape details...")
            
            # Scrape details for all URLs (prioritized order)
            self._scrape_detailed_listings_task(urls, db)
            
            logger.info(f"Completed scraping details for {self.site_name}")
            
        except Exception as e:
            logger.error(f"Error scraping details for {self.site_name}: {e}", exc_info=True)
            raise
        finally:
            if not db_session:  # Only close if we created the session
                db.close()

    def start_browser(self):
        """Initialize the browser with persistent profile"""
        # Don't start if browser already exists
        if self.driver is not None:
            logger.info("Browser already started, skipping...")
            return

        logger.info(f"Starting undetected Chrome browser for {self.site_name}...")
        options = uc.ChromeOptions()

        # Disable headless mode for undetected-chromedriver (causes connection issues)
        # Use --window-position to hide window instead if needed
        if self.headless:
            logger.warning(
                "Headless mode disabled for undetected-chromedriver compatibility"
            )
            # options.add_argument('--headless=new')  # Disabled - causes issues
            # Move window off-screen
            options.add_argument("--window-position=0,0")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")

        # Use persistent profile directory to save login session
        if self.profile_dir:
            profile_path = os.path.abspath(self.profile_dir)

            # Create profile directory if it doesn't exist
            os.makedirs(profile_path, exist_ok=True)

            options.add_argument(f"--user-data-dir={profile_path}")
            logger.info(f"Using browser profile: {profile_path}")

        try:
            self.driver = uc.Chrome(options=options, version_main=None)

            if not self.headless:
                self.driver.maximize_window()

            # Set reasonable timeouts
            self.driver.set_page_load_timeout(45)
            self.driver.set_script_timeout(30)

            logger.info(f"Browser started successfully for {self.site_name}")
        except Exception:
            logger.error(f"Failed to start browser for {self.site_name}", exc_info=True)
            raise

    def close_browser(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info(f"Browser closed for {self.site_name}")
            except Exception:
                logger.error(f"Error closing browser for {self.site_name}", exc_info=True)
            finally:
                self.driver = None

    def _get_db_service(self, db_session):
        """
        Get or create DatabaseService instance from db_session
        
        Args:
            db_session: Database session object
            
        Returns:
            DatabaseService instance or None if db_session is None
        """
        if db_session is None:
            return None
        
        try:
            from app.services.database_service import DatabaseService
            return DatabaseService(db_session)
        except Exception:
            logger.error(f"Error creating DatabaseService for {self.site_name}", exc_info=True)
            return None

    def _save_listing(self, listing_data: Dict, target_site: str, db_session) -> bool:
        """
        Save a single listing to the database
        
        Args:
            listing_data: Dictionary containing listing data
            target_site: Target site name for database saving
            db_session: Database session object
            
        Returns:
            True if saved successfully, False otherwise
        """
        if db_session is None or not listing_data or 'error' in listing_data:
            return False
        
        try:
            db_service = self._get_db_service(db_session)
            if db_service:
                db_service.create_or_update_listing(listing_data, target_site)
                logger.debug(f"💾 Saved listing to database: {listing_data.get('raw_url', 'unknown')}")
                return True
        except Exception:
            logger.error(
                f"Error saving listing to database: {listing_data.get('raw_url', 'unknown')}",
                exc_info=True
            )
        return False

    def _save_listings_batch(
        self, 
        listings: List[Dict], 
        target_site: str, 
        db_session,
        update_status: bool = True
    ) -> int:
        """
        Save multiple listings to the database with error handling
        
        Args:
            listings: List of dictionaries containing listing data
            target_site: Target site name for database saving
            db_session: Database session object
            update_status: Whether to update scraping_status with saved count
            
        Returns:
            Number of listings successfully saved
        """
        if db_session is None or not listings:
            return 0
        
        saved_count = 0
        db_service = self._get_db_service(db_session)
        
        if not db_service:
            logger.warning(f"⚠️  No db_service available - skipping database save for {len(listings)} listings")
            return 0
        
        for listing_data in listings:
            try:
                if self._save_listing(listing_data, target_site, db_session):
                    saved_count += 1
            except Exception:
                logger.error(
                    f"Error saving listing: {listing_data.get('raw_url', 'unknown')}",
                    exc_info=True
                )
                continue
        
        # Update scraping status if requested
        if update_status:
            current_saved = self.scraping_status.get("listings_saved", 0)
            self.scraping_status["listings_saved"] = current_saved + saved_count
        
        return saved_count

    @abstractmethod
    def get_all_listings_basic(
        self,
        max_pages: Optional[int] = None,
        db_session=None,
        target_site: str = None,
    ) -> List[Dict]:
        """
        Scrape all listings (url, title, price) from all available pages.
        Must be implemented by each child class.

        Args:
            max_pages: Maximum number of pages to scrape (None for all pages)
            db_session: Optional database session to save listings immediately after each page
            target_site: Target site name for database saving

        Returns:
            List of dictionaries with 'url', 'title', 'price', 'currency' keys
        """
        pass

    @abstractmethod
    def extract_detailed_data(
        self,
        listing_url: str,
        total_urls: int = 0,
        current_index: int = 0,
        db_session=None,
        target_site: str = None,
    ) -> Dict:
        """
        Extract detailed data from a single listing page and save to database.
        Must be implemented by each child class.

        Args:
            listing_url: URL of the listing detail page
            total_urls: Total number of URLs to scrape (for progress tracking)
            current_index: Current index in the URL list (for progress tracking)
            db_session: Optional database session to save listing immediately after extraction
            target_site: Target site name for database saving

        Returns:
            Dictionary containing all extracted data
        """
        pass

