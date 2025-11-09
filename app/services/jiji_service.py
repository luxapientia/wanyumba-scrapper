"""
Jiji.co.tz Real Estate Scraper Service
Service for scraping real estate listings from jiji.co.tz
"""

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import logging
import os
from urllib.parse import urlparse, urlunparse

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class JijiService:
    """Jiji scraper service with login functionality for detailed real estate data"""

    _instance: Optional["JijiService"] = None

    def __init__(
        self, email: str, password: str, headless: bool = False, profile_dir: str = None
    ):
        self.base_url = "https://jiji.co.tz"
        self.real_estate_url = f"{self.base_url}/real-estate"
        self.login_url = f"{self.base_url}/login"
        self.email = email
        self.password = password
        self.headless = headless
        self.profile_dir = (
            profile_dir or "./browser_profile"
        )  # Default profile directory
        self.driver = None
        self.listings = []
        self.detailed_listings = []
        self.is_logged_in = False
        self.is_scraping = False  # Track if currently scraping
        self.should_stop = False  # Flag to stop scraping gracefully
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
    def get_instance(cls) -> "JijiService":
        """Get or create singleton instance of JijiService"""
        if cls._instance is None:
            from app.core.config import settings

            logger.info("Initializing Jiji scraper...")
            try:
                cls._instance = cls(
                    email=settings.JIJI_EMAIL or "",
                    password=settings.JIJI_PASSWORD or "",
                    profile_dir=settings.JIJI_PROFILE_DIR,
                    headless=settings.SCRAPER_HEADLESS,
                )
                cls._instance.start_browser()

                # Try to login
                try:
                    if cls._instance.login():
                        logger.info("✓ Jiji scraper ready (logged in)")
                    else:
                        logger.warning("⚠ Jiji scraper ready (login failed)")
                except Exception:
                    logger.warning("⚠ Jiji login error", exc_info=True)

            except Exception:
                logger.error("Failed to initialize Jiji scraper", exc_info=True)
                cls._instance = None
                raise

        return cls._instance

    @classmethod
    def close_instance(cls):
        """Close the singleton instance and browser"""
        if cls._instance:
            try:
                cls._instance.close_browser()
                logger.info("✓ Jiji scraper closed")
            except Exception:
                logger.error("Error closing Jiji scraper", exc_info=True)
            finally:
                cls._instance = None

    @classmethod
    def is_ready(cls) -> bool:
        """Check if the scraper instance is ready"""
        return cls._instance is not None and cls._instance.driver is not None

    @classmethod
    def is_scraping_now(cls) -> bool:
        """Check if the scraper is currently scraping"""
        return cls._instance is not None and cls._instance.is_scraping

    @classmethod
    def get_status(cls) -> Optional[Dict]:
        """Get the current scraping status"""
        if cls._instance:
            return cls._instance.scraping_status.copy()
        return None

    @classmethod
    def stop_scraping(cls):
        """Stop the current scraping operation"""
        if cls._instance:
            cls._instance.should_stop = True
            logger.info("Stop flag set for Jiji scraper")

    def _check_should_stop(self) -> bool:
        """Check if scraping should be stopped"""
        return self.should_stop

    def _broadcast_status(self):
        """Broadcast scraping status via WebSocket"""
        try:
            from app.core.websocket_manager import manager

            manager.broadcast_sync(
                {
                    "type": "scraping_status",
                    "target_site": self.scraping_status.get("target_site"),
                    "data": self.scraping_status.copy(),
                }
            )
        except Exception:
            logger.debug("Error broadcasting status", exc_info=True)

    def start_browser(self):
        """Initialize the browser with persistent profile"""
        # Don't start if browser already exists
        if self.driver is not None:
            logger.info("Browser already started, skipping...")
            return

        logger.info("Starting undetected Chrome browser...")
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
            logger.info("Using browser profile: %s", profile_path)

        try:
            self.driver = uc.Chrome(options=options, version_main=None)

            if not self.headless:
                self.driver.maximize_window()

            # Set reasonable timeouts
            self.driver.set_page_load_timeout(45)
            self.driver.set_script_timeout(30)

            logger.info("Browser started successfully")
        except Exception:
            logger.error("Failed to start browser", exc_info=True)
            raise

    def close_browser(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except Exception:
                logger.error("Error closing browser", exc_info=True)

    def has_cloudflare_challenge(self) -> bool:
        """Check if the current page has a Cloudflare challenge"""
        try:
            page_source = self.driver.page_source
            # Check for common Cloudflare challenge indicators
            if "Just a moment" in page_source or "Checking your browser" in page_source:
                return True
            if "Cloudflare" in page_source[:1000]:
                return True
            return False
        except Exception:
            logger.debug("Error checking for Cloudflare", exc_info=True)
            return False

    def wait_for_cloudflare(self, timeout: int = 30):
        """Wait for Cloudflare challenge to complete"""
        logger.info("Waiting for Cloudflare challenge to complete...")
        time.sleep(5)

        start_time = time.time()
        while time.time() - start_time < timeout:
            page_source = self.driver.page_source
            if (
                "Just a moment" not in page_source
                and "Cloudflare" not in page_source[:1000]
            ):
                logger.info("Cloudflare challenge bypassed successfully!")
                return True
            time.sleep(2)

        logger.warning("Cloudflare challenge timeout")
        return False

    def check_if_logged_in(self) -> bool:
        """Check if already logged in from saved session"""
        try:
            # Look for user profile/menu indicators
            user_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".b-user-menu, .qa-user-menu, .b-app-header-profile-menu, .b-seller-block__name",
            )

            # Check if sign in button is present (means not logged in)
            signin_buttons = self.driver.find_elements(
                By.CSS_SELECTOR, "a[href='/?auth=Login']"
            )

            if user_elements or not signin_buttons:
                logger.info("✅ Already logged in from saved session!")
                self.is_logged_in = True
                return True

            return False
        except Exception:
            logger.warning("Could not check login status", exc_info=True)
            return False

    def login(self):
        """Login to jiji.co.tz through modal"""
        try:
            logger.info("Navigating to main page...")
            self.driver.get(self.base_url)

            # Wait for Cloudflare
            self.wait_for_cloudflare()
            time.sleep(3)

            # Check if already logged in
            if self.check_if_logged_in():
                logger.info("Skipping login - already authenticated!")
                return True

            # Step 1: Click "Sign in" link to open modal
            logger.info("Step 1: Looking for 'Sign in' button...")
            signin_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/?auth=Login']"))
            )
            logger.info("Clicking 'Sign in' button...")
            signin_link.click()
            time.sleep(2)

            # Step 2: Click "E-mail or phone" button in modal
            logger.info("Step 2: Looking for 'E-mail or phone' button...")
            email_phone_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//button[contains(@class, 'fw-button') and contains(., 'E-mail or phone')]",
                    )
                )
            )
            logger.info("Clicking 'E-mail or phone' button...")
            email_phone_button.click()
            time.sleep(2)

            # Step 3: Enter email
            logger.info("Step 3: Entering email...")
            email_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input.qa-login-field, input#emailOrPhone")
                )
            )
            email_input.clear()
            time.sleep(0.5)
            email_input.send_keys(self.email)
            logger.info("Email entered: %s", self.email)
            time.sleep(1)

            # Step 4: Enter password
            logger.info("Step 4: Entering password...")
            password_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input.qa-password-field, input#password")
                )
            )
            password_input.clear()
            time.sleep(0.5)
            password_input.send_keys(self.password)
            logger.info("Password entered")
            time.sleep(1)

            # Step 5: Click SIGN IN button
            logger.info("Step 5: Clicking 'SIGN IN' button...")
            signin_submit = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.qa-login-submit"))
            )
            signin_submit.click()

            # Wait for login to complete
            logger.info("Waiting for login to complete...")
            time.sleep(5)

            # Check if login was successful
            # Look for user profile/menu indicators
            try:
                # Check URL changed from auth=Login
                current_url = self.driver.current_url

                # Look for user menu or profile elements
                user_elements = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    ".b-user-menu, .qa-user-menu, .b-app-header-profile-menu",
                )

                # Check if we're no longer on the login modal
                auth_in_url = "auth=Login" in current_url

                if not auth_in_url or user_elements:
                    logger.info("✅ Login successful!")
                    self.is_logged_in = True
                    self.driver.save_screenshot("login_success.png")
                    return True
                else:
                    logger.warning("Login may have failed")
                    self.driver.save_screenshot("login_uncertain.png")
                    # Save page source for debugging
                    with open("login_result.html", "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                    return False

            except Exception:
                logger.warning("Could not verify login status", exc_info=True)
                self.driver.save_screenshot("login_check_error.png")
                return False

        except Exception:
            logger.error("Login failed", exc_info=True)
            self.driver.save_screenshot("login_error.png")
            # Save page source for debugging
            try:
                with open("login_error.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                logger.info("Saved error page HTML to login_error.html")
            except Exception:
                logger.debug("Could not save error page HTML", exc_info=True)
            return False

    def is_404_page(self, soup: BeautifulSoup) -> bool:
        """Check if the current page is a 404 error page"""
        # Check for 404 indicators
        if soup.find("div", class_="b-404"):
            return True
        if soup.find("h2", string=re.compile(r"404.*oops", re.IGNORECASE)):
            return True
        if "404 - oops!" in soup.get_text():
            return True
        return False

    def get_all_listings_basic(
        self,
        max_pages: Optional[int] = None,
        db_session=None,
        target_site: str = "jiji",
    ) -> List[Dict]:
        """
        Scrape all listings (url, title, price) from all available pages.
        Stops automatically when two consecutive pages return 404.

        Args:
            max_pages: Maximum number of pages to scrape (None for all pages)
            db_session: Optional database session to save listings immediately after each page
            target_site: Target site name for database saving ('jiji', 'kupatana', etc.)

        Returns:
            List of dictionaries with 'url', 'title', 'price', 'currency' keys
        """
        # Set scraping status and reset stop flag
        self.is_scraping = True
        self.should_stop = False

        # Preserve auto cycle fields if they exist
        auto_cycle_running = self.scraping_status.get("auto_cycle_running", False)
        cycle_number = self.scraping_status.get("cycle_number")
        phase = self.scraping_status.get("phase")
        wait_minutes = self.scraping_status.get("wait_minutes")

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

        logger.info(
            "Starting to scrape all listings (url, title, price) from all pages..."
        )
        all_listings = []
        page_num = 1
        consecutive_404_count = 0  # Track consecutive 404 pages
        total_saved = 0  # Track total saved to database

        # Initialize database service if session is provided
        db_service = None
        if db_session:
            from app.services.database_service import DatabaseService

            db_service = DatabaseService(db_session)
            logger.info("✅ Database service initialized for %s", target_site)
        else:
            logger.warning(
                "⚠️  No db_session provided - listings will not be saved to database"
            )

        try:
            while True:
                # Check if stop flag is set
                if self.should_stop:
                    logger.info("Stop flag detected. Stopping scraping operation.")
                    break

                # Check if we've reached max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(
                        "Reached maximum page limit (%s). Stopping pagination.", max_pages
                    )
                    break

                try:
                    # Construct URL
                    if page_num == 1:
                        url = self.real_estate_url
                    else:
                        url = f"{self.real_estate_url}?page={page_num}"

                    logger.info("Fetching page %s... (%s)", page_num, url)
                    self.driver.get(url)

                    # Check if Cloudflare challenge is present
                    if self.has_cloudflare_challenge():
                        logger.info(
                            "Cloudflare challenge detected on page %s - waiting for bypass...",
                            page_num,
                        )
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30)
                        if not cloudflare_bypassed:
                            logger.warning(
                                "Cloudflare bypass may have failed, but continuing..."
                            )
                    else:
                        logger.debug("No Cloudflare challenge on page %s", page_num)
                        time.sleep(2)  # Brief wait for page stability

                    # Parse page
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")

                    # Check if this is a 404 page
                    if self.is_404_page(soup):
                        consecutive_404_count += 1
                        logger.info(
                            "⚠️  Page %s returned 404. (Consecutive 404 count: %s)",
                            page_num,
                            consecutive_404_count,
                        )

                        # If two consecutive pages are 404, stop
                        if consecutive_404_count >= 2:
                            logger.info(
                                "⚠️  Two consecutive pages returned 404. Stopping pagination."
                            )
                            break

                        # Continue to next page to check if it's also 404
                        page_num += 1
                        continue
                    else:
                        # Reset counter if we get a valid page
                        consecutive_404_count = 0

                    # Find all listing cards
                    listing_cards = soup.find_all("a", class_="b-list-advert-base")

                    if not listing_cards:
                        logger.warning(
                            "No listings found on page %s. Refreshing page and retrying...",
                            page_num,
                        )
                        # Refresh the page and try again
                        self.driver.refresh()

                        # Parse page again after refresh
                        soup = BeautifulSoup(self.driver.page_source, "html.parser")
                        listing_cards = soup.find_all("a", class_="b-list-advert-base")

                        if not listing_cards:
                            logger.warning(
                                "No listings found on page %s after refresh. Moving to next page.",
                                page_num,
                            )
                            page_num += 1
                            continue
                        else:
                            logger.info(
                                "Found %s listings after refresh on page %s",
                                len(listing_cards),
                                page_num,
                            )

                    # Extract data from each listing card
                    page_listings = []
                    for card in listing_cards:
                        try:
                            # Extract URL
                            href = card.get("href")
                            if not href:
                                continue

                            full_url = (
                                href
                                if href.startswith("http")
                                else f"{self.base_url}{href}"
                            )

                            # Remove all query parameters from URL
                            parsed_url = urlparse(full_url)
                            # Reconstruct URL without query parameters
                            full_url = urlunparse(
                                (
                                    parsed_url.scheme,
                                    parsed_url.netloc,
                                    parsed_url.path,
                                    parsed_url.params,
                                    "",  # Remove query string
                                    "",  # Remove fragment
                                )
                            )

                            # Extract title
                            title_elem = card.find(
                                "div", class_="b-list-advert__item-title"
                            )
                            if not title_elem:
                                # Try alternative selector
                                title_elem = card.find(
                                    "div", class_="b-advert-title-inner"
                                )
                            if not title_elem:
                                title_elem = card.find("h3")

                            title = (
                                title_elem.get_text(strip=True) if title_elem else "N/A"
                            )

                            # Extract price
                            # The price is in div.qa-advert-price with currency and amount on separate lines
                            price_elem = card.find("div", class_="qa-advert-price")
                            if not price_elem:
                                # Try alternative selectors
                                price_elem = card.find(
                                    "div", class_="b-list-advert__item-price"
                                )
                            if not price_elem:
                                price_elem = card.find("div", class_="b-advert-price")
                            if not price_elem:
                                price_elem = card.find(
                                    "span", class_="qa-advert-price-view-value"
                                )

                            # Parse price and currency
                            currency = None
                            price_value = None

                            if price_elem:
                                # Get text and clean it up (remove extra whitespace, join lines)
                                price_text = price_elem.get_text(
                                    separator=" ", strip=True
                                )
                                # Clean up multiple spaces
                                price_text = " ".join(price_text.split())

                                # Extract currency
                                if "TSh" in price_text or "TZS" in price_text:
                                    currency = "TSh"
                                    price_text = (
                                        price_text.replace("TSh", "")
                                        .replace("TZS", "")
                                        .strip()
                                    )
                                elif "USD" in price_text:
                                    currency = "USD"
                                    price_text = price_text.replace("USD", "").strip()
                                elif "$" in price_text:
                                    currency = "USD"
                                    price_text = price_text.replace("$", "").strip()
                                elif "€" in price_text:
                                    currency = "EUR"
                                    price_text = price_text.replace("€", "").strip()

                                # Try to parse numeric value
                                try:
                                    price_cleaned = (
                                        price_text.replace(",", "")
                                        .replace(" ", "")
                                        .strip()
                                    )
                                    if price_cleaned:
                                        price_value = float(price_cleaned)
                                except (ValueError, TypeError):
                                    pass

                            listing_data = {
                                "raw_url": full_url,
                                "title": title,
                                "price": price_value,
                                "price_currency": currency,
                                "source": "jiji",
                            }

                            page_listings.append(listing_data)

                        except Exception:
                            logger.debug(
                                "Error extracting data from listing card", exc_info=True
                            )
                            continue

                    if page_listings:
                        all_listings.extend(page_listings)
                        logger.info(
                            "✅ Page %s: Found %s listings (Total: %s)",
                            page_num,
                            len(page_listings),
                            len(all_listings),
                        )

                        # Update scraping status
                        self.scraping_status["current_page"] = page_num
                        self.scraping_status["pages_scraped"] = page_num
                        self.scraping_status["listings_found"] = len(all_listings)

                        # Save to database immediately if db_session is provided
                        logger.debug(
                            "Checking db_service: %s, db_session provided: %s",
                            db_service is not None,
                            db_session is not None,
                        )
                        if db_service:
                            logger.info(
                                "💾 Starting to save %s listings from page %s to database...",
                                len(page_listings),
                                page_num,
                            )
                            page_saved = 0
                            for listing_data in page_listings:
                                try:
                                    db_service.create_or_update_listing(
                                        listing_data, target_site
                                    )
                                    page_saved += 1
                                except Exception:
                                    logger.error(
                                        "Error saving listing %s",
                                        listing_data.get("url"),
                                        exc_info=True,
                                    )
                                    continue
                            total_saved += page_saved
                            self.scraping_status["listings_saved"] = total_saved
                            logger.info(
                                "💾 Page %s: Saved %s listings to database (Total saved: %s)",
                                page_num,
                                page_saved,
                                total_saved,
                            )
                        else:
                            logger.warning(
                                "⚠️  db_service is None - skipping database save for page %s",
                                page_num,
                            )

                        # Broadcast status update
                        self._broadcast_status()
                    else:
                        logger.warning(
                            "No valid listings extracted from page %s. Stopping pagination.",
                            page_num,
                        )
                        break

                    page_num += 1

                except Exception as e:
                    logger.error("Error on page %s: %s", page_num, e)
                    # If we get an error, try one more page before giving up
                    page_num += 1
                    # Safety limit (very high to allow many pages)
                    if page_num > 1000:
                        logger.warning(
                            "Reached safety limit (1000 pages). Stopping pagination."
                        )
                        break
                    continue

            logger.info(
                "✅ Scraped %s listings from %s pages",
                len(all_listings),
                page_num - 1,
            )
            if db_service:
                logger.info("💾 Total saved to database: %s listings", total_saved)
            return all_listings
        finally:
            # Check stop flag before resetting it
            was_stopped = self.should_stop
            
            # Always reset scraping status and stop flag
            self.is_scraping = False
            self.should_stop = False

            # Update final status (preserve auto cycle fields)
            if was_stopped:
                self.scraping_status["status"] = "stopped"
            else:
                self.scraping_status["status"] = "completed"
            self.scraping_status["current_page"] = 0
            
            # If not part of auto cycle, clear phase
            if not self.scraping_status.get("auto_cycle_running"):
                self.scraping_status["phase"] = None
            
            self._broadcast_status()

            logger.info("Scraping completed, status reset")

    def extract_detailed_data(
        self,
        listing_url: str,
        total_urls: int = 0,
        current_index: int = 0,
        db_session=None,
        target_site: str = "jiji",
    ) -> Dict:
        """
        Extract detailed data from a single listing page and save to database

        Args:
            listing_url: URL of the listing detail page
            total_urls: Total number of URLs to scrape (for progress tracking)
            current_index: Current index in the URL list (for progress tracking)
            db_session: Optional database session to save listing immediately after extraction
            target_site: Target site name for database saving ('jiji', 'kupatana', etc.)

        Returns:
            Dictionary containing all extracted data
        """
        # Set scraping status (don't reset stop flag - it may have been set by user)
        self.is_scraping = True

        # Update scraping status for details (status should already be initialized by the calling function)
        # Just update the current URL and progress
        self.scraping_status["current_url"] = listing_url
        # -1 because we're about to process this URL
        self.scraping_status["urls_scraped"] = current_index - 1
        if total_urls > 0:
            self.scraping_status["total_urls"] = total_urls

        self._broadcast_status()

        try:
            # Check if stop flag is set before starting
            if self.should_stop:
                logger.info("Stop flag detected. Skipping detailed extraction.")
                return {
                    "raw_url": listing_url,
                    "error": "Scraping was stopped",
                    "scraped_at": datetime.now().isoformat(),
                }
            logger.info("Scraping: %s", listing_url)
            self.driver.get(listing_url)

            # Check if Cloudflare challenge is present
            if self.has_cloudflare_challenge():
                logger.info("Cloudflare challenge detected - waiting for bypass...")
                cloudflare_bypassed = self.wait_for_cloudflare(timeout=30)
                if not cloudflare_bypassed:
                    logger.warning(
                        "Cloudflare bypass may have failed, but continuing..."
                    )
            else:
                logger.debug("No Cloudflare challenge detected")
                time.sleep(1)  # Brief wait for page stability

            # Check if stop flag is set after page load
            if self.should_stop:
                logger.info(
                    "Stop flag detected after page load. Stopping detailed extraction."
                )
                return {
                    "raw_url": listing_url,
                    "error": "Scraping was stopped",
                    "scraped_at": datetime.now().isoformat(),
                }

            data = {"url": listing_url, "scraped_at": datetime.now().isoformat()}

            # Get initial page HTML
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Check if stop flag is set after parsing
            if self.should_stop:
                logger.info(
                    "Stop flag detected after parsing. Stopping detailed extraction."
                )
                return {
                    "raw_url": listing_url,
                    "error": "Scraping was stopped",
                    "scraped_at": datetime.now().isoformat(),
                }

            # Extract title
            title_elem = soup.find("h1", class_="qa-advert-title")
            if title_elem:
                # Get the inner div text
                inner = title_elem.find("div", class_="b-advert-title-inner")
                data["title"] = (
                    inner.get_text(strip=True)
                    if inner
                    else title_elem.get_text(strip=True)
                )
            else:
                data["title"] = "N/A"

            # Extract listing type from title (sale, rent, lease, etc.)
            listing_type = None
            title_lower = (data.get("title") or "").lower()
            if (
                "for rent" in title_lower
                or "to rent" in title_lower
                or "rent" in title_lower
            ):
                listing_type = "rent"
            elif (
                "for sale" in title_lower
                or "to sell" in title_lower
                or "sale" in title_lower
            ):
                listing_type = "sale"
            elif (
                "for lease" in title_lower
                or "to lease" in title_lower
                or "lease" in title_lower
            ):
                listing_type = "lease"

            data["listing_type"] = listing_type

            # Extract price and currency
            price_elem = soup.find("span", class_="qa-advert-price-view-value")
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                # Parse currency and numeric value
                # Examples: "TSh 1,000,000", "USD 5,000", "$500"
                currency = None
                price_value = None

                # Try to extract currency
                if "TSh" in price_text or "TZS" in price_text:
                    currency = "TSh"
                    price_text = (
                        price_text.replace("TSh", "").replace("TZS", "").strip()
                    )
                elif "USD" in price_text:
                    currency = "USD"
                    price_text = price_text.replace("USD", "").strip()
                elif "$" in price_text:
                    currency = "USD"
                    price_text = price_text.replace("$", "").strip()
                elif "€" in price_text:
                    currency = "EUR"
                    price_text = price_text.replace("€", "").strip()

                # Try to parse numeric value (remove commas, spaces)
                try:
                    # Remove all commas and spaces
                    price_cleaned = price_text.replace(",", "").replace(" ", "").strip()
                    if price_cleaned:
                        price_value = float(price_cleaned)
                except (ValueError, TypeError):
                    pass

                data["currency"] = currency
                data["price"] = price_value
            else:
                data["currency"] = None
                data["price"] = None

            # Extract location
            location_elem = soup.find("div", class_="b-advert-info-statistics--region")
            if location_elem:
                # Remove the SVG icon and get text
                location_text = location_elem.get_text(strip=True)
                # Remove time info like "43 min ago"
                location_parts = location_text.split(",")
                if len(location_parts) >= 3:
                    data["location"] = ", ".join(location_parts[:3]).strip()
                else:
                    data["location"] = location_text
            else:
                data["location"] = "N/A"

            # Extract description
            desc_elem = soup.find("div", class_="qa-advert-description")
            if desc_elem:
                desc_text = desc_elem.find("span", class_="qa-description-text")
                data["description"] = (
                    desc_text.get_text(strip=True)
                    if desc_text
                    else desc_elem.get_text(strip=True)
                )
            else:
                data["description"] = "N/A"

            # Initialize individual fields for structured data (matching DB schema)
            property_type = None
            bedrooms = None
            bathrooms = None
            parking_space = None
            facilities = []
            attributes = {}

            # Method 1: Extract icon attributes (House/Apartment, Bedrooms, Bathrooms, Parking Space, etc.)
            icon_attrs = soup.find_all("div", class_="b-advert-icon-attribute")
            for icon_attr in icon_attrs:
                # Get the span text which contains the attribute value
                span_elem = icon_attr.find("span")
                if span_elem:
                    text = span_elem.get_text(strip=True)
                    # Parse the text to extract key and value
                    # Examples: "House", "3 bedrooms", "2 bathrooms", "Parking Space"
                    if text:
                        text_lower = text.lower()
                        # Check if it's a property type
                        if text_lower in [
                            "house",
                            "apartment",
                            "villa",
                            "bungalow",
                            "flat",
                            "studio",
                            "land",
                            "commercial property",
                        ]:
                            property_type = text
                        # Check if it contains a number (like "3 bedrooms")
                        elif re.match(r"^\d+\s+(bedroom|bathroom)", text_lower):
                            parts = text.split()
                            if "bedroom" in text_lower:
                                bedrooms = int(parts[0])
                            elif "bathroom" in text_lower:
                                bathrooms = int(parts[0])
                        # Check if it's parking space
                        elif "parking" in text_lower:
                            # Try to extract number if present
                            match = re.search(r"(\d+)", text)
                            if match:
                                parking_space = int(match.group(1))
                            else:
                                parking_space = 1  # Default to 1 if no number specified
                        else:
                            # Generic attribute
                            attributes[text] = "Yes"

            # Initialize property size fields
            property_size = None
            property_size_unit = None

            # Method 2: Extract regular tile attributes (Property Size, Condition, Furnishing, etc.)
            attr_items = soup.find_all("div", class_="b-advert-attribute")
            for attr in attr_items:
                key_elem = attr.find("div", class_="b-advert-attribute__key")
                val_elem = attr.find("div", class_="b-advert-attribute__value")

                if key_elem and val_elem:
                    key = key_elem.get_text(strip=True)
                    value = val_elem.get_text(strip=True)

                    # Special handling for Property Size to extract number and unit separately
                    if key == "Property Size":
                        # Value format: "700 sqm" or "700sqm"
                        # Extract number and unit using regex
                        match = re.search(r"([\d,\.]+)\s*(\w+)", value)
                        if match:
                            try:
                                # Remove commas and convert to float
                                property_size = float(match.group(1).replace(",", ""))
                                # sqm, sqft, etc.
                                property_size_unit = match.group(2)
                            except (ValueError, TypeError, AttributeError):
                                pass

                    # Store all attributes for reference
                    attributes[key] = value

            # Method 3: Extract facilities (Dining Area, Air Conditioning, Hot Water, etc.)
            facility_tags = soup.find_all("div", class_="b-advert-attributes__tag")
            for tag in facility_tags:
                facility_text = tag.get_text(strip=True)
                if facility_text and facility_text not in facilities:
                    facilities.append(facility_text)

            # Store structured data
            data["property_type"] = property_type
            data["bedrooms"] = bedrooms
            data["bathrooms"] = bathrooms
            data["parking_space"] = parking_space
            data["property_size"] = property_size
            data["property_size_unit"] = property_size_unit
            data["facilities"] = facilities
            data["attributes"] = attributes

            # Extract images - try multiple methods
            images = []

            # Method 1: Find images with src containing jijistatic
            img_elements = soup.find_all("img", class_="b-slider-image")
            for img in img_elements:
                img_url = img.get("src")
                if img_url and "jijistatic" in img_url and img_url not in images:
                    images.append(img_url)

            # Method 2: Find picture elements with content attribute
            if not images:
                picture_elements = soup.find_all(
                    "picture",
                    {"content": lambda x: x and "jijistatic" in x if x else False},
                )
                for pic in picture_elements:
                    img_url = pic.get("content")
                    if img_url and img_url not in images:
                        images.append(img_url)

            # Method 3: Fallback to data-src
            if not images:
                img_elements = soup.find_all(
                    "img",
                    {"data-src": lambda x: x and "jijistatic" in x if x else False},
                )
                for img in img_elements:
                    img_url = img.get("data-src")
                    if img_url and img_url not in images:
                        images.append(img_url)

            data["images"] = images[:20]  # Limit to first 20 images
            data["image_count"] = len(data["images"])

            # Extract contact information (matching DB schema: contact_name, contact_phone, contact_email)
            contact_name = None
            contact_phone = []

            seller_name_elem = soup.find("div", class_="b-seller-block__name")
            if seller_name_elem:
                contact_name = seller_name_elem.get_text(strip=True)

            # Check if stop flag is set before contact extraction
            if self.should_stop:
                logger.info(
                    "Stop flag detected before contact extraction. Stopping detailed extraction."
                )
                return {
                    "raw_url": listing_url,
                    "error": "Scraping was stopped",
                    "scraped_at": datetime.now().isoformat(),
                }

            # Click "Show contact" button to reveal phone numbers
            try:
                logger.info("Clicking 'Show contact' button...")

                # Wait for page to stabilize
                time.sleep(1)

                # Check stop flag again after wait
                if self.should_stop:
                    logger.info("Stop flag detected. Skipping contact extraction.")
                else:
                    # Find all show contact buttons (there might be multiple)
                    # Note: Can be <a> or <div> elements, so we don't specify element type
                    contact_buttons = self.driver.find_elements(
                        By.CSS_SELECTOR, ".qa-show-contact, .js-show-contact"
                    )

                    if not contact_buttons:
                        logger.warning("No 'Show contact' button found")
                    else:
                        # Try to click the first visible button
                        clicked = False
                        for idx, button in enumerate(contact_buttons):
                            try:
                                # Scroll to button
                                self.driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center'});",
                                    button,
                                )
                                time.sleep(0.5)

                                # Try regular click first
                                try:
                                    button.click()
                                    clicked = True
                                    logger.info(
                                        "Clicked 'Show contact' button #%s", idx + 1
                                    )
                                    break
                                except Exception:
                                    # If regular click fails, try JavaScript click
                                    self.driver.execute_script(
                                        "arguments[0].click();", button
                                    )
                                    clicked = True
                                    logger.info(
                                        "Clicked 'Show contact' button #%s (via JavaScript)",
                                        idx + 1,
                                    )
                                    break
                            except Exception:
                                logger.debug(
                                    "Could not click button #%s", idx + 1, exc_info=True
                                )
                                continue

                        if clicked:
                            # Check stop flag before waiting
                            if self.should_stop:
                                logger.info(
                                    "Stop flag detected. Skipping phone extraction."
                                )
                            else:
                                # Wait for phone numbers to appear
                                time.sleep(2)

                                # Check stop flag again after wait
                                if self.should_stop:
                                    logger.info(
                                        "Stop flag detected. Stopping phone extraction."
                                    )
                                else:
                                    # Get updated HTML after clicking
                                    soup = BeautifulSoup(
                                        self.driver.page_source, "html.parser"
                                    )

                                    # Extract all phone numbers from the popover
                                    phone_numbers = []

                                    # Method 1: Find all phone divs in the popover (multiple phones)
                                    phone_divs = soup.find_all(
                                        "div",
                                        class_="b-show-contacts-popover-item__phone",
                                    )
                                    for phone_div in phone_divs:
                                        phone = phone_div.get_text(strip=True)
                                        if phone and phone not in phone_numbers:
                                            phone_numbers.append(phone)

                                    # Method 2: Single phone with qa-show-contact-phone class
                                    if not phone_numbers:
                                        phone_spans = soup.find_all(
                                            ["span", "div"],
                                            class_="qa-show-contact-phone",
                                        )
                                        for phone_span in phone_spans:
                                            phone = phone_span.get_text(strip=True)
                                            # Validate it's a phone number (starts with 0 and has 9+ digits)
                                            if phone and re.match(r"^0\d{9,}$", phone):
                                                if phone not in phone_numbers:
                                                    phone_numbers.append(phone)
                                                    logger.info(
                                                        "Found single phone via qa-show-contact-phone: %s",
                                                        phone,
                                                    )

                                    # Method 3: Find phone links with tel: href
                                    if not phone_numbers:
                                        phone_links = soup.find_all(
                                            "a",
                                            {
                                                "href": lambda x: (
                                                    x and "tel:" in x if x else False
                                                )
                                            },
                                        )
                                        for phone_link in phone_links:
                                            phone = (
                                                phone_link.get("href")
                                                .replace("tel:", "")
                                                .strip()
                                            )
                                            if phone and re.match(r"^0\d{9,}$", phone):
                                                if phone not in phone_numbers:
                                                    phone_numbers.append(phone)
                                                    logger.info(
                                                        "Found phone via tel: link: %s", phone
                                                    )

                                    # Method 4: Try alternative selector
                                    if not phone_numbers:
                                        alt_phone_divs = soup.find_all(
                                            "div", class_="b-seller-contacts__phone"
                                        )
                                        for alt_div in alt_phone_divs:
                                            phone = alt_div.get_text(strip=True)
                                            if phone and re.match(r"^0\d{9,}$", phone):
                                                if phone not in phone_numbers:
                                                    phone_numbers.append(phone)

                                    if phone_numbers:
                                        contact_phone = phone_numbers
                                        logger.info(
                                            "✅ Extracted %s phone number(s): %s",
                                            len(phone_numbers),
                                            ", ".join(phone_numbers),
                                        )
                                    else:
                                        logger.warning(
                                            "No phone numbers found after clicking"
                                        )

            except Exception:
                logger.warning("Error during phone extraction", exc_info=True)

            # Store contact info in data (matching DB schema)
            data["contact_name"] = contact_name
            data["contact_phone"] = contact_phone
            # Email not typically available from scrapers
            data["contact_email"] = []

            # Extract views count
            views_text = soup.get_text()
            views_match = re.search(r"(\d+)\s*views?", views_text, re.IGNORECASE)
            data["views"] = views_match.group(0) if views_match else "N/A"

            # Extract posted date/time
            location_time_elem = soup.find(
                "div", class_="b-advert-info-statistics--region"
            )
            if location_time_elem:
                text = location_time_elem.get_text(strip=True)
                # Extract time like "43 min ago" from end
                time_match = re.search(
                    r"(\d+\s+(?:min|hour|day|week|month|year)s?\s+ago)", text
                )
                data["posted_date"] = time_match.group(1) if time_match else "N/A"
            else:
                data["posted_date"] = "N/A"

            # Extract listing ID
            listing_id = (
                listing_url.split("/")[-1].split(".")[0].split("?")[0]
                if "/" in listing_url
                else "N/A"
            )
            data["listing_id"] = listing_id

            # Parse location into structured fields
            # Location format: "City, District, Region" or "District, Region"
            location_text = data.get("location", "")
            country = "Tanzania"
            region = None
            city = None
            district = None
            address_text = location_text

            if location_text and location_text != "N/A":
                location_parts = [part.strip() for part in location_text.split(",")]
                if len(location_parts) >= 3:
                    city = location_parts[0]
                    district = location_parts[1]
                    region = location_parts[2]
                elif len(location_parts) == 2:
                    district = location_parts[0]
                    region = location_parts[1]
                elif len(location_parts) == 1:
                    region = location_parts[0]

            # Extract source_listing_id from URL
            # URL format: https://jiji.co.tz/goba/land-and-plots-for-sale/plot-for-sale-goba-lastanza-5Pu0dt7TQY9Q38ZCAxEkmTeR.html
            source_listing_id = None
            if listing_url:
                # Extract the ID from the end of the URL (after the last dash before .html)
                url_parts = listing_url.rstrip("/").split("/")[-1]
                if url_parts.endswith(".html"):
                    url_parts = url_parts[:-5]  # Remove .html
                # The ID is typically after the last dash
                if "-" in url_parts:
                    source_listing_id = url_parts.split("-")[-1]

            # Determine price_period based on listing_type
            price_period = None
            listing_type = data.get("listing_type")
            if listing_type == "rent":
                price_period = "month"
            elif listing_type == "sale":
                price_period = "once"

            # Convert property_size to living_area_sqm (assuming it's already in sqm)
            living_area_sqm = data.get("property_size")  # Already numeric
            # If unit is sqft, convert to sqm
            property_size_unit = data.get("property_size_unit", "").lower()
            if living_area_sqm and "sqft" in property_size_unit:
                living_area_sqm = living_area_sqm * 0.092903  # Convert sqft to sqm

            # Convert contact_phone from array to single string (first phone)
            contact_phone_list = data.get("contact_phone", [])
            agent_phone = contact_phone_list[0] if contact_phone_list else None

            # Return data in format compatible with new database schema
            result = {
                "raw_url": data.get("url"),
                "source": "jiji",
                "source_listing_id": source_listing_id,
                "scrape_timestamp": datetime.now().isoformat(),
                "title": data.get("title"),
                "description": data.get("description"),
                "property_type": data.get("property_type"),
                "listing_type": listing_type,
                "status": "active",  # Assume active if we can scrape it
                "price": data.get("price"),  # Numeric value
                # Currency code (TSh, USD, etc.)
                "price_currency": data.get("currency"),
                "price_period": price_period,
                "country": country,
                "region": region,
                "city": city,
                "district": district,
                "address_text": address_text,
                "latitude": None,
                "longitude": None,
                "bedrooms": data.get("bedrooms"),
                "bathrooms": data.get("bathrooms"),
                "living_area_sqm": living_area_sqm,
                "land_area_sqm": None,  # Not available from Jiji
                "images": data.get("images", []),
                "agent_name": data.get("contact_name"),
                "agent_phone": agent_phone,
                "agent_whatsapp": None,  # Not available from Jiji
                "agent_email": None,  # Not available from Jiji
                "agent_website": None,  # Not available from Jiji
                "agent_profile_url": None,  # Not available from Jiji
            }

            logger.info(
                "✅ Extracted: %s...",
                result["title"][:50] if result.get("title") else "N/A",
            )

            # Save to database if db_session is provided
            if db_session and "error" not in result:
                try:
                    from app.services.database_service import DatabaseService

                    db_service = DatabaseService(db_session)
                    db_service.create_or_update_listing(result, target_site)
                    logger.info("💾 Saved listing to database: %s", listing_url)
                except Exception:
                    logger.error(
                        "Error saving listing to database", exc_info=True
                    )

            return result

        except Exception:
            logger.error(
                "Error extracting details from %s", listing_url, exc_info=True
            )
            return {
                "raw_url": listing_url,
                "error": "Extraction failed",
                "scraped_at": datetime.now().isoformat(),
            }
        finally:
            # Update status after extraction
            if self.scraping_status.get("type") == "details":
                # Only update if not stopped (if stopped, status will be updated by the loop)
                if not self.should_stop:
                    self.scraping_status["urls_scraped"] = current_index + 1
                self.scraping_status["current_url"] = None

                # If stopped, update status
                if self.should_stop:
                    self.scraping_status["status"] = "stopped"

                self._broadcast_status()

            # Note: Don't reset is_scraping and should_stop here
            # They will be reset by the calling function (_scrape_detailed_listings_task)
            # when the loop completes or breaks
