"""
Kupatana.com Real Estate Scraper Service
Service for scraping real estate listings from kupatana.com
"""

import time
import logging
import re
import random
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import os

from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KupatanaService:
    """Kupatana scraper service for real estate listings"""

    _instance: Optional['KupatanaService'] = None

    def __init__(self, headless: bool = False, profile_dir: str = None):
        """
        Initialize the scraper

        Args:
            headless: Run browser in headless mode
            profile_dir: Directory to save browser profile (for persistent sessions)
        """
        self.base_url = "https://kupatana.com"
        self.headless = headless
        # Default profile directory
        self.profile_dir = profile_dir or "./kupatana_browser_profile"
        self.driver = None
        self.is_scraping = False  # Track if currently scraping
        self.should_stop = False  # Flag to stop scraping gracefully
        self.scraping_status = {
            'type': None,  # 'listings' or 'details' or 'auto_cycle' or None
            'target_site': None,
            'current_page': 0,
            'total_pages': None,
            'pages_scraped': 0,
            'listings_found': 0,
            'listings_saved': 0,
            'current_url': None,
            'total_urls': 0,
            'urls_scraped': 0,
            'status': 'idle',  # 'idle', 'scraping', 'completed', 'error', 'stopped'
            'auto_cycle_running': False,
            'cycle_number': None,
            'phase': None,  # 'basic_listings', 'details', 'waiting'
            'wait_minutes': None,
        }

    @classmethod
    def get_instance(cls) -> 'KupatanaService':
        """Get or create singleton instance of KupatanaService"""
        if cls._instance is None:
            from app.core.config import settings
            logger.info("Initializing Kupatana scraper...")
            try:
                cls._instance = cls(
                    profile_dir=settings.KUPATANA_PROFILE_DIR,
                    headless=settings.SCRAPER_HEADLESS
                )
                cls._instance.start_browser()

                # Navigate to homepage to initialize
                try:
                    cls._instance.driver.set_page_load_timeout(30)
                    cls._instance.driver.get(
                        "https://kupatana.com/tz/search/real-estate")
                    time.sleep(3)  # Wait for page to settle
                    logger.info(
                        "✓ Kupatana scraper ready (navigated to homepage)")
                except Exception as e:
                    # Even if navigation times out, the page might still be usable
                    logger.warning(
                        f"⚠ Initial navigation warning (page may still be loading): {str(e)[:100]}")
                    logger.info("✓ Kupatana scraper ready")

            except Exception as e:
                logger.error(f"Failed to initialize Kupatana scraper: {e}")
                cls._instance = None
                raise

        return cls._instance

    @classmethod
    def close_instance(cls):
        """Close the singleton instance and browser"""
        if cls._instance:
            try:
                cls._instance.close_browser()
                logger.info("✓ Kupatana scraper closed")
            except Exception as e:
                logger.error(f"Error closing Kupatana scraper: {e}")
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
            logger.info("Stop flag set for Kupatana scraper")

    def _check_should_stop(self) -> bool:
        """Check if scraping should be stopped"""
        return self.should_stop

    def _broadcast_status(self):
        """Broadcast scraping status via WebSocket"""
        try:
            from app.core.websocket_manager import manager
            manager.broadcast_sync({
                'type': 'scraping_status',
                'target_site': self.scraping_status.get('target_site'),
                'data': self.scraping_status.copy()
            })
        except Exception as e:
            logger.debug(f"Error broadcasting status: {e}")

    def start_browser(self):
        """Start the undetected Chrome browser with persistent profile"""
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
                "Headless mode disabled for undetected-chromedriver compatibility")
            # options.add_argument('--headless=new')  # Disabled - causes issues
            # Move window off-screen
            options.add_argument('--window-position=0,0')

        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')

        # Use persistent profile directory to save session
        if self.profile_dir:
            profile_path = os.path.abspath(self.profile_dir)

            # Create profile directory if it doesn't exist
            os.makedirs(profile_path, exist_ok=True)

            options.add_argument(f'--user-data-dir={profile_path}')
            logger.info(f"Using browser profile: {profile_path}")

        try:
            self.driver = uc.Chrome(options=options, version_main=None)

            if not self.headless:
                self.driver.maximize_window()

            # Set reasonable timeouts
            self.driver.set_page_load_timeout(45)
            self.driver.set_script_timeout(30)

            logger.info("Browser started successfully")
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            raise

    def close_browser(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")

    def wait_for_page_load(self, timeout: int = 15):
        """Wait for page to load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script(
                    'return document.readyState') == 'complete'
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            logger.warning("Page load timeout - continuing anyway")
        except Exception as e:
            logger.warning(
                f"Page load error: {str(e)[:100]} - continuing anyway")

    def is_404_page(self, soup: BeautifulSoup) -> bool:
        """Check if the current page is a 404 error page"""
        # Check for common 404 indicators on Kupatana
        # This may need adjustment based on actual 404 page structure
        if soup.find('div', class_='error-404'):
            return True
        if soup.find('h1', string=re.compile(r'404|not found', re.IGNORECASE)):
            return True
        if '404' in soup.get_text() and 'not found' in soup.get_text().lower():
            return True

        # Check if no listings found on page
        product_list = soup.find('div', class_='search-product-list')
        if product_list:
            listings = product_list.find_all(
                'div', class_='product-list__item')
            if not listings:
                return True

        return False

    def get_all_listings_basic(self, max_pages: Optional[int] = None,
                               db_session=None, target_site: str = 'kupatana') -> List[Dict]:
        """
        Scrape all listings (url, title, price, currency) from all available pages.
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
        auto_cycle_running = self.scraping_status.get('auto_cycle_running', False)
        cycle_number = self.scraping_status.get('cycle_number')
        phase = self.scraping_status.get('phase')
        wait_minutes = self.scraping_status.get('wait_minutes')

        # Initialize scraping status
        self.scraping_status = {
            'type': 'listings',
            'target_site': target_site,
            'current_page': 0,
            'total_pages': max_pages,
            'pages_scraped': 0,
            'listings_found': 0,
            'listings_saved': 0,
            'current_url': None,
            'total_urls': 0,
            'urls_scraped': 0,
            'status': 'scraping',
            'auto_cycle_running': auto_cycle_running,
            'cycle_number': cycle_number,
            'phase': phase if auto_cycle_running else None,
            'wait_minutes': wait_minutes,
        }
        self._broadcast_status()

        logger.info(
            "Starting to scrape all listings (url, title, price) from all pages...")
        all_listings = []
        seen_urls = set()  # Track URLs we've already seen to detect duplicates
        page_num = 1
        consecutive_404_count = 0  # Track consecutive 404 pages
        consecutive_no_new_count = 0  # Track consecutive pages with no new listings
        total_saved = 0  # Track total saved to database

        # Initialize database service if session is provided
        db_service = None
        if db_session:
            from app.services.database_service import DatabaseService
            db_service = DatabaseService(db_session)

        try:
            while True:
                # Check if stop flag is set
                if self.should_stop:
                    logger.info(
                        "Stop flag detected. Stopping scraping operation.")
                    break

                # Check if we've reached max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(
                        f"Reached maximum page limit ({max_pages}). Stopping pagination.")
                    break

                try:
                    # Construct URL
                    if page_num == 1:
                        url = f"{self.base_url}/tz/search/real-estate"
                    else:
                        url = f"{self.base_url}/tz/search/real-estate?page={page_num}"
                    
                    logger.info(f"Fetching page {page_num}... ({url})")
                    
                    try:
                        self.driver.get(url)
                        self.wait_for_page_load()
                    except Exception as nav_error:
                        logger.warning(f"Navigation warning on page {page_num}: {str(nav_error)[:100]}")
                        # Continue anyway, page might have loaded partially
                        time.sleep(3)
                    
                    # Parse page
                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    
                    # Check if this is a 404 page
                    if self.is_404_page(soup):
                        consecutive_404_count += 1
                        logger.info(f"⚠️  Page {page_num} returned 404. (Consecutive 404 count: {consecutive_404_count})")
                        
                        # If two consecutive pages are 404, stop
                        if consecutive_404_count >= 2:
                            logger.info(f"⚠️  Two consecutive pages returned 404. Stopping pagination.")
                            break
                        
                        # Continue to next page to check if it's also 404
                        page_num += 1
                        continue
                    else:
                        # Reset counter if we get a valid page
                        consecutive_404_count = 0
                    
                    # Find all listing cards
                    listing_cards = soup.find_all('div', class_='product-list__item')
                    
                    if not listing_cards:
                        logger.warning(f"No listings found on page {page_num}. Refreshing page and retrying...")
                        # Refresh the page and try again
                        self.driver.refresh()
                        time.sleep(3)  # Wait for page to reload
                        self.wait_for_page_load()
                        
                        # Parse page again after refresh
                        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                        listing_cards = soup.find_all('div', class_='product-list__item')
                        
                        if not listing_cards:
                            logger.warning(f"No listings found on page {page_num} after refresh. Moving to next page.")
                            page_num += 1
                            continue
                        else:
                            logger.info(f"Found {len(listing_cards)} listings after refresh on page {page_num}")
                    
                    # For pages after the first, skip the first 8 listings (they are duplicates)
                    if page_num == 1:
                        # First page: process all listings
                        cards_to_process = listing_cards
                        logger.info(f"Page {page_num}: Processing all {len(cards_to_process)} listings (first page)")
                    else:
                        # Subsequent pages: skip first 8 listings
                        if len(listing_cards) > 8:
                            cards_to_process = listing_cards[8:]
                            logger.info(f"Page {page_num}: Skipping first 8 duplicate listings, processing {len(cards_to_process)} listings")
                        else:
                            # If page has 8 or fewer listings, they're all duplicates, skip this page
                            logger.info(f"Page {page_num}: Only {len(listing_cards)} listings found (all duplicates), skipping page")
                            consecutive_no_new_count += 1
                            if consecutive_no_new_count >= 2:
                                logger.info(f"⚠️  Two consecutive pages with no new listings. Stopping pagination.")
                                break
                            page_num += 1
                            continue
                    
                    # Extract data from each listing card
                    page_listings = []
                    new_listings_count = 0  # Track new listings on this page
                    for card in cards_to_process:
                        try:
                            # Extract URL
                            link = card.find('a')
                            if not link:
                                continue
                            
                            href = link.get('href')
                            if not href:
                                continue
                            
                            full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                            
                            # Extract title
                            title_elem = card.find('h3', class_='product-item__title')
                            title = title_elem.get_text(strip=True) if title_elem else 'N/A'
                            
                            # Extract price
                            price_elem = card.find('div', class_='product-item__price')
                            
                            # Parse price and currency
                            currency = None
                            price_value = None
                            
                            if price_elem:
                                # Get text and clean it up
                                price_text = price_elem.get_text(strip=True)
                                
                                # Extract currency
                                if 'TZS' in price_text or 'TSh' in price_text:
                                    currency = 'TSh'
                                    price_text = price_text.replace('TZS', '').replace('TSh', '').strip()
                                elif 'USD' in price_text:
                                    currency = 'USD'
                                    price_text = price_text.replace('USD', '').strip()
                                elif '$' in price_text:
                                    currency = 'USD'
                                    price_text = price_text.replace('$', '').strip()
                                elif '€' in price_text:
                                    currency = 'EUR'
                                    price_text = price_text.replace('€', '').strip()
                                
                                # Try to parse numeric value
                                try:
                                    # Remove all spaces and get numeric value
                                    price_cleaned = price_text.replace(' ', '').strip()
                                    if price_cleaned:
                                        price_value = float(price_cleaned)
                                except:
                                    pass
                            
                            listing_data = {
                                'raw_url': full_url,
                                'title': title,
                                'price': price_value,
                                'price_currency': currency,
                                'source': 'kupatana'
                            }
                            
                            # Check if this URL is new (not seen before)
                            if full_url not in seen_urls:
                                seen_urls.add(full_url)
                                page_listings.append(listing_data)
                                new_listings_count += 1
                            else:
                                logger.debug(f"Skipping duplicate listing: {full_url}")
                        
                        except Exception as e:
                            logger.debug(f"Error extracting data from listing card: {e}")
                            continue
                    
                    # Check if we found any new listings on this page
                    if new_listings_count == 0:
                        consecutive_no_new_count += 1
                        logger.info(f"⚠️  Page {page_num}: No new listings found (Consecutive no-new count: {consecutive_no_new_count})")
                        
                        # If two consecutive pages have no new listings, stop
                        if consecutive_no_new_count >= 2:
                            logger.info(f"⚠️  Two consecutive pages with no new listings. Stopping pagination.")
                            break
                    else:
                        # Reset counter if we found new listings
                        consecutive_no_new_count = 0
                    
                    if page_listings:
                        all_listings.extend(page_listings)
                        logger.info(f"✅ Page {page_num}: Found {new_listings_count} new listings, {len(page_listings)} total (Total: {len(all_listings)})")
                        
                        # Update scraping status
                        self.scraping_status['current_page'] = page_num
                        self.scraping_status['pages_scraped'] = page_num
                        self.scraping_status['listings_found'] = len(all_listings)
                        
                        # Save to database immediately if db_session is provided
                        if db_service:
                            page_saved = 0
                            for listing_data in page_listings:
                                try:
                                    db_service.create_or_update_listing(listing_data, target_site)
                                    page_saved += 1
                                except Exception as e:
                                    logger.error(f"Error saving listing {listing_data.get('url')}: {e}")
                                    continue
                            total_saved += page_saved
                            self.scraping_status['listings_saved'] = total_saved
                            logger.info(f"💾 Page {page_num}: Saved {page_saved} listings to database (Total saved: {total_saved})")
                        
                        # Broadcast status update
                        self._broadcast_status()
                    else:
                        logger.warning(f"Page {page_num}: No valid listings extracted. Moving to next page.")
                    
                    # Random delay before next page
                    time.sleep(random.uniform(2, 4))
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    # If we get an error, try one more page before giving up
                    page_num += 1
                    if page_num > 1000:  # Safety limit (very high to allow many pages)
                        logger.warning("Reached safety limit (1000 pages). Stopping pagination.")
                        break
                    continue
            
            logger.info(f"✅ Scraped {len(all_listings)} listings from {page_num - 1} pages")
            if db_service:
                logger.info(f"💾 Total saved to database: {total_saved} listings")
            return all_listings
        finally:
            # Check stop flag before resetting it
            was_stopped = self.should_stop
            
            # Always reset scraping status and stop flag
            self.is_scraping = False
            self.should_stop = False
            
            # Update final status (preserve auto cycle fields)
            if was_stopped:
                self.scraping_status['status'] = 'stopped'
            else:
                self.scraping_status['status'] = 'completed'
            self.scraping_status['current_page'] = 0
            
            # If not part of auto cycle, clear phase
            if not self.scraping_status.get('auto_cycle_running'):
                self.scraping_status['phase'] = None
            
            self._broadcast_status()
                
            logger.info("Scraping completed, status reset")
    
    def extract_phone_from_tel_link(self, soup: BeautifulSoup) -> List[str]:
        """Extract phone numbers from tel: links"""
        phones = []
        
        # Find all tel: links
        tel_links = soup.find_all('a', href=re.compile(r'^tel:'))
        
        for link in tel_links:
            tel_href = link.get('href', '')
            # Extract number from tel:+255784899175
            phone = tel_href.replace('tel:', '').replace('+255', '0').strip()
            
            # Validate phone number (Tanzanian format: 0XXXXXXXXX)
            if phone and re.match(r'^0\d{9}$', phone):
                if phone not in phones:
                    phones.append(phone)
        
        return phones
    
    def extract_detailed_data(self, url: str, total_urls: int = 0, current_index: int = 0, 
                             db_session=None, target_site: str = 'kupatana') -> Dict:
        """
        Extract detailed information from a listing page and save to database
        
        Args:
            url: URL of the listing detail page
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
        self.scraping_status['current_url'] = url
        self.scraping_status['urls_scraped'] = current_index - 1  # -1 because we're about to process this URL
        if total_urls > 0:
            self.scraping_status['total_urls'] = total_urls
        
        self._broadcast_status()
        logger.info(f"🔍 Extracting data from: {url}")
        
        try:
            # Check if stop flag is set before starting
            if self.should_stop:
                logger.info("Stop flag detected. Skipping detailed extraction.")
                return {
                    'raw_url': url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }
            self.driver.get(url)
            self.wait_for_page_load()
            
            # Check if stop flag is set after page load
            if self.should_stop:
                logger.info("Stop flag detected after page load. Stopping detailed extraction.")
                return {
                    'raw_url': url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }
            
            # Get page source
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Check if stop flag is set after parsing
            if self.should_stop:
                logger.info("Stop flag detected after parsing. Stopping detailed extraction.")
                return {
                    'raw_url': url,
                    'error': 'Scraping was stopped',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract title
            title_elem = soup.find('h1', class_='product-details__title')
            if title_elem:
                title = title_elem.get_text(strip=True)
            else:
                # Fallback to any h1
                title_elem = soup.find('h1')
                title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract listing type from title (sale, rent, lease, etc.)
            listing_type = None
            if title:
                title_lower = title.lower()
                if 'for rent' in title_lower or 'to rent' in title_lower or 'rent' in title_lower:
                    listing_type = 'rent'
                elif 'for sale' in title_lower or 'to sell' in title_lower or 'sale' in title_lower:
                    listing_type = 'sale'
                elif 'for lease' in title_lower or 'to lease' in title_lower or 'lease' in title_lower:
                    listing_type = 'lease'
            
            # Extract price and currency
            price_elem = soup.find('h2', class_='product-details__price')
            currency = None
            price_value = None
            
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                # Parse currency and numeric value
                # Examples: "TZS 4 500 000", "USD 5,000", "$500"
                if 'TZS' in price_text or 'TSh' in price_text:
                    currency = 'TSh'
                    price_text = price_text.replace('TZS', '').replace('TSh', '').strip()
                elif 'USD' in price_text:
                    currency = 'USD'
                    price_text = price_text.replace('USD', '').strip()
                elif '$' in price_text:
                    currency = 'USD'
                    price_text = price_text.replace('$', '').strip()
                elif '€' in price_text:
                    currency = 'EUR'
                    price_text = price_text.replace('€', '').strip()
                
                # Try to parse numeric value (remove spaces, commas)
                try:
                    price_cleaned = price_text.replace(' ', '').replace(',', '').strip()
                    if price_cleaned:
                        price_value = float(price_cleaned)
                except:
                    pass
            
            # Extract location
            location = None
            # Method 1: From product-details__location span (inside product-details__meta)
            location_elem = soup.find('span', class_='product-details__location')
            if location_elem:
                location = location_elem.get_text(strip=True)
            # Method 2: From info-box__bubble (map location)
            if not location:
                info_box = soup.find('div', class_='info-box__bubble')
                if info_box:
                    location_text = info_box.get_text(strip=True)
                    # Remove icon text if present (like "icon-map")
                    location = location_text.strip()
            
            # Extract description
            desc_elem = soup.find('p', class_='product-details__description--text')
            if desc_elem:
                description = desc_elem.get_text(strip=True)
            else:
                # Fallback method
                desc_heading = soup.find(string=re.compile(r'Description', re.IGNORECASE))
                if desc_heading:
                    desc_parent = desc_heading.find_parent()
                    if desc_parent:
                        desc_elem = desc_parent.find_next_sibling('p')
                        description = desc_elem.get_text(strip=True) if desc_elem else None
                    else:
                        description = None
                else:
                    description = None
            
            # Initialize structured data fields (matching Jiji format)
            property_type = None
            bedrooms = None
            bathrooms = None
            parking_space = None
            property_size = None
            property_size_unit = None
            attributes = {}
            
            # Extract property type from categories section
            # Find the Categories heading first
            categories_heading = soup.find('h4', class_='custom-card__title', string=re.compile(r'Categories', re.IGNORECASE))
            if categories_heading:
                # Find the parent custom-card div
                categories_section = categories_heading.find_parent('div', class_='custom-card')
                if categories_section:
                    # Find all category tags (div.ant-tag) within the categories section
                    category_tags = categories_section.find_all('div', class_='ant-tag')
                    for tag in category_tags:
                        category_text = tag.get_text(strip=True)
                        if category_text:
                            category_lower = category_text.lower()
                            # Skip "Real estate" as it's too generic
                            if 'real estate' in category_lower and len(category_text.split()) <= 2:
                                continue
                            
                            # Parse category to extract property type
                            # Examples: "Houses - Apartments for Rent" -> "House" or "Apartment"
                            if 'house' in category_lower and 'apartment' not in category_lower:
                                property_type = 'House'
                            elif 'apartment' in category_lower:
                                property_type = 'Apartment'
                            elif 'house' in category_lower and 'apartment' in category_lower:
                                # If both mentioned, check description or title for more specific info
                                if description and 'standalone' in description.lower():
                                    property_type = 'House'
                                else:
                                    property_type = 'House'  # Default to house if both mentioned
                            elif 'villa' in category_lower:
                                property_type = 'Villa'
                            elif 'bungalow' in category_lower:
                                property_type = 'Bungalow'
                            elif 'land' in category_lower:
                                property_type = 'Land'
                            elif 'commercial' in category_lower:
                                property_type = 'Commercial Property'
                            elif 'flat' in category_lower:
                                property_type = 'Flat'
                            elif 'studio' in category_lower:
                                property_type = 'Studio'
                            # Use the most specific category (usually the last one)
                            if property_type:
                                break
            
            # Also check breadcrumb for property type
            if not property_type:
                breadcrumb = soup.find('div', class_='product-breadcrumb')
                if breadcrumb:
                    breadcrumb_links = breadcrumb.find_all('a', href=re.compile(r'/tz/search/'))
                    for link in breadcrumb_links:
                        link_text = link.get_text(strip=True)
                        if link_text:
                            link_lower = link_text.lower()
                            if 'house' in link_lower and 'apartment' not in link_lower:
                                property_type = 'House'
                            elif 'apartment' in link_lower:
                                property_type = 'Apartment'
                            elif 'house' in link_lower and 'apartment' in link_lower:
                                property_type = 'House'  # Default to house if both mentioned
                            elif 'villa' in link_lower:
                                property_type = 'Villa'
                            elif 'land' in link_lower:
                                property_type = 'Land'
                            if property_type:
                                break
            
            # Also check description and title for property type keywords
            if not property_type:
                text_to_check = (title or '') + ' ' + (description or '')
                text_lower = text_to_check.lower()
                if 'standalone' in text_lower or 'stand alone' in text_lower:
                    property_type = 'House'
                elif 'apartment' in text_lower or 'flat' in text_lower:
                    property_type = 'Apartment'
                elif 'villa' in text_lower:
                    property_type = 'Villa'
                elif 'bungalow' in text_lower:
                    property_type = 'Bungalow'
                elif 'land' in text_lower and 'plot' in text_lower:
                    property_type = 'Land'
                elif 'commercial' in text_lower:
                    property_type = 'Commercial Property'
            
            # Extract details/attributes from product-details__attributes
            details_section = soup.find('div', class_='product-details__attributes')
            if details_section:
                # Find all attribute rows (ant-row-flex with product-details__attributes--text)
                attr_rows = details_section.find_all('div', class_='ant-row-flex')
                for row in attr_rows:
                    # Find all divs with ant-col classes (ant-col-xs-12, ant-col-sm-10, etc.)
                    cols = row.find_all('div', class_=re.compile(r'ant-col'))
                    if len(cols) >= 2:
                        key = cols[0].get_text(strip=True)
                        value = cols[1].get_text(strip=True)
                        if key and value:
                            # Try to extract structured data
                            key_lower = key.lower()
                            value_lower = value.lower()
                            
                            # Property type
                            if key_lower in ['type', 'property type', 'category']:
                                if value_lower in ['house', 'apartment', 'villa', 'bungalow', 'flat', 'studio', 'land', 'commercial property']:
                                    property_type = value
                            
                            # Bedrooms - also check description for "3 bdrsms" or "3 bedrooms"
                            elif 'bedroom' in key_lower or 'bed' in key_lower or 'bdr' in key_lower:
                                match = re.search(r'(\d+)', value)
                                if match:
                                    bedrooms = int(match.group(1))
                            
                            # Bathrooms
                            elif 'bathroom' in key_lower or 'bath' in key_lower:
                                match = re.search(r'(\d+)', value)
                                if match:
                                    bathrooms = int(match.group(1))
                            
                            # Parking - also check description for "parking can accomodate 8 cars"
                            elif 'parking' in key_lower:
                                match = re.search(r'(\d+)', value)
                                if match:
                                    parking_space = int(match.group(1))
                                elif 'yes' in value_lower or 'available' in value_lower:
                                    parking_space = 1
                            
                            # Property size
                            elif 'size' in key_lower or 'area' in key_lower:
                                match = re.search(r'([\d,\.]+)\s*(\w+)', value)
                                if match:
                                    try:
                                        property_size = float(match.group(1).replace(',', ''))
                                        property_size_unit = match.group(2)  # sqm, sqft, etc.
                                    except:
                                        pass
                            
                            # Store all attributes
                            attributes[key] = value
            
            # Also try to extract structured data from description if not found in attributes
            if description:
                desc_lower = description.lower()
                
                # Extract bedrooms from description (e.g., "3 bdrsms")
                if bedrooms is None:
                    bed_match = re.search(r'(\d+)\s*(?:bedroom|bdr|bed)', desc_lower)
                    if bed_match:
                        bedrooms = int(bed_match.group(1))
                
                # Extract parking from description (e.g., "parking can accomodate 8 cars")
                if parking_space is None:
                    parking_match = re.search(r'parking.*?(\d+)\s*(?:car|space)', desc_lower)
                    if parking_match:
                        parking_space = int(parking_match.group(1))
                    elif 'parking' in desc_lower and ('yes' in desc_lower or 'available' in desc_lower):
                        parking_space = 1
            
            # Extract images
            images = []
            
            # Method 1: Find images in image gallery (specific class)
            gallery_imgs = soup.find_all('img', class_='image-gallery-image')
            for img in gallery_imgs:
                img_url = img.get('src')
                if img_url and 'http' in img_url:
                    full_img_url = urljoin(self.base_url, img_url)
                    if full_img_url not in images:
                        images.append(full_img_url)
            
            # Method 2: Find all images with src (fallback)
            if not images:
                img_elements = soup.find_all('img', src=True)
                for img in img_elements:
                    img_url = img.get('src')
                    # Filter out icons, logos, avatars
                    if img_url and ('http' in img_url or img_url.startswith('//')):
                        # Exclude small images (likely icons/logos)
                        if ('icon' not in img_url.lower() and 
                            'logo' not in img_url.lower() and 
                            'avatar' not in img_url.lower() and
                            'thumbnail' not in img_url.lower()):
                            full_img_url = urljoin(self.base_url, img_url)
                            if full_img_url not in images:
                                images.append(full_img_url)
            
            # Limit to reasonable number
            images = images[:20]
            
            # Extract contact information (matching Jiji format)
            contact_name = None
            contact_phone = []
            
            # Extract seller name
            seller_name_elem = soup.find('h4', class_='product-chat__avatar__title')
            if seller_name_elem:
                contact_name = seller_name_elem.get_text(strip=True)
            else:
                # Fallback: Find in product-user-info__avatar__title
                seller_name_elem = soup.find('h4', class_='product-user-info__avatar__title')
                if seller_name_elem:
                    contact_name = seller_name_elem.get_text(strip=True)
            
            # Extract phone numbers from tel: links
            phones = self.extract_phone_from_tel_link(soup)
            contact_phone = phones
            
            if phones:
                logger.info(f"✅ Extracted {len(phones)} phone number(s): {', '.join(phones)}")
            
            # Parse location into structured fields
            # Location format from Kupatana can vary - try to parse it
            country = 'Tanzania'
            region = None
            city = None
            district = None
            address_text = location
            
            if location:
                # Try to parse location string (e.g., "Dar es Salaam, Kinondoni" or "Dar es Salaam")
                location_parts = [part.strip() for part in location.split(',')]
                if len(location_parts) >= 2:
                    city = location_parts[0]
                    district = location_parts[1]
                    if len(location_parts) >= 3:
                        region = location_parts[2]
                    else:
                        region = city  # Use city as region if not specified separately
                elif len(location_parts) == 1:
                    city = location_parts[0]
                    region = city
            
            # Extract source_listing_id from URL
            # URL format: https://kupatana.com/tz/products/standalone-house-for-rent-in-mbezi-beach-3-bedrooms-123456
            source_listing_id = None
            if url:
                # Extract the ID from the end of the URL (typically the last segment)
                url_parts = url.rstrip('/').split('/')[-1]
                # Try to extract numeric ID if present
                id_match = re.search(r'-(\d+)$', url_parts)
                if id_match:
                    source_listing_id = id_match.group(1)
                else:
                    # Use the entire slug as ID if no numeric ID found
                    source_listing_id = url_parts
            
            # Determine price_period based on listing_type
            price_period = None
            if listing_type == 'rent':
                price_period = 'month'
            elif listing_type == 'sale':
                price_period = 'once'
            
            # Convert property_size to living_area_sqm (assuming it's already in sqm)
            living_area_sqm = property_size  # Already numeric
            # If unit is sqft, convert to sqm
            if living_area_sqm and property_size_unit and 'sqft' in property_size_unit.lower():
                living_area_sqm = living_area_sqm * 0.092903  # Convert sqft to sqm
            
            # Convert contact_phone from array to single string (first phone)
            agent_phone = contact_phone[0] if contact_phone else None
            
            # Return data in format compatible with new database schema
            result = {
                'raw_url': url,
                'source': 'kupatana',
                'source_listing_id': source_listing_id,
                'scrape_timestamp': datetime.now().isoformat(),
                'title': title,
                'description': description,
                'property_type': property_type,
                'listing_type': listing_type,
                'status': 'active',  # Assume active if we can scrape it
                'price': price_value,  # Numeric value
                'price_currency': currency,  # Currency code (TSh, USD, etc.)
                'price_period': price_period,
                'country': country,
                'region': region,
                'city': city,
                'district': district,
                'address_text': address_text,
                'latitude': None,
                'longitude': None,
                'bedrooms': bedrooms,
                'bathrooms': bathrooms,
                'living_area_sqm': living_area_sqm,
                'land_area_sqm': None,  # Not typically available from Kupatana
                'images': images,
                'agent_name': contact_name,
                'agent_phone': agent_phone,
                'agent_whatsapp': None,  # Not available from Kupatana
                'agent_email': None,  # Not available from Kupatana
                'agent_website': None,  # Not available from Kupatana
                'agent_profile_url': None  # Not available from Kupatana
            }
            
            logger.info(f"✅ Extracted: {result['title'][:50] if result.get('title') else 'Unknown'}...")
            
            # Save to database if db_session is provided
            if db_session and 'error' not in result:
                try:
                    from app.services.database_service import DatabaseService
                    db_service = DatabaseService(db_session)
                    db_service.create_or_update_listing(result, target_site)
                    logger.info(f"💾 Saved listing to database: {url}")
                except Exception as e:
                    logger.error(f"Error saving listing to database: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from {url}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {
                'raw_url': url,
                'error': str(e),
                'scraped_at': datetime.now().isoformat()
            }
        finally:
            # Note: Status updates are handled by the calling function (_scrape_detailed_listings_task)
            # We don't update status here to avoid race conditions
            pass
    
