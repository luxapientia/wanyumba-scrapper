"""
Kupatana.com Real Estate Scraper Service
Service for scraping real estate listings from kupatana.com
"""

import time
import logging
import json
import re
import random
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
        self.profile_dir = profile_dir or "./kupatana_browser_profile"  # Default profile directory
        self.driver = None
        self.detailed_listings = []
        self.is_scraping = False  # Track if currently scraping
        self.should_stop = False  # Flag to stop scraping gracefully
        self.scraping_status = {
            'type': None,  # 'listings' or 'details' or None
            'target_site': None,
            'current_page': 0,
            'total_pages': None,
            'pages_scraped': 0,
            'listings_found': 0,
            'listings_saved': 0,
            'current_url': None,
            'total_urls': 0,
            'urls_scraped': 0,
            'status': 'idle'  # 'idle', 'scraping', 'completed', 'error', 'stopped'
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
                    cls._instance.driver.get("https://kupatana.com/tz/search/real-estate")
                    time.sleep(3)  # Wait for page to settle
                    logger.info("✓ Kupatana scraper ready (navigated to homepage)")
                except Exception as e:
                    # Even if navigation times out, the page might still be usable
                    logger.warning(f"⚠ Initial navigation warning (page may still be loading): {str(e)[:100]}")
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
            logger.warning("Headless mode disabled for undetected-chromedriver compatibility")
            # options.add_argument('--headless=new')  # Disabled - causes issues
            options.add_argument('--window-position=0,0')  # Move window off-screen
        
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
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            logger.warning("Page load timeout - continuing anyway")
        except Exception as e:
            logger.warning(f"Page load error: {str(e)[:100]} - continuing anyway")
    
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
            listings = product_list.find_all('div', class_='product-list__item')
            if not listings:
                return True
        
        return False
    
    def get_all_listings_basic(self, max_pages: Optional[int] = None,
                               db_session = None, target_site: str = 'kupatana') -> List[Dict]:
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
            'status': 'scraping'
        }
        self._broadcast_status()
        
        logger.info("Starting to scrape all listings (url, title, price) from all pages...")
        all_listings = []
        page_num = 1
        consecutive_404_count = 0  # Track consecutive 404 pages
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
                    logger.info("Stop flag detected. Stopping scraping operation.")
                    break
                
                # Check if we've reached max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(f"Reached maximum page limit ({max_pages}). Stopping pagination.")
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
                    
                    # Extract data from each listing card
                    page_listings = []
                    for card in listing_cards:
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
                                'url': full_url,
                                'title': title,
                                'price': price_value,
                                'currency': currency
                            }
                            
                            page_listings.append(listing_data)
                            
                        except Exception as e:
                            logger.debug(f"Error extracting data from listing card: {e}")
                            continue
                    
                    if page_listings:
                        all_listings.extend(page_listings)
                        logger.info(f"✅ Page {page_num}: Found {len(page_listings)} listings (Total: {len(all_listings)})")
                        
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
                        else:
                            logger.warning(f"No valid listings extracted from page {page_num}. Stopping pagination.")
                            break
                        
                        # Broadcast status update
                        self._broadcast_status()
                    
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
            # Always reset scraping status and stop flag
            self.is_scraping = False
            self.should_stop = False
            
            # Update final status
            if self.should_stop:
                self.scraping_status['status'] = 'stopped'
            else:
                self.scraping_status['status'] = 'completed'
            self.scraping_status['current_page'] = 0
            self._broadcast_status()
            
            logger.info("Scraping completed, status reset")
    
    def get_listing_urls(self, max_pages: int = 1) -> List[str]:
        """
        Get all listing URLs from search result pages
        
        Args:
            max_pages: Number of pages to scrape
            
        Returns:
            List of listing URLs
        """
        listing_urls = []
        
        for page_num in range(1, max_pages + 1):
            try:
                # Navigate to search page
                search_url = f"{self.base_url}/tz/search/real-estate?page={page_num}"
                logger.info(f"📄 Fetching page {page_num}: {search_url}")
                
                self.driver.get(search_url)
                self.wait_for_page_load()
                
                # Get page source and parse
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Find all listing links
                # Pattern: /tz/houses-apartments-for-rent/p/{title}/{id}
                links = soup.find_all('a', href=re.compile(r'/tz/.+/p/.+/.+'))
                
                page_urls = []
                for link in links:
                    href = link.get('href')
                    if href and '/p/' in href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in listing_urls and full_url not in page_urls:
                            page_urls.append(full_url)
                
                logger.info(f"✅ Found {len(page_urls)} listings on page {page_num}")
                listing_urls.extend(page_urls)
                
                time.sleep(2)  # Be polite to the server
                
            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                continue
        
        logger.info(f"📊 Total listings found: {len(listing_urls)}")
        return listing_urls
    
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
    
    def extract_detailed_data(self, url: str, total_urls: int = 0, current_index: int = 0) -> Dict:
        """
        Extract detailed information from a listing page
        
        Args:
            url: URL of the listing detail page
            total_urls: Total number of URLs to scrape (for progress tracking)
            current_index: Current index in the URL list (for progress tracking)
            
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
                    'url': url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }
            self.driver.get(url)
            self.wait_for_page_load()
            
            # Check if stop flag is set after page load
            if self.should_stop:
                logger.info("Stop flag detected after page load. Stopping detailed extraction.")
                return {
                    'url': url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }
            
            # Get page source
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Check if stop flag is set after parsing
            if self.should_stop:
                logger.info("Stop flag detected after parsing. Stopping detailed extraction.")
                return {
                    'url': url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }
            
            # Initialize data structure
            data = {
                'url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract listing ID from URL
            url_parts = url.rstrip('/').split('/')
            if len(url_parts) >= 2:
                data['listing_id'] = url_parts[-1]
            
            # Extract title (using specific class)
            title_elem = soup.find('h1', class_='product-details__title')
            if title_elem:
                data['title'] = title_elem.get_text(strip=True)
            else:
                # Fallback to any h1
                title_elem = soup.find('h1')
                data['title'] = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            # Extract price (using specific class)
            price_elem = soup.find('h2', class_='product-details__price')
            if price_elem:
                data['price'] = price_elem.get_text(strip=True)
            else:
                # Fallback to any h2
                price_elem = soup.find('h2')
                data['price'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            # Extract posted date from meta section
            meta_elem = soup.find('p', class_='product-details__meta')
            if meta_elem:
                # Extract date (first part before location span)
                date_text = meta_elem.get_text(strip=True)
                # Remove location part if present
                location_span = meta_elem.find('span', class_='product-details__location')
                if location_span:
                    location_text = location_span.get_text(strip=True)
                    date_text = date_text.replace(location_text, '').strip()
                data['posted_date'] = date_text
            else:
                data['posted_date'] = 'N/A'
            
            # Extract location (multiple methods)
            location = None
            
            # Method 1: From product-details__location span
            location_elem = soup.find('span', class_='product-details__location')
            if location_elem:
                location = location_elem.get_text(strip=True)
            
            # Method 2: From info-box__bubble (map location)
            if not location:
                info_box = soup.find('div', class_='info-box__bubble')
                if info_box:
                    # Remove icon and get text
                    location_text = info_box.get_text(strip=True)
                    # Clean up (remove icon text if present)
                    location = location_text
            
            data['location'] = location if location else 'N/A'
            
            # Extract description (using specific class)
            desc_elem = soup.find('p', class_='product-details__description--text')
            if desc_elem:
                data['description'] = desc_elem.get_text(strip=True)
            else:
                # Fallback method
                desc_heading = soup.find(string=re.compile(r'Description', re.IGNORECASE))
                if desc_heading:
                    desc_parent = desc_heading.find_parent()
                    if desc_parent:
                        desc_elem = desc_parent.find_next_sibling('p')
                        data['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
                    else:
                        data['description'] = 'N/A'
                else:
                    data['description'] = 'N/A'
            
            # Extract details/attributes
            attributes = {}
            details_heading = soup.find(string=re.compile(r'Details', re.IGNORECASE))
            if details_heading:
                details_parent = details_heading.find_parent()
                if details_parent:
                    # Find all attribute pairs (generic elements with key-value)
                    detail_items = details_parent.find_next_siblings()
                    for item in detail_items:
                        # Look for key-value pairs in nested divs/generics
                        children = item.find_all(recursive=False)
                        if len(children) >= 2:
                            key = children[0].get_text(strip=True)
                            value = children[1].get_text(strip=True)
                            if key and value:
                                attributes[key] = value
            
            data['attributes'] = attributes
            
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
            data['images'] = images[:20]
            data['image_count'] = len(data['images'])
            
            # Extract seller information
            seller_info = {}
            
            # Method 1: Find seller name using specific class
            seller_name_elem = soup.find('h4', class_='product-chat__avatar__title')
            if seller_name_elem:
                seller_info['name'] = seller_name_elem.get_text(strip=True)
            else:
                # Fallback: Find in any h4 (skip common headings)
                seller_headings = soup.find_all('h4')
                for heading in seller_headings:
                    text = heading.get_text(strip=True)
                    if text and text not in ['User', 'Categories', 'Location']:
                        seller_info['name'] = text
                        break
            
            # Extract member since information
            member_elem = soup.find('div', class_='product-chat__avatar__member')
            if member_elem:
                member_text = member_elem.get_text(strip=True)
                seller_info['member_since'] = member_text
            else:
                # Fallback
                member_text = soup.find(string=re.compile(r'Member since', re.IGNORECASE))
                if member_text:
                    seller_info['member_since'] = member_text.strip()
            
            # Extract verification status
            verification = soup.find(string=re.compile(r'verified', re.IGNORECASE))
            if verification:
                seller_info['verification_status'] = verification.strip()
            
            # Extract phone numbers from tel: links
            phones = self.extract_phone_from_tel_link(soup)
            seller_info['phones'] = phones
            seller_info['phone_count'] = len(phones)
            
            if phones:
                logger.info(f"✅ Extracted {len(phones)} phone number(s): {', '.join(phones)}")
            else:
                logger.warning("⚠️ No phone numbers found")
            
            data['seller_info'] = seller_info
            
            # Extract category path
            categories = []
            category_links = soup.find_all('a', href=re.compile(r'/tz/search/'))
            for link in category_links:
                cat_text = link.get_text(strip=True)
                if cat_text and cat_text not in categories:
                    categories.append(cat_text)
            data['categories'] = categories
            
            logger.info(f"✅ Successfully extracted data for: {data.get('title', 'Unknown')}")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from {url}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {
                'url': url,
                'error': str(e),
                'scraped_at': datetime.now().isoformat()
            }
        finally:
            # Note: Status updates are handled by the calling function (_scrape_detailed_listings_task)
            # We don't update status here to avoid race conditions
            pass
    
    def scrape_detailed_listings(self, max_pages: int = 1, max_listings: Optional[int] = None):
        """
        Main scraping function
        
        Args:
            max_pages: Number of search result pages to scrape
            max_listings: Maximum number of detailed listings to extract (None for all)
        """
        try:
            self.start_browser()
            
            # Get listing URLs
            logger.info(f"{'='*70}")
            logger.info("STEP 1: Collecting listing URLs")
            logger.info(f"{'='*70}")
            
            listing_urls = self.get_listing_urls(max_pages=max_pages)
            
            if not listing_urls:
                logger.warning("No listings found!")
                return
            
            # Limit listings if specified
            if max_listings:
                listing_urls = listing_urls[:max_listings]
                logger.info(f"Limiting to first {max_listings} listings")
            
            # Extract detailed data
            logger.info(f"\n{'='*70}")
            logger.info(f"STEP 2: Extracting detailed data from {len(listing_urls)} listings")
            logger.info(f"{'='*70}\n")
            
            for idx, url in enumerate(listing_urls, 1):
                logger.info(f"[{idx}/{len(listing_urls)}] Processing listing...")
                data = self.extract_detailed_data(url)
                self.detailed_listings.append(data)
                
                # Small delay between requests
                time.sleep(2)
            
            logger.info(f"\n{'='*70}")
            logger.info("✅ SCRAPING COMPLETED")
            logger.info(f"{'='*70}\n")
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.close_browser()
        
        return self.detailed_listings
    
    def save_to_json(self, filename: str = None):
        """Save detailed data to JSON"""
        if not filename:
            filename = f"kupatana_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.detailed_listings, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Saved to {filename}")
        return filename
    
    def save_to_csv(self, filename: str = None):
        """Save detailed data to CSV"""
        if not filename:
            filename = f"kupatana_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not self.detailed_listings:
            logger.warning("No data to save")
            return None
        
        # Flatten nested data for CSV
        flat_data = []
        for listing in self.detailed_listings:
            flat = listing.copy()
            
            # Convert nested dictionaries to strings
            if 'attributes' in flat and isinstance(flat['attributes'], dict):
                for key, value in flat['attributes'].items():
                    flat[f'attr_{key}'] = value
                del flat['attributes']
            
            if 'seller_info' in flat and isinstance(flat['seller_info'], dict):
                for key, value in flat['seller_info'].items():
                    # Handle list of phone numbers
                    if key == 'phones' and isinstance(value, list):
                        flat[f'seller_{key}'] = '; '.join(value)
                    else:
                        flat[f'seller_{key}'] = value
                del flat['seller_info']
            
            if 'images' in flat and isinstance(flat['images'], list):
                flat['images'] = '; '.join(flat['images'])
            
            if 'categories' in flat and isinstance(flat['categories'], list):
                flat['categories'] = '; '.join(flat['categories'])
            
            flat_data.append(flat)
        
        # Get all keys
        fieldnames = set()
        for item in flat_data:
            fieldnames.update(item.keys())
        fieldnames = sorted(fieldnames)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_data)
        
        logger.info(f"💾 Saved to {filename}")
        return filename
    
    def print_summary(self):
        """Print summary of scraped data"""
        if not self.detailed_listings:
            logger.info("No data scraped yet")
            return
        
        logger.info(f"\n{'='*70}")
        logger.info(f"KUPATANA SCRAPING SUMMARY")
        logger.info(f"{'='*70}")
        logger.info(f"Total listings scraped: {len(self.detailed_listings)}")
        
        # Count successful extractions
        successful = sum(1 for l in self.detailed_listings if 'error' not in l)
        logger.info(f"Successful extractions: {successful}")
        
        # Count with images
        with_images = sum(1 for l in self.detailed_listings if l.get('image_count', 0) > 0)
        logger.info(f"Listings with images: {with_images}")
        
        # Count with seller info
        with_seller = sum(1 for l in self.detailed_listings if l.get('seller_info'))
        logger.info(f"Listings with seller info: {with_seller}")
        
        # Count with phone numbers
        with_phones = sum(1 for l in self.detailed_listings 
                         if l.get('seller_info', {}).get('phone_count', 0) > 0)
        logger.info(f"Listings with phone numbers: {with_phones}")
        
        # Count total phone numbers extracted
        total_phones = sum(l.get('seller_info', {}).get('phone_count', 0) 
                          for l in self.detailed_listings)
        logger.info(f"Total phone numbers extracted: {total_phones}")
        
        # Count total images
        total_images = sum(l.get('image_count', 0) for l in self.detailed_listings)
        logger.info(f"Total images extracted: {total_images}")
        
        # Show sample of attributes found
        all_attrs = set()
        for listing in self.detailed_listings:
            if 'attributes' in listing and isinstance(listing['attributes'], dict):
                all_attrs.update(listing['attributes'].keys())
        
        if all_attrs:
            logger.info(f"\nAttribute types found ({len(all_attrs)}):")
            for attr in sorted(all_attrs):
                logger.info(f"  - {attr}")
        
        logger.info(f"{'='*70}\n")
