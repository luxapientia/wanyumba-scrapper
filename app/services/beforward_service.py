"""
BE FORWARD Homes Scraper Service

This service handles scraping real estate listings from homes.beforward.jp
"""
import logging
import re
import time
import json
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from app.core.config import settings
from app.services.base_scraper_service import BaseScraperService

logger = logging.getLogger(__name__)


class BeForwardService(BaseScraperService):
    """Scraper service for BE FORWARD Homes website"""

    _instance: Optional['BeForwardService'] = None
    _lock = None

    # Listing and property types
    LISTING_TYPES = ['buy', 'rent']
    PROPERTY_TYPES = ['house', 'apartment', 'land', 'commercial']
    COUNTRY = 'tanzania'

    def __init__(self):
        """Initialize BE FORWARD scraper service"""
        super().__init__(
            site_name="beforward",
            base_url="https://homes.beforward.jp",
            profile_dir=settings.BEFORWARD_PROFILE_DIR
        )
        logger.info("BeForwardService initialized")

    @classmethod
    def get_instance(cls) -> 'BeForwardService':
        """Get singleton instance of BeForwardService"""
        import threading
        if cls._lock is None:
            cls._lock = threading.Lock()

        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    # Start browser
                    try:
                        cls._instance.start_browser()
                        # Navigate to main page to initialize
                        cls._instance.driver.get(cls._instance.base_url)
                        time.sleep(3)
                        logger.info("✓ BE FORWARD browser initialized and ready")
                    except Exception as e:
                        logger.error(f"Failed to initialize BE FORWARD browser: {e}")
                        cls._instance = None
                        raise
        return cls._instance

    @classmethod
    def close_instance(cls):
        """Close the singleton instance and cleanup resources"""
        if cls._instance is not None:
            try:
                cls._instance.close()
                cls._instance = None
                logger.info("BE FORWARD instance closed")
            except Exception as e:
                logger.error(f"Error closing BE FORWARD instance: {e}")

    def wait_for_page_load(self, timeout: int = 10):
        """Wait for page to load completely"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            logger.warning("Page load timeout - continuing anyway")

    def parse_price(self, price_str: str) -> Optional[float]:
        """
        Parse price string to float
        
        Args:
            price_str: Price string like "USD 120,000" or "TZS 276,000,000"
            
        Returns:
            Price as float or None if parsing fails
        """
        if not price_str:
            return None
        try:
            # Remove currency symbols, commas, and spaces
            price_clean = re.sub(r'[^\d.]', '', price_str)
            return float(price_clean) if price_clean else None
        except (ValueError, AttributeError):
            return None

    def extract_listing_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract listing ID from BE FORWARD URL
        
        Args:
            url: Listing URL like "/detail/buy/house/all/tanzania/.../65673"
            
        Returns:
            Listing ID or None if not found
        """
        # Extract the numeric ID from the end of the URL
        match = re.search(r'/(\d+)$', url)
        return match.group(1) if match else None

    def has_listings_on_page(self) -> bool:
        """
        Check if current page has any listings
        
        Returns:
            True if listings found, False otherwise
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            # BE FORWARD uses /detail/ URLs
            listings = soup.find_all('a', href=re.compile(r'/detail/'))
            return len(listings) > 0
        except Exception as e:
            logger.error(f"Error checking for listings: {e}")
            return False

    def get_total_pages_from_pagination(self) -> int:
        """
        Extract total pages from pagination
        BE FORWARD uses buttons (not links) for pagination.
        The last button shows the total page count (e.g., "1124")
        
        Returns:
            Total number of pages or 1 if not found
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find pagination container (uses class with pagination in name)
            pagination = soup.find('div', class_=re.compile(r'pagination', re.IGNORECASE))
            if not pagination:
                logger.warning("Pagination container not found")
                return 1
            
            # Find all page number buttons/divs
            # Pattern: <button><div class="...pageNumber...">1124</div></button>
            page_numbers = pagination.find_all('div', class_=re.compile(r'pageNumber', re.IGNORECASE))
            
            max_page = 1
            for page_elem in page_numbers:
                page_text = page_elem.get_text(strip=True)
                
                # Skip ellipsis ("...")
                if page_text == '...':
                    continue
                
                # Try to parse as integer
                try:
                    page_num = int(page_text)
                    max_page = max(max_page, page_num)
                except ValueError:
                    continue
            
            logger.info(f"✓ Detected {max_page} total pages from pagination")
            return max_page
            
        except Exception as e:
            logger.error(f"Error extracting total pages: {e}")
            return 1

    def _scrape_current_page_listings(self, listing_type: str, property_type: str) -> List[Dict]:
        """
        Scrape listings from the current page
        
        Args:
            listing_type: 'buy' or 'rent'
            property_type: 'house', 'apartment', 'land', or 'commercial'
            
        Returns:
            List of listing dictionaries
        """
        listings = []
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find all listing cards
            # BE FORWARD uses /detail/ URLs: /detail/buy/house/all/tanzania/.../123
            listing_elements = soup.find_all('a', href=re.compile(r'/detail/'))
            
            logger.info(f"Found {len(listing_elements)} listing links on page")
            
            for element in listing_elements:
                try:
                    href = element.get('href', '')
                    if not href:
                        continue
                    
                    # Make absolute URL
                    listing_url = f"{self.base_url}{href}" if href.startswith('/') else href
                    
                    # Extract basic info
                    title_elem = element.find('h3') or element.find('div', class_='title')
                    title = title_elem.get_text(strip=True) if title_elem else None
                    
                    # Extract price
                    price_elem = element.find('span', class_='price') or element.find('div', class_='price')
                    price_str = price_elem.get_text(strip=True) if price_elem else None
                    price = self.parse_price(price_str) if price_str else None
                    
                    # Extract location
                    location_elem = element.find('span', class_='location') or element.find('div', class_='location')
                    location = location_elem.get_text(strip=True) if location_elem else None
                    
                    # Extract property details (bedrooms, bathrooms, area)
                    bedrooms = None
                    bathrooms = None
                    area = None
                    
                    details_container = element.find('div', class_='property-details')
                    if details_container:
                        detail_items = details_container.find_all(['span', 'div'])
                        for item in detail_items:
                            text = item.get_text(strip=True).lower()
                            if 'bed' in text:
                                bed_match = re.search(r'(\d+)', text)
                                if bed_match:
                                    bedrooms = int(bed_match.group(1))
                            elif 'bath' in text:
                                bath_match = re.search(r'(\d+)', text)
                                if bath_match:
                                    bathrooms = int(bath_match.group(1))
                            elif 'sqm' in text or 'm²' in text or 'm2' in text:
                                area_match = re.search(r'([\d,]+)', text)
                                if area_match:
                                    area_str = area_match.group(1).replace(',', '')
                                    area = float(area_str)
                    
                    listing_data = {
                        'raw_url': listing_url,
                        'title': title,
                        'price': price,
                        'address_text': location,
                        'bedrooms': bedrooms,
                        'bathrooms': bathrooms,
                        'living_area_sqm': area,
                        'property_type': property_type,
                        'listing_type': listing_type,
                        'source': self.site_name,
                        'country': 'Tanzania'
                    }
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting listing data: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping current page: {e}")
        
        return listings

    def get_all_listings_basic(self, max_pages: Optional[int] = None, db_session=None, target_site: str = None) -> List[Dict]:
        """
        Scrape basic listing information from all pages for all listing/property type combinations
        
        Args:
            max_pages: Maximum pages to scrape per combination (None = all)
            db_session: Database session (optional, will create if not provided)
            target_site: Target site name (unused, for compatibility)
            
        Returns:
            List of listing dictionaries
        """
        # Ensure browser is started (will skip if already running)
        self.start_browser()
        
        # Check if browser is ready
        if self.driver is None:
            logger.error("Failed to start browser")
            return []
        
        self.is_scraping = True
        self.should_stop = False
        self.listings = []
        
        try:
            # Initialize status
            self._init_listings_status(target_site=self.site_name, max_pages=max_pages)
            self._broadcast_status()
            
            # Iterate through all combinations
            for listing_type in self.LISTING_TYPES:
                if self.should_stop:
                    logger.info("Stop flag detected, breaking listing type loop")
                    break
                
                for property_type in self.PROPERTY_TYPES:
                    if self.should_stop:
                        logger.info("Stop flag detected, breaking property type loop")
                        break
                    
                    logger.info(f"Starting scrape for {listing_type}/{property_type}")
                    
                    # Build search URL
                    search_url = f"{self.base_url}/search/{listing_type}/{property_type}/all/{self.COUNTRY}"
                    
                    try:
                        # Navigate to search page
                        self.driver.get(search_url)
                        self.wait_for_page_load()
                        
                        # Get total pages
                        total_pages = self.get_total_pages_from_pagination()
                        if max_pages:
                            total_pages = min(total_pages, max_pages)
                        
                        logger.info(f"Will scrape {total_pages} pages for {listing_type}/{property_type}")
                        self._update_status_field('total_pages', total_pages)
                        self._broadcast_status()
                        
                        # Scrape each page
                        for page_num in range(1, total_pages + 1):
                            if self.should_stop:
                                logger.info("Stop flag detected, breaking page loop")
                                break
                            
                            logger.info(f"Scraping page {page_num}/{total_pages} for {listing_type}/{property_type}")
                            
                            # Navigate to page if not the first one
                            if page_num > 1:
                                page_url = f"{search_url}?page={page_num}"
                                self.driver.get(page_url)
                                self.wait_for_page_load()
                            
                            # Scrape listings from current page
                            page_listings = self._scrape_current_page_listings(listing_type, property_type)
                            
                            if page_listings:
                                self.listings.extend(page_listings)
                                logger.info(f"Found {len(page_listings)} listings on page {page_num}")
                                
                                # Save batch to database
                                if db_session and len(page_listings) > 0:
                                    saved_count = self._save_listings_batch(page_listings, self.site_name, db_session)
                                    logger.info(f"💾 Saved {saved_count} listings from page {page_num}")
                            else:
                                logger.warning(f"No listings found on page {page_num}")
                            
                            # Update progress
                            self._update_status_field('current_page', page_num, broadcast=False)
                            self._update_status_field('pages_scraped', page_num, broadcast=False)
                            self._update_status_field('listings_found', len(self.listings), broadcast=False)
                            self._broadcast_status()
                            
                            # Small delay between pages
                            time.sleep(2)
                    
                    except Exception as e:
                        logger.error(f"Error scraping {listing_type}/{property_type}: {e}")
                        continue
            
            logger.info(f"✓ Scraping complete! Found {len(self.listings)} total listings")
            
            # Update final status
            self._update_status_field('status', 'completed', broadcast=False)
            self._update_status_field('listings_found', len(self.listings), broadcast=False)
            self._broadcast_status()
            
            return self.listings
            
        except Exception as e:
            logger.error(f"Error in BE FORWARD basic scraping: {e}", exc_info=True)
            # Update error status
            self._update_status_field('status', 'error', broadcast=False)
            self._update_status_field('error_message', str(e), broadcast=False)
            self._broadcast_status()
            return self.listings
        
        finally:
            # Finalize and reset flags
            was_stopped = self.should_stop
            self._finalize_status(was_stopped=was_stopped)
            self.is_scraping = False

    def extract_detailed_data(self, listing_url: str, current_index: int = 0, total_urls: int = 0, db_session=None, target_site: str = None) -> Dict:
        """
        Extract detailed data from a single listing page using embedded JSON
        
        Args:
            listing_url: URL of the listing to scrape
            current_index: Current index in batch (for progress tracking)
            total_urls: Total URLs to scrape (for progress tracking)
            db_session: Database session (optional)
            target_site: Target site name (not used, for compatibility)
            
        Returns:
            Dictionary with detailed listing data
        """
        # Ensure browser is started (will skip if already running)
        self.start_browser()
        
        # Check if browser is ready
        if self.driver is None:
            logger.error("Failed to start browser for detail scraping")
            return {}
        
        try:
            # Update progress if this is being called from the base class task
            if current_index > 0 and total_urls > 0:
                self._update_url_progress(listing_url, current_index, total_urls)
                self._broadcast_status()
            
            logger.info(f"Extracting details from: {listing_url}")
            
            # Navigate to listing page
            self.driver.get(listing_url)
            self.wait_for_page_load()
            
            html_content = self.driver.page_source
            
            # Initialize detailed data with required fields
            detailed_data = {
                'raw_url': listing_url,
                'source': self.site_name,
                'scrape_timestamp': datetime.now(),
                'status': 'active',
                'country': 'Tanzania'
            }
            
            # Extract JSON data from script tag (MOST EFFICIENT METHOD)
            # The page has embedded JSON with format: "propertyInfo":{...}
            json_match = re.search(r'"propertyInfo":\{([^}]+(?:\{[^}]+\})?[^}]*)\}', html_content)
            if json_match:
                try:
                    # Build complete JSON string
                    json_str = '{' + json_match.group(0).replace('"propertyInfo":', '"propertyInfo":') + '}'
                    # Extract the propertyInfo object more carefully
                    prop_start = html_content.find('"propertyInfo":{')
                    if prop_start != -1:
                        # Find matching closing brace
                        brace_count = 0
                        prop_data_start = prop_start + len('"propertyInfo":')
                        for i in range(prop_data_start, len(html_content)):
                            if html_content[i] == '{':
                                brace_count += 1
                            elif html_content[i] == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_str = html_content[prop_data_start:i+1]
                                    break
                        
                        property_info = json.loads(json_str)
                        logger.info(f"✓ Successfully parsed JSON data for {listing_url}")
                        
                        # Map JSON fields to detailed_data
                        detailed_data['title'] = property_info.get('property_name_free') or property_info.get('property_name')
                        detailed_data['description'] = property_info.get('description', '')
                        
                        # Property type and listing type
                        # const_transaction_type_id: 1=sale, 2=rent
                        # const_property_type_id: 1=house, 2=apartment, 3=land, 4=commercial
                        transaction_types = {1: 'buy', 2: 'rent'}
                        property_types = {1: 'house', 2: 'apartment', 3: 'land', 4: 'commercial'}
                        detailed_data['listing_type'] = transaction_types.get(property_info.get('const_transaction_type_id'), 'buy')
                        detailed_data['property_type'] = property_types.get(property_info.get('const_property_type_id'), 'house')
                        
                        # Price (use USD price)
                        if property_info.get('price_usd'):
                            detailed_data['price'] = float(property_info['price_usd'])
                            detailed_data['price_currency'] = 'USD'
                        elif property_info.get('price'):
                            # Fallback to TZS price if USD not available
                            detailed_data['price'] = float(property_info['price'])
                            currency_map = {1: 'USD', 2: 'TZS', 3: 'ZMW'}
                            detailed_data['price_currency'] = currency_map.get(property_info.get('const_currency_id'), 'TZS')
                        
                        # Location
                        address = property_info.get('address', '')
                        if address:
                            detailed_data['address_text'] = address
                            # Parse location parts (e.g., "Madale, Dar es-Salaam, Tanzania")
                            parts = [p.strip() for p in address.split(',')]
                            if len(parts) >= 2:
                                detailed_data['district'] = parts[0]
                                detailed_data['city'] = parts[1] if len(parts) > 1 else parts[0]
                                detailed_data['region'] = parts[1] if len(parts) > 1 else parts[0]
                        
                        # Property details
                        if property_info.get('bedrooms'):
                            detailed_data['bedrooms'] = int(property_info['bedrooms'])
                        if property_info.get('baths'):
                            detailed_data['bathrooms'] = int(property_info['baths'])
                        if property_info.get('floor_size'):
                            detailed_data['living_area_sqm'] = float(property_info['floor_size'])
                        if property_info.get('land_size'):
                            detailed_data['land_area_sqm'] = float(property_info['land_size'])
                        
                        # Agent information
                        if property_info.get('agent_id'):
                            detailed_data['agent_profile_url'] = f"{self.base_url}/agent/{property_info['agent_id']}"
                        
                        # Extract images from JSON
                        images = []
                        # Look for image URLs in the JSON (pattern: "image-cdn-homes.beforward.jp")
                        img_pattern = r'https://image-cdn-homes\.beforward\.jp/images/[^"\'\\]+'
                        img_matches = re.findall(img_pattern, html_content)
                        for img_url in img_matches:
                            if 'property' in img_url.lower() or 'agent-user' in img_url.lower():
                                if img_url not in images and 'icon' not in img_url.lower() and 'logo' not in img_url.lower():
                                    images.append(img_url)
                        
                        detailed_data['images'] = images[:20]
                        logger.info(f"✓ Extracted {len(detailed_data['images'])} images from JSON")
                        
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON: {json_err}")
                    # Fallback to HTML parsing if JSON fails
                    pass
            
            # Fallback: Extract from HTML if JSON extraction failed or incomplete
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract title if not found in JSON
            if not detailed_data.get('title'):
                title_match = re.search(r'<title>([^|]+)', html_content)
                if title_match:
                    detailed_data['title'] = title_match.group(1).strip()
            
            # Extract description if not found in JSON
            if not detailed_data.get('description'):
                paragraphs = soup.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 100:
                        detailed_data['description'] = text
                        break
            
            # Extract agent name from HTML
            agent_link = soup.find('a', href=re.compile(r'/agent/\d+'))
            if agent_link:
                agent_text = agent_link.find_parent()
                if agent_text:
                    for sibling in agent_text.find_all(recursive=False):
                        text = sibling.get_text(strip=True)
                        if text and len(text) > 3 and len(text) < 100:
                            detailed_data['agent_name'] = text
                            break
            
            # Extract agent WhatsApp/phone from HTML
            whatsapp_match = re.search(r'whatsapp\.com/send\?phone=([+\d]+)', html_content)
            if whatsapp_match:
                phone = whatsapp_match.group(1)
                phone = phone.replace('+255', '0') if phone.startswith('+255') else phone
                detailed_data['agent_whatsapp'] = phone
                if not detailed_data.get('agent_phone'):
                    detailed_data['agent_phone'] = phone
            
            # Save to database if db_session provided
            if db_session:
                try:
                    saved = self._save_listing(detailed_data, self.site_name, db_session)
                    if saved:
                        logger.info(f"✓ Saved detailed data for {listing_url}")
                except Exception as save_error:
                    logger.error(f"Error saving listing: {save_error}")
            
            return detailed_data
            
        except Exception as e:
            logger.error(f"Error extracting details from {listing_url}: {e}", exc_info=True)
            return {'raw_url': listing_url, 'error': str(e)}

