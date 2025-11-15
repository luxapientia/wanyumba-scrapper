"""
IPH (Intercity Property Hub) Scraper Service

This service handles scraping real estate listings from iph.co.tz
"""
import logging
import re
import time
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


class IPHService(BaseScraperService):
    """Scraper service for IPH (Intercity Property Hub) website"""

    _instance: Optional['IPHService'] = None
    _lock = None

    def __init__(self):
        """Initialize IPH (Intercity Property Hub) scraper service"""
        super().__init__(
            site_name="iph",
            base_url="https://iph.co.tz",
            profile_dir=settings.IPH_PROFILE_DIR
        )
        self.search_url = f"{self.base_url}/properties"
        logger.info("IPHService initialized")

    @classmethod
    def get_instance(cls) -> 'IPHService':
        """Get singleton instance of IPHService"""
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
                        # Navigate to properties page to initialize
                        cls._instance.driver.get(f"{cls._instance.base_url}/properties")
                        time.sleep(3)
                        logger.info("✓ IPH browser initialized and ready")
                    except Exception as e:
                        logger.error(f"Failed to initialize IPH browser: {e}")
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
                logger.info("IPH instance closed")
            except Exception as e:
                logger.error(f"Error closing IPH instance: {e}")

    def wait_for_page_load(self, timeout: int = 10):
        """Wait for page to load completely"""
        try:
            
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            logger.warning("Page load timeout - continuing anyway")

    def extract_listing_id_from_url(self, url: str) -> Optional[str]:
        """Extracts the listing ID (slug) from the URL."""
        match = re.search(r'/properties/([a-z0-9-]+)$', url)
        if match:
            return match.group(1)
        return None

    def has_listings_on_page(self) -> bool:
        """Check if the current page contains any listings."""
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        listings = soup.find_all('div', class_='property-listing')
        return len(listings) > 0

    def get_total_pages_from_pagination(self) -> int:
        """
        Extract total pages from pagination.
        IPH shows text like "Found 1 - 15 Of 306 Results" and page numbers.
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Look for pagination links
            pagination_links = soup.select('ul.pagination li.page-item a.page-link')
            if not pagination_links:
                return 1
            
            max_page = 1
            for link in pagination_links:
                try:
                    page_text = link.get_text(strip=True)
                    if page_text.isdigit():
                        page_num = int(page_text)
                        max_page = max(max_page, page_num)
                except (ValueError, AttributeError):
                    continue
            
            logger.info(f"✓ Detected {max_page} total pages from pagination")
            return max_page
        except Exception as e:
            logger.error(f"Error extracting total pages: {e}")
            return 1

    def parse_price(self, price_str: str) -> Optional[float]:
        """
        Parse price string to float, handling various formats like:
        - TZS 50,000 / day
        - TZS 1.54 billion
        - TZS 20 million
        """
        if not price_str:
            return None
        try:
            # Convert to lowercase for easier matching
            price_lower = price_str.lower()
            
            # Extract the numeric part (with possible decimal)
            number_match = re.search(r'([\d,.]+)', price_str)
            if not number_match:
                return None
            
            # Get the number and clean it
            number_str = number_match.group(1).replace(',', '')
            price_value = float(number_str)
            
            # Check for multipliers (billion, million, thousand, etc.)
            if 'billion' in price_lower:
                price_value *= 1_000_000_000
            elif 'million' in price_lower:
                price_value *= 1_000_000
            elif 'thousand' in price_lower or 'k' in price_lower:
                price_value *= 1_000
            
            return price_value
            
        except Exception as e:
            logger.warning(f"Could not parse price '{price_str}': {e}")
        return None
    
    def parse_price_details(self, price_str: str) -> Dict[str, Optional[str]]:
        """
        Parse price string to extract currency, period, and value
        Returns: {'currency': 'TZS', 'period': 'day', 'price': 50000.0}
        """
        result = {
            'currency': None,
            'period': None,
            'price': None
        }
        
        if not price_str:
            return result
        
        try:
            # Extract currency (TZS, USD, EUR, etc.)
            currency_match = re.search(r'(TZS|USD|EUR|KES|UGX)', price_str, re.IGNORECASE)
            if currency_match:
                result['currency'] = currency_match.group(1).upper()
            
            # Extract period (day, month, year)
            period_match = re.search(r'/(day|month|year|week)', price_str, re.IGNORECASE)
            if period_match:
                result['period'] = period_match.group(1).lower()
            
            # Extract price value
            result['price'] = self.parse_price(price_str)
            
        except Exception as e:
            logger.warning(f"Could not parse price details '{price_str}': {e}")
        
        return result

    def _scrape_current_page_listings(self, seen_ids: set) -> List[Dict]:
        """
        Scrape listings from the current page
        
        Args:
            seen_ids: Set of listing IDs already scraped
            
        Returns:
            List of listing dictionaries from current page
        """
        page_listings = []
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find all listing cards
            listing_elements = soup.find_all('div', class_='property-listing')
            
            if not listing_elements:
                logger.warning("No listing cards found on the current page.")
                return []
            
            logger.info(f"Found {len(listing_elements)} listing cards")
            
            for element in listing_elements:
                try:
                    # Extract URL
                    link_elem = element.find('a', href=re.compile(r'/properties/[a-z0-9-]+'))
                    if not link_elem:
                        continue
                    href = link_elem.get('href', '')
                    listing_url = f"{self.base_url}{href}" if href.startswith('/') else href
                    
                    # Extract listing ID
                    listing_id = self.extract_listing_id_from_url(listing_url)
                    if not listing_id or listing_id in seen_ids:
                        continue
                    
                    seen_ids.add(listing_id)
                    
                    # Extract title
                    title_elem = element.find('h4', class_='listing-name')
                    title = title_elem.get_text(strip=True) if title_elem else None
                    
                    # Extract price, currency, and period
                    price_elem = element.find('h6', class_='listing-card-info-price')
                    price_str = price_elem.get_text(strip=True) if price_elem else None
                    
                    price = None
                    currency = None
                    price_period = None
                    
                    if price_str:
                        price_details = self.parse_price_details(price_str)
                        price = price_details['price']
                        currency = price_details['currency']
                        price_period = price_details['period']
                    
                    # Extract location
                    location_elem = element.find('span', class_='listing-location')
                    location = location_elem.get_text(strip=True) if location_elem else None
                    
                    # Extract listing type
                    listing_type_elem = element.find('span', class_='prt-types')
                    listing_type_text = listing_type_elem.get_text(strip=True).lower() if listing_type_elem else ''
                    listing_type = 'buy' if 'buy' in listing_type_text or 'sell' in listing_type_text else ('rent' if 'rent' in listing_type_text else None)
                    
                    # Extract beds, baths, area from list-fx-features
                    beds = None
                    baths = None
                    area = None
                    
                    feature_icons = element.find_all('div', class_='listing-card-info-icon')
                    for icon in feature_icons:
                        text = icon.get_text(strip=True)
                        
                        bed_match = re.search(r'(\d+)\s*Bed', text, re.IGNORECASE)
                        if bed_match:
                            beds = int(bed_match.group(1))
                        
                        bath_match = re.search(r'(\d+)\s*Bath', text, re.IGNORECASE)
                        if bath_match:
                            baths = int(bath_match.group(1))
                        
                        area_match = re.search(r'([\d,.]+)\s*m²', text, re.IGNORECASE)
                        if area_match:
                            area_str = area_match.group(1).replace(',', '')
                            area = float(area_str)
                    
                    listing_data = {
                        'raw_url': listing_url,
                        'title': title,
                        'price': price,
                        'price_currency': currency,
                        'price_period': price_period,
                        'address_text': location,
                        'bedrooms': beds,
                        'bathrooms': baths,
                        'living_area_sqm': area,
                        'property_type': None,  # Will be determined in detailed data
                        'listing_type': listing_type,
                        'source': self.site_name,
                        'country': 'Tanzania'
                    }
                    
                    page_listings.append(listing_data)
                    self.listings.append(listing_data)
                    
                except Exception as e:
                    logger.error(f"Error processing listing element: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping current page: {e}")
        
        return page_listings

    def get_all_listings_basic(self, max_pages: Optional[int] = None, db_session=None, target_site: str = None) -> List[Dict]:
        """
        Scrape basic listing information from all pages.
        
        Args:
            max_pages: Maximum number of pages to scrape
            db_session: Database session for saving listings
            target_site: Target site name (not used, for compatibility with base class)
            
        Returns:
            List of basic listing dictionaries
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
        seen_ids = set()
        consecutive_empty_pages = 0

        try:
            # Initialize status with total_pages (we'll discover from pagination)
            self._init_listings_status(target_site=self.site_name, max_pages=None)
            self._broadcast_status()

            logger.info(f"Starting IPH scraping from: {self.search_url}")
            
            page_num = 1
            total_pages = None
            
            # Scrape pages until we hit consecutive empty pages or max_pages
            while True:
                if self.should_stop:
                    logger.info("Stop signal received")
                    break

                # Check if we've reached max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(f"Reached maximum page limit ({max_pages}). Stopping.")
                    break

                # Navigate to page
                page_url = f"{self.search_url}?page={page_num}"
                logger.info(f"Scraping page {page_num}: {page_url}")
                
                self.driver.get(page_url)
                self.wait_for_page_load()

                # Get total pages from first page
                if page_num == 1:
                    total_pages = self.get_total_pages_from_pagination()
                    self._update_status_field('total_pages', total_pages, broadcast=False)
                    logger.info(f"✓ Found {total_pages} total pages")

                # Check if page has listings
                if not self.has_listings_on_page():
                    consecutive_empty_pages += 1
                    logger.warning(f"⚠️  Page {page_num} has no listings. (Consecutive empty: {consecutive_empty_pages})")
                    
                    # If 2 consecutive pages are empty, we've reached the end
                    if consecutive_empty_pages >= 2:
                        logger.info("⚠️  Two consecutive pages with no listings. Stopping pagination.")
                        break
                    
                    # Continue to next page to check if it's also empty
                    page_num += 1
                    continue
                else:
                    # Reset counter if we find listings
                    consecutive_empty_pages = 0

                # Update status
                self._update_status_field('current_page', page_num, broadcast=False)
                self._update_status_field('pages_scraped', page_num, broadcast=False)
                self._broadcast_status()

                # Parse page
                page_listings = self._scrape_current_page_listings(seen_ids)
                logger.info(f"Found {len(page_listings)} new listings on page {page_num}")

                # Save batch to database
                if db_session and len(page_listings) > 0:
                    saved_count = self._save_listings_batch(page_listings, self.site_name, db_session)
                    logger.info(f"💾 Saved {saved_count} listings from page {page_num}")

                # Check if we've reached the last page
                if total_pages and page_num >= total_pages:
                    logger.info(f"Reached last page ({total_pages}). Stopping.")
                    break

                # Move to next page
                page_num += 1
                
                # Add delay between pages to avoid overwhelming the server
                time.sleep(2)

            logger.info(f"✓ Scraping complete! Found {len(self.listings)} unique listings across {page_num} pages")

            # Update final status
            self._update_status_field('status', 'completed', broadcast=False)
            self._update_status_field('listings_found', len(self.listings), broadcast=False)
            self._update_status_field('pages_scraped', page_num, broadcast=False)
            self._broadcast_status()

            return self.listings

        except Exception as e:
            logger.error(f"Error in IPH basic scraping: {e}", exc_info=True)
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
        Extract detailed data from a single listing page.
        """
        self.start_browser()
        if self.driver is None:
            logger.error("Failed to start browser for detail scraping")
            return {}
        
        try:
            if current_index > 0 and total_urls > 0:
                self._update_url_progress(listing_url, current_index, total_urls)
                self._broadcast_status()
            
            logger.info(f"Extracting details from: {listing_url}")
            self.driver.get(listing_url)
            self.wait_for_page_load()
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            detailed_data = {
                'raw_url': listing_url,
                'source': self.site_name,
                'scrape_timestamp': datetime.now(),
                'status': 'active',
                'country': 'Tanzania'
            }
            
            # Extract title
            title_elem = soup.find('h3')
            if title_elem:
                detailed_data['title'] = title_elem.get_text(strip=True)
            
            # Extract listing type
            listing_type_elem = soup.find('span', class_='prt-types')
            if listing_type_elem:
                listing_type_text = listing_type_elem.get_text(strip=True).lower()
                detailed_data['listing_type'] = 'buy' if 'buy' in listing_type_text or 'sell' in listing_type_text else ('rent' if 'rent' in listing_type_text else None)
            
            # Extract price, currency, and period
            price_elem = soup.find('h3', class_='prt-price-fix')
            if price_elem:
                price_str = price_elem.get_text(strip=True)
                price_details = self.parse_price_details(price_str)
                detailed_data['price'] = price_details['price']
                detailed_data['price_currency'] = price_details['currency'] or 'TZS'  # Default to TZS
                if price_details['period']:
                    detailed_data['price_period'] = price_details['period']
            
            # Extract location from the prt-detail-title-desc section
            # Look for span containing the map marker icon
            detail_desc = soup.find('div', class_='prt-detail-title-desc')
            if detail_desc:
                # Find span with map marker icon
                location_span = detail_desc.find('i', class_='lni-map-marker')
                if location_span and location_span.parent:
                    location_text = location_span.parent.get_text(strip=True)
                    # Example: "Mobile Number: 0763 321 074; Lumumba and Narung'ombe Street opposite Bin Slum Tyres., Dar es Salaam"
                    
                    # Extract phone number if present (as fallback for agent phone)
                    phone_match = re.search(r'(?:Mobile\s+Number|Phone|Tel):\s*([\d\s]+)', location_text, re.IGNORECASE)
                    if phone_match:
                        phone_num = phone_match.group(1).strip().replace(' ', '')
                        # Store as fallback (will be overridden by agent phone if available)
                        if not detailed_data.get('agent_phone'):
                            detailed_data['agent_phone'] = phone_num
                    
                    # Try to extract just the address part (after semicolon if present)
                    if ';' in location_text:
                        address_parts = location_text.split(';', 1)
                        if len(address_parts) > 1:
                            location_text = address_parts[1].strip()
                    
                    # Also remove phone number prefix if still present
                    location_text = re.sub(r'^.*?(?:Mobile\s+Number|Phone|Tel):\s*[\d\s]+;?\s*', '', location_text, flags=re.IGNORECASE)
                    
                    detailed_data['address_text'] = location_text
                    
                    # Parse location parts by comma
                    # Example: "Lumumba and Narung'ombe Street opposite Bin Slum Tyres., Dar es Salaam"
                    parts = [p.strip() for p in location_text.split(',') if p.strip()]
                    if len(parts) >= 1:
                        # Last part is usually the city/region
                        detailed_data['city'] = parts[-1]
                        detailed_data['region'] = parts[-1]
                        if len(parts) >= 2:
                            # Second to last could be district or part of street address
                            detailed_data['district'] = parts[-2]
            
            # Extract property details from detail_features list
            detail_features = soup.find('ul', class_='detail_features')
            if detail_features:
                for li in detail_features.find_all('li'):
                    text = li.get_text(strip=True)
                    
                    if 'Bedrooms:' in text:
                        bed_match = re.search(r'(\d+)', text)
                        if bed_match:
                            detailed_data['bedrooms'] = int(bed_match.group(1))
                    
                    if 'Bathrooms:' in text:
                        bath_match = re.search(r'(\d+)', text)
                        if bath_match:
                            detailed_data['bathrooms'] = int(bath_match.group(1))
                    
                    if 'Floors:' in text:
                        floor_match = re.search(r'(\d+)', text)
                        if floor_match:
                            detailed_data['floors'] = int(floor_match.group(1))
                    
                    if 'Property Type:' in text:
                        property_type_text = text.replace('Property Type:', '').strip().lower()
                        # Map to standard types
                        if 'house' in property_type_text:
                            detailed_data['property_type'] = 'house'
                        elif 'apartment' in property_type_text or 'flat' in property_type_text:
                            detailed_data['property_type'] = 'apartment'
                        elif 'land' in property_type_text or 'plot' in property_type_text:
                            detailed_data['property_type'] = 'land'
                        elif 'hotel' in property_type_text or 'lodge' in property_type_text:
                            detailed_data['property_type'] = 'hotel'
                        elif 'commercial' in property_type_text or 'office' in property_type_text or 'warehouse' in property_type_text:
                            detailed_data['property_type'] = 'commercial'
                        else:
                            detailed_data['property_type'] = property_type_text
            
            # Extract description
            description_panel = soup.find('div', id='clTwo')
            if description_panel:
                description_body = description_panel.find('div', class_='block-body')
                if description_body:
                    detailed_data['description'] = description_body.get_text(strip=True)
            
            # Extract amenities
            amenities = []
            amenities_panel = soup.find('div', id='clThree')
            if amenities_panel:
                amenity_items = amenities_panel.find_all('li')
                for item in amenity_items:
                    amenity_text = item.get_text(strip=True)
                    if amenity_text:
                        amenities.append(amenity_text)
            detailed_data['amenities'] = amenities
            
            # Extract images from gallery
            images = []
            gallery = soup.find('ul', class_='list-gallery-inline')
            if gallery:
                image_links = gallery.find_all('a', class_='mfp-gallery')
                for link in image_links:
                    img_url = link.get('href')
                    if img_url:
                        if img_url.startswith('/'):
                            img_url = f"{self.base_url}{img_url}"
                        if img_url not in images:
                            images.append(img_url)
            detailed_data['images'] = images[:20]
            
            # Extract agent information from sidebar
            agent_section = soup.find('div', class_='sides-widget')
            if agent_section:
                # Agent name
                agent_name_elem = agent_section.find('h4')
                if agent_name_elem:
                    agent_name = agent_name_elem.get_text(strip=True)
                    detailed_data['agent_name'] = agent_name
                    # Extract agent profile URL
                    agent_link = agent_name_elem.find('a')
                    if agent_link:
                        agent_href = agent_link.get('href', '')
                        if agent_href:
                            detailed_data['agent_profile_url'] = f"{self.base_url}{agent_href}" if agent_href.startswith('/') else agent_href
                
                # Agent phone
                phone_link = agent_section.find('a', href=re.compile(r'tel:'))
                if phone_link:
                    phone = phone_link.get('href', '').replace('tel:', '')
                    detailed_data['agent_phone'] = phone
                    # WhatsApp is typically the same as phone
                    detailed_data['agent_whatsapp'] = phone
            
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

