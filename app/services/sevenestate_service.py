"""
Seven Estate Scraper Service

This service handles scraping real estate listings from sevenestate.co.tz
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


class SevenEstateService(BaseScraperService):
    """Scraper service for Seven Estate website"""

    _instance: Optional['SevenEstateService'] = None
    _lock = None

    def __init__(self):
        """Initialize Seven Estate scraper service"""
        super().__init__(
            site_name="sevenestate",
            base_url="https://www.sevenestate.co.tz",
            profile_dir=settings.SEVENESTATE_PROFILE_DIR
        )
        self.search_url = f"{self.base_url}/search.php"
        logger.info("SevenEstateService initialized")

    @classmethod
    def get_instance(cls) -> 'SevenEstateService':
        """Get singleton instance of SevenEstateService"""
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
                        # Navigate to search page to initialize
                        cls._instance.driver.get(cls._instance.search_url)
                        time.sleep(3)
                        logger.info("✓ SevenEstate browser initialized and ready")
                    except Exception as e:
                        logger.error(f"Failed to initialize SevenEstate browser: {e}")
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
                logger.info("SevenEstate instance closed")
            except Exception as e:
                logger.error(f"Error closing SevenEstate instance: {e}")

    def wait_for_page_load(self, timeout: int = 10):
        """Wait for page to fully load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            logger.warning("Page load timeout - continuing anyway")

    def parse_price(self, price_str: str) -> Optional[float]:
        """
        Parse price string to float
        
        Args:
            price_str: Price string like "USD 2,300,000" or "USD2,300,000"
            
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
        Extract listing ID from URL
        
        Args:
            url: Full URL or relative path like "/viewlisting.php?id=4887"
            
        Returns:
            Listing ID or None if not found
        """
        match = re.search(r'id=(\d+)', url)
        return match.group(1) if match else None

    def has_listings_on_page(self) -> bool:
        """
        Check if current page has any listings
        
        Returns:
            True if listings are found, False otherwise
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            articles = soup.find_all('article')
            return len(articles) > 0
        except Exception as e:
            logger.warning(f"Error checking for listings: {e}")
            return False

    def get_all_listings_basic(self, max_pages: int = 500, db_session=None, target_site: str = None) -> List[Dict]:
        """
        Scrape basic listing information from Seven Estate
        Uses standard pagination to navigate through pages.
        Stops automatically when consecutive pages return no results.
        
        Args:
            max_pages: Maximum number of pages to scrape
            db_session: Database session for saving
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
        consecutive_empty_count = 0  # Track consecutive pages with no listings

        try:
            # Initialize status with None for total_pages (we'll discover as we go)
            self._init_listings_status(target_site=self.site_name, max_pages=None)
            self._broadcast_status()

            logger.info(f"Starting Seven Estate scraping from: {self.search_url}")
            
            page_num = 0
            
            # Scrape pages until we hit consecutive empty pages or max_pages
            while True:
                if self.should_stop:
                    logger.info("Stop signal received")
                    break

                # Check if we've reached max_pages limit
                if max_pages and page_num >= max_pages:
                    logger.info(f"Reached maximum page limit ({max_pages}). Stopping.")
                    break

                # Navigate to page (page numbers are 0-indexed)
                page_url = f"{self.search_url}?page={page_num}"
                logger.info(f"Scraping page {page_num + 1}: {page_url}")
                
                self.driver.get(page_url)
                self.wait_for_page_load()

                # Check if page has listings
                if not self.has_listings_on_page():
                    consecutive_empty_count += 1
                    logger.warning(f"⚠️  Page {page_num + 1} has no listings. (Consecutive empty: {consecutive_empty_count})")
                    
                    # If 2 consecutive pages are empty, we've reached the end
                    if consecutive_empty_count >= 2:
                        logger.info("⚠️  Two consecutive pages with no listings. Stopping pagination.")
                        break
                    
                    # Continue to next page to check if it's also empty
                    page_num += 1
                    continue
                else:
                    # Reset counter if we find listings
                    consecutive_empty_count = 0

                # Update status
                self._update_status_field('current_page', page_num + 1, broadcast=False)
                self._update_status_field('pages_scraped', page_num + 1, broadcast=False)
                self._broadcast_status()

                # Parse page
                page_listings = self._scrape_current_page_listings(seen_ids)
                logger.info(f"Found {len(page_listings)} new listings on page {page_num + 1}")

                # Save batch to database
                if db_session and len(page_listings) > 0:
                    saved_count = self._save_listings_batch(page_listings, self.site_name, db_session)
                    logger.info(f"💾 Saved {saved_count} listings from page {page_num + 1}")

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
            logger.error(f"Error in Seven Estate basic scraping: {e}", exc_info=True)
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
            
            # Find all article elements (each represents a listing)
            articles = soup.find_all('article')
            logger.info(f"Found {len(articles)} article elements")
            
            for article in articles:
                try:
                    # Extract listing URL and ID
                    title_link = article.find('h3')
                    if not title_link:
                        continue
                    
                    link = title_link.find('a')
                    if not link or not link.get('href'):
                        continue
                    
                    listing_url = link.get('href')
                    
                    # Make URL absolute if it's relative
                    if listing_url.startswith('/'):
                        listing_url = f"{self.base_url}{listing_url}"
                    elif not listing_url.startswith('http'):
                        listing_url = f"{self.base_url}/{listing_url}"
                    
                    # Extract listing ID
                    listing_id = self.extract_listing_id_from_url(listing_url)
                    if not listing_id or listing_id in seen_ids:
                        continue
                    
                    seen_ids.add(listing_id)
                    
                    # Extract title
                    title = link.get_text(strip=True)
                    
                    # Extract location
                    location = None
                    location_elem = article.find('div', class_='item_location')
                    if location_elem:
                        location = location_elem.get_text(strip=True)
                    
                    # Extract property type
                    property_type = None
                    type_badges = article.find_all('li')
                    if type_badges:
                        property_type = type_badges[0].get_text(strip=True)
                    
                    # Extract price
                    price = None
                    price_elem = article.find('div', class_='price_area')
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        price = self.parse_price(price_text)
                    
                    # Extract description
                    description = None
                    desc_elem = article.find('p')
                    if desc_elem:
                        description = desc_elem.get_text(strip=True)
                    
                    # Extract bedrooms, bathrooms, area
                    bedrooms = None
                    bathrooms = None
                    area = None
                    
                    info_list = article.find_all('li')
                    if len(info_list) >= 3:
                        # First item is bedrooms
                        bed_text = info_list[0].get_text(strip=True)
                        if bed_text and bed_text != '-':
                            try:
                                bedrooms = int(bed_text)
                            except ValueError:
                                pass
                        
                        # Second item is bathrooms
                        bath_text = info_list[1].get_text(strip=True)
                        if bath_text and bath_text != '-':
                            try:
                                bathrooms = int(bath_text)
                            except ValueError:
                                pass
                    
                    # Extract property ID (from the last list item)
                    property_id = None
                    if len(info_list) >= 4:
                        property_id = info_list[3].get_text(strip=True)
                    
                    # Create listing dictionary
                    listing = {
                        'raw_url': listing_url,
                        'title': title,
                        'location': location,
                        'property_type': property_type,
                        'price': price,
                        'description': description,
                        'bedrooms': bedrooms,
                        'bathrooms': bathrooms,
                        'area': area,
                        'property_id': property_id,
                        'source': self.site_name
                    }
                    
                    page_listings.append(listing)
                    self.listings.append(listing)
                    
                    # Update status
                    self._update_status_field('listings_found', len(self.listings), broadcast=False)
                    
                except Exception as e:
                    logger.warning(f"Error parsing article: {e}")
                    continue
            
            # Broadcast status after processing page
            self._broadcast_status()
            
        except Exception as e:
            logger.error(f"Error scraping current page: {e}", exc_info=True)
        
        return page_listings

    def extract_detailed_data(
        self,
        listing_url: str,
        total_urls: int = 0,
        current_index: int = 0,
        db_session=None,
        target_site: str = None
    ) -> Dict:
        """
        Extract detailed information from a listing page
        
        Args:
            listing_url: URL of the listing detail page
            total_urls: Total number of URLs to scrape (for progress tracking)
            current_index: Current index in the URL list (for progress tracking)
            db_session: Database session for saving
            target_site: Target site name (not used, for compatibility)
            
        Returns:
            Dictionary containing extracted data
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

            # Navigate to detail page
            self.driver.get(listing_url)
            self.wait_for_page_load()
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Initialize detailed data with required fields
            detailed_data = {
                'raw_url': listing_url,
                'source': self.site_name,
                'scrape_timestamp': datetime.now(),
                'status': 'active',  # Assume active if we can scrape it
                'country': 'Tanzania',  # All Seven Estate listings are in Tanzania
            }
            
            # Extract title
            title_elem = soup.find('h1')
            if title_elem:
                detailed_data['title'] = title_elem.get_text(strip=True)
            
            # Extract location and parse it
            location_elem = soup.find('a', href='#')
            if location_elem and location_elem.find(class_='fa-map-marker'):
                location_text = location_elem.get_text(strip=True)
                detailed_data['address_text'] = location_text
                
                # Parse location (e.g., "Mbezi Beach, Dar-es-Salaam")
                if ', ' in location_text:
                    parts = [p.strip() for p in location_text.split(',')]
                    if len(parts) >= 2:
                        detailed_data['district'] = parts[0]
                        detailed_data['city'] = parts[1]
                        detailed_data['region'] = parts[1]  # City is typically the region in Tanzania
                    elif len(parts) == 1:
                        detailed_data['city'] = parts[0]
                else:
                    detailed_data['city'] = location_text
            
            # Extract price and currency
            price_elem = soup.find('strong', text=re.compile(r'Price:'))
            if price_elem and price_elem.parent:
                price_text = price_elem.parent.get_text(strip=True)
                detailed_data['price'] = self.parse_price(price_text)
                
                # Extract currency (usually USD for Seven Estate)
                if 'USD' in price_text or '$' in price_text:
                    detailed_data['price_currency'] = 'USD'
                elif 'TZS' in price_text or 'TSH' in price_text:
                    detailed_data['price_currency'] = 'TZS'
                else:
                    detailed_data['price_currency'] = 'USD'  # Default to USD
                
                # Extract price period (rent/month, sale/once, etc.)
                if 'month' in price_text.lower() or '/month' in price_text.lower():
                    detailed_data['price_period'] = 'month'
                elif 'year' in price_text.lower() or '/year' in price_text.lower():
                    detailed_data['price_period'] = 'year'
                else:
                    detailed_data['price_period'] = 'once'  # Default to one-time payment
            
            # Extract full description
            about_section = soup.find('h3', text=re.compile(r'About This Listing'))
            if about_section:
                desc_elem = about_section.find_next('p')
                if desc_elem:
                    detailed_data['description'] = desc_elem.get_text(strip=True)
            
            # Extract details section
            details_section = soup.find('h3', text=re.compile(r'Details'))
            if details_section:
                details_list = details_section.find_next('ul')
                if details_list:
                    for li in details_list.find_all('li'):
                        text = li.get_text(strip=True)
                        
                        # Property ID (source_listing_id)
                        if 'Property Id:' in text:
                            detailed_data['source_listing_id'] = text.replace('Property Id:', '').strip()
                        
                        # Bedrooms
                        elif 'Bedrooms:' in text:
                            bed_text = text.replace('Bedrooms:', '').strip()
                            if bed_text and bed_text != '-':
                                try:
                                    detailed_data['bedrooms'] = int(bed_text)
                                except ValueError:
                                    pass
                        
                        # Bathrooms
                        elif 'Bathrooms:' in text:
                            bath_text = text.replace('Bathrooms:', '').strip()
                            if bath_text and bath_text != '-':
                                try:
                                    detailed_data['bathrooms'] = int(bath_text)
                                except ValueError:
                                    pass
                        
                        # Property Type and Listing Type
                        elif 'Type:' in text:
                            type_text = text.replace('Type:', '').strip()
                            detailed_data['property_type'] = type_text
                            
                            # Determine listing_type from property_type
                            type_lower = type_text.lower()
                            if 'rent' in type_lower:
                                detailed_data['listing_type'] = 'rent'
                            elif 'sale' in type_lower or 'sell' in type_lower:
                                detailed_data['listing_type'] = 'sale'
                        
                        # Lot Size (land_area_sqm)
                        elif 'Lot Size:' in text:
                            area_text = text.replace('Lot Size:', '').replace('m2', '').replace('sqm', '').strip()
                            if area_text and area_text != 'N/A':
                                try:
                                    detailed_data['land_area_sqm'] = float(area_text.replace(',', ''))
                                except ValueError:
                                    pass
                        
                        # Living Area (if separate from lot size)
                        elif 'Living Area:' in text:
                            area_text = text.replace('Living Area:', '').replace('m2', '').replace('sqm', '').strip()
                            if area_text and area_text != 'N/A':
                                try:
                                    detailed_data['living_area_sqm'] = float(area_text.replace(',', ''))
                                except ValueError:
                                    pass
            
            # Extract agent information
            agent_section = soup.find('h4')
            if agent_section:
                agent_link = agent_section.find('a')
                if agent_link:
                    detailed_data['agent_name'] = agent_link.get_text(strip=True)
                    # Extract agent profile URL
                    agent_href = agent_link.get('href', '')
                    if agent_href:
                        if agent_href.startswith('/'):
                            detailed_data['agent_profile_url'] = f"{self.base_url}{agent_href}"
                        elif agent_href.startswith('http'):
                            detailed_data['agent_profile_url'] = agent_href
            
            # Extract agent contact details
            contact_list = soup.find_all('li')
            for li in contact_list:
                text = li.get_text(strip=True)
                
                # Mobile
                if 'Mobile :' in text:
                    mobile_link = li.find('a', href=re.compile(r'tel:'))
                    if mobile_link:
                        phone = mobile_link.get('href', '').replace('tel:', '').strip()
                        # Normalize phone number (remove dashes, keep +255 format)
                        phone = phone.replace('-', '')
                        if phone.startswith('+255'):
                            phone = '0' + phone[4:]  # Convert +255 to 0
                        detailed_data['agent_phone'] = phone
                        # Also set as WhatsApp (common in Tanzania)
                        detailed_data['agent_whatsapp'] = phone
                
                # Phone (alternative)
                elif 'Phone :' in text and 'agent_phone' not in detailed_data:
                    phone_link = li.find('a', href=re.compile(r'tel:'))
                    if phone_link:
                        phone = phone_link.get('href', '').replace('tel:', '').strip()
                        phone = phone.replace('-', '')
                        if phone.startswith('+255'):
                            phone = '0' + phone[4:]
                        detailed_data['agent_phone'] = phone
                        detailed_data['agent_whatsapp'] = phone
                
                # Email
                elif 'Mail :' in text:
                    email_link = li.find('a', href=re.compile(r'mailto:'))
                    if email_link:
                        detailed_data['agent_email'] = email_link.get('href', '').replace('mailto:', '').strip()
                
                # Website
                elif 'Website :' in text:
                    website_link = li.find('a')
                    if website_link:
                        website_url = website_link.get('href', '')
                        if website_url and website_url != '#':
                            detailed_data['agent_website'] = website_url
            
            # Extract images
            images = []
            
            # Method 1: Find main property image (the large display image link)
            main_image_link = soup.find('a', href=re.compile(r'/images/\d+\.jpg'))
            if main_image_link:
                img_url = main_image_link.get('href', '')
                if img_url:
                    # Convert relative URL to absolute
                    if img_url.startswith('/'):
                        img_url = f"{self.base_url}{img_url}"
                    elif not img_url.startswith('http'):
                        img_url = f"{self.base_url}/{img_url}"
                    images.append(img_url)
            
            # Method 2: Find all img tags with various attributes
            img_elements = soup.find_all('img')
            for img in img_elements:
                # Try different attributes: src, data-src, data-lazy-src
                img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                
                if img_url:
                    # Filter relevant images (property photos, not icons/logos/avatars)
                    if ('/images/' in img_url and 
                        'icon' not in img_url.lower() and 
                        'logo' not in img_url.lower() and
                        'avatar' not in img_url.lower()):
                        
                        # Convert relative URL to absolute
                        if img_url.startswith('/'):
                            img_url = f"{self.base_url}{img_url}"
                        elif not img_url.startswith('http'):
                            img_url = f"{self.base_url}/{img_url}"
                        
                        # Avoid duplicates
                        if img_url not in images:
                            images.append(img_url)
            
            # Method 3: Check for image gallery or slider containers
            gallery_container = soup.find('div', class_=re.compile(r'gallery|slider|carousel', re.IGNORECASE))
            if gallery_container:
                gallery_imgs = gallery_container.find_all('img')
                for img in gallery_imgs:
                    img_url = img.get('src') or img.get('data-src')
                    if img_url and '/images/' in img_url and img_url not in images:
                        if img_url.startswith('/'):
                            img_url = f"{self.base_url}{img_url}"
                        elif not img_url.startswith('http'):
                            img_url = f"{self.base_url}/{img_url}"
                        images.append(img_url)
            
            # Limit to 20 images maximum
            detailed_data['images'] = images[:20]
            
            if images:
                logger.debug(f"✓ Extracted {len(detailed_data['images'])} images from {listing_url}")
            else:
                logger.warning(f"⚠️  No images found for {listing_url}")
            
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

