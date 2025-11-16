"""
Ruaha Assets (ruaha.co.tz) scraper service for real estate listings.
"""
import logging
import re
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from app.services.base_scraper_service import BaseScraperService

logger = logging.getLogger(__name__)


class RuahaService(BaseScraperService):
    """Ruaha Assets scraper service for real estate listings"""

    _instance: Optional['RuahaService'] = None

    def __init__(self, headless: bool = False, profile_dir: str = None):
        super().__init__(
            base_url="https://www.ruaha.co.tz",
            headless=headless,
            profile_dir=profile_dir or "./ruaha_browser_profile",
            site_name="ruaha"
        )
        self.ads_url = f"{self.base_url}/ads"
        self.listings = []
        self.detailed_listings = []

    @classmethod
    def get_instance(cls) -> 'RuahaService':
        """
        Get or create singleton instance of RuahaService
        
        Returns:
            Singleton instance of RuahaService
        """
        if cls._instance is None:
            from app.core.config import settings
            logger.info("Initializing Ruaha scraper...")
            try:
                cls._instance = cls(
                    profile_dir=getattr(settings, 'RUAHA_PROFILE_DIR', './ruaha_browser_profile'),
                    headless=settings.SCRAPER_HEADLESS
                )
                cls._instance.start_browser()

                # Navigate to homepage to initialize
                try:
                    cls._instance.driver.set_page_load_timeout(30)
                    cls._instance.driver.get("https://www.ruaha.co.tz/ads")
                    logger.info("✓ Ruaha scraper initialized successfully")
                except Exception as nav_error:
                    logger.warning(f"Could not navigate to Ruaha homepage: {nav_error}")
                    # Continue anyway, browser is started

            except Exception as e:
                logger.error(f"Failed to initialize Ruaha scraper: {e}", exc_info=True)
                cls._instance = None
                raise

        return cls._instance

    @classmethod
    def close_instance(cls):
        """Close and cleanup singleton instance"""
        if cls._instance is not None:
            cls._instance.close_browser()
            cls._instance = None
            logger.info("Closed Ruaha scraper instance")

    @classmethod
    def is_ready(cls) -> bool:
        """Check if instance exists and is ready"""
        return cls._instance is not None and cls._instance.driver is not None

    @classmethod
    def is_scraping_now(cls) -> bool:
        """Check if currently scraping"""
        return cls._instance is not None and cls._instance.is_scraping

    @classmethod
    def get_status(cls) -> Dict:
        """Get current scraping status"""
        if cls._instance is not None:
            return cls._instance.scraping_status.copy()
        return {}

    @classmethod
    def stop_scraping(cls):
        """Stop current scraping operation"""
        if cls._instance is not None:
            cls._instance.should_stop = True
            cls._instance.is_scraping = False  # Immediately reset the flag
            logger.info("Stop signal sent to Ruaha scraper")

    def wait_for_page_load(self, timeout: int = 10):
        """Wait for page to load completely"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(5)  # Wait longer for React to render initial content
            logger.info("Page loaded, React content rendered")
        except TimeoutException:
            logger.warning("Page load timeout, continuing anyway")

    def parse_price(self, price_text: str) -> tuple[Optional[float], str]:
        """
        Parse price text to extract amount and currency
        
        Args:
            price_text: Price string like "TSH 1,260,000,000" or "USD 480,000"
            
        Returns:
            Tuple of (price_amount, currency)
        """
        if not price_text:
            return None, "TZS"
        
        # Extract currency
        currency = "TZS"  # Default
        if "USD" in price_text.upper():
            currency = "USD"
        elif "TSH" in price_text.upper():
            currency = "TZS"
        
        # Extract numeric value
        # Remove currency, commas, and extract numbers
        numbers = re.sub(r'[^\d.]', '', price_text.replace(',', ''))
        
        try:
            price = float(numbers) if numbers else None
            return price, currency
        except ValueError:
            logger.warning(f"Could not parse price: {price_text}")
            return None, currency

    def extract_listing_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract listing ID from URL
        URL format: /ads/property-type-location-price-LISTING_ID
        Example: /ads/commercial-property-for-sale-kizota-dodoma-1260000000-rndz5zb4ctdbte1xag6ih7yg
        Returns: rndz5zb4ctdbte1xag6ih7yg (just the ID, not the full path)
        """
        # Pattern: find the unique ID after the last occurrence of -DIGITS-
        # This extracts only the alphanumeric ID at the very end (max 100 chars to fit DB)
        match = re.search(r'-(\d+)-([a-zA-Z0-9_-]+)$', url)
        if match:
            listing_id = match.group(2)  # Return only the ID part, not the price
            # Truncate to 100 chars to fit database constraint
            return listing_id[:100] if len(listing_id) > 100 else listing_id
        return None

    def scroll_and_load_more(self, seen_urls: set, db_session=None, max_scrolls: int = None) -> int:
        """
        Scroll page to trigger infinite scroll loading and scrape progressively
        
        Args:
            seen_urls: Set of URLs already scraped
            db_session: Database session for progressive saving
            max_scrolls: Maximum number of scroll attempts (default: 50)
            
        Returns:
            Number of scrolls performed
        """
        # Default to 50 if None is passed
        if max_scrolls is None:
            max_scrolls = 50
            
        scroll_count = 0
        no_new_content_count = 0
        last_listing_count = len(self.listings)
        
        logger.info(f"Starting progressive scroll scraping (max {max_scrolls} scrolls)")
        
        for i in range(max_scrolls):
            if self._check_should_stop():
                break
            
            # Scroll to bottom smoothly
            self.driver.execute_script("""
                window.scrollTo({
                    top: document.body.scrollHeight,
                    behavior: 'smooth'
                });
            """)
            
            scroll_count += 1
            logger.info(f"Scroll {scroll_count}/{max_scrolls}")
            
            # Wait for intersection observer to trigger and React to render
            time.sleep(5)  # Longer wait for API call + render
            
            # Scrape newly visible content
            self._scrape_current_page_listings(seen_urls)
            current_count = len(self.listings)
            
            new_listings = current_count - last_listing_count
            if new_listings > 0:
                logger.info(f"✓ Found {new_listings} new listings! Total: {current_count}")
                
                # Save the new batch to database immediately
                if db_session:
                    new_batch = self.listings[last_listing_count:]  # Get only new listings
                    saved_count = self._save_listings_batch(new_batch, self.site_name, db_session)
                    logger.info(f"💾 Saved {saved_count} new listings to database")
                
                last_listing_count = current_count
                no_new_content_count = 0
                
                # Update progress
                self._update_status_field('pages_scraped', scroll_count)
                self._update_status_field('listings_found', current_count)
                self._broadcast_status()
            else:
                no_new_content_count += 1
                logger.info(f"No new listings found ({no_new_content_count}/3)")
                
                # If no new listings for 3 consecutive scrolls, we're done
                if no_new_content_count >= 3:
                    logger.info(f"No new content after {no_new_content_count} attempts. Scraping complete.")
                    break
        
        logger.info(f"Scrolling complete. Total scrolls: {scroll_count}, Total listings: {len(self.listings)}")
        return scroll_count

    def get_all_listings_basic(self, max_pages: int = 500, db_session=None, target_site: str = None) -> List[Dict]:
        """
        Scrape basic listing information from Ruaha
        Uses infinite scroll to load all listings
        
        Args:
            max_pages: Maximum number of scrolls (simulates pages)
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
        seen_urls = set()

        try:
            self._init_listings_status(target_site=self.site_name)
            self._broadcast_status()

            logger.info(f"Starting Ruaha basic scraping (max scrolls: {max_pages})")
            logger.info(f"Navigating to: {self.ads_url}")
            
            # Navigate to ads page
            self.driver.get(self.ads_url)
            self.wait_for_page_load()
            
            # Initial scrape of visible content
            logger.info("Scraping initial visible listings...")
            self._scrape_current_page_listings(seen_urls)
            logger.info(f"Initial scrape: {len(self.listings)} listings found")
            
            # Save initial batch to database
            if db_session and len(self.listings) > 0:
                saved_count = self._save_listings_batch(self.listings, self.site_name, db_session)
                logger.info(f"💾 Saved {saved_count} initial listings to database")
            
            # Progressive scroll + scrape (with progressive saving)
            # Keep status as 'scraping' (don't change to 'scrolling')
            self._update_status_field('total_pages', max_pages, broadcast=False)
            self._broadcast_status()
            
            logger.info("Starting progressive scroll scraping...")
            scroll_count = self.scroll_and_load_more(seen_urls, db_session=db_session, max_scrolls=max_pages)
            
            logger.info(f"✓ Scraping complete! Found {len(self.listings)} unique listings after {scroll_count} scrolls")

            # Update final status
            self._update_status_field('status', 'completed')
            self._update_status_field('listings_found', len(self.listings))
            self._broadcast_status()

            return self.listings

        except Exception as e:
            logger.error(f"Error in Ruaha basic scraping: {e}", exc_info=True)
            # Update error status
            self._update_status_field('status', 'error')
            self._update_status_field('error_message', str(e))
            self._broadcast_status()
            return self.listings
        finally:
            # Finalize and reset flags
            was_stopped = self.should_stop
            self._finalize_status(was_stopped=was_stopped)
            self.is_scraping = False

    def _scrape_current_page_listings(self, seen_urls: set):
        """
        Scrape listings currently visible on the page
        
        Args:
            seen_urls: Set of URLs already scraped
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Find all listing cards
        # Pattern: /ads/property-type-location-price-id
        # Example: /ads/house-and-apartments-for-sale-kinyerezi-dar-es-salaam-500000000-if1rj0f92pfsvyeeinffrwpe
        listing_links = soup.find_all('a', href=re.compile(r'/ads/[a-z0-9-]+-\d+-[a-zA-Z0-9_-]+$'))
        
        new_listings_this_pass = 0
        
        for link in listing_links:
            if self._check_should_stop():
                break
            
            href = link.get('href', '')
            if not href or href in seen_urls:
                continue
            
            seen_urls.add(href)
            new_listings_this_pass += 1
            
            # Build full URL
            raw_url = f"{self.base_url}{href}" if href.startswith('/') else href
            
            # Extract listing ID
            source_listing_id = self.extract_listing_id_from_url(href)
            
            # Find parent card to extract more info
            card = link.find_parent('div', recursive=True)
            if not card:
                card = link
            
            # Extract title
            title_elem = card.find(['h6', 'h5', 'h4'])
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract price
            price_elem = card.find(string=re.compile(r'TSH|USD'))
            price = None
            price_currency = "TZS"
            if price_elem:
                price_text = price_elem.strip()
                price, price_currency = self.parse_price(price_text)
            
            # Extract location
            # Look for location icon (can be img or svg)
            location = None
            location_elem = card.find('img', alt=re.compile(r'location', re.I))
            if not location_elem:
                # Try finding svg with location-related data-icon
                location_elem = card.find('svg', {'data-icon': re.compile(r'location|pin', re.I)})
            
            if location_elem:
                location_text = location_elem.find_next(string=True)
                if location_text:
                    location = location_text.strip()
            
            # Extract property type and listing type from URL or text
            property_type = None
            listing_type = None
            
            if 'house-and-apartments' in href:
                property_type = "House and Apartments"
            elif 'land-and-plot' in href:
                property_type = "Land and Plot"
            elif 'commercial-property' in href:
                property_type = "Commercial Property"
            elif 'vacation-bnb' in href:
                property_type = "Vacation/BnB Homes"
            elif 'event-centers' in href:
                property_type = "Event Centers and Venues"
            elif 'joint-venture' in href:
                property_type = "Joint Venture"
            
            if '-for-rent' in href:
                listing_type = "rent"
            elif '-for-sale' in href:
                listing_type = "sale"
            
            # Extract agent name
            agent_elem = card.find('a', href=re.compile(r'/agents/'))
            agent_name = agent_elem.get_text(strip=True) if agent_elem else None
            
            listing_data = {
                'raw_url': raw_url,
                'source': self.site_name,
                'source_listing_id': source_listing_id,
                'title': title,
                'price': price,
                'price_currency': price_currency,
                'property_type': property_type,
                'listing_type': listing_type,
                'agent_name': agent_name,
                'address_text': location,
            }
            
            self.listings.append(listing_data)
        
        logger.info(f"Scrape pass: {new_listings_this_pass} new listings added, {len(self.listings)} total")

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
            
            # Parse page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract title
            title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'title|heading', re.I))
            if not title_elem:
                title_elem = soup.find(['h1', 'h2'])
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract description
            desc_heading = soup.find(string=re.compile(r'Description', re.I))
            description = None
            if desc_heading:
                desc_elem = desc_heading.find_next(['p', 'div'])
                if desc_elem:
                    description = desc_elem.get_text(strip=True)
            
            # Extract price
            price_elem = soup.find(string=re.compile(r'TSH|USD'))
            price = None
            price_currency = "TZS"
            price_period = "once"
            if price_elem:
                price_text = price_elem.strip()
                price, price_currency = self.parse_price(price_text)
                
                # Check for period
                if 'month' in price_text.lower():
                    price_period = "month"
                elif 'year' in price_text.lower():
                    price_period = "year"
                elif 'night' in price_text.lower():
                    price_period = "night"
            
            # Extract location from info-list
            # Look for li containing location-pin icon
            location = None
            city = None
            district = None
            region = None
            country = "Tanzania"
            
            info_list = soup.find('ul', class_='info-list')
            if info_list:
                # Find the li with location-pin icon
                location_li = info_list.find('svg', {'data-icon': 'location-pin'})
                if location_li and location_li.parent:
                    location_text = location_li.parent.get_text(strip=True)
                    location = location_text
                    
                    # Parse location (e.g., "Oyster Bay, Dar es Salaam" or "Kizota, Dodoma")
                    parts = [p.strip() for p in location.split(',') if p.strip()]
                    if len(parts) >= 2:
                        district = parts[0]
                        city = parts[1]
                        region = parts[1]  # City is also the region in Tanzania
                    elif len(parts) == 1:
                        city = parts[0]
                        region = parts[0]
            
            # Extract features
            features_section = soup.find(string=re.compile(r'Features', re.I))
            
            bedrooms = None
            bathrooms = None
            car_parking = None
            land_area_sqm = None
            built_area_sqm = None
            floors = None
            
            if features_section:
                features_div = features_section.find_parent(['div', 'section'])
                if features_div:
                    # Look for specific features
                    for text in features_div.stripped_strings:
                        # Car parking
                        if 'car parking' in text.lower():
                            match = re.search(r'(\d+)', text)
                            if match:
                                car_parking = int(match.group(1))
                        
                        # Land size
                        if 'land size' in text.lower() or 'sqm' in text.lower():
                            match = re.search(r'(\d+(?:,\d+)?)\s*sqm', text, re.I)
                            if match:
                                land_area_sqm = float(match.group(1).replace(',', ''))
                        
                        # Built area
                        if 'built area' in text.lower():
                            match = re.search(r'(\d+(?:,\d+)?)\s*sqm', text, re.I)
                            if match:
                                built_area_sqm = float(match.group(1).replace(',', ''))
                        
                        # Floors
                        if 'floor' in text.lower():
                            match = re.search(r'(\d+)', text)
                            if match:
                                floors = int(match.group(1))
                        
                        # Bedrooms
                        if 'bedroom' in text.lower() or 'bed' in text.lower():
                            match = re.search(r'(\d+)', text)
                            if match:
                                bedrooms = int(match.group(1))
                        
                        # Bathrooms
                        if 'bathroom' in text.lower() or 'bath' in text.lower():
                            match = re.search(r'(\d+)', text)
                            if match:
                                bathrooms = int(match.group(1))
            
            # Extract images
            images = []
            img_elements = soup.find_all('img', src=re.compile(r'ruaha-assets-app-bucket'))
            for img in img_elements[:20]:  # Limit to 20 images
                src = img.get('src', '')
                if src and 'ruaha-assets-app-bucket' in src:
                    # Get original image URL
                    if '_next/image' in src:
                        match = re.search(r'url=([^&]+)', src)
                        if match:
                            from urllib.parse import unquote
                            images.append(unquote(match.group(1)))
                    else:
                        images.append(src)
            
            # Extract agent info
            agent_name = None
            agent_phone = None
            
            agent_link = soup.find('a', href=re.compile(r'/agents/'))
            if agent_link:
                agent_name = agent_link.get_text(strip=True)
            
            # Phone might be in footer or contact section
            phone_link = soup.find('a', href=re.compile(r'tel:'))
            if phone_link:
                phone_text = phone_link.get('href', '').replace('tel:', '')
                # Normalize phone number
                phone_text = phone_text.strip().replace(' ', '')
                if phone_text.startswith('+255'):
                    agent_phone = phone_text.replace('+255', '0')
                else:
                    agent_phone = phone_text
            
            # Extract property type and listing type from URL
            property_type = None
            listing_type = None
            
            if 'house-and-apartments' in listing_url:
                property_type = "House and Apartments"
            elif 'land-and-plot' in listing_url:
                property_type = "Land and Plot"
            elif 'commercial-property' in listing_url:
                property_type = "Commercial Property"
            elif 'vacation-bnb' in listing_url:
                property_type = "Vacation/BnB Homes"
            elif 'event-centers' in listing_url:
                property_type = "Event Centers and Venues"
            elif 'joint-venture' in listing_url:
                property_type = "Joint Venture"
            
            if '-for-rent' in listing_url:
                listing_type = "rent"
            elif '-for-sale' in listing_url:
                listing_type = "sale"
            
            # Extract listing ID
            source_listing_id = self.extract_listing_id_from_url(listing_url)
            
            detailed_data = {
                'raw_url': listing_url,
                'source': self.site_name,
                'source_listing_id': source_listing_id,
                'title': title,
                'description': description,
                'property_type': property_type,
                'listing_type': listing_type,
                'price': price,
                'price_currency': price_currency,
                'price_period': price_period,
                'country': country,
                'region': region,
                'city': city,
                'district': district,
                'address_text': location,
                'bedrooms': bedrooms,
                'bathrooms': bathrooms,
                'living_area_sqm': built_area_sqm,
                'land_area_sqm': land_area_sqm,
                'images': images,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
            }
            
            # Save to database
            if db_session:
                self._save_listing(detailed_data, self.site_name, db_session)

            logger.info(f"Scraped details for: {title}")
            
            # Rate limiting
            time.sleep(2)
            
            return detailed_data

        except Exception as e:
            logger.error(f"Error scraping detail page {listing_url}: {e}", exc_info=True)
            return {}

