"""
MakaziMapya.com Real Estate Scraper Service
Service for scraping real estate listings from makazimapya.com
"""

import time
import logging
import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from app.services.base_scraper_service import BaseScraperService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MakaziMapyaService(BaseScraperService):
    """MakaziMapya scraper service for real estate listings"""

    _instance: Optional['MakaziMapyaService'] = None

    def __init__(self, headless: bool = False, profile_dir: str = None):
        """
        Initialize the scraper

        Args:
            headless: Run browser in headless mode
            profile_dir: Directory to save browser profile (for persistent sessions)
        """
        # Initialize base class
        super().__init__(
            base_url="https://makazimapya.com",
            headless=headless,
            profile_dir=profile_dir or "./makazimapya_browser_profile",
            site_name="makazimapya"
        )

    @classmethod
    def get_instance(cls) -> 'MakaziMapyaService':
        """Get or create singleton instance of MakaziMapyaService"""
        if cls._instance is None:
            from app.core.config import settings
            logger.info("Initializing MakaziMapya scraper...")
            try:
                cls._instance = cls(
                    profile_dir=getattr(settings, 'MAKAZIMAPYA_PROFILE_DIR', './makazimapya_browser_profile'),
                    headless=settings.SCRAPER_HEADLESS
                )
                cls._instance.start_browser()

                # Navigate to homepage to initialize
                try:
                    cls._instance.driver.set_page_load_timeout(30)
                    cls._instance.driver.get("https://makazimapya.com/listings")
                    time.sleep(3)  # Wait for page to settle
                    logger.info("✓ MakaziMapya scraper ready (navigated to listings page)")
                except Exception as e:
                    # Even if navigation times out, the page might still be usable
                    logger.warning(
                        f"⚠ Initial navigation warning (page may still be loading): {str(e)[:100]}")
                    logger.info("✓ MakaziMapya scraper ready")

            except Exception as e:
                logger.error(f"Failed to initialize MakaziMapya scraper: {e}")
                cls._instance = None
                raise

        return cls._instance

    @classmethod
    def close_instance(cls):
        """Close the singleton instance and browser"""
        if cls._instance:
            try:
                cls._instance.close_browser()
                logger.info("✓ MakaziMapya scraper closed")
            except Exception as e:
                logger.error(f"Error closing MakaziMapya scraper: {e}")
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
            cls._instance.is_scraping = False  # Immediately reset the flag
            logger.info("Stop flag set for MakaziMapya scraper")

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

    def parse_price(self, price_text: str) -> tuple[Optional[float], Optional[str]]:
        """
        Parse price text to extract numeric value and currency
        
        Args:
            price_text: Price string (e.g., "Sh. 300,000")
            
        Returns:
            Tuple of (price_value, currency)
        """
        if not price_text:
            return None, None
        
        # Clean the text
        price_text = price_text.strip()
        
        # Default currency is TZS (Tanzanian Shillings)
        currency = 'TZS'
        
        # Remove currency indicators
        if 'Sh.' in price_text or 'TZS' in price_text or 'TSh' in price_text:
            currency = 'TZS'
            price_text = re.sub(r'Sh\.|TZS|TSh', '', price_text, flags=re.IGNORECASE).strip()
        elif 'USD' in price_text or '$' in price_text:
            currency = 'USD'
            price_text = re.sub(r'USD|\$', '', price_text, flags=re.IGNORECASE).strip()
        elif '€' in price_text or 'EUR' in price_text:
            currency = 'EUR'
            price_text = re.sub(r'€|EUR', '', price_text, flags=re.IGNORECASE).strip()
        
        # Remove commas and spaces, then try to parse
        try:
            price_cleaned = price_text.replace(',', '').replace(' ', '').strip()
            if price_cleaned:
                price_value = float(price_cleaned)
                return price_value, currency
        except (ValueError, AttributeError):
            pass
        
        return None, None

    def extract_listing_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract listing UUID from MakaziMapya URL
        
        Args:
            url: Full listing URL (e.g., "/listings/nyumba-ya-vyumba-viwili-inapangishwa-kimara-dar-es-salaam/c347f678-f83b-452e-8114-73602b83f18a")
            
        Returns:
            UUID string or None
        """
        try:
            # URL pattern: /listings/{slug}/{uuid}
            match = re.search(r'/listings/[^/]+/([a-f0-9\-]{36})', url)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

    def get_total_pages_from_pagination(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract total pages from pagination element
        
        Args:
            soup: BeautifulSoup object of the listings page
            
        Returns:
            Total number of pages or None
        """
        try:
            # Look for pagination text like "Page 1 of 9782"
            pagination_text = soup.find(string=re.compile(r'Page \d+ of \d+', re.IGNORECASE))
            if pagination_text:
                match = re.search(r'Page \d+ of (\d+)', pagination_text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        except Exception as e:
            logger.debug(f"Error extracting total pages: {e}")
        return None

    def get_all_listings_basic(
        self,
        max_pages: Optional[int] = None,
        db_session=None,
        target_site: str = 'makazimapya'
    ) -> List[Dict]:
        """
        Scrape all listings (url, title, price, currency) from all available pages.

        Args:
            max_pages: Maximum number of pages to scrape (None for all pages)
            db_session: Optional database session to save listings immediately after each page
            target_site: Target site name for database saving

        Returns:
            List of dictionaries with 'raw_url', 'title', 'price', 'price_currency', 'source' keys
        """
        # Initialize scraping status
        self._init_listings_status(target_site, max_pages)

        logger.info("Starting to scrape all listings (url, title, price) from MakaziMapya...")
        all_listings = []
        seen_urls = set()  # Track URLs we've already seen to detect duplicates
        page_num = 1
        total_saved = 0  # Track total saved to database

        # Initialize database service if session is provided
        db_service = self._get_db_service(db_session)
        if db_service:
            logger.info("✅ Database service initialized for %s", target_site)

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
                        url = f"{self.base_url}/listings"
                    else:
                        url = f"{self.base_url}/listings?page={page_num}"

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

                    # Get total pages from pagination (first page only)
                    if page_num == 1:
                        total_pages = self.get_total_pages_from_pagination(soup)
                        if total_pages:
                            self._update_status_field('total_pages', total_pages)
                            logger.info(f"Total pages detected: {total_pages}")

                    # Find all listing cards - need to identify the correct selector
                    # Based on browser snapshot, listings appear to be in cards
                    # Try multiple possible selectors
                    listing_cards = (
                        soup.find_all('article') or
                        soup.find_all('div', class_=re.compile(r'listing|card|item', re.IGNORECASE)) or
                        soup.select('a[href*="/listings/"]')
                    )

                    # If we found links, extract parent containers
                    if not listing_cards or (listing_cards and all(card.name == 'a' for card in listing_cards)):
                        # Try to find listing containers by looking for links to detail pages
                        listing_links = soup.find_all('a', href=re.compile(r'/listings/[^/]+/[a-f0-9\-]{36}'))
                        if listing_links:
                            # Get parent containers of these links
                            listing_cards = []
                            for link in listing_links:
                                # Try to find the card container (go up the DOM tree)
                                parent = link.find_parent(['article', 'div', 'li'])
                                if parent and parent not in listing_cards:
                                    listing_cards.append(parent)

                    if not listing_cards:
                        logger.warning(f"No listings found on page {page_num}. This might be the last page.")
                        # If we're on page 1 and no listings, something is wrong
                        if page_num == 1:
                            logger.error("No listings found on first page. Check selectors.")
                        break

                    logger.info(f"Found {len(listing_cards)} listing cards on page {page_num}")

                    # Extract data from each listing card
                    page_listings = []
                    new_listings_count = 0

                    for card in listing_cards:
                        try:
                            # Find the link to detail page
                            link = card.find('a', href=re.compile(r'/listings/[^/]+/[a-f0-9\-]{36}'))
                            if not link:
                                continue

                            href = link.get('href', '')
                            if not href:
                                continue

                            # Construct full URL
                            if href.startswith('http'):
                                full_url = href
                            elif href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                            else:
                                full_url = urljoin(self.base_url, href)

                            # Skip if we've seen this URL before
                            if full_url in seen_urls:
                                continue

                            seen_urls.add(full_url)

                            # Extract title - try multiple selectors
                            title = None
                            title_elem = (
                                card.find('h1') or
                                card.find('h2') or
                                card.find('h3') or
                                link.find(['h1', 'h2', 'h3']) or
                                link
                            )
                            if title_elem:
                                title = title_elem.get_text(strip=True)

                            # Extract price - look for text containing "Sh."
                            price_value = None
                            currency = None
                            price_elem = card.find(string=re.compile(r'Sh\.|TZS|TSh', re.IGNORECASE))
                            if price_elem:
                                price_text = price_elem.strip()
                                price_value, currency = self.parse_price(price_text)
                            else:
                                # Try to find price in a div or span
                                price_div = card.find(['div', 'span', 'p'], string=re.compile(r'Sh\.|TZS|TSh', re.IGNORECASE))
                                if price_div:
                                    price_text = price_div.get_text(strip=True)
                                    price_value, currency = self.parse_price(price_text)

                            # Extract listing ID from URL
                            listing_id = self.extract_listing_id_from_url(full_url)

                            listing_data = {
                                'raw_url': full_url,
                                'title': title or 'N/A',
                                'price': price_value,
                                'price_currency': currency,
                                'source': 'makazimapya',
                                'source_listing_id': listing_id
                            }

                            page_listings.append(listing_data)
                            new_listings_count += 1

                        except Exception as e:
                            logger.debug(f"Error extracting data from listing card: {e}")
                            continue

                    # Check if we found any new listings on this page
                    if new_listings_count == 0:
                        logger.info(f"⚠️  Page {page_num}: No new listings found. Stopping pagination.")
                        break

                    if page_listings:
                        all_listings.extend(page_listings)
                        logger.info(f"✅ Page {page_num}: Found {new_listings_count} new listings (Total: {len(all_listings)})")

                        # Update scraping status
                        self._update_page_progress(page_num, len(all_listings))

                        # Save to database immediately if db_session is provided
                        if db_service:
                            logger.info(f"💾 Starting to save {len(page_listings)} listings from page {page_num} to database...")
                            page_saved = self._save_listings_batch(
                                page_listings,
                                target_site,
                                db_session,
                                update_status=True
                            )
                            total_saved += page_saved
                            logger.info(f"💾 Page {page_num}: Saved {page_saved} listings to database (Total saved: {total_saved})")

                        # Broadcast status update
                        self._broadcast_status()
                    else:
                        logger.warning(f"Page {page_num}: No valid listings extracted. Moving to next page.")

                    # Delay before next page
                    time.sleep(2)
                    page_num += 1

                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}", exc_info=True)
                    # If we get an error, try one more page before giving up
                    page_num += 1
                    if page_num > 1000:  # Safety limit
                        logger.warning("Reached safety limit (1000 pages). Stopping pagination.")
                        break
                    continue

            logger.info(f"✅ Scraped {len(all_listings)} listings from {page_num - 1} pages")
            if db_service:
                logger.info(f"💾 Total saved to database: {total_saved} listings")
            return all_listings

        finally:
            # Finalize scraping status
            was_stopped = self.should_stop
            self._finalize_status(was_stopped=was_stopped)
            logger.info("Scraping completed, status reset")

    def extract_detailed_data(
        self,
        listing_url: str,
        total_urls: int = 0,
        current_index: int = 0,
        db_session=None,
        target_site: str = 'makazimapya'
    ) -> Dict:
        """
        Extract detailed information from a listing page and save to database

        Args:
            listing_url: URL of the listing detail page
            total_urls: Total number of URLs to scrape (for progress tracking)
            current_index: Current index in the URL list (for progress tracking)
            db_session: Optional database session to save listing immediately after extraction
            target_site: Target site name for database saving

        Returns:
            Dictionary containing all extracted data
        """
        # Set scraping status (don't reset stop flag - it may have been set by user)
        self.is_scraping = True

        # Update scraping status for details
        self._update_url_progress(
            current_url=listing_url,
            current_index=current_index - 1,  # -1 because we're about to process this URL
            total_urls=total_urls if total_urls > 0 else None
        )
        logger.info(f"🔍 Extracting data from: {listing_url}")

        try:
            # Check if stop flag is set before starting
            if self.should_stop:
                logger.info("Stop flag detected. Skipping detailed extraction.")
                return {
                    'raw_url': listing_url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }

            self.driver.get(listing_url)
            self.wait_for_page_load()

            # Check if stop flag is set after page load
            if self.should_stop:
                logger.info("Stop flag detected after page load. Stopping detailed extraction.")
                return {
                    'raw_url': listing_url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }

            # Get page source
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Check if stop flag is set after parsing
            if self.should_stop:
                logger.info("Stop flag detected after parsing. Stopping detailed extraction.")
                return {
                    'raw_url': listing_url,
                    'error': 'Scraping was stopped',
                    'scraped_at': datetime.now().isoformat()
                }

            # Extract title (h1 on detail page)
            title = None
            title_elem = soup.find('h1')
            if title_elem:
                title = title_elem.get_text(strip=True)

            # Extract listing type from title (rent/sale)
            listing_type = None
            if title:
                title_lower = title.lower()
                if 'inapangishwa' in title_lower or 'panga' in title_lower or 'rent' in title_lower:
                    listing_type = 'rent'
                elif 'inauzwa' in title_lower or 'nunua' in title_lower or 'sale' in title_lower:
                    listing_type = 'sale'

            # Extract price
            price_value = None
            currency = None
            price_elem = soup.find(string=re.compile(r'Sh\.|TZS|TSh', re.IGNORECASE))
            if price_elem:
                price_text = price_elem.strip()
                price_value, currency = self.parse_price(price_text)
            else:
                # Try to find price in a div or span
                price_div = soup.find(['div', 'span', 'p'], string=re.compile(r'Sh\.|TZS|TSh', re.IGNORECASE))
                if price_div:
                    price_text = price_div.get_text(strip=True)
                    price_value, currency = self.parse_price(price_text)

            # Extract location
            location = None
            location_link = soup.find('a', href=re.compile(r'/listings\?location='))
            if location_link:
                location = location_link.get_text(strip=True)

            # Parse location into components (e.g., "Kimara, Ubungo, Dar Es Salaam")
            country = 'Tanzania'
            region = None
            city = None
            district = None
            address_text = location

            if location:
                parts = [p.strip() for p in location.split(',')]
                if len(parts) >= 3:
                    district = parts[0] if parts[0] else None
                    region = parts[1] if parts[1] else None
                    city = parts[2] if parts[2] else None
                elif len(parts) == 2:
                    district = parts[0] if parts[0] else None
                    city = parts[1] if parts[1] else None
                elif len(parts) == 1:
                    city = parts[0] if parts[0] else None

            # Extract description
            description = None
            # Look for paragraph in article or main content
            article = soup.find('article')
            if article:
                desc_para = article.find('p')
                if desc_para:
                    description = desc_para.get_text(strip=True)

            # Extract images
            images = []
            # Find all img tags in the gallery
            img_tags = soup.find_all('img')
            for img in img_tags:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if src:
                    # Convert relative URLs to absolute
                    if src.startswith('http'):
                        images.append(src)
                    elif src.startswith('/'):
                        images.append(f"{self.base_url}{src}")
                    else:
                        images.append(urljoin(self.base_url, src))

            # Remove duplicates while preserving order
            seen_images = set()
            unique_images = []
            for img in images:
                if img not in seen_images:
                    seen_images.add(img)
                    unique_images.append(img)
            images = unique_images

            # Extract property type from URL or title
            property_type = None
            if title:
                title_lower = title.lower()
                if 'nyumba' in title_lower or 'house' in title_lower:
                    if 'apartment' in title_lower:
                        property_type = 'Apartment'
                    else:
                        property_type = 'House'
                elif 'viwanja' in title_lower or 'plot' in title_lower or 'land' in title_lower:
                    property_type = 'Land'
                elif 'mashamba' in title_lower or 'farm' in title_lower:
                    property_type = 'Farm'
                elif 'frame' in title_lower or 'retail' in title_lower:
                    property_type = 'Retail Space'
                elif 'ofisi' in title_lower or 'office' in title_lower:
                    property_type = 'Office'

            # Extract seller information
            agent_name = None
            agent_phone = None
            agent_whatsapp = None
            agent_instagram = None

            # Find seller section
            seller_section = soup.find('div', class_=re.compile(r'seller|agent|contact', re.IGNORECASE))
            if not seller_section:
                # Try to find by heading
                seller_heading = soup.find(['h2', 'h3', 'h4', 'h5', 'h6'], string=re.compile(r'seller|agent|dalali', re.IGNORECASE))
                if seller_heading:
                    seller_section = seller_heading.find_parent()

            if seller_section:
                # Extract seller name
                name_elem = seller_section.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'div'])
                if name_elem:
                    agent_name = name_elem.get_text(strip=True)

                # Extract phone from tel: links
                tel_links = seller_section.find_all('a', href=re.compile(r'^tel:'))
                if tel_links:
                    tel_href = tel_links[0].get('href', '')
                    phone = tel_href.replace('tel:', '').replace('+255', '0').strip()
                    if phone:
                        agent_phone = phone

                # Extract WhatsApp phone number (not the full URL)
                whatsapp_links = seller_section.find_all('a', href=re.compile(r'wa\.me|whatsapp', re.IGNORECASE))
                if whatsapp_links:
                    whatsapp_url = whatsapp_links[0].get('href', '')
                    # Extract phone number from URL like: https://wa.me/255689138795?text=...
                    whatsapp_match = re.search(r'(?:wa\.me|whatsapp\.com)/(\+?\d+)', whatsapp_url)
                    if whatsapp_match:
                        phone = whatsapp_match.group(1).replace('+255', '0')
                        agent_whatsapp = phone if phone else None

                # Extract Instagram link
                instagram_links = seller_section.find_all('a', href=re.compile(r'instagram\.com', re.IGNORECASE))
                if instagram_links:
                    agent_instagram = instagram_links[0].get('href', '')

            # Extract listing ID from URL
            listing_id = self.extract_listing_id_from_url(listing_url)

            # Build the data dictionary
            listing_data = {
                'raw_url': listing_url,
                'title': title,
                'description': description,
                'price': price_value,
                'price_currency': currency,
                'property_type': property_type,
                'listing_type': listing_type,
                'country': country,
                'region': region,
                'city': city,
                'district': district,
                'address_text': address_text,
                'images': images,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
                'agent_whatsapp': agent_whatsapp,
                'source': 'makazimapya',
                'source_listing_id': listing_id,
                'scrape_timestamp': datetime.now(),
                'status': 'active'
            }

            # Update progress
            self._update_url_progress(
                current_url=listing_url,
                current_index=current_index,
                total_urls=total_urls if total_urls > 0 else None
            )

            # Save to database if session is provided
            if db_session:
                db_service = self._get_db_service(db_session)
                if db_service:
                    try:
                        saved = self._save_listing(listing_data, target_site, db_session)
                        if saved:
                            self._update_status_field('listings_saved', self.scraping_status.get('listings_saved', 0) + 1)
                            logger.info(f"💾 Saved listing to database: {listing_url}")
                    except Exception as e:
                        logger.error(f"Error saving listing to database: {e}")

            # Broadcast status update
            self._broadcast_status()

            return listing_data

        except Exception as e:
            logger.error(f"Error extracting detailed data from {listing_url}: {e}", exc_info=True)
            return {
                'raw_url': listing_url,
                'error': str(e),
                'scraped_at': datetime.now().isoformat()
            }

