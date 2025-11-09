"""
Database service for CRUD operations
"""
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.models.real_estate import RealEstateListing
from app.models.agent import Agent
from typing import List, Optional, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service class for database operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_or_update_agent(self, phone: str, name: Optional[str] = None, email: Optional[str] = None) -> Optional[Agent]:
        """
        Create new agent or update existing one based on phone number (unique key)
        
        Args:
            phone: Agent phone number (unique identifier)
            name: Agent name
            email: Agent email
            
        Returns:
            Agent object or None if phone is invalid
        """
        if not phone:
            logger.debug("No phone provided, skipping agent creation")
            return None
        
        # Check if agent exists by phone
        existing_agent = self.db.query(Agent).filter(
            Agent.phone == phone
        ).first()
        
        if existing_agent:
            # Update existing agent if new data is provided
            updated = False
            if name and name != existing_agent.name:
                existing_agent.name = name
                updated = True
            if email and email != existing_agent.email:
                existing_agent.email = email
                updated = True
            
            if updated:
                existing_agent.updated_at = datetime.utcnow()
                self.db.commit()
                self.db.refresh(existing_agent)
                logger.debug("Updated agent with phone: %s", phone)
            else:
                logger.debug("Agent with phone %s already exists, no updates needed", phone)
            
            return existing_agent
        else:
            # Create new agent
            agent = Agent(
                phone=phone,
                name=name,
                email=email
            )
            self.db.add(agent)
            self.db.commit()
            self.db.refresh(agent)
            logger.info("Created new agent with phone: %s", phone)
            return agent
    
    def create_or_update_listing(self, data: dict, target_site: str) -> RealEstateListing:
        """
        Create new listing or update existing one
        
        Args:
            data: Scraper data dictionary
            target_site: 'jiji', 'kupatana', etc.
            
        Returns:
            RealEstateListing object
        """
        raw_url = data.get('raw_url')
        if not raw_url:
            raise ValueError("raw_url is required in data dictionary")
        
        # Save agent to database if agent_phone is provided
        agent_phone = data.get('agent_phone')
        if agent_phone:
            agent_name = data.get('agent_name')
            agent_email = data.get('agent_email')
            self.create_or_update_agent(
                phone=agent_phone,
                name=agent_name,
                email=agent_email
            )
        
        # Check if listing exists
        existing = self.db.query(RealEstateListing).filter(
            RealEstateListing.raw_url == raw_url
        ).first()
        
        if existing:
            # Check if agent_name is in the data to determine update strategy
            has_agent_name = 'agent_name' in data and data.get(
                'agent_name') is not None

            if has_agent_name:
                # Full update: Update all fields directly from data
                # Update fields one by one to avoid issues with from_scraper_data
                if 'source' in data:
                    existing.source = data.get('source')
                if 'source_listing_id' in data:
                    existing.source_listing_id = data.get('source_listing_id')
                if 'scrape_timestamp' in data:
                    # Convert ISO string to datetime if needed
                    scrape_ts = data.get('scrape_timestamp')
                    if isinstance(scrape_ts, str):
                        # Remove 'Z' suffix if present and parse
                        scrape_ts = scrape_ts.replace('Z', '+00:00')
                        scrape_ts = datetime.fromisoformat(scrape_ts)
                    existing.scrape_timestamp = scrape_ts
                if 'title' in data:
                    existing.title = data.get('title')
                if 'description' in data:
                    existing.description = data.get('description')
                if 'property_type' in data:
                    existing.property_type = data.get('property_type')
                if 'listing_type' in data:
                    existing.listing_type = data.get('listing_type')
                if 'status' in data:
                    existing.status = data.get('status')
                if 'price' in data:
                    existing.price = data.get('price')
                if 'price_currency' in data:
                    existing.price_currency = data.get('price_currency')
                if 'price_period' in data:
                    existing.price_period = data.get('price_period')
                if 'country' in data:
                    existing.country = data.get('country')
                if 'region' in data:
                    existing.region = data.get('region')
                if 'city' in data:
                    existing.city = data.get('city')
                if 'district' in data:
                    existing.district = data.get('district')
                if 'address_text' in data:
                    existing.address_text = data.get('address_text')
                if 'latitude' in data:
                    existing.latitude = data.get('latitude')
                if 'longitude' in data:
                    existing.longitude = data.get('longitude')
                if 'bedrooms' in data:
                    existing.bedrooms = data.get('bedrooms')
                if 'bathrooms' in data:
                    existing.bathrooms = data.get('bathrooms')
                if 'living_area_sqm' in data:
                    existing.living_area_sqm = data.get('living_area_sqm')
                if 'land_area_sqm' in data:
                    existing.land_area_sqm = data.get('land_area_sqm')
                if 'images' in data:
                    existing.images = data.get('images')
                if 'agent_name' in data:
                    existing.agent_name = data.get('agent_name')
                if 'agent_phone' in data:
                    existing.agent_phone = data.get('agent_phone')
                if 'agent_whatsapp' in data:
                    existing.agent_whatsapp = data.get('agent_whatsapp')
                if 'agent_email' in data:
                    existing.agent_email = data.get('agent_email')
                if 'agent_website' in data:
                    existing.agent_website = data.get('agent_website')
                if 'agent_profile_url' in data:
                    existing.agent_profile_url = data.get('agent_profile_url')

                # Update updated_at only for full updates
                existing.updated_at = datetime.now()
                logger.debug(
                    "Full update: Updated all fields for listing %s", raw_url)
            else:
                # Partial update: Update only title, price, and price_currency
                # Do NOT update updated_at for partial updates
                if 'title' in data:
                    existing.title = data.get('title')
                if 'price' in data:
                    existing.price = data.get('price')
                if 'price_currency' in data:
                    existing.price_currency = data.get('price_currency')

                logger.debug(
                    "Partial update: Updated title, price, price_currency for listing %s (updated_at not changed)", raw_url)

            self.db.commit()
            self.db.refresh(existing)
            return existing
        else:
            # Create new listing directly from data
            # Convert scrape_timestamp if it's a string
            scrape_timestamp = data.get('scrape_timestamp')
            if isinstance(scrape_timestamp, str):
                # Remove 'Z' suffix if present and parse
                scrape_timestamp = scrape_timestamp.replace('Z', '+00:00')
                scrape_timestamp = datetime.fromisoformat(scrape_timestamp)

            listing = RealEstateListing(
                raw_url=data.get('raw_url'),
                source=data.get('source', target_site),
                source_listing_id=data.get('source_listing_id'),
                scrape_timestamp=scrape_timestamp,
                title=data.get('title'),
                description=data.get('description'),
                property_type=data.get('property_type'),
                listing_type=data.get('listing_type'),
                status=data.get('status', 'active'),
                price=data.get('price'),
                price_currency=data.get('price_currency'),
                price_period=data.get('price_period'),
                country=data.get('country'),
                region=data.get('region'),
                city=data.get('city'),
                district=data.get('district'),
                address_text=data.get('address_text'),
                latitude=data.get('latitude'),
                longitude=data.get('longitude'),
                bedrooms=data.get('bedrooms'),
                bathrooms=data.get('bathrooms'),
                living_area_sqm=data.get('living_area_sqm'),
                land_area_sqm=data.get('land_area_sqm'),
                images=data.get('images', []),
                agent_name=data.get('agent_name'),
                agent_phone=data.get('agent_phone'),
                agent_whatsapp=data.get('agent_whatsapp'),
                agent_email=data.get('agent_email'),
                agent_website=data.get('agent_website'),
                agent_profile_url=data.get('agent_profile_url')
            )
            # Ensure created_at is set
            if listing.created_at is None:
                listing.created_at = datetime.now()
            self.db.add(listing)
            self.db.commit()
            self.db.refresh(listing)
            return listing
    
    def get_all_listings(self, lightweight: bool = False, 
                        target_site: Optional[str] = None,
                        limit: Optional[int] = None) -> List[Dict]:
        """
        Get all listings from database
        
        Args:
            lightweight: If True, return only raw_url, title, price, price_currency
            target_site: Filter by source ('jiji', 'kupatana', etc.)
            limit: Maximum number of results
            
        Returns:
            List of dictionaries
        """
        query = self.db.query(RealEstateListing)
        
        if target_site:
            query = query.filter(RealEstateListing.source == target_site)
        
        query = query.order_by(RealEstateListing.created_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        listings = query.all()
        return [listing.to_dict(include_details=not lightweight) for listing in listings]
    
    def get_listing_by_url(self, url: str) -> Optional[Dict]:
        """
        Get single listing by URL
        
        Args:
            url: Listing URL (raw_url)
            
        Returns:
            Dictionary or None
        """
        listing = self.db.query(RealEstateListing).filter(
            RealEstateListing.raw_url == url
        ).first()
        
        return listing.to_dict() if listing else None
    
    def get_listings_by_urls(self, urls: List[str]) -> List[Dict]:
        """
        Get multiple listings by URLs
        
        Args:
            urls: List of URLs (raw_url)
            
        Returns:
            List of dictionaries
        """
        listings = self.db.query(RealEstateListing).filter(
            RealEstateListing.raw_url.in_(urls)
        ).all()
        
        return [listing.to_dict() for listing in listings]
    
    def delete_listing(self, url: str) -> bool:
        """
        Delete listing by URL
        
        Args:
            url: Listing URL (raw_url)
            
        Returns:
            True if deleted, False if not found
        """
        listing = self.db.query(RealEstateListing).filter(
            RealEstateListing.raw_url == url
        ).first()
        
        if listing:
            self.db.delete(listing)
            self.db.commit()
            return True
        return False
    
    def get_statistics(self) -> Dict:
        """
        Get database statistics (only counts detailed listings with agent_name)
        
        Returns:
            Dictionary with statistics
        """
        # Only count listings with agent_name (scraped in detail)
        total = self.db.query(RealEstateListing).filter(
            RealEstateListing.agent_name.isnot(None)
        ).count()
        jiji_count = self.db.query(RealEstateListing).filter(
            RealEstateListing.source == 'jiji',
            RealEstateListing.agent_name.isnot(None)
        ).count()
        kupatana_count = self.db.query(RealEstateListing).filter(
            RealEstateListing.source == 'kupatana',
            RealEstateListing.agent_name.isnot(None)
        ).count()
        
        return {
            'total_listings': total,
            'jiji_listings': jiji_count,
            'kupatana_listings': kupatana_count,
            'last_updated': datetime.now().isoformat()
        }
    
    def search_listings(self, query: str, limit: int = 50) -> List[Dict]:
        """
        Search listings by title, location, or description (only detailed listings)
        
        Args:
            query: Search query string
            limit: Maximum results
            
        Returns:
            List of matching listings with agent_name
        """
        search_term = f"%{query}%"
        
        listings = self.db.query(RealEstateListing).filter(
            RealEstateListing.agent_name.isnot(None),  # Only detailed listings
            or_(
                RealEstateListing.title.ilike(search_term),
                RealEstateListing.address_text.ilike(search_term),
                RealEstateListing.city.ilike(search_term),
                RealEstateListing.district.ilike(search_term),
                RealEstateListing.region.ilike(search_term),
                RealEstateListing.description.ilike(search_term)
            )
        ).limit(limit).all()
        
        return [listing.to_dict() for listing in listings]

    def get_unique_property_types(self) -> List[str]:
        """
        Get all unique property types from the database (only from detailed listings)

        Returns:
            List of unique property type strings (excluding None/null values)
        """
        property_types = self.db.query(RealEstateListing.property_type).filter(
            RealEstateListing.agent_name.isnot(None),  # Only detailed listings
            RealEstateListing.property_type.isnot(None),
            RealEstateListing.property_type != ''
        ).distinct().all()

        # Extract strings from tuples and sort
        return sorted([pt[0] for pt in property_types if pt[0]])
