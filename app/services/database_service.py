"""
Database service for CRUD operations
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from app.models.real_estate import RealEstateListing
from typing import List, Optional, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service class for database operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_or_update_listing(self, data: dict, target_site: str) -> RealEstateListing:
        """
        Create new listing or update existing one
        
        Args:
            data: Scraper data dictionary
            target_site: 'jiji', 'kupatana', etc.
            
        Returns:
            RealEstateListing object
        """
        url = data.get('url')
        if not url:
            raise ValueError("URL is required in data dictionary")
        
        # Check if listing exists
        existing = self.db.query(RealEstateListing).filter(
            RealEstateListing.url == url
        ).first()
        
        if existing:
            # Check if property_type is in the data to determine update strategy
            has_property_type = 'property_type' in data and data.get('property_type') is not None
            
            if has_property_type:
                # Full update: Update all fields with data passed by parameter
                updated_listing = RealEstateListing.from_scraper_data(data, target_site)
                
                # Copy all attributes to existing record (except url and created_at)
                for key, value in updated_listing.__dict__.items():
                    if not key.startswith('_') and key not in ['url', 'created_at']:
                        setattr(existing, key, value)
                
                # Update updated_at only for full updates
                existing.updated_at = datetime.now()
                logger.debug(f"Full update: Updated all fields for listing {url}")
            else:
                # Partial update: Update only title, price, and currency
                # Do NOT update updated_at for partial updates
                if 'title' in data:
                    existing.title = data.get('title')
                if 'price' in data:
                    existing.price = data.get('price')
                if 'currency' in data:
                    existing.currency = data.get('currency')
                
                logger.debug(f"Partial update: Updated title, price, currency for listing {url} (updated_at not changed)")
            
            self.db.commit()
            self.db.refresh(existing)
            return existing
        else:
            # Create new listing
            listing = RealEstateListing.from_scraper_data(data, target_site)
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
            lightweight: If True, return only url, title, price, currency
            target_site: Filter by target_site ('jiji', 'kupatana', etc.)
            limit: Maximum number of results
            
        Returns:
            List of dictionaries
        """
        query = self.db.query(RealEstateListing)
        
        if target_site:
            query = query.filter(RealEstateListing.target_site == target_site)
        
        query = query.order_by(RealEstateListing.created_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        listings = query.all()
        return [listing.to_dict(include_details=not lightweight) for listing in listings]
    
    def get_listing_by_url(self, url: str) -> Optional[Dict]:
        """
        Get single listing by URL
        
        Args:
            url: Listing URL
            
        Returns:
            Dictionary or None
        """
        listing = self.db.query(RealEstateListing).filter(
            RealEstateListing.url == url
        ).first()
        
        return listing.to_dict() if listing else None
    
    def get_listings_by_urls(self, urls: List[str]) -> List[Dict]:
        """
        Get multiple listings by URLs
        
        Args:
            urls: List of URLs
            
        Returns:
            List of dictionaries
        """
        listings = self.db.query(RealEstateListing).filter(
            RealEstateListing.url.in_(urls)
        ).all()
        
        return [listing.to_dict() for listing in listings]
    
    def delete_listing(self, url: str) -> bool:
        """
        Delete listing by URL
        
        Args:
            url: Listing URL
            
        Returns:
            True if deleted, False if not found
        """
        listing = self.db.query(RealEstateListing).filter(
            RealEstateListing.url == url
        ).first()
        
        if listing:
            self.db.delete(listing)
            self.db.commit()
            return True
        return False
    
    def get_statistics(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with statistics
        """
        total = self.db.query(func.count(RealEstateListing.url)).scalar()
        jiji_count = self.db.query(func.count(RealEstateListing.url)).filter(
            RealEstateListing.target_site == 'jiji'
        ).scalar()
        kupatana_count = self.db.query(func.count(RealEstateListing.url)).filter(
            RealEstateListing.target_site == 'kupatana'
        ).scalar()
        
        return {
            'total_listings': total,
            'jiji_listings': jiji_count,
            'kupatana_listings': kupatana_count,
            'last_updated': datetime.now().isoformat()
        }
    
    def search_listings(self, query: str, limit: int = 50) -> List[Dict]:
        """
        Search listings by title, location, or description
        
        Args:
            query: Search query string
            limit: Maximum results
            
        Returns:
            List of matching listings
        """
        search_term = f"%{query}%"
        
        listings = self.db.query(RealEstateListing).filter(
            or_(
                RealEstateListing.title.ilike(search_term),
                RealEstateListing.location.ilike(search_term),
                RealEstateListing.description.ilike(search_term)
            )
        ).limit(limit).all()
        
        return [listing.to_dict() for listing in listings]

