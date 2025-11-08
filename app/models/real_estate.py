"""
SQLAlchemy model for real estate listings
"""
from sqlalchemy import Column, String, Text, Integer, Float, DateTime, JSON, ARRAY
from sqlalchemy.sql import func
from app.core.database import Base
from datetime import datetime


class RealEstateListing(Base):
    """
    Model for storing real estate listing data
    """
    __tablename__ = "real_estate_listings"
    
    # Primary key - URL is unique identifier
    url = Column(String(500), primary_key=True, index=True)
    
    # Source website
    target_site = Column(String(50), nullable=False, index=True)  # 'jiji', 'kupatana', etc.
    
    # Basic information
    title = Column(Text, nullable=True)
    price = Column(Float, nullable=True)  # Numeric price value
    currency = Column(String(10), nullable=True)  # 'TSh', 'USD', etc.
    location = Column(Text, nullable=True, index=True)
    description = Column(Text, nullable=True)
    
    # Property details (extracted from attributes for easy querying)
    bedrooms = Column(Integer, nullable=True, index=True)
    bathrooms = Column(Integer, nullable=True)
    parking_space = Column(Integer, nullable=True)  # Number of parking spaces
    property_size = Column(Float, nullable=True)
    property_size_unit = Column(String(20), nullable=True)  # 'sqm', 'sqft', etc.
    property_type = Column(String(50), nullable=True, index=True)  # 'house', 'apartment', etc.
    listing_type = Column(String(50), nullable=True, index=True)  # 'rent', 'sale', 'lease', etc.
    
    # JSON/Array fields
    attributes = Column(JSON, nullable=True)                      # Additional property details
    images = Column(ARRAY(String), nullable=True)                 # Array of image URLs
    facilities = Column(ARRAY(String), nullable=True)             # Array of facilities/amenities
    
    # Contact information
    contact_name = Column(String(200), nullable=True)
    contact_phone = Column(ARRAY(String), nullable=True)          # Array of phone numbers
    contact_email = Column(ARRAY(String), nullable=True)          # Array of email addresses
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<RealEstateListing(url='{self.url}', title='{self.title}', target_site='{self.target_site}')>"
    
    def to_dict(self, include_details=True):
        """
        Convert model to dictionary
        
        Args:
            include_details: If False, only return url, title, price (lightweight)
        """
        base_dict = {
            'url': self.url,
            'title': self.title,
            'price': self.price,
            'currency': self.currency,
        }
        
        if not include_details:
            return base_dict
        
        # Full details
        return {
            **base_dict,
            'targetSite': self.target_site,
            'location': self.location,
            'description': self.description,
            'bedrooms': self.bedrooms,
            'bathrooms': self.bathrooms,
            'parkingSpace': self.parking_space,
            'propertySize': self.property_size,
            'propertySizeUnit': self.property_size_unit,
            'propertyType': self.property_type,
            'listingType': self.listing_type,
            'attributes': self.attributes or {},
            'images': self.images or [],
            'facilities': self.facilities or [],
            'contactName': self.contact_name,
            'contactPhone': self.contact_phone or [],
            'contactEmail': self.contact_email or [],
            'createdAt': self.created_at.isoformat() if self.created_at else None,
            'updatedAt': self.updated_at.isoformat() if self.updated_at else None,
        }
    
    @classmethod
    def from_scraper_data(cls, data: dict, target_site: str):
        """
        Create model instance from scraper data
        
        Args:
            data: Dictionary from jiji_scraper or kupatana_scraper
            target_site: 'jiji', 'kupatana', etc.
        """
        # Extract price and currency
        # Scraper now provides price as numeric value and currency as separate field
        price = data.get('price')
        currency = data.get('currency')
        
        # Backward compatibility: if price is still a string, parse it
        if isinstance(price, str):
            price_str = price
            price = None
            currency = None
            
            # Try to extract currency from price string
            if price_str and price_str != 'N/A':
                if 'TSh' in price_str or 'TZS' in price_str:
                    currency = 'TSh'
                    price_str = price_str.replace('TSh', '').replace('TZS', '').strip()
                elif 'USD' in price_str or '$' in price_str:
                    currency = 'USD'
                    price_str = price_str.replace('USD', '').replace('$', '').strip()
                
                # Try to parse numeric value
                try:
                    price_cleaned = price_str.replace(',', '').replace(' ', '').strip()
                    if price_cleaned:
                        price = float(price_cleaned)
                except:
                    pass
        
        # Extract bedrooms and bathrooms - prioritize direct fields from scraper
        attributes = data.get('attributes', {})
        
        # Bedrooms - check direct field first, then attributes
        bedrooms = data.get('bedrooms')
        if bedrooms is None and attributes:
            bedrooms_str = attributes.get('Bedrooms') or attributes.get('bedrooms')
            if bedrooms_str:
                try:
                    bedrooms = int(str(bedrooms_str).strip())
                except:
                    pass
        
        # Bathrooms - check direct field first, then attributes
        bathrooms = data.get('bathrooms')
        if bathrooms is None and attributes:
            bathrooms_str = attributes.get('Bathrooms') or attributes.get('bathrooms')
            if bathrooms_str:
                try:
                    bathrooms = int(str(bathrooms_str).strip())
                except:
                    pass
        
        # Parking space - check direct field first, then attributes
        parking_space = data.get('parking_space')
        if parking_space is None and attributes:
            parking_str = attributes.get('Parking Space') or attributes.get('parkingSpace') or attributes.get('Parking')
            if parking_str:
                try:
                    parking_space = int(str(parking_str).strip())
                except:
                    pass
        
        # Property type - check direct field first, then attributes
        property_type = data.get('property_type')
        if not property_type and attributes:
            property_type = attributes.get('Property Type') or attributes.get('propertyType')
        
        # Property size - extract from attributes
        property_size = None
        property_size_unit = None
        if attributes:
            size_str = attributes.get('Property Size') or attributes.get('propertySize') or attributes.get('Square Metres')
            if size_str:
                import re
                # Extract number and unit
                match = re.search(r'([\d.]+)\s*(\w+)?', str(size_str))
                if match:
                    try:
                        property_size = float(match.group(1))
                        property_size_unit = match.group(2) or 'sqm'
                    except:
                        pass
        
        # Extract listing type - prioritize direct field from scraper
        listing_type = data.get('listing_type')
        
        # Fallback: determine from title if not provided
        if not listing_type:
            title_lower = (data.get('title') or '').lower()
            if 'for rent' in title_lower or 'to rent' in title_lower or 'rent' in title_lower:
                listing_type = 'rent'
            elif 'for sale' in title_lower or 'to sell' in title_lower or 'sale' in title_lower:
                listing_type = 'sale'
            elif 'for lease' in title_lower or 'to lease' in title_lower or 'lease' in title_lower:
                listing_type = 'lease'
        
        # Extract contact info (support both old seller_info format and new direct format)
        contact_name = data.get('contact_name')
        contact_phone = data.get('contact_phone', [])
        
        # Backward compatibility: also check seller_info format
        if not contact_name or not contact_phone:
            seller_info = data.get('seller_info', {})
            if seller_info:
                contact_name = contact_name or seller_info.get('name')
                contact_phone = contact_phone or seller_info.get('phones', [])
        
        if isinstance(contact_phone, str):
            contact_phone = [contact_phone]
        
        # Extract facilities - prioritize direct field from scraper
        facilities = data.get('facilities', [])
        if not facilities and attributes:
            # Fallback to extracting from attributes if not provided as direct field
            facilities_str = attributes.get('Facilities') or attributes.get('facilities')
            if facilities_str:
                if isinstance(facilities_str, str):
                    facilities = [f.strip() for f in facilities_str.split(',')]
                elif isinstance(facilities_str, list):
                    facilities = facilities_str
        
        return cls(
            url=data.get('url'),
            target_site=target_site,
            title=data.get('title'),
            price=price,
            currency=currency,
            location=data.get('location'),
            description=data.get('description'),
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            parking_space=parking_space,
            property_size=property_size,
            property_size_unit=property_size_unit,
            property_type=property_type,
            listing_type=listing_type,
            attributes=attributes,
            images=data.get('images', []),
            facilities=facilities,
            contact_name=contact_name,
            contact_phone=contact_phone,
            contact_email=[]  # Email not typically available from scrapers
        )

