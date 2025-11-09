"""
SQLAlchemy model for real estate listings
"""
from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, Float, DateTime, ARRAY
from app.core.database import Base


class RealEstateListing(Base):
    """
    Model for storing real estate listing data
    """
    __tablename__ = "real_estate_listings"

    # Primary key - URL is unique identifier
    raw_url = Column(String(500), primary_key=True, index=True)

    # Source information
    # 'jiji', 'kupatana', etc.
    source = Column(String(50), nullable=False, index=True)
    source_listing_id = Column(String(100), nullable=True, index=True)
    scrape_timestamp = Column(DateTime(timezone=True), nullable=True)

    # Basic information
    title = Column(Text, nullable=True)
    description = Column(Text, nullable=True)

    # Property classification
    # 'apartment', 'house', 'land', 'commercial', 'other'
    property_type = Column(String(50), nullable=True, index=True)
    listing_type = Column(String(50), nullable=True,
                          index=True)  # 'rent', 'sale'
    # 'active', 'inactive', 'unknown'
    status = Column(String(20), nullable=True, index=True)

    # Pricing
    price = Column(Float, nullable=True)  # Numeric price value
    price_currency = Column(String(10), nullable=True)  # 'TZS', 'USD', etc.
    # 'once', 'month', 'year', etc.
    price_period = Column(String(20), nullable=True)

    # Location (structured)
    country = Column(String(100), nullable=True, index=True)
    region = Column(String(100), nullable=True, index=True)
    city = Column(String(100), nullable=True, index=True)
    district = Column(String(100), nullable=True, index=True)
    address_text = Column(Text, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    # Property details
    bedrooms = Column(Integer, nullable=True, index=True)
    bathrooms = Column(Integer, nullable=True)
    # Living area in square meters
    living_area_sqm = Column(Float, nullable=True)
    land_area_sqm = Column(Float, nullable=True)  # Land area in square meters

    # Images
    images = Column(ARRAY(String), nullable=True)  # Array of image URLs

    # Agent/Contact information
    agent_name = Column(String(200), nullable=True)
    agent_phone = Column(String(50), nullable=True)
    agent_whatsapp = Column(String(50), nullable=True)
    agent_email = Column(String(200), nullable=True)
    agent_website = Column(String(500), nullable=True)
    agent_profile_url = Column(String(500), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<RealEstateListing(raw_url='{self.raw_url}', title='{self.title}', source='{self.source}')>"

    def to_dict(self, include_details=True):
        """
        Convert model to dictionary

        Args:
            include_details: If False, only return raw_url, title, price (lightweight)
        """
        base_dict = {
            'rawUrl': self.raw_url,
            'title': self.title,
            'price': self.price,
            'priceCurrency': self.price_currency,
        }

        if not include_details:
            return base_dict

        # Full details
        return {
            **base_dict,
            'source': self.source,
            'sourceListingId': self.source_listing_id,
            'scrapeTimestamp': self.scrape_timestamp.isoformat() if self.scrape_timestamp else None,
            'description': self.description,
            'propertyType': self.property_type,
            'listingType': self.listing_type,
            'status': self.status,
            'pricePeriod': self.price_period,
            'country': self.country,
            'region': self.region,
            'city': self.city,
            'district': self.district,
            'addressText': self.address_text,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'bedrooms': self.bedrooms,
            'bathrooms': self.bathrooms,
            'livingAreaSqm': self.living_area_sqm,
            'landAreaSqm': self.land_area_sqm,
            'images': self.images or [],
            'agentName': self.agent_name,
            'agentPhone': self.agent_phone,
            'agentWhatsapp': self.agent_whatsapp,
            'agentEmail': self.agent_email,
            'agentWebsite': self.agent_website,
            'agentProfileUrl': self.agent_profile_url,
            'createdAt': self.created_at.isoformat() if self.created_at else None,
            'updatedAt': self.updated_at.isoformat() if self.updated_at else None,
        }
