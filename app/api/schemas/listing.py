"""
Pydantic schemas for listing endpoints
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class ListingBasicResponse(BaseModel):
    """Basic listing response (lightweight)"""
    url: str
    title: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    target_site: str


class ListingResponse(BaseModel):
    """Full listing response with all details"""
    url: str
    target_site: str
    title: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    parking_space: Optional[int] = None
    property_size: Optional[float] = None
    property_size_unit: Optional[str] = None
    property_type: Optional[str] = None
    listing_type: Optional[str] = None
    
    attributes: Optional[Dict[str, Any]] = None
    images: Optional[List[str]] = None
    facilities: Optional[List[str]] = None
    
    contact_name: Optional[str] = None
    contact_phone: Optional[List[str]] = None
    contact_email: Optional[List[str]] = None
    
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ListingCreateRequest(BaseModel):
    """Request to create/update a listing"""
    target_site: str = Field(..., description="Site name (jiji, kupatana)")
    data: Dict[str, Any] = Field(..., description="Scraper data dictionary")


class ListingBulkCreateRequest(BaseModel):
    """Request to bulk create/update listings"""
    target_site: str = Field(..., description="Site name (jiji, kupatana)")
    data_list: List[Dict[str, Any]] = Field(..., description="List of scraper data dictionaries")


class StatisticsResponse(BaseModel):
    """Database statistics response"""
    total_listings: int
    jiji_listings: int
    kupatana_listings: int
    last_updated: str

