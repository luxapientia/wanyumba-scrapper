"""
Pydantic schemas for listings endpoints
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ListingBase(BaseModel):
    """Base listing schema"""
    rawUrl: str
    source: str
    sourceListingId: Optional[str] = None
    scrapeTimestamp: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    propertyType: Optional[str] = None
    listingType: Optional[str] = None
    status: Optional[str] = None
    price: Optional[float] = None
    priceCurrency: Optional[str] = None
    pricePeriod: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    addressText: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    livingAreaSqm: Optional[float] = None
    landAreaSqm: Optional[float] = None
    images: Optional[List[str]] = []
    agentName: Optional[str] = None
    agentPhone: Optional[str] = None
    agentWhatsapp: Optional[str] = None
    agentEmail: Optional[str] = None
    agentWebsite: Optional[str] = None
    agentProfileUrl: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None


class ListingDetail(ListingBase):
    """Detailed listing schema (same as base for now)"""
    pass


class ListingsResponse(BaseModel):
    """Response schema for listings list with pagination"""
    listings: List[ListingBase]
    total: int
    page: int
    limit: int
    totalPages: int


class StatisticsResponse(BaseModel):
    """Response schema for statistics"""
    total_listings: int
    jiji_listings: int
    kupatana_listings: int
    last_updated: str

