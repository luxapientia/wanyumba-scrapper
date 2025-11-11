"""
Listings endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from app.core.database import get_db
from app.services.database_service import DatabaseService
from app.api.schemas.listings import ListingsResponse, ListingDetail, StatisticsResponse
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/property-types", response_model=List[str])
async def get_property_types(db: Session = Depends(get_db)):
    """
    Get all unique property types from the database
    
    Returns a sorted list of all property types that exist in the listings.
    """
    try:
        db_service = DatabaseService(db)
        property_types = db_service.get_unique_property_types()
        return property_types
    except Exception as e:
        logger.error(f"Error fetching property types: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch property types")


@router.get("/", response_model=ListingsResponse)
async def get_listings(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(25, ge=1, le=100, description="Items per page"),
    source: Optional[str] = Query(
        None, description="Filter by source (jiji, kupatana)"),
    search: Optional[str] = Query(
        None, description="Search in title, location, description"),
    sortBy: Optional[str] = Query("created_at", description="Sort by field"),
    sortOrder: Optional[str] = Query(
        "desc", description="Sort order (asc, desc)"),
    propertyType: Optional[str] = Query(
        None, description="Filter by property type"),
    listingType: Optional[str] = Query(
        None, description="Filter by listing type (rent, sale)"),
    minPrice: Optional[float] = Query(None, description="Minimum price"),
    maxPrice: Optional[float] = Query(None, description="Maximum price"),
    bedrooms: Optional[int] = Query(
        None, description="Filter by number of bedrooms"),
    city: Optional[str] = Query(None, description="Filter by city"),
    region: Optional[str] = Query(None, description="Filter by region"),
    phone: Optional[str] = Query(
        None, description="Filter by agent phone number"),
    db: Session = Depends(get_db)
):
    """
    Get listings with pagination, filtering, and sorting

    - **page**: Page number (starts from 1)
    - **limit**: Number of items per page (max 100)
    - **source**: Filter by source site (jiji, kupatana)
    - **search**: Search query for title, location, description
    - **sortBy**: Field to sort by (created_at, price, title, etc.)
    - **sortOrder**: Sort order (asc, desc)
    - **propertyType**: Filter by property type
    - **listingType**: Filter by listing type (rent, sale)
    - **minPrice**: Minimum price filter
    - **maxPrice**: Maximum price filter
    - **bedrooms**: Filter by number of bedrooms
    - **city**: Filter by city
    - **region**: Filter by region
    - **phone**: Filter by agent phone number (normalizes phone numbers for matching)
    """
    try:
        db_service = DatabaseService(db)

        # Build query
        from app.models.real_estate import RealEstateListing
        from sqlalchemy import or_, and_

        query = db.query(RealEstateListing)

        # Only fetch listings with agent_name (scraped in detail)
        query = query.filter(RealEstateListing.agent_name.isnot(None))

        # Apply filters
        if source and source != 'all':
            query = query.filter(RealEstateListing.source == source)

        if propertyType:
            query = query.filter(
                RealEstateListing.property_type == propertyType)

        if listingType:
            query = query.filter(RealEstateListing.listing_type == listingType)

        if minPrice is not None:
            query = query.filter(RealEstateListing.price >= minPrice)

        if maxPrice is not None:
            query = query.filter(RealEstateListing.price <= maxPrice)

        if bedrooms is not None:
            query = query.filter(RealEstateListing.bedrooms == bedrooms)

        if city:
            query = query.filter(RealEstateListing.city.ilike(f"%{city}%"))

        if region:
            query = query.filter(RealEstateListing.region.ilike(f"%{region}%"))

        if phone:
            # Normalize phone number for matching (remove spaces, dashes, parentheses)
            import re
            normalized_phone = re.sub(r'[\s\-\(\)]', '', phone).strip()
            
            # Filter by agent_phone with normalized matching
            # Try multiple patterns to catch different phone number formats
            phone_patterns = [
                normalized_phone,  # Exact normalized match
                phone,  # Original format
                f"%{normalized_phone}%",  # Contains normalized
                f"%{phone}%",  # Contains original
            ]
            
            # Create OR conditions for all patterns
            phone_conditions = [
                RealEstateListing.agent_phone.ilike(pattern) for pattern in phone_patterns
            ]
            
            # Filter by agent_phone matching any of the patterns
            query = query.filter(or_(*phone_conditions))

        if search:
            search_term = f"%{search}%"
            query = query.filter(
                or_(
                    RealEstateListing.title.ilike(search_term),
                    RealEstateListing.description.ilike(search_term),
                    RealEstateListing.address_text.ilike(search_term),
                    RealEstateListing.city.ilike(search_term),
                    RealEstateListing.district.ilike(search_term),
                    RealEstateListing.region.ilike(search_term),
                )
            )

        # Get total count before pagination
        total = query.count()

        # Apply sorting
        sort_field = getattr(RealEstateListing, sortBy,
                             RealEstateListing.created_at)
        if sortOrder == 'desc':
            query = query.order_by(sort_field.desc())
        else:
            query = query.order_by(sort_field.asc())

        # Apply pagination
        offset = (page - 1) * limit
        listings = query.offset(offset).limit(limit).all()

        # Calculate total pages
        total_pages = (total + limit - 1) // limit

        return {
            "listings": [listing.to_dict(include_details=True) for listing in listings],
            "total": total,
            "page": page,
            "limit": limit,
            "pages": total_pages
        }
    except Exception as e:
        logger.error(f"Error fetching listings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics", response_model=StatisticsResponse)
async def get_statistics(db: Session = Depends(get_db)):
    """
    Get database statistics

    Returns counts of total listings, jiji listings, and kupatana listings
    """
    try:
        db_service = DatabaseService(db)
        stats = db_service.get_statistics()
        return stats
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_listings(
    q: str = Query(..., description="Search query"),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db)
):
    """
    Search listings by query

    - **q**: Search query (searches in title, location, description)
    - **limit**: Maximum number of results (default 50, max 100)
    """
    try:
        db_service = DatabaseService(db)
        results = db_service.search_listings(q, limit)
        return results
    except Exception as e:
        logger.error(f"Error searching listings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{url:path}", response_model=ListingDetail)
async def get_listing(url: str, db: Session = Depends(get_db)):
    """
    Get a single listing by URL

    - **url**: The listing URL (raw_url from database)
    """
    try:
        db_service = DatabaseService(db)
        listing = db_service.get_listing_by_url(url)

        if not listing:
            raise HTTPException(status_code=404, detail="Listing not found")

        return listing
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching listing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{url:path}")
async def delete_listing(url: str, db: Session = Depends(get_db)):
    """
    Delete a listing by URL

    - **url**: The listing URL (raw_url from database)
    """
    try:
        db_service = DatabaseService(db)
        success = db_service.delete_listing(url)

        if not success:
            raise HTTPException(status_code=404, detail="Listing not found")

        return {"status": "success", "message": "Listing deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting listing: {e}")
        raise HTTPException(status_code=500, detail=str(e))
