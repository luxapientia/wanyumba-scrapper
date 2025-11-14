"""
Pydantic schemas for scraping endpoints
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class ScrapeAllRequest(BaseModel):
    """Request to scrape all listings from a site"""
    target_site: str = Field(...,
                             description="Site to scrape (jiji, kupatana)")
    max_pages: Optional[int] = Field(
        None, description="Maximum pages to scrape")
    headless: bool = Field(True, description="Run browser in headless mode")
    save_to_db: bool = Field(True, description="Save results to database")


class ScrapeSelectedRequest(BaseModel):
    """Request to scrape detailed data for selected URLs"""
    urls: List[str] = Field(..., description="List of URLs to scrape")
    target_site: str = Field(...,
                             description="Site to scrape (jiji, kupatana)")
    headless: bool = Field(True, description="Run browser in headless mode")
    save_to_db: bool = Field(True, description="Save results to database")


class ScrapeResponse(BaseModel):
    """Response for scraping operations"""
    status: str = Field(..., description="Status (started, completed, failed)")
    message: str
    target_site: str
    count: Optional[int] = None
    data: Optional[List[Dict[str, Any]]] = None


class ScrapeDetailedResponse(BaseModel):
    """Response for detailed scraping operations"""
    status: str = Field(..., description="Status (started, completed, failed)")
    message: str
    target_site: str
    urls_count: int
    success_count: Optional[int] = None
    data: Optional[List[Dict[str, Any]]] = None


class StopScrapingRequest(BaseModel):
    """Request to stop scraping operation"""
    target_site: str = Field(...,
                             description="Site to stop scraping (jiji, kupatana)")


class AutoCycleRequest(BaseModel):
    """Request to start automatic scraping cycle"""
    target_site: str = Field(...,
                             description="Site to scrape (jiji or kupatana)")
    max_pages: Optional[int] = Field(
        None, description="Maximum pages to scrape per cycle")
    cycle_delay_minutes: int = Field(
        30, description="Delay between cycles in minutes (default: 30)")
    headless: bool = Field(True, description="Run browser in headless mode")


class ScrapingStatusResponse(BaseModel):
    """Response for scraping status - dynamic dictionary with scraper names as keys"""
    model_config = {"extra": "allow"}  # Allow extra fields for dynamic scraper names
