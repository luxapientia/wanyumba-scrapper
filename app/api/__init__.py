"""
API package initialization
"""
from fastapi import APIRouter
from app.api.routes import scraping, websocket, listings

# Create main API router
api_router = APIRouter()

# Include all route modules
api_router.include_router(
    scraping.router, prefix="/scraping", tags=["Scraping"])
api_router.include_router(
    listings.router, prefix="/listings", tags=["Listings"])
api_router.include_router(websocket.router, prefix="/ws", tags=["WebSocket"])
