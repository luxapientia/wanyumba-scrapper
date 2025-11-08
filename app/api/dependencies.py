"""
Shared API dependencies
"""
from fastapi import Header, HTTPException, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from typing import Optional


async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """
    Verify API key (optional, for future authentication)
    
    Currently disabled for development.
    Uncomment and configure when ready for production.
    """
    # if not x_api_key:
    #     raise HTTPException(status_code=401, detail="API key required")
    # if x_api_key != settings.SECRET_KEY:
    #     raise HTTPException(status_code=403, detail="Invalid API key")
    pass


def get_database_service(db: Session = Depends(get_db)):
    """Get database service instance"""
    from app.services.database_service import DatabaseService
    return DatabaseService(db)

