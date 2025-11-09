"""
Agents endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from app.core.database import get_db
from app.services.database_service import DatabaseService
from app.api.schemas.agent import AgentsResponse, AgentDetail
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=AgentsResponse)
async def get_agents(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(25, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search in name, phone, email"),
    sortBy: Optional[str] = Query("created_at", description="Sort by field"),
    sortOrder: Optional[str] = Query("desc", description="Sort order (asc, desc)"),
    db: Session = Depends(get_db)
):
    """
    Get paginated list of agents with optional filters
    
    - **page**: Page number (default: 1)
    - **limit**: Items per page (default: 25, max: 100)
    - **search**: Search term for name, phone, or email
    - **sortBy**: Field to sort by (default: created_at)
    - **sortOrder**: Sort order - asc or desc (default: desc)
    """
    try:
        db_service = DatabaseService(db)
        
        result = db_service.get_agents(
            page=page,
            limit=limit,
            search=search,
            sort_by=sortBy,
            sort_order=sortOrder
        )
        
        return result
    except Exception as e:
        logger.error("Error fetching agents: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch agents")


@router.get("/{agent_id}", response_model=AgentDetail)
async def get_agent(
    agent_id: int,
    db: Session = Depends(get_db)
):
    """
    Get agent by ID
    
    - **agent_id**: Agent ID
    """
    try:
        db_service = DatabaseService(db)
        agent = db_service.get_agent_by_id(agent_id)
        
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        return agent.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching agent %s: %s", agent_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch agent")


@router.delete("/{agent_id}")
async def delete_agent(
    agent_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete agent by ID
    
    - **agent_id**: Agent ID
    """
    try:
        db_service = DatabaseService(db)
        success = db_service.delete_agent(agent_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Agent not found")
        
        return {"message": "Agent deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error deleting agent %s: %s", agent_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete agent")

