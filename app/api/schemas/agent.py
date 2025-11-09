"""
Agent schemas for API responses
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class AgentBase(BaseModel):
    """Base agent schema"""
    id: int
    name: Optional[str]
    phone: str
    email: Optional[str]
    createdAt: Optional[str]
    updatedAt: Optional[str]

    class Config:
        from_attributes = True


class AgentDetail(AgentBase):
    """Detailed agent schema"""
    pass


class AgentsResponse(BaseModel):
    """Response schema for agents list"""
    agents: List[AgentBase]
    total: int
    page: int
    limit: int
    pages: int

