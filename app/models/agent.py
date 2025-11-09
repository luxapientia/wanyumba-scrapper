"""
SQLAlchemy model for agents
"""
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime
from app.core.database import Base


class Agent(Base):
    """
    Model for storing agent/contact information
    """
    __tablename__ = "agents"

    # Primary key
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Agent information
    name = Column(String(200), nullable=True)
    phone = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(200), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Agent(id={self.id}, name='{self.name}', phone='{self.phone}')>"

    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'phone': self.phone,
            'email': self.email,
            'createdAt': self.created_at.isoformat() if self.created_at else None,
            'updatedAt': self.updated_at.isoformat() if self.updated_at else None,
        }

