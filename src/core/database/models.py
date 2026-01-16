"""
Database models for PostgreSQL persistence.
Uses SQLAlchemy ORM for database operations.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# Import user models to ensure they are registered with Base
from .user_models import User, PasswordResetToken


class QueryHistory(Base):
    """Query history for Q&A feature."""
    __tablename__ = 'query_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(255), unique=True, nullable=False, index=True)
    
    # Query details
    user_query = Column(Text, nullable=False)
    sql_query = Column(Text)
    query_type = Column(String(50))  # 'sql', 'rag', 'hybrid'
    
    # Results
    result_data = Column(JSON)
    result_summary = Column(Text)
    
    # Performance
    execution_time = Column(Float)
    tokens_used = Column(Integer)
    
    # Metadata
    status = Column(String(50), default='success')  # success, failed, timeout
    error_message = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_query_created', 'created_at'),
        Index('idx_query_status', 'status'),
    )
    
    def __repr__(self):
        return f"<QueryHistory(id={self.id}, query_id='{self.query_id}')>"


class LLMUsage(Base):
    """Track LLM API usage and costs."""
    __tablename__ = 'llm_usage'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # LLM details
    provider = Column(String(50), nullable=False)  # 'openai', 'anthropic', 'gemini'
    model = Column(String(100), nullable=False)
    
    # Usage metrics
    prompt_tokens = Column(Integer, default=0)
    completion_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)
    
    # Cost tracking
    cost = Column(Float, default=0.0)
    
    # Context
    operation = Column(String(100))  # 'auto_analysis', 'rag_summary', 'query', etc.
    request_id = Column(String(255), index=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_llm_provider_created', 'provider', 'created_at'),
        Index('idx_llm_operation_created', 'operation', 'created_at'),
    )
    
    def __repr__(self):
        return f"<LLMUsage(id={self.id}, provider='{self.provider}', model='{self.model}', tokens={self.total_tokens})>"




class Campaign(Base):
    """Campaign data model."""
    __tablename__ = 'campaigns'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(String(255), unique=True, nullable=False, index=True)
    campaign_name = Column(String(255), nullable=False)
    
    # Dimensions
    platform = Column(String(100), index=True)
    channel = Column(String(100))
    objective = Column(String(100))
    status = Column(String(50), default='active')
    funnel_stage = Column(String(50))
    audience = Column(String(255))
    creative_type = Column(String(100))
    placement = Column(String(100))
    
    # Metrics
    spend = Column(Float, default=0.0)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    conversions = Column(Integer, default=0)
    
    # Derived Metrics (stored for fast retrieval)
    ctr = Column(Float, default=0.0)
    cpc = Column(Float, default=0.0)
    cpa = Column(Float, default=0.0)
    roas = Column(Float, default=0.0)
    
    # Timing
    date = Column(DateTime, index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    # Metadata
    additional_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Composite indexes for common query patterns
    __table_args__ = (
        # Date-based queries (most common)
        Index('idx_campaign_date_platform', 'date', 'platform'),
        Index('idx_campaign_date_status', 'date', 'status'),
        
        # Platform and objective filtering
        Index('idx_campaign_platform_objective', 'platform', 'objective'),
        Index('idx_campaign_platform_status', 'platform', 'status'),
        
        # Time range queries
        Index('idx_campaign_start_end_date', 'start_date', 'end_date'),
        
        # Performance queries (spend-based sorting)
        Index('idx_campaign_date_spend', 'date', 'spend'),
        Index('idx_campaign_platform_spend', 'platform', 'spend'),
        
        # Multi-dimensional analysis
        Index('idx_campaign_platform_channel_date', 'platform', 'channel', 'date'),
        Index('idx_campaign_funnel_objective', 'funnel_stage', 'objective'),
    )
    
    # Relationships
    analyses = relationship("Analysis", back_populates="campaign", cascade="all, delete-orphan")
    context = relationship("CampaignContext", back_populates="campaign", uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Campaign(id={self.id}, name='{self.campaign_name}', platform='{self.platform}')>"


class Analysis(Base):
    """Campaign analysis results."""
    __tablename__ = 'analyses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_id = Column(String(255), unique=True, nullable=False, index=True)
    campaign_id = Column(Integer, ForeignKey('campaigns.id'), nullable=False, index=True)
    
    analysis_type = Column(String(50), nullable=False) # 'auto', 'rag', 'visual'
    
    # Results
    insights = Column(JSON)
    recommendations = Column(JSON)
    metrics = Column(JSON)
    executive_summary = Column(JSON)
    
    status = Column(String(50), default='pending')
    execution_time = Column(Float)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    completed_at = Column(DateTime)
    
    # Indexes for analysis queries
    __table_args__ = (
        Index('idx_analysis_campaign_type', 'campaign_id', 'analysis_type'),
        Index('idx_analysis_created_status', 'created_at', 'status'),
    )
    
    # Relationships
    campaign = relationship("Campaign", back_populates="analyses")


class CampaignContext(Base):
    """Contextual information for a campaign."""
    __tablename__ = 'campaign_contexts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(Integer, ForeignKey('campaigns.id'), unique=True, nullable=False)
    
    context_data = Column(JSON)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    campaign = relationship("Campaign", back_populates="context")
