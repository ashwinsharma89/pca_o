"""
KG-RAG API Models

Pydantic models for KG-RAG API requests and responses.
"""

from typing import Dict, Any, List, Optional
from datetime import date
from pydantic import BaseModel, Field


class KGQueryRequest(BaseModel):
    """Request model for KG-RAG query."""
    
    query: str = Field(
        ...,
        description="Natural language query",
        min_length=3,
        max_length=1000,
        examples=["Compare Search vs Social ROAS", "Top 10 campaigns by spend"]
    )
    
    date_from: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)",
        examples=["2025-01-01"]
    )
    
    date_to: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)",
        examples=["2025-12-31"]
    )
    
    platform: Optional[str] = Field(
        None,
        description="Platform filter",
        examples=["meta", "google_ads"]
    )
    
    use_llm: bool = Field(
        False,
        description="Force LLM-based query generation"
    )
    
    limit: int = Field(
        20,
        ge=1,
        le=1000,
        description="Maximum results to return"
    )


class KGQueryResult(BaseModel):
    """Single query result row."""
    
    data: Dict[str, Any]


class KGQuerySummary(BaseModel):
    """Summary statistics for query results."""
    
    count: int
    total_spend: Optional[float] = None
    total_impressions: Optional[int] = None
    total_clicks: Optional[int] = None
    total_conversions: Optional[float] = None
    total_revenue: Optional[float] = None
    avg_ctr: Optional[float] = None
    avg_cpc: Optional[float] = None
    avg_roas: Optional[float] = None


class KGQueryMetadata(BaseModel):
    """Query execution metadata."""
    
    query: str
    intent: str
    confidence: float
    routing: str  # "template" or "llm"
    cypher: Optional[str] = None
    execution_time_ms: Optional[float] = None
    template_name: Optional[str] = None


class KGQueryResponse(BaseModel):
    """Response model for KG-RAG query."""
    
    success: bool
    data: List[Dict[str, Any]] = []
    summary: Optional[KGQuerySummary] = None
    metadata: KGQueryMetadata
    error: Optional[str] = None


class KGSchemaResponse(BaseModel):
    """Response with graph schema information."""
    
    nodes: List[Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    platforms: List[str]
    stats: Dict[str, Any]
    error: Optional[str] = None


class KGHealthResponse(BaseModel):
    """Health check response."""
    
    status: str
    graph_db_connected: bool
    db_path: str
    node_count: Optional[int] = None
    relationship_count: Optional[int] = None


class CypherExecuteRequest(BaseModel):
    """Request to execute raw Cypher (admin only)."""
    
    cypher: str = Field(
        ...,
        description="Cypher query to execute",
        min_length=10
    )
    
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Query parameters"
    )


class CypherExecuteResponse(BaseModel):
    """Response from raw Cypher execution."""
    
    success: bool
    results: List[Dict[str, Any]] = []
    count: int = 0
    execution_time_ms: Optional[float] = None
    error: Optional[str] = None


class TemplateListResponse(BaseModel):
    """List of available query templates."""
    
    templates: List[Dict[str, Any]]
    total: int
