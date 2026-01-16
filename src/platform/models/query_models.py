from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
import pandas as pd

class QueryRequest(BaseModel):
    """Strict contract for query requests."""
    question: str = Field(..., description="The natural language question to process")
    use_templates: bool = Field(True, description="Whether to try templates before LLM")
    knowledge_mode: bool = Field(False, description="Whether to use RAG knowledge base")
    user_context: Optional[Dict[str, Any]] = Field(None, description="Optional user-specific context")

class QueryResponse(BaseModel):
    """Strict contract for query responses."""
    success: bool
    answer: str
    sql: Optional[str] = None
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    fixes_applied: Optional[List[str]] = []
    
    class Config:
        arbitrary_types_allowed = True

class EvaluationResult(BaseModel):
    """Model for tracking evaluation outcomes."""
    scenario_id: str
    question: str
    pass_status: bool
    error_message: Optional[str] = None
    generated_sql: Optional[str] = None
    latency_ms: float
