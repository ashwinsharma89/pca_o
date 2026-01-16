"""
KG-RAG Settings Configuration

Manages Neo4j connection settings, feature flags, and module configuration.
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class KGRAGSettings(BaseSettings):
    """Configuration settings for KG-RAG module."""
    
    # =====================
    # Neo4j Connection
    # =====================
    neo4j_uri: str = Field(
        default="bolt://127.0.0.1:7687",
        description="Neo4j Bolt protocol URI"
    )
    neo4j_user: str = Field(
        default="neo4j",
        description="Neo4j username"
    )
    neo4j_password: str = Field(
        default="pca_kg_rag_2026",
        description="Neo4j password"
    )
    neo4j_database: str = Field(
        default="neo4j",
        description="Neo4j database name"
    )
    
    # Connection Pool
    neo4j_max_connection_lifetime: int = Field(
        default=3600,
        description="Max connection lifetime in seconds"
    )
    neo4j_max_connection_pool_size: int = Field(
        default=50,
        description="Max connections in pool"
    )
    neo4j_connection_acquisition_timeout: int = Field(
        default=60,
        description="Timeout for acquiring connection"
    )
    
    # =====================
    # Feature Flags
    # =====================
    kg_rag_enabled: bool = Field(
        default=True,
        description="Enable KG-RAG query routing"
    )
    kg_rag_fallback_to_sql: bool = Field(
        default=True,
        description="Fallback to SQL if KG query fails"
    )
    kg_rag_use_llm_for_novel_queries: bool = Field(
        default=True,
        description="Use LLM for queries not matching templates"
    )
    
    # =====================
    # Query Settings
    # =====================
    query_timeout_seconds: int = Field(
        default=30,
        description="Query execution timeout"
    )
    max_results_per_query: int = Field(
        default=1000,
        description="Maximum rows returned per query"
    )
    template_match_threshold: float = Field(
        default=0.85,
        description="Confidence threshold for template matching"
    )
    
    # =====================
    # ETL Settings
    # =====================
    etl_batch_size: int = Field(
        default=1000,
        description="Batch size for Neo4j UNWIND operations"
    )
    etl_parallel_workers: int = Field(
        default=4,
        description="Number of parallel ETL workers"
    )
    
    # =====================
    # Context Settings
    # =====================
    max_context_tokens: int = Field(
        default=4000,
        description="Max tokens for LLM context"
    )
    max_sample_campaigns: int = Field(
        default=10,
        description="Max sample campaigns in context"
    )
    
    # =====================
    # Logging
    # =====================
    log_cypher_queries: bool = Field(
        default=True,
        description="Log generated Cypher queries"
    )
    log_query_latency: bool = Field(
        default=True,
        description="Log query execution latency"
    )
    
    class Config:
        env_prefix = "KG_RAG_"
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env


# Singleton instance
_settings: Optional[KGRAGSettings] = None


def get_kg_rag_settings() -> KGRAGSettings:
    """Get or create KG-RAG settings singleton."""
    global _settings
    if _settings is None:
        _settings = KGRAGSettings()
    return _settings
