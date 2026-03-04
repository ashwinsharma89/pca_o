"""
KG-RAG Settings Configuration

Manages KùzuDB path, feature flags, and module configuration.
KùzuDB is an embedded graph database — no server URI or credentials required.
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class KGRAGSettings(BaseSettings):
    """Configuration settings for KG-RAG module."""

    # =====================
    # KùzuDB Storage
    # =====================
    kuzu_db_path: str = Field(
        default="./kuzu_db",
        description="Path to the KùzuDB database directory (created if absent)"
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
        description="Query execution timeout in seconds"
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
        description="Batch size for UNWIND operations"
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
        extra = "ignore"


# Singleton instance
_settings: Optional[KGRAGSettings] = None


def get_kg_rag_settings() -> KGRAGSettings:
    """Get or create KG-RAG settings singleton."""
    global _settings
    if _settings is None:
        _settings = KGRAGSettings()
    return _settings
