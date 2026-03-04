"""
KG-RAG API Router

FastAPI routes for knowledge graph operations.
"""

import logging
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException

from src.kg_rag.client.connection import get_kuzu_connection
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kg_rag", tags=["kg_rag"])


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """Check KG-RAG health status."""
    try:
        conn = get_kuzu_connection()
        is_connected = conn.is_connected()

        return {
            "status": "healthy" if is_connected else "unhealthy",
            "graph_db_connected": is_connected,
            "db_path": get_kg_rag_settings().kuzu_db_path,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "graph_db_connected": False,
            "error": str(e),
        }


@router.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get knowledge graph statistics."""
    try:
        conn = get_kuzu_connection()
        stats = conn.execute_query("""
            MATCH (n) RETURN count(n) AS node_count
        """)
        
        rel_stats = conn.execute_query("""
            MATCH ()-[r]->() RETURN count(r) AS relationship_count
        """)

        return {
            "node_count": stats[0]["node_count"] if stats else 0,
            "relationship_count": rel_stats[0]["relationship_count"] if rel_stats else 0,
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def execute_query(query: str) -> Dict[str, Any]:
    """Execute a Cypher query."""
    try:
        conn = get_kuzu_connection()
        results = conn.execute_query(query)
        return {"results": results}
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
