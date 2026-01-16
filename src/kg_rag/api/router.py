"""
KG-RAG API Router

FastAPI router for Knowledge Graph RAG queries.
"""

import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.kg_rag.api.models import (
    KGQueryRequest,
    KGQueryResponse,
    KGQueryMetadata,
    KGQuerySummary,
    KGSchemaResponse,
    KGHealthResponse,
    CypherExecuteRequest,
    CypherExecuteResponse,
    TemplateListResponse,
)
from src.kg_rag.query.query_router import QueryRouter
from src.kg_rag.query.intent_classifier import IntentClassifier
from src.kg_rag.query.result_formatter import ResultFormatter
from src.kg_rag.query.cypher_generator import CypherGenerator
from src.kg_rag.context.context_builder import ContextBuilder
from src.kg_rag.client.connection import get_neo4j_connection
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/kg", tags=["Knowledge Graph"])


def get_router() -> QueryRouter:
    """Get query router instance."""
    return QueryRouter()


def get_formatter() -> ResultFormatter:
    """Get result formatter instance."""
    return ResultFormatter()


@router.post("/query", response_model=KGQueryResponse)
async def query_knowledge_graph(request: KGQueryRequest) -> KGQueryResponse:
    """
    Query the Knowledge Graph with natural language.
    
    The system will:
    1. Classify the query intent
    2. Route to a template (high confidence) or LLM (low confidence)
    3. Execute the Cypher query
    4. Format and return results
    """
    start_time = time.time()
    
    try:
        settings = get_kg_rag_settings()
        
        if not settings.kg_rag_enabled:
            raise HTTPException(status_code=503, detail="KG-RAG is disabled")
        
        # Build context
        context = {
            "date_from": request.date_from,
            "date_to": request.date_to,
            "platform": request.platform,
            "limit": request.limit,
        }
        
        # Route query
        query_router = get_router()
        
        if request.use_llm:
            # Force LLM generation
            generator = CypherGenerator()
            gen_result = generator.generate(request.query, context)
            
            if gen_result["success"]:
                # Execute generated Cypher
                conn = get_neo4j_connection()
                results = conn.execute_query(gen_result["cypher"])
                
                # Format
                formatter = get_formatter()
                formatted = formatter.format(results)
                
                execution_time = (time.time() - start_time) * 1000
                
                return KGQueryResponse(
                    success=True,
                    data=formatted.data,
                    summary=KGQuerySummary(**formatted.summary),
                    metadata=KGQueryMetadata(
                        query=request.query,
                        intent="llm_generated",
                        confidence=1.0,
                        routing="llm",
                        cypher=gen_result["cypher"],
                        execution_time_ms=execution_time,
                    )
                )
            else:
                return KGQueryResponse(
                    success=False,
                    error=gen_result.get("error", "LLM generation failed"),
                    metadata=KGQueryMetadata(
                        query=request.query,
                        intent="unknown",
                        confidence=0,
                        routing="llm",
                    )
                )
        else:
            # Use standard routing
            result = query_router.route(request.query, context)
            
            execution_time = (time.time() - start_time) * 1000
            
            if result["success"]:
                # Format results
                formatter = get_formatter()
                formatted = formatter.format(result.get("results", []))
                
                return KGQueryResponse(
                    success=True,
                    data=formatted.data,
                    summary=KGQuerySummary(**formatted.summary),
                    metadata=KGQueryMetadata(
                        query=request.query,
                        intent=result.get("intent", "unknown"),
                        confidence=result.get("confidence", 0),
                        routing=result.get("routing", "template"),
                        cypher=result.get("cypher"),
                        execution_time_ms=execution_time,
                    )
                )
            else:
                return KGQueryResponse(
                    success=False,
                    error=result.get("error"),
                    metadata=KGQueryMetadata(
                        query=request.query,
                        intent=result.get("intent", "unknown"),
                        confidence=result.get("confidence", 0),
                        routing=result.get("routing", "unknown"),
                    )
                )
    
    except Exception as e:
        logger.error(f"KG query failed: {e}")
        return KGQueryResponse(
            success=False,
            error=str(e),
            metadata=KGQueryMetadata(
                query=request.query,
                intent="error",
                confidence=0,
                routing="error",
            )
        )


@router.get("/health", response_model=KGHealthResponse)
async def kg_health_check() -> KGHealthResponse:
    """Check Knowledge Graph health."""
    try:
        conn = get_neo4j_connection()
        health = conn.health_check()
        
        # Get counts
        node_count = None
        rel_count = None
        
        if health["connected"]:
            try:
                counts = conn.execute_query("""
                    MATCH (n) WITH count(n) as nodes
                    MATCH ()-[r]->() WITH nodes, count(r) as rels
                    RETURN nodes, rels
                """)
                if counts:
                    node_count = counts[0].get("nodes")
                    rel_count = counts[0].get("rels")
            except Exception:
                pass
        
        return KGHealthResponse(
            status=health["status"],
            neo4j_connected=health["connected"],
            neo4j_uri=health.get("uri", "unknown"),
            node_count=node_count,
            relationship_count=rel_count,
        )
    
    except Exception as e:
        return KGHealthResponse(
            status="error",
            neo4j_connected=False,
            neo4j_uri="unknown",
        )


@router.get("/summary")
async def get_performance_summary():
    """
    Get comprehensive auto-generated performance summary.
    
    Returns breakdowns by Platform, Channel, Funnel, Device, Age
    plus data-driven insights (What Worked, What Didn't, Optimizations).
    """
    try:
        from src.kg_rag.query.summary_service import SummaryService
        service = SummaryService()
        return service.generate_summary()
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema", response_model=KGSchemaResponse)
async def get_schema() -> KGSchemaResponse:
    """Get Knowledge Graph schema information."""
    try:
        builder = ContextBuilder()
        
        nodes = builder._get_node_info()
        rels = builder._get_relationship_info()
        platforms = builder._get_sample_platforms()
        stats = builder._get_general_stats()
        
        return KGSchemaResponse(
            nodes=nodes,
            relationships=rels,
            platforms=platforms,
            stats=stats,
        )
    
    except Exception as e:
        logger.error(f"Failed to get schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=TemplateListResponse)
async def list_templates() -> TemplateListResponse:
    """List available query templates."""
    query_router = get_router()
    templates = query_router.get_available_templates()
    
    return TemplateListResponse(
        templates=templates,
        total=len(templates),
    )


@router.post("/execute", response_model=CypherExecuteResponse)
async def execute_cypher(request: CypherExecuteRequest) -> CypherExecuteResponse:
    """
    Execute raw Cypher query (for advanced users).
    
    WARNING: Only read queries are allowed.
    """
    start_time = time.time()
    
    # Security check - no write operations
    cypher_upper = request.cypher.upper()
    dangerous = ["CREATE", "DELETE", "REMOVE", "DROP", "MERGE", "SET"]
    
    for keyword in dangerous:
        if keyword in cypher_upper:
            return CypherExecuteResponse(
                success=False,
                error=f"Write operation not allowed: {keyword}",
            )
    
    try:
        conn = get_neo4j_connection()
        results = conn.execute_query(request.cypher, request.params)
        
        execution_time = (time.time() - start_time) * 1000
        
        return CypherExecuteResponse(
            success=True,
            results=results,
            count=len(results),
            execution_time_ms=execution_time,
        )
    
    except Exception as e:
        return CypherExecuteResponse(
            success=False,
            error=str(e),
        )


@router.get("/context")
async def get_query_context(
    query: str = Query(..., description="Natural language query")
) -> dict:
    """Get the context that would be used for a query."""
    builder = ContextBuilder()
    
    schema_context = builder.build_schema_context()
    query_context = builder.build_query_context(query)
    compact_context = builder.build_compact_context()
    
    # Classify query
    classifier = IntentClassifier()
    match = classifier.classify(query)
    
    return {
        "query": query,
        "intent": match.intent.value,
        "confidence": match.confidence,
        "entities": match.entities,
        "schema_context_length": len(schema_context),
        "query_context_length": len(query_context),
        "compact_context": compact_context,
    }
