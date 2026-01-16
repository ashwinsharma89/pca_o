"""
Analysis Router - AI-powered campaign analysis endpoints

Handles analysis operations:
- AI-powered global campaign analysis
- Auto-Analysis with RAG-enhanced summaries
"""

from fastapi import APIRouter, HTTPException, Depends, Request, Response
from typing import Dict, Any
import logging
import pandas as pd
import numpy as np

from src.interface.api.middleware.auth import get_current_user
from src.interface.api.middleware.rate_limit import limiter
from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.database.repositories import AnalysisRepository
from src.core.utils.column_mapping import find_column
from src.engine.analytics.auto_insights import MediaAnalyticsExpert
from src.interface.api.v1.models import GlobalAnalysisRequest
from src.platform.knowledge.vector_store import HybridRetriever, VectorStoreConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["analysis"])


@router.post("/analyze/global")
@limiter.limit("5/minute")
async def analyze_global_campaigns(
    request: Request,
    analysis_req: GlobalAnalysisRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    response: Response = None
):
    """
    Perform deep AI analysis on ALL campaign data (Auto Analysis).
    
    Config options:
        - use_rag_summary: bool (default True)
        - include_benchmarks: bool (default True)
        - analysis_depth: str ('Quick'|'Standard'|'Deep', default 'Standard')
        - include_recommendations: bool (default True)
    """
    if response:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    try:
        use_rag = analysis_req.use_rag_summary
        include_recommendations = analysis_req.include_recommendations
        analysis_depth = analysis_req.analysis_depth or "Standard"
        include_benchmarks = getattr(analysis_req, 'include_benchmarks', True)
        
        logger.info(f"Analysis config: RAG={use_rag}, Benchmarks={include_benchmarks}, Depth={analysis_depth}")
        
        # RAG Context Retrieval
        enrichment_context = ""
        if use_rag:
            try:
                # Use campaign objective as query, default if missing
                query = getattr(analysis_req, 'campaign_objective', None) or "Campaign performance analysis and optimization strategy"
                
                logger.info(f"Retrieving context for query: {query}")
                
                # Initialize retriever (will load existing indices)
                retriever = HybridRetriever(
                    config=VectorStoreConfig(),
                    use_keyword=True,
                    use_rerank=True 
                )
                
                # Fetch top 3 relevant chunks
                results = retriever.search(query=query, top_k=3)
                
                if results:
                    # Format context for LLM
                    context_parts = []
                    for i, res in enumerate(results):
                        source = res.get('metadata', {}).get('source', 'Unknown Source')
                        text = res.get('text', '').strip()
                        context_parts.append(f"[Source: {source}]\n{text}")
                    
                    enrichment_context = "\n\n".join(context_parts)
                    logger.info(f"Retrieved {len(results)} context chunks. Length: {len(enrichment_context)} chars")
                else:
                    logger.warning("No context retrieved from knowledge base.")
                    
            except Exception as e:
                # Fail open - don't block analysis if RAG fails
                logger.error(f"RAG Retrieval failed: {e}")
                enrichment_context = ""
        
        duckdb_mgr = get_duckdb_manager()
        duckdb_mgr = get_duckdb_manager()
        
        # Use Polars for faster data retrieval and initial processing
        try:
            df = duckdb_mgr.get_campaigns_polars()
            is_empty = df.height == 0 if hasattr(df, 'height') else True
        except Exception as e:
            logger.warning(f"Polars fetch failed, falling back to Pandas: {e}")
            df = duckdb_mgr.get_campaigns()
            is_empty = df.empty

        if is_empty:
            return {
                "insights": {"performance_summary": {}, "pattern_insights": ["No data available for analysis."]},
                "recommendations": []
            }
            
        # Note: Column normalization and type safety are now handled within get_campaigns_polars
        # and analyze_all handles the Polars->Pandas conversion internally where needed.
            
        reasoning_agent = MediaAnalyticsExpert()
        
        try:
            analysis_result = reasoning_agent.analyze_all(
                df,
                use_rag_summary=use_rag,
                enrichment_context=enrichment_context,
                campaign_objective=getattr(analysis_req, 'campaign_objective', 'Maximize ROI')
            )
        except Exception as e:
            import traceback
            logger.error(f"analyze_all failed: {e}\n{traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Analysis Engine Failed: {str(e)}")
        

        # Generate RAG-enhanced summary
        logger.info(f"Checking RAG eligibility: use_rag={use_rag}")
        if use_rag:
            try:
                logger.info("Calling generate_executive_summary_with_rag...")
                rag_summary = reasoning_agent.generate_executive_summary_with_rag(
                    analysis_result,
                    campaign_objective=analysis_req.campaign_objective,
                    custom_time_period=analysis_req.time_period,
                    enrichment_context=enrichment_context  # Pass locally retrieved context
                )
                logger.info(f"RAG Summary returned type: {type(rag_summary)}")
                if rag_summary:
                    analysis_result['executive_summary'] = rag_summary
                    logger.info("Successfully attached RAG summary to analysis result.")
                else:
                    logger.error("RAG Summary returned None/Empty")
            except Exception as e:
                import traceback
                logger.error(f"RAG summary generation failed: {e}\n{traceback.format_exc()}")

        
        # Format insights and recommendations
        if 'insights' in analysis_result and isinstance(analysis_result['insights'], list):
            analysis_result['insights'] = [
                item['insight'] if isinstance(item, dict) and 'insight' in item else str(item)
                for item in analysis_result['insights']
            ]

        if 'recommendations' in analysis_result and isinstance(analysis_result['recommendations'], list):
            analysis_result['recommendations'] = [
                item.get('recommendation', item.get('rationale', str(item))) if isinstance(item, dict) else str(item)
                for item in analysis_result['recommendations']
            ]
            
        if not include_recommendations:
            analysis_result['recommendations'] = []
            
        # Clean and serialize
        from fastapi.encoders import jsonable_encoder
        import json
        
        def clean_nan_values(data):
            if isinstance(data, dict):
                return {k: clean_nan_values(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [clean_nan_values(item) for item in data]
            elif isinstance(data, np.ndarray):
                return clean_nan_values(data.tolist())
            elif isinstance(data, (float, np.floating)):
                return None if np.isnan(data) or np.isinf(data) else float(data)
            elif isinstance(data, (int, np.integer)):
                return int(data)
            elif isinstance(data, np.bool_):
                return bool(data)
            elif isinstance(data, (np.dtype, type)):
                return str(data)
            elif hasattr(data, 'item'):
                try:
                    return clean_nan_values(data.item())
                except:
                    return str(data)
            return data

        try:
            cleaned_result = clean_nan_values(analysis_result)
            return jsonable_encoder(cleaned_result, custom_encoder={
                pd.Timestamp: lambda dt: dt.isoformat(),
                pd.Period: lambda p: str(p),
                np.integer: lambda i: int(i),
                np.floating: lambda f: None if np.isnan(f) or np.isinf(f) else float(f)
            })
        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            def custom_serializer(obj):
                if hasattr(obj, 'isoformat'): return obj.isoformat()
                if isinstance(obj, set): return list(obj)
                return str(obj)
            return json.loads(json.dumps(analysis_result, default=custom_serializer))
        
    except Exception as e:
        logger.error(f"Global analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/funnel-stats")
async def get_funnel_stats(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get aggregated funnel stage performance data using DuckDB."""
    try:
        duckdb_mgr = get_duckdb_manager()
        
        if not duckdb_mgr.has_data():
            return {"data": [], "count": 0}
        
        df = duckdb_mgr.get_aggregated_data(group_by="Funnel")
        
        if df.empty:
            return {"data": [], "count": 0}
        
        stage_order = {'Upper': 1, 'Middle': 2, 'Lower': 3, 'TOFU': 1, 'MOFU': 2, 'BOFU': 3}
        result = [
            {
                'stage': str(row['name']), 'spend': float(row['spend']),
                'impressions': int(row['impressions']), 'clicks': int(row['clicks']),
                'conversions': int(row['conversions']), 'ctr': float(row['ctr']),
                'cpc': float(row['cpc']), 'cpa': float(row['cpa'])
            }
            for _, row in df.iterrows()
        ]
        
        result.sort(key=lambda x: stage_order.get(x['stage'], 999))
        return {"data": result, "count": len(result)}
        
    except Exception as e:
        logger.error(f"Failed to get funnel stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/audience-stats")
async def get_audience_stats(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get aggregated audience segment performance data using DuckDB."""
    try:
        duckdb_mgr = get_duckdb_manager()
        
        if not duckdb_mgr.has_data():
            return {"data": [], "count": 0}
        
        df = duckdb_mgr.get_aggregated_data(group_by="Audience_Segment")
        
        if df.empty:
            return {"data": [], "count": 0}
        
        result = [
            {
                'name': str(row['name']), 'spend': float(row['spend']),
                'impressions': int(row['impressions']), 'clicks': int(row['clicks']),
                'conversions': int(row['conversions']), 'ctr': float(row['ctr']),
                'cvr': round((row['conversions'] / row['clicks'] * 100) if row['clicks'] > 0 else 0, 2),
                'cpa': float(row['cpa'])
            }
            for _, row in df.iterrows()
        ]
        
        result.sort(key=lambda x: x['spend'], reverse=True)
        return {"data": result, "count": len(result)}
        
    except Exception as e:
        logger.error(f"Failed to get audience stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
