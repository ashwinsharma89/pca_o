"""
Chat Router - NL-to-SQL and RAG Knowledge Base endpoints

Handles chat operations:
- NL-to-SQL queries on campaign data
- RAG knowledge base for marketing insights
- Suggested questions
"""

from fastapi import APIRouter, HTTPException, Depends, Request
from typing import Dict, Any, Optional
import logging
import pandas as pd
import numpy as np
import math
import os

from src.interface.api.middleware.auth import get_current_user
from src.core.database.connection import get_db
from src.core.database.repositories import CampaignRepository, AnalysisRepository
from src.core.database.duckdb_manager import CAMPAIGNS_PATTERN, CAMPAIGNS_DIR
from src.interface.api.v1.models import ChatRequest
# from src.engine.agents.enhanced_reasoning_agent import PatternDetector

logger = logging.getLogger(__name__)

# Initialize NL-to-SQL query engine
query_engine = None

def get_query_engine():
    global query_engine
    if query_engine is None:
        from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
        query_engine = NaturalLanguageQueryEngine(api_key=os.getenv("OPENAI_API_KEY", "dummy"))
    return query_engine

router = APIRouter(prefix="/campaigns", tags=["chat"])


@router.get("/suggested-questions")
async def get_suggested_questions(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get suggested questions users can ask."""
    try:
        from src.platform.query_engine.query_templates import get_suggested_questions
        return {"suggestions": get_suggested_questions()}
    except Exception as e:
        logger.error(f"Failed to get suggested questions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chat")
async def chat_global(
    request: Request,
    chat_request: ChatRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Chat with ALL campaign data using RAG/NL-to-SQL.
    
    Supports two modes:
    - knowledge_mode=False (default): Use NL-to-SQL for data queries
    - knowledge_mode=True: Use RAG knowledge base for marketing insights
    
    When use_rag_context=True, RAG context is added to enhance SQL answers.
    """
    try:
        question = chat_request.question
        if not question or not question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        
        # Check for API Key if not in knowledge mode
        api_key = os.getenv("OPENAI_API_KEY", "dummy")
        if not chat_request.knowledge_mode and api_key == "dummy":
            logger.warning("OpenAI API key is missing. LLM queries will likely fail.")
        
        # 1. KNOWLEDGE MODE (RAG)
        if chat_request.knowledge_mode:
            return await _handle_knowledge_mode_query(question)
        
        # 2. DATA MODE
        from src.core.database.duckdb_manager import get_duckdb_manager
        db_manager = get_duckdb_manager()
        
        if not db_manager.has_data():
            return {"success": True, "answer": "No campaign data found. Please upload a dynamic data file first.", "sql": ""}
            
        logger.info(f"Chat processing question: {question}")
        
        # Load and verify data
        engine = get_query_engine()
        try:
            # nl_to_sql engine expects a path string. For partitioned reads, we pass the pattern.
            engine.load_parquet_data(str(CAMPAIGNS_PATTERN), table_name="all_campaigns")
        except Exception as load_err:
            logger.error(f"Failed to load data for chat: {load_err}")
            return {"success": False, "error": f"Failed to load data: {str(load_err)}"}
        
        # A. Try NL-to-SQL
        try:
            result = engine.ask(question)
        except Exception as ask_err:
            logger.error(f"NL-to-SQL crashed: {ask_err}")
            result = {"success": False, "error": str(ask_err)}
        
        # B. Template Fallback
        if not result.get('success'):
            logger.info("Attempting local template fallback...")
            try:
                from src.platform.query_engine.template_generator import load_schema_from_parquet, generate_templates_for_schema
                schema_columns = load_schema_from_parquet(str(CAMPAIGNS_PATTERN))
                if schema_columns:
                    dynamic_templates = generate_templates_for_schema(schema_columns)
                    template = next((t for t in dynamic_templates.values() if t.matches(question)), None)
                    
                    if template:
                        logger.info(f"Using template fallback: {template.name}")
                        import duckdb
                        conn = duckdb.connect(':memory:')
                        conn.execute("CREATE VIEW all_campaigns AS SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)", [str(CAMPAIGNS_PATTERN)])
                        df = conn.execute(template.sql).fetchdf()
                        
                        result = {
                            "success": True,
                            "answer": f"I used an analytical template for {template.name} to answer your question.",
                            "sql_query": template.sql,
                            "results": df
                        }
            except Exception as t_err:
                logger.error(f"Template fallback failed: {t_err}")

        # C. Post-processing (RAG, Summary, Charts)
        if result.get('success'):
            final_result = {
                "success": True,
                "answer": result.get('answer') or '',
                "sql_query": result.get('sql_query') or result.get('sql') or '',
                "data": []
            }
            
            # Handle DataFrame results
            results_df = result.get('results')
            if isinstance(results_df, pd.DataFrame) and not results_df.empty:
                final_result['data'] = results_df.head(100).to_dict('records')
                
                # Generate summary and chart
                summary_and_chart = _generate_summary_and_chart(question, results_df)
                if not final_result['answer'] or final_result['answer'] == '':
                    final_result['answer'] = summary_and_chart.get('summary', '')
                final_result['chart'] = summary_and_chart.get('chart')
            
            # Add RAG context if enabled
            if chat_request.use_rag_context:
                rag_context = _get_rag_context_for_question(question)
                if rag_context:
                    final_result['answer'] += f"\n\n💡 **Insights:**\n{rag_context}"
                    final_result['rag_enhanced'] = True
            
            # D. Pattern & Reasoning Injection
            try:
                # Load full data for pattern detection
                # Load sample data for pattern detection (limit to 10k rows for speed)
                import duckdb
                full_df = duckdb.connect().execute(f"SELECT * FROM read_parquet('{str(CAMPAIGNS_PATTERN)}', hive_partitioning=true, union_by_name=true) LIMIT 10000").df()
                
                # Check for API key before running potentially expensive AI patterns
                sys_api_key = os.getenv("OPENAI_API_KEY", "dummy")
                if sys_api_key != "dummy":
                    from src.engine.agents.enhanced_reasoning_agent import PatternDetector
                    detector = PatternDetector()
                    patterns = detector.detect_all(full_df)
                else:
                    logger.info("Skipping pattern detection (Missing API Key)")
                    patterns = {}
                
                pattern_insights = []
                
                # Trends
                if patterns.get('trends', {}).get('detected'):
                    t = patterns['trends']
                    pattern_insights.append(f"📈 **Trend:** {t.get('description', 'Performance changing')}")
                
                # Anomalies
                if patterns.get('anomalies', {}).get('detected'):
                    a = patterns['anomalies']
                    pattern_insights.append(f"⚠️ **Anomaly:** {a.get('description', 'Unusual activity detected')}")
                
                # Fatigue
                if patterns.get('creative_fatigue', {}).get('detected'):
                    c = patterns['creative_fatigue']
                    pattern_insights.append(f"🎨 **Creative Fatigue:** {c.get('evidence', {}).get('recommendation', 'Check creatives')}")
                
                # Saturation
                if patterns.get('audience_saturation', {}).get('detected'):
                    s = patterns['audience_saturation']
                    pattern_insights.append(f"👥 **Audience:** {s.get('recommendation', 'Saturation detected')}")
                
                if pattern_insights:
                    final_result['answer'] += "\n\n**🤖 AI Analysis:**\n" + "\n".join(pattern_insights)
                    
            except Exception as p_err:
                logger.warning(f"Pattern injection failed: {p_err}")
            
            return _convert_numpy_types(final_result)
        
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Unexpected error in chat_global: {e}")
        return {"success": False, "error": f"An error occurred: {str(e)}"}


def _convert_numpy_types(obj):
    """Helper function to convert numpy types to Python types."""
    if isinstance(obj, pd.DataFrame):
        return obj
    elif isinstance(obj, dict):
        return {k: _convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_numpy_types(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        val = float(obj)
        return None if math.isnan(val) or math.isinf(val) else val
    elif isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return obj


def _generate_summary_and_chart(question: str, df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summary overview and chart data from query results."""
    q_lower = question.lower()
    summary_parts = []
    chart_data = None
    
    try:
        if df.empty:
            return {'summary': '', 'chart': None}
        
        num_rows = len(df)
        summary_parts.append(f"**Results Overview:** Found {num_rows} {'row' if num_rows == 1 else 'rows'}")
        
        # Generate stats for key metrics if present
        for col in ['spend', 'total_spend', 'conversions', 'total_conversions', 'clicks', 'roas', 'ctr']:
            if col in df.columns:
                total = df[col].sum()
                avg = df[col].mean()
                if col in ['spend', 'total_spend']:
                    summary_parts.append(f"• Total Spend: ${total:,.2f} (Avg: ${avg:,.2f})")
                elif col in ['conversions', 'total_conversions']:
                    summary_parts.append(f"• Total Conversions: {total:,.0f} (Avg: {avg:,.1f})")
                elif col == 'clicks':
                    summary_parts.append(f"• Total Clicks: {total:,.0f}")
                elif col == 'roas':
                    summary_parts.append(f"• Average ROAS: {avg:.2f}x")
                elif col == 'ctr':
                    summary_parts.append(f"• Average CTR: {avg:.2%}")
        
        is_top_query = any(word in q_lower for word in ['top', 'best', 'highest', 'lowest', 'worst'])
        
        # Generate chart data
        if num_rows > 0 and num_rows <= 20:
            label_col = None
            value_col = None
            
            for col in ['funnel_stage', 'platform', 'channel', 'campaign_name', 'campaign', 'name', 'month', 'date']:
                if col in df.columns:
                    label_col = col
                    break
            
            value_priority = ['stage_conversions', 'stage_clicks', 'stage_impressions', 'stage_spend', 'total_spend', 'spend', 'total_conversions', 'conversions', 'roas', 'avg_roas', 'clicks', 'impressions', 'ctr']
            for col in value_priority:
                if col in df.columns:
                    value_col = col
                    break
            
            if label_col and value_col:
                if 'funnel' in q_lower or label_col == 'funnel_stage':
                    chart_type = 'funnel'
                elif 'time' in q_lower or 'month' in q_lower or 'trend' in q_lower or 'date' in label_col:
                    chart_type = 'line'
                elif 'compare' in q_lower or 'vs' in q_lower:
                    chart_type = 'bar'
                elif num_rows <= 6:
                    chart_type = 'pie'
                else:
                    chart_type = 'bar'
                
                chart_data = {
                    'type': chart_type,
                    'title': f"{value_col.replace('_', ' ').title()} by {label_col.replace('_', ' ').title()}",
                    'labels': df[label_col].astype(str).tolist()[:10],
                    'values': df[value_col].tolist()[:10],
                    'label_key': label_col,
                    'value_key': value_col
                }
                
                if is_top_query and num_rows >= 1:
                    top_label = df.iloc[0][label_col]
                    top_value = df.iloc[0][value_col]
                    if isinstance(top_value, (int, float)):
                        if '_pct' in value_col.lower() or 'change' in value_col.lower():
                            summary_parts.append(f"🏆 **Top Performer:** {top_label} with {top_value:+.1f}%")
                        elif 'spend' in value_col.lower():
                            summary_parts.append(f"🏆 **Top Performer:** {top_label} with ${top_value:,.2f}")
                        elif 'roas' in value_col.lower():
                            summary_parts.append(f"🏆 **Top Performer:** {top_label} with {top_value:.2f}x ROAS")
                        else:
                            summary_parts.append(f"🏆 **Top Performer:** {top_label} with {top_value:,.2f}")
        
        return {'summary': '\n'.join(summary_parts), 'chart': chart_data}
        
    except Exception as e:
        logger.warning(f"Failed to generate summary/chart: {e}")
        return {'summary': '', 'chart': None}


async def _handle_knowledge_mode_query(question: str) -> Dict[str, Any]:
    """Handle knowledge-mode queries using RAG knowledge bases."""
    try:
        from src.platform.knowledge.causal_kb_rag import get_knowledge_base
        from src.platform.knowledge.benchmark_engine import DynamicBenchmarkEngine
        
        kb = get_knowledge_base()
        benchmark_engine = DynamicBenchmarkEngine()
        
        q_lower = question.lower()
        answer_parts = []
        sources = []
        
        # Check for specific metric questions
        if "roas" in q_lower:
            info = kb.knowledge['metrics'].get('ROAS', {})
            if info:
                answer_parts.append(f"**ROAS Insight:**\n- **Traditional Calculation:** {info.get('traditional', 'N/A')}\n- **Causal ROAS:** {info.get('causal', 'N/A')}\n- **Key Insight:** {info.get('interpretation', 'N/A')}\n- **⚠️ Common Pitfall:** {info.get('common_pitfall', 'N/A')}")
                sources.append("Marketing Knowledge Base - ROAS")
                
        if "cpa" in q_lower or "cost per acquisition" in q_lower:
            info = kb.knowledge['metrics'].get('CPA', {})
            if info:
                answer_parts.append(f"**CPA Insight:**\n- **Traditional Calculation:** {info.get('traditional', 'N/A')}\n- **Causal CPA:** {info.get('causal', 'N/A')}\n- **Key Insight:** {info.get('interpretation', 'N/A')}\n- **⚠️ Common Pitfall:** {info.get('common_pitfall', 'N/A')}")
                sources.append("Marketing Knowledge Base - CPA")
                
        if "ctr" in q_lower or "click through" in q_lower:
            info = kb.knowledge['metrics'].get('CTR', {})
            if info:
                answer_parts.append(f"**CTR Insight:**\n- **Traditional Calculation:** {info.get('traditional', 'N/A')}\n- **Causal CTR:** {info.get('causal', 'N/A')}\n- **Key Insight:** {info.get('interpretation', 'N/A')}")
                sources.append("Marketing Knowledge Base - CTR")
        
        # Check for benchmark questions
        if "benchmark" in q_lower or "industry" in q_lower or "average" in q_lower:
            platform = None
            if "google" in q_lower:
                platform = "google_search"
            elif "linkedin" in q_lower:
                platform = "linkedin"
            elif "meta" in q_lower or "facebook" in q_lower:
                platform = "meta"
            
            business_model = "B2B" if "b2b" in q_lower else "B2C"
            industry = "saas" if "saas" in q_lower else ("e_commerce" if "ecommerce" in q_lower or "e-commerce" in q_lower else "default")
            
            if platform:
                benchmarks = benchmark_engine.get_contextual_benchmarks(
                    channel=platform, business_model=business_model, industry=industry
                )
                if benchmarks.get('benchmarks'):
                    bench_info = []
                    for metric, ranges in benchmarks['benchmarks'].items():
                        if isinstance(ranges, dict):
                            bench_info.append(f"- **{metric.upper()}:** Excellent: {ranges.get('excellent', 'N/A')}, Good: {ranges.get('good', 'N/A')}")
                    if bench_info:
                        answer_parts.append(f"**Industry Benchmarks ({benchmarks['context']}):**\n" + "\n".join(bench_info))
                        sources.append(f"Benchmark Engine - {platform}")
        
        # Check for best practices
        if "best practice" in q_lower or "recommend" in q_lower or "optimize" in q_lower:
            practices = kb.knowledge.get('best_practices', [])[:3]
            if practices:
                practice_info = [f"- **{p.get('practice', 'N/A')}:** {p.get('description', 'N/A')}" for p in practices if isinstance(p, dict)]
                if practice_info:
                    answer_parts.append(f"**Best Practices:**\n" + "\n".join(practice_info))
                    sources.append("Marketing Knowledge Base - Best Practices")
        
        # Check for pitfalls
        if "pitfall" in q_lower or "mistake" in q_lower or "avoid" in q_lower:
            pitfalls = kb.knowledge.get('pitfalls', [])[:3]
            if pitfalls:
                pitfall_info = [f"- **{p.get('pitfall', 'N/A')}:** {p.get('description', 'N/A')}" for p in pitfalls if isinstance(p, dict)]
                if pitfall_info:
                    answer_parts.append(f"**Common Pitfalls to Avoid:**\n" + "\n".join(pitfall_info))
                    sources.append("Marketing Knowledge Base - Pitfalls")
        
        # Check for causal methods
        if "causal" in q_lower or "a/b test" in q_lower or "experiment" in q_lower:
            methods = kb.knowledge.get('methods', {})
            method_info = [f"- **{m.get('name', k)}:** Use when: {', '.join(m.get('when_to_use', [])[:2])}" for k, m in methods.items() if isinstance(m, dict)]
            if method_info:
                answer_parts.append(f"**Causal Analysis Methods:**\n" + "\n".join(method_info[:3]))
                sources.append("Marketing Knowledge Base - Causal Methods")
        
        # Default response if no matches
        if not answer_parts:
            answer_parts.append("I can help you with marketing insights! Try asking about:\n- **Metrics:** ROAS, CPA, CTR, CVR interpretation\n- **Benchmarks:** Industry averages by platform (Google, Meta, LinkedIn)\n- **Best Practices:** Campaign optimization strategies\n- **Pitfalls:** Common mistakes to avoid\n- **Causal Analysis:** A/B testing, experiment design")
            sources.append("Marketing Knowledge Base")
        
        return {
            "success": True,
            "answer": "\n\n".join(answer_parts),
            "knowledge_mode": True,
            "sources": sources,
            "sql": "N/A (Knowledge Mode)"
        }
        
    except Exception as e:
        logger.error(f"Knowledge mode query failed: {e}")
        return {"success": False, "error": str(e), "answer": f"Failed to retrieve knowledge: {str(e)}"}


def _get_rag_context_for_question(question: str) -> str:
    """Get relevant RAG context to enhance SQL answers."""
    try:
        from src.platform.knowledge.causal_kb_rag import get_knowledge_base
        
        kb = get_knowledge_base()
        q_lower = question.lower()
        context_parts = []
        
        for metric in ['ROAS', 'CPA', 'CTR', 'CVR']:
            if metric.lower() in q_lower:
                info = kb.knowledge['metrics'].get(metric, {})
                if info:
                    context_parts.append(f"*{metric}:* {info.get('interpretation', '')}")
        
        return " ".join(context_parts) if context_parts else ""
        
    except Exception as e:
        logger.warning(f"Failed to get RAG context: {e}")
        return ""
