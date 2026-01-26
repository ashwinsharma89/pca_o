
import pandas as pd
import time
from typing import Dict, Any, List, Optional, Union
from loguru import logger
import polars as pl

from src.engine.agents.prompt_templates import get_prompt

# Import new modular components
from .metrics_calculator import MetricsCalculator
from .business_rules import BusinessRules
from .recommendations import RecommendationEngine
from .text_cleaner import TextCleaner
from .llm_service import LLMService

class AnalyticsOrchestrator:
    """
    Manager class that coordinates the entire analytics pipeline.
    Replaces the monolithic MediaAnalyticsExpert.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.llm_service = LLMService(api_key=api_key)
        self.text_cleaner = TextCleaner()
        self.metrics_engine = MetricsCalculator()
        
    def analyze_campaigns(self, df: Union[pd.DataFrame, pl.DataFrame]) -> Dict[str, Any]:
        """
        Main entry point for analysis.
        """
        start_time = time.time()
        logger.info(f"Starting modular analysis on {len(df)} rows")
        
        # 1. Handle Input (Polars Optimization)
        pdf = None
        
        if isinstance(df, pl.DataFrame):
            if df.height == 0:
                 return {"error": "No data provided"}
            
            # Already Polars - assume DuckDBManager cleaned it or do basic checks
            # DuckDBManager.get_campaigns_polars already does Title Case normalization (Spend, Campaign)
            # But MetricsCalculator might expect lowercase 'spend' etc, based on line 42?
            # Let's check line 42 of previous file: 
            # column_map = {"spend_usd": "spend", ...} and df.columns = df.columns.str.lower()
            # The previous code normalized to LOWERCASE.
            # DuckDBManager.get_campaigns_polars normalizes to TITLE CASE (Spend, Campaign).
            
            # We need to align them. MetricsCalculator usually prefers lower_case or snake_case for internal logic?
            # Let's Normalize to matches previous Orchestrator behavior: Custom Mapping + Lowercase.
            
            # For Polars, efficient renaming:
            # First, check if we have the Title Case columns from DuckDB
            # "Spend", "Campaign", "Impressions"
            
            # already Polars
            pdf = df
            
            # 1. Lowercase columns safely
            # If duplicates would result, handle them? DuckDB output should be clean but let's be safe.
            safe_renames = {}
            existing_after_lower = set()
            
            for c in pdf.columns:
                lower = c.lower()
                if c != lower:
                   if lower in pdf.columns:
                       # Collision (e.g. have 'Spend' and 'spend')
                       # Don't rename yet, handled by coalescing or just ignore
                       pass
                   else:
                       safe_renames[c] = lower
            
            if safe_renames:
                pdf = pdf.rename(safe_renames)
            
            # 2. Map standard columns (Coalesce safe)
            base_map = {
               "spend_usd": "spend", "revenue_usd": "revenue", "campaign_name": "campaign",
               "cost": "spend", "amount_spent": "spend", "ad_group": "ad group"
            }
            
            for old_col, new_col in base_map.items():
                if old_col in pdf.columns:
                    if new_col in pdf.columns and old_col != new_col:
                        # Coalesce: fill nulls in target with source, then drop source
                        pdf = pdf.with_columns(pl.col(new_col).fill_null(pl.col(old_col))).drop(old_col)
                    elif new_col not in pdf.columns:
                        # Simple rename
                        pdf = pdf.rename({old_col: new_col})

        else:
             # Pandas Path (Legacy)
            if df.empty:
                return {"error": "No data provided"}
    
            # Normalize columns to lowercase for consistent access
            df.columns = df.columns.str.lower()
            
            # KEY FIX: Normalize column names
            column_map = {
                "spend_usd": "spend",
                "revenue_usd": "revenue",
                "campaign_name": "campaign",
                "campaign name": "campaign",
                "cost": "spend",
                "amount_spent": "spend"
            }
            df = df.rename(columns=column_map)
    
            pdf = pl.from_pandas(df)
        
        # 2. Calculate Token-Efficient Metrics (Vectorized)
        # Global metrics
        global_metrics = MetricsCalculator.calculate_core_metrics(pdf).to_dicts()[0]
        
        # Calculate Breakdowns (Platform, Campaign)
        breakdowns = self._calculate_breakdowns(pdf)
        
        # Merge breakdowns into metrics
        metrics_output = {
            "overview": global_metrics,
            **breakdowns
        }
        
        # 3. Apply Business Rules (Logic)
        status = BusinessRules.evaluate_performance(global_metrics)
        
        # 4. Generate Recommendations (Advisor)
        recommendations = RecommendationEngine.generate_recommendations(global_metrics, status)
        
        # 5. Generate LLM Narrative (Creative)
        # We construct a highly optimized prompt with just the necessary data
        # Using the Dentsu template now as the standard
        prompt = self._construct_prompt(global_metrics, status, recommendations)
        
        try:
            response_text = self.llm_service.generate_completion(
                prompt=prompt,
                system_prompt="You are a senior media buyer. Return valid JSON."
            )
            
            # Parse JSON from the Dentsu template output
            import json
            import re
            
            # Find JSON block using regex if it's wrapped in text
            json_match = re.search(r'(\{.*\})', response_text, re.DOTALL)
            if json_match:
                cleaned = json_match.group(1)
            else:
                cleaned = response_text.replace('```json', '').replace('```', '').strip()
            
            try:
                result_json = json.loads(cleaned)
                # Use the overview/brief as the simple narrative
                narrative = result_json.get('overview', result_json.get('brief', 'Analysis generated.'))
            except:
                # Fallback if specific keys missing or partial JSON
                narrative = response_text[:500] + "..."

            # 6. Clean Output (Janitor)
            narrative = self.text_cleaner.strip_italics(narrative)
        except Exception as e:
            logger.error(f"LLM Generation failed: {e}")
            narrative = "Analysis could not be generated due to a service error."
            
        return {
            "metrics": metrics_output,
            "status": status,
            "recommendations": recommendations,
            "narrative": narrative,
            "execution_time": time.time() - start_time
        }
    
    def _calculate_breakdowns(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calculate breakdowns by common dimensions using Polars."""
        breakdowns = {}
        
        # Platform Breakdown
        if "platform" in df.columns or "Platform" in df.columns:
            col = "platform" if "platform" in df.columns else "Platform"
            try:
                platform_metrics = MetricsCalculator.calculate_aggregated_metrics(df, [col])
                breakdowns["by_platform"] = platform_metrics.to_dicts()
            except Exception as e:
                logger.warning(f"Platform breakdown failed: {e}")
                breakdowns["by_platform"] = []

        # Campaign Breakdown
        if "campaign" in df.columns or "Campaign" in df.columns:
            col = "campaign" if "campaign" in df.columns else "Campaign"
            try:
                camp_metrics = MetricsCalculator.calculate_aggregated_metrics(df, [col])
                breakdowns["by_campaign"] = camp_metrics.to_dicts()
            except Exception as e:
                logger.warning(f"Campaign breakdown failed: {e}")
                breakdowns["by_campaign"] = []
                
        return breakdowns
    
    def _construct_prompt(self, metrics: Dict, status: Dict, recs: List) -> str:
        """Construct a data-rich prompt for the LLM using templates."""
        from src.engine.agents.prompt_templates import get_prompt
        
        # Format metrics string for the template
        metrics_str = (
            f"- Spend: ${metrics.get('spend', 0):.2f}\n"
            f"- ROAS: {metrics.get('roas', 0):.2f}x\n"
            f"- CTR: {metrics.get('ctr', 0):.2f}%"
        )
        
        # Use Dentsu template
        return get_prompt(
            "rag_executive_summary",
            objective="Performance Review",
            context="Standard Campaign Analysis",
            metrics=metrics_str,
            status=status,
            recommendations=RecommendationEngine._format_for_llm(recs) if hasattr(RecommendationEngine, '_format_for_llm') else str(recs[:3])
        )

    def generate_rag_summary(self, analysis_result: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Generate a RAG-enhanced executive summary.
        
        Args:
            analysis_result: The dictionary returned by analyze_campaigns
            **kwargs: Extra context (campaign_objective, enrichment_context, etc.)
        """
        try:
            metrics = analysis_result.get('metrics', {}).get('overview', {})
            status = analysis_result.get('status', {})
            recs = analysis_result.get('recommendations', [])
            
            enrichment_context = kwargs.get('enrichment_context', '')
            objective = kwargs.get('campaign_objective', 'Maximize ROI')
            
            # Enhanced Prompt via Template
            prompt = get_prompt(
                "rag_executive_summary",
                objective=objective,
                context=enrichment_context,
                metrics=f"- Spend: ${metrics.get('spend', 0):.2f}\n- ROAS: {metrics.get('roas', 0):.2f}\n- CPA: ${metrics.get('cpa', 0):.2f}",
                status=status,
                recommendations=RecommendationEngine._format_for_llm(recs) if hasattr(RecommendationEngine, '_format_for_llm') else str(recs[:3])
            )
            
            start_rag = time.time()
            response_text = self.llm_service.generate_completion(
                prompt=prompt,
                system_prompt="Return valid JSON only."
            )
            elapsed = time.time() - start_rag
            
            # Parse JSON - More robustly
            import json
            import re
            
            # Find JSON block using regex if it's wrapped in text
            json_match = re.search(r'(\{.*\})', response_text, re.DOTALL)
            if json_match:
                cleaned = json_match.group(1)
            else:
                cleaned = response_text.replace('```json', '').replace('```', '').strip()
                
            try:
                result_json = json.loads(cleaned)
            except Exception as json_err:
                logger.error(f"JSON Parse failed: {json_err}, text: {cleaned[:100]}...")
                # Try simple recovery or raise
                raise json_err
            
            logger.info(f"RAG JSON Parsed: keys={list(result_json.keys())}")
            
            # ENRICHMENT: Inject system metadata required by frontend
            if 'rag_metadata' not in result_json or not isinstance(result_json['rag_metadata'], dict):
                result_json['rag_metadata'] = {}
                
            model_name = getattr(self.llm_service, 'model', 'unknown-model')
            
            # SCHEMA MAPPING: Backend (overview, key_takeaways) -> Frontend (brief, detailed)
            # Ensure "brief" exists
            if 'brief' not in result_json:
                result_json['brief'] = result_json.get('overview', result_json.get('summary', ''))
            
            # Ensure "detailed" exists
            if 'detailed' not in result_json:
                takeaways = result_json.get('key_takeaways', result_json.get('insights', []))
                if isinstance(takeaways, list):
                    result_json['detailed'] = "\n".join([f"- {t}" for t in takeaways])
                else:
                    result_json['detailed'] = str(takeaways)

            # Final check - if still empty, use fallback from analysis_result
            if not result_json['brief'] or len(result_json['brief']) < 10:
                 result_json['brief'] = analysis_result.get('executive_summary', 'Advanced analysis completed.')

            # Safely inject fields
            result_json['rag_metadata'].update({
                "model": model_name,
                "latency": round(elapsed, 2),
                "tokens_input": len(prompt) // 4,
                "tokens_output": len(response_text) // 4,
                "retrieval_count": len(result_json['rag_metadata'].get('sources_used', enrichment_context.split('\n\n') if enrichment_context else []))
            })
            
            return result_json
            
        except Exception as e:
            logger.error(f"RAG Generation failed: {e}")
            fallback = {
                "brief": "Analysis completed. RAG-enhanced summary was unavailable due to a service error.",
                "detailed": "We've analyzed your campaigns using standard metrics. Please check the charts and breakdown tables below for specific performance data.",
                "rag_metadata": {
                    "error": str(e),
                    "model": "service-fallback",
                    "latency": 0.0,
                    "tokens_input": 0,
                    "tokens_output": 0,
                    "retrieval_count": 0,
                    "sources_used": ["System Core"],
                    "confidence_score": 0.0
                }
            }
            return fallback
