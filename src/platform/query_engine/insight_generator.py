"""
KG Insight Generator
Uses LLM to provide natural language guidance and insights for KG query results.
"""

import logging
from typing import Dict, Any, List, Optional
import os
from src.platform.knowledge.causal_kb_rag import get_knowledge_base
from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine

logger = logging.getLogger(__name__)

class KGInsightGenerator:
    """
    Generates natural language guidance and insights for Knowledge Graph results.
    """
    
    def __init__(self):
        # reuse the same logic for determining available models
        self.engine = NaturalLanguageQueryEngine(api_key=os.getenv("OPENAI_API_KEY", "dummy"))
        self.kb = get_knowledge_base()

    def generate_guidance(self, query: str, results: List[Dict[str, Any]], summary: Dict[str, Any]) -> str:
        """
        Produce high-quality human guidance for the given query and results.
        """
        if not self.engine.available_models:
            logger.warning("No LLM models available for guidance generation.")
            return "Enable LLM (Add API key) to see deep technical guidance here."

        # Get domain context from Causal KB
        relevant_metrics = [m for m in ["ROAS", "CPA", "CTR", "CVR", "CPC"] if m.lower() in query.lower()]
        domain_insights = []
        for m in relevant_metrics:
            guidance = self.kb.get_interpretation_guidance(m, 0, 0)
            if guidance and guidance.get('insights'):
                domain_insights.extend(guidance['insights'])

        # Build prompt
        prompt = f"""
You are a Senior Marketing Data Scientist. Provide "Executive Guidance" based on the following Knowledge Graph results.

USER QUERY: {query}

TOTALS:
- Total Spend: ${summary.get('total_spend', 0):,.2f}
- Total Impressions: {summary.get('total_impressions', 0):,}
- Total Click: {summary.get('total_clicks', 0):,}
- Total Conversions: {summary.get('total_conversions', 0):,}
- Avg CTR: {summary.get('avg_ctr', 0)}%
- Avg CPC: ${summary.get('avg_cpc', 0):,.2f}
- Avg CPA: ${summary.get('avg_cpa', 0):,.2f}

DETAILED BREAKDOWN (Top 10 items):
{self._format_results(results)}

DOMAIN KNOWLEDGE context:
{chr(10).join(domain_insights)}

TASK:
1. Briefly interpret the data (what's the "so what?").
2. Highlight any efficiency wins or red flags (ROAS vs Spend).
3. Provide 2 specific, actionable optimization recommendations.
4. Keep it concise (under 200 words). Use markdown bolding for key terms.

GUIDANCE:
"""
        try:
             # Use the new generic completion method
             guidance = self.engine.generate_completion(
                 prompt=prompt,
                 system_prompt="You are a Senior Marketing Analytics Expert. Give clear, actionable advice.",
                 temperature=0.7,
                 max_tokens=600
             )
             return guidance
        except Exception as e:
            logger.error(f"Guidance generation failed: {e}")
            return "Unable to generate guidance at this time."

    def _format_results(self, results: List[Dict[str, Any]]) -> str:
        lines = []
        for i, r in enumerate(results[:10]):
            name = r.get('name', 'N/A')
            spend = r.get('spend', 0)
            roas = r.get('roas', 0)
            lines.append(f"- {name}: Spend ${spend:,.2f}, ROAS {roas}x")
        return "\n".join(lines)
