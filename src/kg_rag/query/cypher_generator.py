"""
KG-RAG Cypher Generator

Uses LLM to generate Cypher queries for novel/complex questions.
Falls back when templates don't match with high confidence.
"""

import logging
import json
from typing import Dict, Any, Optional, List

from src.kg_rag.context.context_builder import ContextBuilder
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


# System prompt for Cypher generation
CYPHER_SYSTEM_PROMPT = """You are a Neo4j Cypher query expert for a marketing analytics Knowledge Graph.

## Schema Overview

**Nodes:**
- Channel (id, name) - 4 types: search, social, display, programmatic
- Platform (id, name, supports_keywords, supports_placements, supports_video_metrics, supports_reach, supports_b2b_targeting)
- Campaign (id, name, platform_id, objective, status, budget, impressions_total, clicks_total, spend_total, conversions_total, revenue_total)
- Targeting (campaign_id, device_types[], age_range, gender, geo_countries[], interests[], bid_strategy, funnel_stage)
- Metric (id, campaign_id, date, impressions, clicks, spend, conversions, revenue, video_plays, video_completes)
- Placement (id, name, type, category, impressions, clicks, spend, conversions, viewability_rate)
- Keyword (id, text, match_type, quality_score, impressions, clicks, spend, conversions)

**Relationships:**
- (Channel)-[:CATEGORIZES]->(Platform)
- (Platform)<-[:BELONGS_TO]-(Campaign)
- (Campaign)-[:HAS_TARGETING]->(Targeting)
- (Campaign)-[:HAS_PERFORMANCE]->(Metric)
- (Campaign)-[:CONTAINS]->(EntityGroup)-[:HAS_PLACEMENT]->(Placement)
- (Campaign)-[:CONTAINS]->(EntityGroup)-[:HAS_KEYWORD]->(Keyword)

## Rules
1. Use aggregation functions (SUM, AVG, COUNT) for metrics
2. Calculate CTR as: clicks * 100.0 / impressions
3. Calculate CPC as: spend / clicks
4. Calculate ROAS as: revenue / spend
5. Calculate CPA as: spend / conversions
6. Always handle division by zero with CASE statements
7. Use date() function for date comparisons
8. Order results by most relevant metric
9. Limit results to reasonable count (default 20)
10. Return only the Cypher query, no explanations

## Output Format
Return ONLY the Cypher query, nothing else.
"""


class CypherGenerator:
    """
    Generate Cypher queries using LLM for novel questions.
    
    Uses schema context and few-shot examples to guide generation.
    
    Usage:
        generator = CypherGenerator()
        cypher = generator.generate("What's the best performing ad type on Meta?")
    """
    
    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 1000
    ):
        """
        Initialize generator.
        
        Args:
            model: LLM model to use
            max_tokens: Max tokens in response
        """
        self._settings = get_kg_rag_settings()
        self._model = model
        self._max_tokens = max_tokens
        self._context_builder = ContextBuilder()
        
        # Few-shot examples
        self._examples = self._get_examples()
    
    def generate(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate Cypher for a natural language query.
        
        Args:
            query: Natural language question
            context: Optional context (date range, filters)
            
        Returns:
            Dict with cypher, reasoning, confidence
        """
        # Build context
        schema_context = self._context_builder.build_schema_context()
        
        # Build prompt
        prompt = self._build_prompt(query, schema_context, context)
        
        # Call LLM
        try:
            cypher = self._call_llm(prompt)
            
            # Validate Cypher (basic)
            is_valid, error = self._validate_cypher(cypher)
            
            if not is_valid:
                return {
                    "success": False,
                    "cypher": cypher,
                    "error": f"Invalid Cypher: {error}",
                    "query": query,
                }
            
            return {
                "success": True,
                "cypher": cypher,
                "query": query,
                "model": self._model,
            }
            
        except Exception as e:
            logger.error(f"Cypher generation failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query,
            }
    
    def _build_prompt(
        self,
        query: str,
        schema_context: str,
        context: Optional[Dict[str, Any]]
    ) -> str:
        """Build the prompt for LLM."""
        prompt_parts = [
            "Generate a Cypher query for this question:",
            f'"{query}"',
            "",
            "## Examples",
        ]
        
        # Add examples
        for example in self._examples[:3]:
            prompt_parts.append(f"Q: {example['question']}")
            prompt_parts.append(f"Cypher:\n```cypher\n{example['cypher']}\n```")
            prompt_parts.append("")
        
        # Add context
        if context:
            prompt_parts.append("## Context")
            if context.get("date_from"):
                prompt_parts.append(f"Date range: {context['date_from']} to {context.get('date_to', 'today')}")
            if context.get("platform"):
                prompt_parts.append(f"Platform filter: {context['platform']}")
            prompt_parts.append("")
        
        prompt_parts.append("## Your Query")
        prompt_parts.append("Generate Cypher for the question above. Return ONLY the Cypher code.")
        
        return "\n".join(prompt_parts)
    
    def _call_llm(self, prompt: str) -> str:
        """Call LLM to generate Cypher."""
        # Try to use litellm if available
        try:
            import litellm
            
            response = litellm.completion(
                model=self._model,
                messages=[
                    {"role": "system", "content": CYPHER_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self._max_tokens,
                temperature=0.1,
            )
            
            content = response.choices[0].message.content
            
            # Extract Cypher from markdown code blocks if present
            if "```cypher" in content:
                start = content.find("```cypher") + 9
                end = content.find("```", start)
                return content[start:end].strip()
            elif "```" in content:
                start = content.find("```") + 3
                end = content.find("```", start)
                return content[start:end].strip()
            
            return content.strip()
            
        except ImportError:
            logger.warning("litellm not available, using fallback")
            return self._fallback_generation(prompt)
    
    def _fallback_generation(self, prompt: str) -> str:
        """Fallback Cypher generation without LLM."""
        # Return a safe default query
        return """
MATCH (c:Campaign)
RETURN c.name AS campaign,
       c.platform_id AS platform,
       c.spend_total AS spend,
       c.conversions_total AS conversions,
       CASE WHEN c.spend_total > 0 THEN c.revenue_total / c.spend_total ELSE 0 END AS roas
ORDER BY spend DESC
LIMIT 20
        """.strip()
    
    def _validate_cypher(self, cypher: str) -> tuple[bool, Optional[str]]:
        """
        Validate Cypher query (basic checks).
        
        Returns:
            (is_valid, error_message)
        """
        cypher_upper = cypher.upper()
        
        # Must have RETURN or RETURN *
        if "RETURN" not in cypher_upper and "CREATE" not in cypher_upper:
            return False, "Missing RETURN clause"
        
        # Prevent destructive operations
        dangerous_keywords = ["DELETE", "REMOVE", "DROP", "DETACH"]
        for keyword in dangerous_keywords:
            if keyword in cypher_upper:
                return False, f"Dangerous keyword: {keyword}"
        
        # Check for MATCH
        if "MATCH" not in cypher_upper and "CALL" not in cypher_upper:
            return False, "Missing MATCH clause"
        
        # Basic syntax check
        if cypher_upper.count("(") != cypher_upper.count(")"):
            return False, "Unbalanced parentheses"
        
        if cypher_upper.count("[") != cypher_upper.count("]"):
            return False, "Unbalanced brackets"
        
        return True, None
    
    def _get_examples(self) -> List[Dict[str, str]]:
        """Get few-shot examples for Cypher generation."""
        return [
            {
                "question": "What's the total spend by channel?",
                "cypher": """MATCH (ch:Channel)-[:CATEGORIZES]->(p:Platform)<-[:BELONGS_TO]-(c:Campaign)
RETURN ch.name AS channel,
       SUM(c.spend_total) AS total_spend,
       count(c) AS campaigns
ORDER BY total_spend DESC"""
            },
            {
                "question": "Show me campaigns with ROAS below 1",
                "cypher": """MATCH (c:Campaign)
WHERE c.spend_total > 100 AND c.revenue_total IS NOT NULL
WITH c, CASE WHEN c.spend_total > 0 THEN c.revenue_total / c.spend_total ELSE 0 END AS roas
WHERE roas < 1
RETURN c.name AS campaign,
       c.platform_id AS platform,
       c.spend_total AS spend,
       c.revenue_total AS revenue,
       roas
ORDER BY roas ASC
LIMIT 20"""
            },
            {
                "question": "What's the CTR trend over the last 7 days?",
                "cypher": """MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE m.date >= date() - duration('P7D')
WITH m.date AS date,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks
RETURN date,
       impressions,
       clicks,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
ORDER BY date"""
            },
            {
                "question": "Which targeting settings have the best conversion rate?",
                "cypher": """MATCH (c:Campaign)-[:HAS_TARGETING]->(t:Targeting)
WHERE c.clicks_total > 100
WITH t.device_types AS devices,
     t.age_range AS age,
     SUM(c.clicks_total) AS clicks,
     SUM(c.conversions_total) AS conversions
RETURN devices, age, clicks, conversions,
       CASE WHEN clicks > 0 THEN conversions * 100.0 / clicks ELSE 0 END AS cvr
ORDER BY cvr DESC
LIMIT 20"""
            },
            {
                "question": "Compare video completion rates across platforms",
                "cypher": """MATCH (p:Platform)<-[:BELONGS_TO]-(c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE m.video_plays IS NOT NULL AND m.video_plays > 0
WITH p.name AS platform,
     SUM(m.video_plays) AS video_plays,
     SUM(m.video_completes) AS video_completes
RETURN platform,
       video_plays,
       video_completes,
       CASE WHEN video_plays > 0 THEN video_completes * 100.0 / video_plays ELSE 0 END AS vtr
ORDER BY vtr DESC"""
            },
        ]
