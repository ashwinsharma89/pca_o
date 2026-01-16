"""
Smart Query Interpretation Layer
Generates context-aware interpretations using marketing domain knowledge
Leverages nl_to_sql.py intelligence for accurate, specific interpretations
"""
import json
from typing import List, Dict, Any
from loguru import logger
from src.core.config.llm_router import LLMRouter, TaskType


class SmartQueryInterpreter:
    """Generates smart, context-aware query interpretations with NO fallbacks."""
    
    def __init__(self):
        """Initialize the smart interpreter."""
        config = LLMRouter.get_model_config(TaskType.QUERY_INTERPRETATION)
        logger.info(f"SmartQueryInterpreter initialized with {config['model']}")
    
    def generate_interpretations(
        self,
        query: str,
        schema_info: Dict[str, Any],
        num_interpretations: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Generate smart, context-aware interpretations.
        
        Args:
            query: User's natural language query
            schema_info: Schema with columns, dtypes, sample data
            num_interpretations: Number of interpretations (default 3)
            
        Returns:
            List of interpretation dicts with interpretation, reasoning, sql_hint
        """
        
        # Build context from schema
        available_columns = schema_info.get('columns', [])
        sample_data = schema_info.get('sample_data', [])
        
        # Detect available metrics
        metrics_available = self._detect_available_metrics(available_columns)
        dimensions_available = self._detect_available_dimensions(available_columns)
        
        prompt = self._build_smart_prompt(
            query=query,
            available_columns=available_columns,
            metrics=metrics_available,
            dimensions=dimensions_available,
            sample_data=sample_data,
            num_interpretations=num_interpretations
        )
        
        try:
            logger.info(f"Generating interpretations for: '{query}'")
            
            # Call GPT-5.1 High Reasoning
            response = LLMRouter.call_llm(
                task_type=TaskType.QUERY_INTERPRETATION,
                prompt=prompt,
                max_tokens=2048,
                temperature=0.7
            )
            
            # Parse JSON response
            interpretations = self._parse_and_validate(response, query, available_columns)
            
            if not interpretations:
                logger.error("LLM returned no valid interpretations")
                raise ValueError("Failed to generate valid interpretations")
            
            logger.info(f"Generated {len(interpretations)} valid interpretations")
            return interpretations
            
        except Exception as e:
            logger.error(f"Interpretation generation failed: {e}", exc_info=True)
            raise  # No fallbacks - fail fast and show error to user
    
    def _detect_available_metrics(self, columns: List[str]) -> Dict[str, bool]:
        """Detect which metrics are available in the data."""
        cols_lower = [c.lower() for c in columns]
        
        return {
            "spend": any(kw in cols_lower for kw in ['spend', 'cost', 'total_spent']),
            "impressions": any(kw in cols_lower for kw in ['impressions', 'impr']),
            "clicks": any(kw in cols_lower for kw in ['clicks', 'click']),
            "conversions": any(kw in cols_lower for kw in ['conversions', 'conv', 'site_visit']),
            "revenue": any(kw in cols_lower for kw in ['revenue', 'conversion_value']),
            "ctr": any(kw in cols_lower for kw in ['ctr', 'click_through_rate']),
            "cpc": any(kw in cols_lower for kw in ['cpc', 'cost_per_click']),
            "cpa": any(kw in cols_lower for kw in ['cpa', 'cost_per_acquisition', 'cost_per_conversion']),
            "roas": any(kw in cols_lower for kw in ['roas', 'return_on_ad_spend']),
        }
    
    def _detect_available_dimensions(self, columns: List[str]) -> Dict[str, bool]:
        """Detect which dimensions are available for grouping."""
        cols_lower = [c.lower() for c in columns]
        
        return {
            "campaign": any(kw in cols_lower for kw in ['campaign', 'campaign_name']),
            "platform": any(kw in cols_lower for kw in ['platform', 'channel', 'source']),
            "date": any(kw in cols_lower for kw in ['date', 'day', 'timestamp']),
            "funnel": any(kw in cols_lower for kw in ['funnel', 'stage', 'funnel_stage']),
            "device": any(kw in cols_lower for kw in ['device', 'device_type']),
            "audience": any(kw in cols_lower for kw in ['audience', 'segment', 'targeting']),
            "creative": any(kw in cols_lower for kw in ['creative', 'ad_name', 'ad_copy']),
        }
    
    def _build_smart_prompt(
        self,
        query: str,
        available_columns: List[str],
        metrics: Dict[str, bool],
        dimensions: Dict[str, bool],
        sample_data: List[Dict],
        num_interpretations: int
    ) -> str:
        """Build an intelligent prompt with full context."""
        
        # Build metrics context
        available_metrics = [k for k, v in metrics.items() if v]
        available_dims = [k for k, v in dimensions.items() if v]
        
        # Detect query intent
        query_lower = query.lower()
        intent_context = ""
        
        if any(kw in query_lower for kw in ['funnel', 'stage']):
            intent_context = """
🎯 FUNNEL QUERY DETECTED:
- User wants funnel stage analysis
- Group by funnel stage
- Show stage-specific metrics (CTR for Awareness, Conversion Rate for Consideration, CPA for Conversion)
- Compare performance across stages
- Identify drop-off points
"""
        elif any(kw in query_lower for kw in ['kpi', 'metric', 'performance']):
            intent_context = """
🎯 KPI/METRICS QUERY DETECTED:
- User wants summary metrics
- Calculate key performance indicators
- Show metrics at different levels (overall, by platform, by campaign)
- Include ROAS, CPA, CTR, Conversion Rate if available
- Provide context (vs benchmarks, trends)
"""
        elif any(kw in query_lower for kw in ['top', 'best', 'worst', 'rank']):
            intent_context = """
🎯 RANKING QUERY DETECTED:
- User wants sorted/ranked results
- Identify ranking criteria (ROAS, Spend, Conversions, etc.)
- Use ORDER BY with LIMIT
- Show top N performers
- Consider different ranking dimensions
"""
        elif any(kw in query_lower for kw in ['trend', 'over time', 'growth', 'compare']):
            intent_context = """
🎯 TREND/COMPARISON QUERY DETECTED:
- User wants time-series or period comparison
- Use DATE_TRUNC for time grouping
- Calculate period-over-period changes
- Show growth rates
- Identify trends (increasing, decreasing, stable)
"""
        
        prompt = f"""You are an expert marketing analyst helping interpret user queries about campaign data.

USER QUERY: "{query}"

AVAILABLE DATA:
Columns: {', '.join(available_columns)}
Available Metrics: {', '.join(available_metrics)}
Available Dimensions: {', '.join(available_dims)}

Sample Data (first row):
{json.dumps(sample_data[0] if sample_data else {}, indent=2)}

{intent_context}

MARKETING DOMAIN KNOWLEDGE:
- ROAS (Return on Ad Spend) = Revenue / Spend. Higher is better. Good ROAS > 3.0x
- CPA (Cost Per Acquisition) = Spend / Conversions. Lower is better.
- CTR (Click Through Rate) = Clicks / Impressions * 100. Higher is better. Good CTR > 2%
- Conversion Rate = Conversions / Clicks * 100. Higher is better.
- Funnel Stages: Awareness → Consideration → Conversion → Retention
- Platform comparison: Google Ads, Meta Ads, LinkedIn, DV360, CM360, Snapchat
- Temporal analysis: Daily, weekly, monthly trends; period-over-period comparisons

CRITICAL RULES:
✓ Generate {num_interpretations} DIFFERENT interpretations covering various angles
✓ Each interpretation must be SPECIFIC to the user's query (not generic)
✓ Use ONLY columns that exist in the schema
✓ Mention specific metrics and dimensions from available data
✓ Provide concrete SQL hints (GROUP BY, ORDER BY, filters, calculations)
✓ Each interpretation should offer a unique perspective or analysis approach
✗ NO generic templates like "show campaigns where spend > median"
✗ NO made-up columns or metrics
✗ NO vague interpretations

EXAMPLES OF GOOD INTERPRETATIONS:

Query: "show funnel performance"
1. "Group by Funnel stage and calculate CTR, Conversion Rate, and CPA for each stage to identify optimization opportunities"
   Reasoning: "Funnel column exists; user wants stage-level metrics to find bottlenecks"
   SQL Hint: "GROUP BY Funnel; calculate SUM(Clicks)/SUM(Impressions)*100 as CTR, SUM(Conversions)/SUM(Clicks)*100 as Conv_Rate, SUM(Spend)/SUM(Conversions) as CPA"

2. "Compare conversion rates between funnel stages to identify drop-off points and calculate drop-off percentages"
   Reasoning: "Funnel analysis requires stage comparison; drop-off rate = (stage1_conv - stage2_conv) / stage1_conv"
   SQL Hint: "Use window functions or self-join to compare adjacent funnel stages; calculate drop-off rates"

3. "Show top 5 campaigns by ROAS within each funnel stage to identify best performers per stage"
   Reasoning: "Combines funnel analysis with performance ranking; helps optimize budget allocation by stage"
   SQL Hint: "GROUP BY Funnel, Campaign_Name; calculate ROAS; use ROW_NUMBER() OVER (PARTITION BY Funnel ORDER BY ROAS DESC)"

Query: "what are major KPIs"
1. "Calculate overall ROAS, CPA, CTR, and Conversion Rate across all campaigns with totals and averages"
   Reasoning: "User wants high-level summary metrics; compute from aggregates not averages"
   SQL Hint: "SELECT SUM(Revenue)/SUM(Spend) as ROAS, SUM(Spend)/SUM(Conversions) as CPA, SUM(Clicks)/SUM(Impressions)*100 as CTR, SUM(Conversions)/SUM(Clicks)*100 as Conv_Rate"

2. "Show KPIs (ROAS, CPA, CTR) by Platform to compare channel performance"
   Reasoning: "Platform dimension available; cross-channel comparison reveals best performing channels"
   SQL Hint: "GROUP BY Platform; calculate metrics per platform; ORDER BY ROAS DESC"

3. "Display KPIs with month-over-month trends to show performance trajectory"
   Reasoning: "Date column available; temporal trends show if performance is improving or declining"
   SQL Hint: "GROUP BY DATE_TRUNC('month', Date); calculate metrics per month; show % change vs previous month"

NOW GENERATE {num_interpretations} INTERPRETATIONS FOR THE USER'S QUERY.

Return ONLY a JSON array (no markdown, no extra text):
[
  {{
    "interpretation": "Specific, actionable interpretation using actual column names",
    "reasoning": "Why this interpretation makes sense given the query and available data",
    "sql_hint": "Concrete SQL approach with specific columns, functions, and clauses"
  }},
  ...
]
"""
        return prompt
    
    def _parse_and_validate(
        self,
        response: str,
        query: str,
        available_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Parse LLM response and validate interpretations."""
        
        # Clean response
        content = response.strip()
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        # Parse JSON
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}\nResponse: {content[:500]}")
            raise ValueError(f"Invalid JSON response from LLM: {e}")
        
        if not isinstance(parsed, list):
            raise ValueError("LLM response is not a JSON array")
        
        # Validate each interpretation
        valid_interpretations = []
        cols_lower = [c.lower() for c in available_columns]
        
        for idx, interp in enumerate(parsed):
            if not isinstance(interp, dict):
                logger.warning(f"Interpretation {idx} is not a dict, skipping")
                continue
            
            interpretation_text = interp.get('interpretation', '')
            reasoning = interp.get('reasoning', '')
            sql_hint = interp.get('sql_hint', '')
            
            if not interpretation_text or not reasoning or not sql_hint:
                logger.warning(f"Interpretation {idx} missing required fields, skipping")
                continue
            
            # Check if interpretation is too generic
            generic_phrases = [
                "show campaigns where",
                "list top campaigns",
                "above the overall median",
                "in the top 20%"
            ]
            if any(phrase in interpretation_text.lower() for phrase in generic_phrases):
                logger.warning(f"Interpretation {idx} is too generic, skipping: {interpretation_text[:100]}")
                continue
            
            # Check if interpretation relates to query
            query_words = set(query.lower().split())
            interp_words = set(interpretation_text.lower().split())
            overlap = len(query_words & interp_words)
            if overlap < 1:
                logger.warning(f"Interpretation {idx} doesn't relate to query, skipping")
                continue
            
            # Valid interpretation
            valid_interpretations.append({
                "interpretation": interpretation_text,
                "reasoning": reasoning,
                "sql_hint": sql_hint,
                "index": len(valid_interpretations)
            })
        
        return valid_interpretations
