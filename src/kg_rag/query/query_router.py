"""
KG-RAG Query Router

Routes natural language queries to templates or LLM-based Cypher generation.
"""

import logging
import os
import re
from typing import Dict, Any, Optional, Tuple, List
from datetime import date, datetime, timedelta

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from src.kg_rag.query.intent_classifier import IntentClassifier, QueryIntent, IntentMatch
from src.kg_rag.client.connection import get_neo4j_connection, Neo4jConnection
from src.kg_rag.config.settings import get_kg_rag_settings

# Import template modules
from src.kg_rag.query.templates import cross_channel, platform, targeting, temporal, placement, anomaly, optimization


logger = logging.getLogger(__name__)


class QueryRouter:
    """
    Route natural language queries to appropriate handlers.
    
    Uses intent classification to route to:
    1. Pre-built Cypher templates (high confidence)
    2. LLM-based Cypher generation (low confidence / novel queries)
    
    Usage:
        router = QueryRouter()
        result = router.route("Compare Search vs Social ROAS")
        # Returns query results with metadata
    """
    
    def __init__(
        self,
        connection: Optional[Neo4jConnection] = None,
        confidence_threshold: float = 0.85
    ):
        """
        Initialize router.
        
        Args:
            connection: Neo4j connection
            confidence_threshold: Min confidence for template routing
        """
        self._conn = connection or get_neo4j_connection()
        self._settings = get_kg_rag_settings()
        self._classifier = IntentClassifier(confidence_threshold)
        self._default_date_range = 30  # days

        # Initialize LLM Clients
        self.gemini_available = False
        google_api_key = os.getenv("GOOGLE_API_KEY")
        if GEMINI_AVAILABLE and google_api_key:
            genai.configure(api_key=google_api_key)
            self.gemini_available = True
        
        self.openai_client = None
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if OPENAI_AVAILABLE and openai_api_key:
            self.openai_client = OpenAI(api_key=openai_api_key)
    
    def route(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Route a natural language query.
        
        Args:
            query: Natural language query
            context: Optional context (date range, filters, etc.)
            
        Returns:
            Dict with results, cypher, metadata
        """
        context = context or {}
        
        # Classify intent
        match = self._classifier.classify(query)
        logger.info(f"Query classified: {match.intent} (confidence: {match.confidence:.2f})")
        
        # Route based on classification
        # Route based on classification
        if self._classifier.should_use_template(match):
            if match.intent == QueryIntent.OPTIMIZATION:
                context.update(match.entities)
                return optimization.OptimizationTemplate().run(context)
            
            return self._route_to_template(query, match, context)
        else:
            return self._route_to_llm(query, match, context)
    
    def _route_to_template(
        self,
        query: str,
        match: IntentMatch,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route to a pre-built template."""
        try:
            cypher, params = self._get_template_query(query, match, context)
            
            if not cypher:
                # Fallback to LLM if no template available
                return self._route_to_llm(query, match, context)
            
            # Execute query
            results = self._conn.execute_query(cypher, params)
            
            return {
                "success": True,
                "results": results,
                "count": len(results),
                "query": query,
                "cypher": cypher,
                "params": params,
                "intent": match.intent.value,
                "confidence": match.confidence,
                "routing": "template",
                "entities": match.entities,
            }
            
        except Exception as e:
            logger.error(f"Template execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query,
                "intent": match.intent.value,
                "routing": "template",
            }
    
    
    def _route_to_llm(
        self,
        query: str,
        match: IntentMatch,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route to LLM for Cypher generation."""
        try:
            # Get schema info for context
            schema = self._conn.get_schema_info()
            
            # Construct prompt
            prompt = f"""
            You are a Neo4j Cypher expert. Generate a Cypher query for the following question.
            
            Schema:
            Node Labels: {schema['labels']}
            Relationship Types: {schema['relationship_types']}
            
            Campaign Node Properties: id, name, platform, channel, start_date, funnel, placement, ad_type
            Metric Node Properties: id, campaign_id, date, spend, impressions, clicks, conversions, revenue, 
                                   channel, platform, funnel, placement, ad_type, age_range, device_types, 
                                   geo_countries, audience_segment, creative_format, targeting_type
            Targeting Node Properties: id, campaign_id, audience_segment, device_types, age_range, geo_countries
            
            Channel Mapping Reference:
            - Social -> SOC
            - Search -> Search
            - Display -> DIS
            - Programmatic -> DIS
            - Connected TV -> CTV
            - Video -> Video
            - Retail Media -> Retail Media
            
            Question: {query}
            
            Rules:
            1. Return ONLY the Cypher query. No markdown, no explanations.
            2. Use proper casing for labels: Campaign, Metric, Targeting.
            3. Campaigns connect to Metrics via HAS_PERFORMANCE relationship.
            4. Metric nodes have spend, impressions, clicks, conversions, revenue as DIRECT NUMERIC properties.
            5. To aggregate metrics, use SUM(m.spend), SUM(m.impressions), etc.
            6. Always LIMIT results to avoid large responses.
            7. For performance queries, include calculated metrics:
               - ROAS = revenue / spend
               - CPA = spend / conversions
               - CTR = clicks / impressions * 100
            8. Handle division by zero with CASE WHEN statements.
            9. Prefer filtering on Metric node properties (m.channel, m.platform, m.funnel) rather than Campaign node properties.
            10. Date Handling (Neo4j 5.x):
                - Use `m.date` (of type Date) for temporal filtering.
                - To get calendar year, use `m.date.year` (e.g., `m.date.year = 2025`).
                - Only use `m.date.weekYear` if specifically doing week-based analysis.
                - To get week, use `m.date.week`.
                - To truncate to week/month, use `date.truncate('week', m.date)` or `date.truncate('month', m.date)`.
                - DO NOT use non-existent fields like `weekOfYear`.
            
            Example - Week on Week Comparison:
            MATCH (m:Metric)
            WHERE m.date >= date($p1_start) AND m.date <= date($p2_end)
            WITH m, 
                 CASE WHEN m.date.week % 2 = 0 THEN 'Week B' ELSE 'Week A' END AS period,
                 SUM(m.spend) AS spend, SUM(m.revenue) AS revenue
            RETURN period, spend, revenue, 
                   CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
            ORDER BY period
            
            Example - Monthly Trend:
            MATCH (m:Metric)
            WITH date.truncate('month', m.date) AS month, SUM(m.spend) AS spend
            RETURN month, spend ORDER BY month
            
            Example - High performance campaigns with calculated metrics:
            MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
            WITH c.name AS campaign,
                 SUM(m.spend) AS spend,
                 SUM(m.impressions) AS impressions,
                 SUM(m.clicks) AS clicks,
                 SUM(m.conversions) AS conversions,
                 SUM(m.revenue) AS revenue
            RETURN campaign, spend, conversions, revenue,
                   CASE WHEN spend > 0 THEN round(revenue / spend, 2) ELSE 0 END AS roas,
                   CASE WHEN conversions > 0 THEN round(spend / conversions, 2) ELSE 0 END AS cpa,
                   CASE WHEN impressions > 0 THEN round(clicks * 100.0 / impressions, 2) ELSE 0 END AS ctr,
                   CASE WHEN impressions > 0 THEN round(spend * 1000.0 / impressions, 2) ELSE 0 END AS cpm
            ORDER BY roas DESC
            LIMIT 10
            
            Example - Platform breakdown:
            MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
            RETURN m.platform AS platform, SUM(m.spend) AS spend, SUM(m.clicks) AS clicks
            ORDER BY spend DESC
            """
            
            cypher = ""
            
            # Try Gemini first (Free/Fast)
            if self.gemini_available:
                try:
                    model = genai.GenerativeModel('gemini-2.5-flash')
                    response = model.generate_content(prompt)
                    cypher = response.text.replace('```cypher', '').replace('```', '').strip()
                except Exception as e:
                    logger.warning(f"Gemini generation failed: {e}")
            
            # Fallback to OpenAI
            if not cypher and self.openai_client:
                try:
                    response = self.openai_client.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0
                    )
                    cypher = response.choices[0].message.content.replace('```cypher', '').replace('```', '').strip()
                except Exception as e:
                    logger.warning(f"OpenAI generation failed: {e}")

            if not cypher:
                return {
                    "success": False,
                    "error": "Failed to generate Cypher query from available LLMs",
                    "query": query,
                    "routing": "llm",
                }

            # Execute generated query
            logger.info(f"Executing LLM-generated Cypher: {cypher}")
            results = self._conn.execute_query(cypher)
            
            return {
                "success": True,
                "results": results,
                "count": len(results),
                "query": query,
                "cypher": cypher,
                "intent": match.intent.value,
                "confidence": match.confidence,
                "routing": "llm",
            }
            
        except Exception as e:
            logger.error(f"LLM routing failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query,
                "intent": match.intent.value,
                "routing": "llm",
            }
    
    def _get_template_query(
        self,
        query: str,
        match: IntentMatch,
        context: Dict[str, Any]
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        """Get the appropriate template query and parameters."""
        intent = match.intent
        entities = match.entities
        
        # Default date range - use data range from KG (2024-01-01 to 2025-01-31)
        # TODO: Query actual min/max dates from Neo4j for dynamic range
        date_to = context.get("date_to") or "2025-01-31"
        date_from = context.get("date_from") or "2024-01-01"
        
        # Route based on intent
        # Route based on intent
        if intent == QueryIntent.CROSS_CHANNEL:
            channels = entities.get("channel", [])
            if isinstance(channels, list) and len(channels) >= 2:
                c1 = self._normalize_channel(channels[0])
                c2 = self._normalize_channel(channels[1])
                return cross_channel.get_compare_channels_query(c1, c2)
            return cross_channel.get_all_channels_breakdown()
        
        elif intent == QueryIntent.PLATFORM_PERFORMANCE:
            platform_id = self._normalize_platform(entities.get("platform"))
            if platform_id:
                return platform.get_platform_overview(platform_id)
            return platform.get_all_platforms_comparison()
        
        elif intent == QueryIntent.TARGETING_ANALYSIS:
            device = entities.get("device")
            if device or "device" in query.lower():
                return targeting.get_device_breakdown()
            return targeting.get_age_breakdown()
        
        elif intent == QueryIntent.TEMPORAL_TREND:
            # Check for generic temporal comparison (period over period)
            query_lower = query.lower()
            
            # 1. Seasonal / YoY Check (Multi-year same month)
            if any(x in query_lower for x in ["yoy", "each year", "every year", "year over year"]):
                month_name = entities.get("month")
                if month_name:
                    month_num = {
                        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
                    }.get(month_name.lower())
                    if month_num:
                        return temporal.get_seasonal_comparison(month_num, month_name.capitalize())

            # 2. Period over Period Comparison
            if any(x in query_lower for x in ["compare", "comparison", " vs ", "wow", "mom", "week on week", "month on month"]):
                try:
                    # Calculate duration and previous period
                    # date_from and date_to are ISO strings (YYYY-MM-DD)
                    d_current_start = datetime.fromisoformat(date_from).date()
                    d_current_end = datetime.fromisoformat(date_to).date()
                    
                    # If specific WoW/MoM phrases exist, default to last 7/30 days comparison
                    if any(x in query_lower for x in ["wow", "week on week"]):
                        d_current_start = d_current_end - timedelta(days=6)
                    elif any(x in query_lower for x in ["mom", "month on month"]):
                        d_current_start = d_current_end - timedelta(days=29)
                    elif "last" in query_lower and entities.get("number"):
                        try:
                            n = int(entities.get("number"))
                            unit = 1 # days
                            if "week" in query_lower:
                                unit = 7
                            elif "month" in query_lower:
                                unit = 30
                            
                            total_days = n * unit
                            # Split into two equal periods
                            period_days = total_days // 2
                            d_current_start = d_current_end - timedelta(days=period_days - 1)
                        except:
                            pass
                    
                    d_prev_end = d_current_start - timedelta(days=1)
                    d_prev_start = d_prev_end - (d_current_end - d_current_start)
                    
                    return temporal.get_period_comparison(
                        d_prev_start.isoformat(),
                        d_prev_end.isoformat(),
                        d_current_start.isoformat(),
                        d_current_end.isoformat()
                    )
                except Exception as e:
                    logger.warning(f"Failed to calculate period comparison dates: {e}")
                    # Fallback to standard trend
            
            # Detect granularity from query
            query_lower = query.lower()
            if "day of week" in query_lower or "dayofweek" in query_lower:
                return temporal.get_day_of_week_analysis(date_from, date_to)
            elif "week" in query_lower:
                return temporal.get_weekly_trend(date_from, date_to)
            elif "month" in query_lower:
                return temporal.get_month_comparison(date_from, date_to)
            
            platform_id = self._normalize_platform(entities.get("platform"))
            if platform_id:
                return temporal.get_platform_trend(platform_id, date_from, date_to)
            return temporal.get_daily_spend_trend(date_from, date_to)
        
        elif intent == QueryIntent.PLACEMENT_ANALYSIS:
            return placement.get_placement_overview()
        
        elif intent == QueryIntent.ANOMALY_DETECTION:
            return anomaly.get_low_roas_campaigns()
        
        elif intent == QueryIntent.TOP_BOTTOM:
            platform_id = self._normalize_platform(entities.get("platform"))
            channel = entities.get("channel")
            limit = int(entities.get("number", 10))
            
            # Detect sorting preference
            sort_by = "performance"
            if "by spend" in query.lower():
                sort_by = "spend"
            
            if platform_id:
                return platform.get_platform_top_campaigns(platform_id, limit, sort_by=sort_by)
            elif channel:
                normalized_channel = self._normalize_channel(channel)
                return platform.get_channel_top_campaigns(normalized_channel, limit, sort_by=sort_by)
            return platform.get_global_top_campaigns(limit, sort_by=sort_by)
        
        elif intent == QueryIntent.BUDGET_ANALYSIS:
            return cross_channel.get_all_channels_breakdown()
        
        elif intent == QueryIntent.AGGREGATION:
            return cross_channel.get_all_channels_breakdown()
        
        return None, {}
    
    def _normalize_platform(self, platform: Optional[str]) -> Optional[str]:
        """Normalize platform name to ID."""
        if not platform:
            return None
        
        mappings = {
            "meta": "meta",
            "facebook": "meta",
            "fb": "meta",
            "google": "google_ads",
            "google ads": "google_ads",
            "linkedin": "linkedin",
            "tiktok": "tiktok",
            "dv360": "dv360",
            "youtube": "youtube",
            "snapchat": "snapchat",
            "pinterest": "pinterest",
        }
        
        return mappings.get(platform.lower(), platform.lower())

    def _normalize_channel(self, channel: str) -> str:
        """Normalize channel name to DB code."""
        if not channel:
            return channel
        
        mappings = {
            "social": "SOC",
            "display": "DIS",
            "search": "Search",
            "programmatic": "DIS",
            "connected tv": "CTV",
            "ctv": "CTV",
            "video": "Video",
            "ooh": "OOH",
            "retail media": "Retail Media",
            "email": "Email",
        }
        return mappings.get(channel.lower(), channel)
    
    def get_available_templates(self) -> List[Dict[str, Any]]:
        """Get list of available query templates."""
        return [
            {"intent": "cross_channel", "examples": ["Compare Search vs Social", "Channel breakdown"]},
            {"intent": "platform", "examples": ["Meta performance", "Google Ads campaigns"]},
            {"intent": "targeting", "examples": ["Device breakdown", "Age performance"]},
            {"intent": "temporal", "examples": ["Daily spend trend", "Weekly performance"]},
            {"intent": "placement", "examples": ["Top placements", "Site performance"]},
            {"intent": "anomaly", "examples": ["Low ROAS campaigns", "High CPC outliers"]},
            {"intent": "ranking", "examples": ["Top 10 campaigns", "Best performing ads"]},
        ]
