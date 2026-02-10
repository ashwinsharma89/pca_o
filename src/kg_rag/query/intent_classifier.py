"""
KG-RAG Intent Classifier

Classifies natural language queries into intent categories.
Uses keyword matching and pattern recognition for routing.
"""

import re
import logging
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


logger = logging.getLogger(__name__)


class QueryIntent(str, Enum):
    """Query intent categories."""
    CROSS_CHANNEL = "cross_channel"      # Compare channels
    PLATFORM_PERFORMANCE = "platform"     # Platform-specific queries
    CAMPAIGN_DETAIL = "campaign"          # Single campaign details
    TARGETING_ANALYSIS = "targeting"      # Targeting dimension analysis
    TEMPORAL_TREND = "temporal"           # Time-series / trends
    PLACEMENT_ANALYSIS = "placement"      # Placement performance
    KEYWORD_ANALYSIS = "keyword"          # Search keyword queries
    AUDIENCE_OVERLAP = "audience"         # Audience analysis
    CREATIVE_COMPARISON = "creative"      # Creative performance
    ANOMALY_DETECTION = "anomaly"         # Outliers / anomalies
    BUDGET_ANALYSIS = "budget"            # Budget / spend analysis
    TOP_BOTTOM = "ranking"                # Top/Bottom N queries
    AGGREGATION = "aggregation"           # General aggregation
    OPTIMIZATION = "optimization"         # Optimization insights
    UNKNOWN = "unknown"                   # Fallback to LLM


@dataclass
class IntentMatch:
    """Result of intent classification."""
    intent: QueryIntent
    confidence: float
    entities: Dict[str, Any]
    matched_patterns: List[str]


class IntentClassifier:
    """
    Classify natural language queries into intent categories.
    
    Uses pattern matching for high-confidence routing to templates.
    Lower confidence queries fall back to LLM-based Cypher generation.
    
    Usage:
        classifier = IntentClassifier()
        result = classifier.classify("Compare Search vs Social ROAS")
        # IntentMatch(intent=CROSS_CHANNEL, confidence=0.95, ...)
    """
    
    # Intent patterns: (regex, intent, base_confidence)
    INTENT_PATTERNS = [
        # Cross-channel comparisons
        (r"compare\s+(\w+)\s+(vs|versus|and)\s+(\w+)", QueryIntent.CROSS_CHANNEL, 0.9),
        (r"(search|social|display|programmatic)\s+vs\s+", QueryIntent.CROSS_CHANNEL, 0.9),
        (r"channel\s+(comparison|performance|breakdown)", QueryIntent.CROSS_CHANNEL, 0.85),
        (r"across\s+(all\s+)?channels", QueryIntent.CROSS_CHANNEL, 0.8),
        
        # Platform performance
        (r"(meta|google|facebook|linkedin|tiktok|dv360|youtube)\s+(performance|campaigns?|ads?)", QueryIntent.PLATFORM_PERFORMANCE, 0.9),
        (r"on\s+(meta|google|facebook|linkedin|tiktok)", QueryIntent.PLATFORM_PERFORMANCE, 0.85),
        (r"(facebook|instagram|google\s*ads?|linkedin\s*ads?)", QueryIntent.PLATFORM_PERFORMANCE, 0.8),
        
        # Campaign details
        (r"campaign\s+(named?|called|id)\s+['\"]?(\w+)", QueryIntent.CAMPAIGN_DETAIL, 0.95),
        (r"show\s+me\s+campaign", QueryIntent.CAMPAIGN_DETAIL, 0.8),
        (r"(details?|info|information)\s+(for|about|on)\s+campaign", QueryIntent.CAMPAIGN_DETAIL, 0.85),
        
        # Targeting analysis
        (r"(mobile|desktop|tablet|device)\s+(performance|targeting|breakdown)", QueryIntent.TARGETING_ANALYSIS, 0.9),
        (r"(age|gender|demographic|location|geo)\s+(breakdown|analysis|performance)", QueryIntent.TARGETING_ANALYSIS, 0.9),
        (r"targeting\s+(by|based\s+on)", QueryIntent.TARGETING_ANALYSIS, 0.85),
        (r"(audience|segment)\s+performance", QueryIntent.TARGETING_ANALYSIS, 0.85),
        
        # Temporal trends
        (r"(trend|trends|over\s+time|daily|weekly|monthly)", QueryIntent.TEMPORAL_TREND, 0.9),
        (r"(week\s+on\s+week|month\s+on\s+month|year\s+on\s+year|wow|mom|yoy|each\s+year|every\s+year|year\s+over\s+year)", QueryIntent.TEMPORAL_TREND, 0.95),
        (r"(last|past)\s+(\d+)\s+(?:days?|weeks?|months?)", QueryIntent.TEMPORAL_TREND, 0.85),
        (r"(q[1-4]|quarter|ytd|mtd|this\s+month|this\s+week)", QueryIntent.TEMPORAL_TREND, 0.85),
        (r"(january|february|march|april|may|june|july|august|september|october|november|december)", QueryIntent.TEMPORAL_TREND, 0.9),
        
        # Placement analysis
        (r"placement(s)?\s+(performance|analysis|breakdown)", QueryIntent.PLACEMENT_ANALYSIS, 0.95),
        (r"(site|app|publisher)\s+performance", QueryIntent.PLACEMENT_ANALYSIS, 0.9),
        (r"(best|worst|top|bottom)\s+placement", QueryIntent.PLACEMENT_ANALYSIS, 0.9),
        
        # Keyword analysis
        (r"keyword(s)?\s+(performance|analysis|breakdown)", QueryIntent.KEYWORD_ANALYSIS, 0.95),
        (r"(search\s+)?term(s)?\s+performance", QueryIntent.KEYWORD_ANALYSIS, 0.9),
        (r"(best|top|converting)\s+keywords?", QueryIntent.KEYWORD_ANALYSIS, 0.9),
        
        # Audience
        (r"audience\s+overlap", QueryIntent.AUDIENCE_OVERLAP, 0.95),
        (r"segment\s+(overlap|comparison)", QueryIntent.AUDIENCE_OVERLAP, 0.9),
        
        # Creative
        (r"(creative|ad)\s+(performance|comparison|analysis)", QueryIntent.CREATIVE_COMPARISON, 0.9),
        (r"(video|image|carousel)\s+(ads?|creatives?)\s+performance", QueryIntent.CREATIVE_COMPARISON, 0.9),
        (r"(best|top|worst)\s+(performing\s+)?(ads?|creatives?)", QueryIntent.CREATIVE_COMPARISON, 0.85),
        
        # Anomalies
        (r"(unusual|anomal|outlier|spike|drop)", QueryIntent.ANOMALY_DETECTION, 0.9),
        (r"(unusually|abnormally)\s+(high|low)", QueryIntent.ANOMALY_DETECTION, 0.9),
        (r"(something\s+)?wrong\s+with", QueryIntent.ANOMALY_DETECTION, 0.7),
        
        # Budget
        (r"(budget|spend|spending)\s+(analysis|breakdown|allocation)", QueryIntent.BUDGET_ANALYSIS, 0.9),
        (r"(over|under)\s*budget", QueryIntent.BUDGET_ANALYSIS, 0.9),
        (r"(pacing|pace)\s+(report|analysis)", QueryIntent.BUDGET_ANALYSIS, 0.85),
        
        # Optimization
        (r"(what\s+performed|what\s+works|best\s+performing)", QueryIntent.OPTIMIZATION, 0.95),
        (r"(optimization|optimize|recommendation|insight)", QueryIntent.OPTIMIZATION, 0.95),
        (r"(scale|cut|pause|increase)\s+(budget|spend)", QueryIntent.OPTIMIZATION, 0.9),
        (r"(what|which)\s+.*(not\s+working|underperforming|performing\s+badly|poorly)", QueryIntent.OPTIMIZATION, 0.9),
        
        # Top/Bottom ranking
        (r"(top|bottom|best|worst)\s+(\d+)", QueryIntent.TOP_BOTTOM, 0.9),
        (r"(top|bottom|best|worst)\s+.*(campaign|platform|ad|channel|funnel|performance)", QueryIntent.TOP_BOTTOM, 0.8),
        (r"(highest|lowest|most|least)\s+\w+\s+(campaigns?|platforms?)", QueryIntent.TOP_BOTTOM, 0.85),
        (r"(rank|ranking|ranked)", QueryIntent.TOP_BOTTOM, 0.8),
        
        # General aggregation
        (r"(total|sum|average|avg|mean)\s+(spend|impressions|clicks|conversions|revenue)", QueryIntent.AGGREGATION, 0.8),
        (r"(overall|aggregate)\s+(performance|metrics)", QueryIntent.AGGREGATION, 0.8),
    ]
    
    # Entity extraction patterns
    ENTITY_PATTERNS = {
        "platform": r"(meta|google|facebook|linkedin|tiktok|dv360|youtube|snapchat|pinterest|cm360|bing)",
        "channel": r"(search|social|display|programmatic)",
        "metric": r"(ctr|cpc|cpm|cpa|roas|spend|impressions|clicks|conversions|revenue|reach)",
        "device": r"(mobile|desktop|tablet|all\s+devices)",
        "dimension": r"(age|gender|geo|location|placement|device|funnel|demographic|creative|audience)",
        "month": r"(january|february|march|april|may|june|july|august|september|october|november|december)",
        "time_period": r"(today|yesterday|last\s+\d+\s+(?:days?|weeks?|months?)|this\s+week|last\s+week|this\s+month|last\s+month|q[1-4]|ytd)",
        "number": r"(\d+)",
        "comparison": r"(vs|versus|compared\s+to|against)",
    }
    
    def __init__(self, confidence_threshold: float = 0.85):
        """
        Initialize classifier.
        
        Args:
            confidence_threshold: Minimum confidence for template routing
        """
        self.confidence_threshold = confidence_threshold
        self._compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), intent, conf)
            for pattern, intent, conf in self.INTENT_PATTERNS
        ]
        self._entity_patterns = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in self.ENTITY_PATTERNS.items()
        }
    
    def classify(self, query: str) -> IntentMatch:
        """
        Classify a natural language query.
        
        Args:
            query: Natural language query string
            
        Returns:
            IntentMatch with intent, confidence, and extracted entities
        """
        query = query.strip()
        
        # Find all matching patterns
        matches: List[Tuple[QueryIntent, float, str]] = []
        
        for pattern, intent, base_confidence in self._compiled_patterns:
            match = pattern.search(query)
            if match:
                # Boost confidence for longer matches
                match_len = len(match.group())
                length_boost = min(match_len / len(query), 0.1)
                confidence = min(base_confidence + length_boost, 0.99)
                matches.append((intent, confidence, pattern.pattern))
        
        # Extract entities
        entities = self._extract_entities(query)
        
        # If no matches, return unknown
        if not matches:
            return IntentMatch(
                intent=QueryIntent.UNKNOWN,
                confidence=0.0,
                entities=entities,
                matched_patterns=[]
            )
        
        # Get best match (highest confidence)
        best_intent, best_confidence, best_pattern = max(matches, key=lambda x: x[1])
        
        # Collect all matched patterns for debugging
        matched_patterns = [m[2] for m in matches]
        
        return IntentMatch(
            intent=best_intent,
            confidence=best_confidence,
            entities=entities,
            matched_patterns=matched_patterns[:5]  # Limit for readability
        )
    
    def _extract_entities(self, query: str) -> Dict[str, Any]:
        """Extract entities from query."""
        entities = {}
        
        for entity_name, pattern in self._entity_patterns.items():
            matches = pattern.findall(query)
            if matches:
                if len(matches) == 1:
                    entities[entity_name] = matches[0].lower()
                else:
                    entities[entity_name] = [m.lower() for m in matches]
        
        return entities
    
    def should_use_template(self, match: IntentMatch) -> bool:
        """
        Determine if query should use a template vs LLM.
        
        Args:
            match: Intent classification result
            
        Returns:
            True if template should be used
        """
        if match.intent == QueryIntent.UNKNOWN:
            return False
        return match.confidence >= self.confidence_threshold
    
    def get_suggested_template(self, match: IntentMatch) -> Optional[str]:
        """
        Get suggested template name for intent.
        
        Args:
            match: Intent classification result
            
        Returns:
            Template name or None
        """
        template_map = {
            QueryIntent.CROSS_CHANNEL: "compare_channels",
            QueryIntent.PLATFORM_PERFORMANCE: "platform_performance",
            QueryIntent.CAMPAIGN_DETAIL: "campaign_details",
            QueryIntent.TARGETING_ANALYSIS: "targeting_breakdown",
            QueryIntent.TEMPORAL_TREND: "temporal_trend",
            QueryIntent.PLACEMENT_ANALYSIS: "placement_performance",
            QueryIntent.KEYWORD_ANALYSIS: "keyword_performance",
            QueryIntent.AUDIENCE_OVERLAP: "audience_overlap",
            QueryIntent.CREATIVE_COMPARISON: "creative_comparison",
            QueryIntent.ANOMALY_DETECTION: "anomaly_detection",
            QueryIntent.BUDGET_ANALYSIS: "budget_analysis",
            QueryIntent.TOP_BOTTOM: "top_bottom_ranking",
            QueryIntent.AGGREGATION: "aggregate_metrics",
            QueryIntent.OPTIMIZATION: "optimization_analysis",
        }
        
        return template_map.get(match.intent)
