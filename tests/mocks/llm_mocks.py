"""
Mock LLM Responses for Agent Testing

Provides deterministic LLM responses for testing without API costs.
"""

from typing import Dict, Any, List
from datetime import datetime


class MockLLMResponse:
    """Mock LLM responses for testing agent intelligence"""
    
    # Pattern Detection Mock Responses
    PATTERN_RESPONSES = {
        "creative_fatigue": {
            "pattern_type": "CREATIVE_FATIGUE",
            "description": "CTR declining 35% over 14 days despite stable impressions",
            "confidence": 0.85,
            "severity": "high",
            "metrics": {
                "ctr_decline_pct": -35.2,
                "days_active": 14,
                "impression_stability": 0.95,
                "initial_ctr": 2.8,
                "current_ctr": 1.8
            },
            "evidence": {
                "frequency": 7.2,
                "ctr_decline": -0.35,
                "recommendation": "Refresh creative within 48 hours - CTR declining significantly"
            }
        },
        "audience_saturation": {
            "pattern_type": "AUDIENCE_SATURATION",
            "description": "CPA increasing 42% while reach plateaus at 4.8 frequency",
            "confidence": 0.78,
            "severity": "high",
            "metrics": {
                "cpa_increase_pct": 42.1,
                "reach_growth_pct": 2.3,
                "frequency": 4.8,
                "saturation_score": 0.82
            },
            "recommendation": "Expand audience targeting or test new audience segments"
        },
        "trend_declining": {
            "pattern_type": "TREND",
            "direction": "declining",
            "description": "3 key metrics showing downward trend",
            "confidence": 0.72,
            "metrics": {
                "ctr_slope": -0.05,
                "cpc_slope": 0.12,
                "conv_rate_slope": -0.03
            }
        },
        "anomaly_spike": {
            "pattern_type": "ANOMALY",
            "description": "Unusual spike in CPC detected",
            "confidence": 0.88,
            "severity": "medium",
            "anomalies": [
                {
                    "metric": "CPC",
                    "count": 3,
                    "severity": "medium",
                    "z_score": 3.2
                }
            ]
        }
    }
    
    # Insight Generation Mock Responses
    INSIGHT_RESPONSES = {
        "creative_fatigue": {
            "text": "Ad creative showing signs of fatigue with 35% CTR decline over 14 days",
            "confidence": 0.85,
            "confidence_level": "high",
            "supporting_data": {
                "initial_ctr": 2.8,
                "current_ctr": 1.8,
                "decline_rate": -0.078,  # per day
                "frequency": 7.2
            },
            "pattern_type": "CREATIVE_FATIGUE",
            "timestamp": datetime.utcnow().isoformat()
        },
        "audience_saturation": {
            "text": "Audience saturation detected: CPA increased 42% while reach plateaued",
            "confidence": 0.78,
            "confidence_level": "high",
            "supporting_data": {
                "cpa_increase": 42.1,
                "reach_growth": 2.3,
                "frequency": 4.8
            },
            "pattern_type": "AUDIENCE_SATURATION",
            "timestamp": datetime.utcnow().isoformat()
        },
        "performance_declining": {
            "text": "Overall performance declining across 3 key metrics",
            "confidence": 0.72,
            "confidence_level": "medium",
            "supporting_data": {
                "declining_metrics": ["CTR", "Conversion Rate"],
                "improving_metrics": [],
                "stable_metrics": ["Impressions"]
            },
            "pattern_type": "TREND",
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    # Recommendation Mock Responses
    RECOMMENDATION_RESPONSES = {
        "creative_fatigue": {
            "action": "Refresh ad creative with new images and copy variations",
            "rationale": "CTR decline of 35% indicates audience fatigue; new creative can restore engagement and improve performance",
            "priority": 2,  # HIGH
            "confidence": 0.82,
            "expected_impact": "15-25% CTR improvement within 3-5 days",
            "estimated_effort": "2-3 hours for creative development",
            "category": "creative_optimization",
            "timestamp": datetime.utcnow().isoformat()
        },
        "audience_saturation": {
            "action": "Expand audience targeting with lookalike audiences or new interest segments",
            "rationale": "Audience saturation (4.8 frequency, 42% CPA increase) indicates need for fresh audience reach",
            "priority": 2,  # HIGH
            "confidence": 0.75,
            "expected_impact": "20-30% CPA reduction, 40-60% reach increase",
            "estimated_effort": "1-2 hours for audience setup",
            "category": "audience_expansion",
            "timestamp": datetime.utcnow().isoformat()
        },
        "budget_reallocation": {
            "action": "Shift 30% of budget from low-performing campaigns to top performers",
            "rationale": "Performance gap of 3.2x ROAS between top and bottom campaigns",
            "priority": 1,  # CRITICAL
            "confidence": 0.88,
            "expected_impact": "25-35% overall ROAS improvement",
            "estimated_effort": "30 minutes for budget adjustment",
            "category": "budget_optimization",
            "timestamp": datetime.utcnow().isoformat()
        },
        "dayparting_optimization": {
            "action": "Increase bids 20% during peak hours (9-11 AM, 7-9 PM), decrease 30% during low-performing hours",
            "rationale": "Conversion rate varies 45% across different hours of the day",
            "priority": 3,  # MEDIUM
            "confidence": 0.71,
            "expected_impact": "10-15% CPA improvement",
            "estimated_effort": "1 hour for bid schedule setup",
            "category": "scheduling_optimization",
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    @classmethod
    def get_pattern_response(cls, pattern_type: str) -> Dict[str, Any]:
        """Get mock pattern detection response"""
        return cls.PATTERN_RESPONSES.get(pattern_type, {
            "pattern_type": "UNKNOWN",
            "description": "No pattern detected",
            "confidence": 0.0,
            "detected": False
        })
    
    @classmethod
    def get_insight_response(cls, pattern_type: str) -> Dict[str, Any]:
        """Get mock insight generation response"""
        return cls.INSIGHT_RESPONSES.get(pattern_type, {
            "text": "No specific insights available",
            "confidence": 0.5,
            "confidence_level": "low",
            "supporting_data": {},
            "pattern_type": "NONE"
        })
    
    @classmethod
    def get_recommendation_response(cls, category: str) -> Dict[str, Any]:
        """Get mock recommendation response"""
        return cls.RECOMMENDATION_RESPONSES.get(category, {
            "action": "Monitor performance and gather more data",
            "rationale": "Insufficient data for specific recommendation",
            "priority": 5,  # OPTIONAL
            "confidence": 0.4,
            "expected_impact": "Unknown",
            "estimated_effort": "Minimal",
            "category": "monitoring"
        })
    
    @classmethod
    def get_all_patterns(cls) -> List[str]:
        """Get list of all available pattern types"""
        return list(cls.PATTERN_RESPONSES.keys())
    
    @classmethod
    def get_all_insights(cls) -> List[str]:
        """Get list of all available insight types"""
        return list(cls.INSIGHT_RESPONSES.keys())
    
    @classmethod
    def get_all_recommendations(cls) -> List[str]:
        """Get list of all available recommendation types"""
        return list(cls.RECOMMENDATION_RESPONSES.keys())


class MockVisionResponse:
    """Mock vision agent responses for image analysis"""
    
    VISION_RESPONSES = {
        "facebook_ads_manager": {
            "platform": "Facebook Ads Manager",
            "metrics": [
                {"name": "Impressions", "value": 125430, "metric_type": "COUNT", "confidence": 0.95},
                {"name": "Clicks", "value": 3521, "metric_type": "COUNT", "confidence": 0.93},
                {"name": "CTR", "value": 2.81, "metric_type": "PERCENTAGE", "confidence": 0.91},
                {"name": "Spend", "value": 1250.50, "metric_type": "CURRENCY", "confidence": 0.96, "unit": "USD"},
                {"name": "CPC", "value": 0.36, "metric_type": "CURRENCY", "confidence": 0.89, "unit": "USD"}
            ],
            "charts": [
                {
                    "chart_type": "line",
                    "title": "Performance Over Time",
                    "data_points": [
                        {"date": "2024-01-01", "impressions": 10000, "clicks": 280},
                        {"date": "2024-01-02", "impressions": 12000, "clicks": 336}
                    ],
                    "confidence": 0.87
                }
            ],
            "overall_confidence": 0.92
        },
        "google_ads_dashboard": {
            "platform": "Google Ads",
            "metrics": [
                {"name": "Impressions", "value": 89234, "metric_type": "COUNT", "confidence": 0.94},
                {"name": "Clicks", "value": 2156, "metric_type": "COUNT", "confidence": 0.92},
                {"name": "Conversions", "value": 127, "metric_type": "COUNT", "confidence": 0.88},
                {"name": "Cost", "value": 892.30, "metric_type": "CURRENCY", "confidence": 0.95, "unit": "USD"}
            ],
            "overall_confidence": 0.91
        }
    }
    
    @classmethod
    def get_vision_response(cls, platform: str) -> Dict[str, Any]:
        """Get mock vision analysis response"""
        return cls.VISION_RESPONSES.get(platform, {
            "platform": "Unknown",
            "metrics": [],
            "charts": [],
            "overall_confidence": 0.0
        })
