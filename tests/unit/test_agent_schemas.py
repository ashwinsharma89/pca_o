"""
Tests for agent output validation schemas.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.engine.agents.schemas import (
    AgentInsight,
    AgentRecommendation,
    DetectedPattern,
    AgentMetadata,
    AgentOutput,
    VisionAgentMetric,
    VisionAgentOutput,
    ConfidenceLevel,
    PriorityLevel,
    MetricType,
    PatternType,
    filter_high_confidence_insights,
    filter_high_confidence_recommendations,
    filter_by_priority
)


class TestAgentInsight:
    """Test AgentInsight schema validation"""
    
    def test_valid_insight(self):
        """Test creating a valid insight"""
        insight = AgentInsight(
            text="CTR is declining over the past 7 days",
            confidence=0.85
        )
        
        assert insight.text == "CTR is declining over the past 7 days"
        assert insight.confidence == 0.85
        assert insight.confidence_level == ConfidenceLevel.HIGH
    
    def test_confidence_level_auto_set(self):
        """Test automatic confidence level setting"""
        high = AgentInsight(text="High confidence insight", confidence=0.9)
        assert high.confidence_level == ConfidenceLevel.HIGH
        
        medium = AgentInsight(text="Medium confidence insight", confidence=0.6)
        assert medium.confidence_level == ConfidenceLevel.MEDIUM
        
        low = AgentInsight(text="Low confidence insight", confidence=0.3)
        assert low.confidence_level == ConfidenceLevel.LOW
    
    def test_text_too_short(self):
        """Test validation fails for text too short"""
        with pytest.raises(ValidationError) as exc_info:
            AgentInsight(text="Too short", confidence=0.5)
        
        assert "at least 10 characters" in str(exc_info.value)
    
    def test_confidence_out_of_range(self):
        """Test validation fails for invalid confidence"""
        with pytest.raises(ValidationError):
            AgentInsight(text="Valid text here", confidence=1.5)
        
        with pytest.raises(ValidationError):
            AgentInsight(text="Valid text here", confidence=-0.1)
    
    def test_with_supporting_data(self):
        """Test insight with supporting data"""
        insight = AgentInsight(
            text="Spend increased by 25%",
            confidence=0.9,
            supporting_data={"spend_change": 0.25, "period": "7d"},
            pattern_type=PatternType.TREND
        )
        
        assert insight.supporting_data["spend_change"] == 0.25
        assert insight.pattern_type == PatternType.TREND


class TestAgentRecommendation:
    """Test AgentRecommendation schema validation"""
    
    def test_valid_recommendation(self):
        """Test creating a valid recommendation"""
        rec = AgentRecommendation(
            action="Increase budget by 20%",
            rationale="CTR is high and CPA is below target, indicating room for growth",
            priority=PriorityLevel.HIGH,
            confidence=0.85
        )
        
        assert rec.action == "Increase budget by 20%"
        assert rec.priority == PriorityLevel.HIGH
        assert rec.confidence == 0.85
    
    def test_priority_validation(self):
        """Test priority level validation"""
        # Valid priorities
        rec1 = AgentRecommendation(
            action="Critical action needed",
            rationale="This is very important for business",
            priority=1,
            confidence=0.9
        )
        assert rec1.priority == PriorityLevel.CRITICAL
        
        # Invalid priority
        with pytest.raises(ValidationError):
            AgentRecommendation(
                action="Invalid priority action",
                rationale="This has invalid priority",
                priority=10,
                confidence=0.5
            )
    
    def test_with_optional_fields(self):
        """Test recommendation with optional fields"""
        rec = AgentRecommendation(
            action="Pause underperforming campaigns",
            rationale="These campaigns have high CPA and low ROAS",
            priority=PriorityLevel.HIGH,
            confidence=0.75,
            expected_impact="Reduce wasted spend by $5000/month",
            estimated_effort="2 hours",
            category="budget_optimization"
        )
        
        assert rec.expected_impact == "Reduce wasted spend by $5000/month"
        assert rec.estimated_effort == "2 hours"
        assert rec.category == "budget_optimization"


class TestAgentOutput:
    """Test complete AgentOutput schema"""
    
    def test_valid_output(self):
        """Test creating valid agent output"""
        output = AgentOutput(
            insights=[
                AgentInsight(text="Insight 1 with sufficient length", confidence=0.8),
                AgentInsight(text="Insight 2 with sufficient length", confidence=0.7)
            ],
            recommendations=[
                AgentRecommendation(
                    action="Take this action now",
                    rationale="Because of these reasons that are long enough",
                    priority=2,
                    confidence=0.85
                )
            ],
            metadata=AgentMetadata(
                agent_name="TestAgent",
                data_points_analyzed=100
            ),
            overall_confidence=0.8
        )
        
        assert len(output.insights) == 2
        assert len(output.recommendations) == 1
        assert output.overall_confidence == 0.8
    
    def test_manual_confidence_override(self):
        """Test manually setting overall confidence"""
        output = AgentOutput(
            insights=[
                AgentInsight(text="High confidence insight here", confidence=0.9),
                AgentInsight(text="Medium confidence insight", confidence=0.6)
            ],
            recommendations=[
                AgentRecommendation(
                    action="High confidence action",
                    rationale="Because we have strong evidence for this",
                    confidence=0.8,
                    priority=1
                )
            ],
            metadata=AgentMetadata(agent_name="TestAgent"),
            overall_confidence=0.95  # Manually set higher than average
        )
        
        # Should use the manually set value
        assert output.overall_confidence == 0.95
    
    def test_empty_output_fails(self):
        """Test that completely empty output fails validation"""
        with pytest.raises(ValidationError) as exc_info:
            AgentOutput(
                insights=[],
                recommendations=[],
                metadata=AgentMetadata(agent_name="TestAgent"),
                overall_confidence=0.5
            )
        
        assert "at least one insight, recommendation, or pattern" in str(exc_info.value)
    
    def test_with_patterns(self):
        """Test output with detected patterns"""
        output = AgentOutput(
            insights=[],
            recommendations=[],
            patterns=[
                DetectedPattern(
                    pattern_type=PatternType.TREND,
                    description="Upward trend in conversions",
                    confidence=0.85
                )
            ],
            metadata=AgentMetadata(agent_name="PatternDetector"),
            overall_confidence=0.85
        )
        
        assert len(output.patterns) == 1
        assert output.patterns[0].pattern_type == PatternType.TREND


class TestVisionAgentOutput:
    """Test VisionAgent specific schemas"""
    
    def test_valid_vision_output(self):
        """Test creating valid vision agent output"""
        output = VisionAgentOutput(
            platform="Google Ads",
            metrics=[
                VisionAgentMetric(
                    name="Spend",
                    value=1000.50,
                    metric_type=MetricType.CURRENCY,
                    confidence=0.95,
                    unit="USD"
                ),
                VisionAgentMetric(
                    name="CTR",
                    value=2.5,
                    metric_type=MetricType.PERCENTAGE,
                    confidence=0.9,
                    unit="%"
                )
            ],
            overall_confidence=0.92
        )
        
        assert output.platform == "Google Ads"
        assert len(output.metrics) == 2
        assert output.metrics[0].value == 1000.50
    
    def test_no_metrics_fails(self):
        """Test that vision output without metrics fails"""
        with pytest.raises(ValidationError) as exc_info:
            VisionAgentOutput(
                platform="Meta Ads",
                metrics=[],
                overall_confidence=0.5
            )
        
        assert "at least one metric" in str(exc_info.value)


class TestFilterFunctions:
    """Test utility filter functions"""
    
    @pytest.fixture
    def sample_output(self):
        """Create sample agent output for testing"""
        return AgentOutput(
            insights=[
                AgentInsight(text="High confidence insight", confidence=0.9),
                AgentInsight(text="Medium confidence insight", confidence=0.6),
                AgentInsight(text="Low confidence insight", confidence=0.3)
            ],
            recommendations=[
                AgentRecommendation(
                    action="Critical action",
                    rationale="Very important reason here",
                    priority=1,
                    confidence=0.95
                ),
                AgentRecommendation(
                    action="Medium priority action",
                    rationale="Moderately important reason",
                    priority=3,
                    confidence=0.7
                ),
                AgentRecommendation(
                    action="Low priority action",
                    rationale="Nice to have improvement",
                    priority=5,
                    confidence=0.5
                )
            ],
            metadata=AgentMetadata(agent_name="TestAgent"),
            overall_confidence=0.7
        )
    
    def test_filter_high_confidence_insights(self, sample_output):
        """Test filtering insights by confidence"""
        high_conf = filter_high_confidence_insights(sample_output, threshold=0.8)
        
        assert len(high_conf) == 1
        assert high_conf[0].confidence == 0.9
    
    def test_filter_high_confidence_recommendations(self, sample_output):
        """Test filtering recommendations by confidence"""
        high_conf = filter_high_confidence_recommendations(sample_output, threshold=0.8)
        
        assert len(high_conf) == 1
        assert high_conf[0].confidence == 0.95
    
    def test_filter_by_priority(self, sample_output):
        """Test filtering recommendations by priority"""
        high_priority = filter_by_priority(sample_output, max_priority=2)
        
        assert len(high_priority) == 1
        assert high_priority[0].priority == PriorityLevel.CRITICAL
        
        medium_priority = filter_by_priority(sample_output, max_priority=3)
        assert len(medium_priority) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
