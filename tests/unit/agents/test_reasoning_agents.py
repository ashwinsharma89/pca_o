"""
Unit Tests for Reasoning Agents

Tests for:
- PatternDetector from enhanced_reasoning_agent.py
- ReasoningAgent interface
- ValidatedReasoningAgent interface
- EnhancedReasoningAgent interface

FIXED: PatternDetector methods use underscore prefix (_detect_trends, etc.)
"""

import pytest
from unittest.mock import Mock, patch


# ============================================================================
# PATTERN DETECTOR TESTS (No LLM required)
# ============================================================================

class TestPatternDetectorFromEnhanced:
    """Unit tests for PatternDetector from enhanced_reasoning_agent."""
    
    @pytest.fixture
    def detector(self):
        """Create PatternDetector instance."""
        from src.engine.agents.enhanced_reasoning_agent import PatternDetector
        return PatternDetector()
    
    def test_initialization(self, detector):
        """Test PatternDetector initialization."""
        assert detector is not None
    
    def test_has_detect_trends_method(self, detector):
        """Test PatternDetector has _detect_trends method (underscore prefix)."""
        assert hasattr(detector, '_detect_trends')
    
    def test_has_detect_anomalies_method(self, detector):
        """Test PatternDetector has _detect_anomalies method."""
        assert hasattr(detector, '_detect_anomalies')
    
    def test_has_detect_creative_fatigue_method(self, detector):
        """Test PatternDetector has _detect_creative_fatigue method."""
        assert hasattr(detector, '_detect_creative_fatigue')
    
    def test_has_detect_audience_saturation_method(self, detector):
        """Test PatternDetector has _detect_audience_saturation method."""
        assert hasattr(detector, '_detect_audience_saturation')
    
    def test_has_detect_seasonality_method(self, detector):
        """Test PatternDetector has _detect_seasonality method."""
        assert hasattr(detector, '_detect_seasonality')
    
    def test_has_find_day_parting_method(self, detector):
        """Test PatternDetector has _find_day_parting method."""
        assert hasattr(detector, '_find_day_parting')
    
    def test_has_analyze_budget_pacing_method(self, detector):
        """Test PatternDetector has _analyze_budget_pacing method."""
        assert hasattr(detector, '_analyze_budget_pacing')
    
    def test_has_identify_performance_clusters_method(self, detector):
        """Test PatternDetector has _identify_performance_clusters method."""
        assert hasattr(detector, '_identify_performance_clusters')
    
    def test_has_detect_all_method(self, detector):
        """Test PatternDetector has detect_all method (public)."""
        assert hasattr(detector, 'detect_all')


# ============================================================================
# REASONING AGENT INTERFACE TESTS (Module-level, no instantiation needed)
# ============================================================================

class TestReasoningAgentInterface:
    """Test reasoning agent module exports."""
    
    def test_reasoning_agent_class_exists(self):
        """Test ReasoningAgent class can be imported."""
        from src.engine.agents.reasoning_agent import ReasoningAgent
        
        assert ReasoningAgent is not None
    
    def test_has_analyze_campaign_method(self):
        """Test ReasoningAgent has analyze_campaign in class definition."""
        from src.engine.agents.reasoning_agent import ReasoningAgent
        
        assert hasattr(ReasoningAgent, 'analyze_campaign')
    
    def test_has_call_llm_method(self):
        """Test ReasoningAgent has _call_llm method."""
        from src.engine.agents.reasoning_agent import ReasoningAgent
        
        assert hasattr(ReasoningAgent, '_call_llm')


class TestValidatedReasoningAgentInterface:
    """Test validated reasoning agent module exports."""
    
    def test_validated_reasoning_agent_class_exists(self):
        """Test ValidatedReasoningAgent class can be imported."""
        from src.engine.agents.validated_reasoning_agent import ValidatedReasoningAgent
        
        assert ValidatedReasoningAgent is not None


class TestEnhancedReasoningAgentInterface:
    """Test enhanced reasoning agent module exports."""
    
    def test_enhanced_reasoning_agent_class_exists(self):
        """Test EnhancedReasoningAgent class can be imported."""
        from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
        
        assert EnhancedReasoningAgent is not None
    
    def test_pattern_detector_class_exists(self):
        """Test PatternDetector class can be imported."""
        from src.engine.agents.enhanced_reasoning_agent import PatternDetector
        
        assert PatternDetector is not None


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
