"""
Integration tests for validated agents and resilience mechanisms.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio

from src.engine.agents.validated_reasoning_agent import ValidatedReasoningAgent
from src.engine.agents.resilient_orchestrator import ResilientMultiAgentOrchestrator
from src.engine.agents.schemas import AgentOutput, AgentInsight, AgentRecommendation


@pytest.fixture
def sample_campaign_data():
    """Create sample campaign data for testing"""
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'Date': dates,
        'Spend': np.random.uniform(1000, 2000, 30),
        'Impressions': np.random.uniform(10000, 20000, 30),
        'Clicks': np.random.uniform(500, 1000, 30),
        'Conversions': np.random.uniform(50, 100, 30),
        'CTR': np.random.uniform(0.04, 0.06, 30),
        'CPC': np.random.uniform(1.5, 2.5, 30),
        'Campaign': ['Campaign_A'] * 30,
        'Platform': ['Google Ads'] * 30
    })


class TestValidatedReasoningAgent:
    """Test validated reasoning agent"""
    
    @pytest.mark.asyncio
    async def test_analyze_returns_agent_output(self, sample_campaign_data):
        """Test that analyze returns AgentOutput schema"""
        agent = ValidatedReasoningAgent()
        
        result = await agent.analyze(sample_campaign_data)
        
        # Should return AgentOutput instance
        assert isinstance(result, AgentOutput)
        assert hasattr(result, 'insights')
        assert hasattr(result, 'recommendations')
        assert hasattr(result, 'metadata')
    
    @pytest.mark.asyncio
    async def test_output_has_confidence_scores(self, sample_campaign_data):
        """Test that output includes confidence scores"""
        agent = ValidatedReasoningAgent()
        
        result = await agent.analyze(sample_campaign_data)
        
        # Overall confidence should be present
        assert 0.0 <= result.overall_confidence <= 1.0
        
        # Individual insights should have confidence
        if result.insights:
            for insight in result.insights:
                assert 0.0 <= insight.confidence <= 1.0
                assert insight.confidence_level is not None
    
    @pytest.mark.asyncio
    async def test_backward_compatibility(self, sample_campaign_data):
        """Test backward compatibility with dict output"""
        agent = ValidatedReasoningAgent()
        
        # Request dict output
        result = await agent.analyze(sample_campaign_data, return_validated=False)
        
        # Should return dict
        assert isinstance(result, dict)
        assert 'insights' in result
        assert 'recommendations' in result


class TestResilientOrchestrator:
    """Test resilient orchestrator"""
    
    @pytest.mark.asyncio
    async def test_orchestrator_initialization(self):
        """Test orchestrator initializes correctly"""
        orchestrator = ResilientMultiAgentOrchestrator()
        
        assert orchestrator.reasoning_agent is not None
        assert orchestrator.base_orchestrator is not None
    
    @pytest.mark.asyncio
    async def test_analyze_with_reasoning(self, sample_campaign_data):
        """Test direct reasoning analysis"""
        orchestrator = ResilientMultiAgentOrchestrator()
        
        result = await orchestrator.analyze_with_reasoning(sample_campaign_data)
        
        # Should return validated output
        assert isinstance(result, AgentOutput)
        assert result.metadata.agent_name == "EnhancedReasoningAgent"
    
    @pytest.mark.asyncio
    async def test_health_status(self):
        """Test health status reporting"""
        orchestrator = ResilientMultiAgentOrchestrator()
        
        health = orchestrator.get_health_status()
        
        assert 'orchestrator' in health
        assert 'reasoning_agent' in health
        assert health['orchestrator'] == 'healthy'
    
    @pytest.mark.asyncio
    async def test_resilience_stats(self, sample_campaign_data):
        """Test resilience statistics tracking"""
        orchestrator = ResilientMultiAgentOrchestrator()
        
        # Run analysis
        await orchestrator.analyze_with_reasoning(sample_campaign_data)
        
        # Get stats
        stats = orchestrator._get_resilience_stats()
        
        assert 'reasoning_agent' in stats
        assert 'total_executions' in stats['reasoning_agent']
        assert stats['reasoning_agent']['total_executions'] >= 1


class TestOutputValidation:
    """Test output validation"""
    
    @pytest.mark.asyncio
    async def test_insights_are_validated(self, sample_campaign_data):
        """Test that insights pass validation"""
        agent = ValidatedReasoningAgent()
        result = await agent.analyze(sample_campaign_data)
        
        for insight in result.insights:
            # Should have required fields
            assert len(insight.text) >= 10
            assert 0.0 <= insight.confidence <= 1.0
            assert insight.timestamp is not None
    
    @pytest.mark.asyncio
    async def test_recommendations_are_validated(self, sample_campaign_data):
        """Test that recommendations pass validation"""
        agent = ValidatedReasoningAgent()
        result = await agent.analyze(sample_campaign_data)
        
        for rec in result.recommendations:
            # Should have required fields
            assert len(rec.action) >= 10
            assert len(rec.rationale) >= 20
            assert 1 <= rec.priority <= 5
            assert 0.0 <= rec.confidence <= 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
