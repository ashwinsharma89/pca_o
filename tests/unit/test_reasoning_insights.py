"""
Tests for EnhancedReasoningAgent insight generation and recommendation logic.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def agent():
    """Create EnhancedReasoningAgent instance"""
    return EnhancedReasoningAgent()


@pytest.fixture
def sample_data():
    """Create sample campaign data"""
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


@pytest.fixture
def mock_patterns():
    """Create mock pattern detection results"""
    return {
        'trends': {
            'detected': True,
            'direction': 'improving',
            'description': '2 metrics improving',
            'metrics': {
                'ctr': {'slope': 0.001, 'r_squared': 0.85, 'direction': 'improving'}
            }
        },
        'anomalies': {
            'detected': False
        },
        'creative_fatigue': {
            'detected': True,
            'severity': 'high',
            'evidence': {
                'frequency': 8.5,
                'ctr_decline': -0.18,
                'recommendation': 'Refresh creative within 48 hours'
            }
        },
        'audience_saturation': {
            'detected': False
        },
        'day_parting_opportunities': {
            'detected': False
        },
        'seasonality': {
            'detected': False
        },
        'budget_pacing': {
            'detected': True,
            'status': 'optimal'
        },
        'performance_clusters': {
            'detected': False
        },
        'conversion_patterns': {
            'detected': False
        }
    }


@pytest.fixture
def mock_benchmarks():
    """Create mock benchmark data"""
    return {
        'benchmarks': {
            'ctr': {'poor': 0.02, 'good': 0.05, 'excellent': 0.08},
            'cpc': {'poor': 3.0, 'good': 2.0, 'excellent': 1.0},
            'conversion_rate': {'poor': 0.02, 'good': 0.05, 'excellent': 0.10}
        },
        'context': {
            'industry': 'ecommerce',
            'platform': 'google_search'
        }
    }


# ============================================================================
# Test Insight Generation
# ============================================================================

class TestInsightGeneration:
    """Test insight generation from patterns and data"""
    
    def test_generate_insights_structure(self, agent, sample_data):
        """Test that insights have correct structure"""
        result = agent.analyze(sample_data)
        
        assert 'insights' in result
        assert 'performance_summary' in result['insights']
        assert 'pattern_insights' in result['insights']
    
    def test_performance_summary(self, agent, sample_data):
        """Test performance summary generation"""
        summary = agent._summarize_performance(sample_data, None)
        
        assert 'total_spend' in summary
        assert 'total_impressions' in summary
        assert 'total_clicks' in summary
        assert 'total_conversions' in summary
        assert 'overall_ctr' in summary
        assert 'overall_conversion_rate' in summary
        
        # Verify calculations
        assert summary['total_spend'] > 0
        assert summary['overall_ctr'] > 0
        assert summary['overall_ctr'] < 1  # Should be a rate
    
    def test_interpret_patterns_with_trends(self, agent, mock_patterns):
        """Test pattern interpretation for trends"""
        insights = agent._interpret_patterns(mock_patterns)
        
        assert isinstance(insights, list)
        assert len(insights) > 0
        
        # Should have insight about improving trend
        trend_insight = next((i for i in insights if 'improving' in i.lower()), None)
        assert trend_insight is not None
    
    def test_interpret_patterns_with_creative_fatigue(self, agent, mock_patterns):
        """Test pattern interpretation for creative fatigue"""
        insights = agent._interpret_patterns(mock_patterns)
        
        # Should have insight about creative fatigue
        fatigue_insight = next((i for i in insights if 'creative fatigue' in i.lower()), None)
        assert fatigue_insight is not None
        assert 'refresh' in fatigue_insight.lower() or '48 hours' in fatigue_insight.lower()
    
    def test_benchmark_comparison(self, agent, sample_data, mock_benchmarks):
        """Test benchmark comparison logic"""
        comparison = agent._compare_to_benchmarks(sample_data, mock_benchmarks)
        
        # Should return comparison results
        assert isinstance(comparison, dict)
        
        # If CTR data exists, should have CTR comparison
        if 'ctr' in comparison:
            assert 'actual' in comparison['ctr']
            assert 'benchmark' in comparison['ctr']
            assert 'status' in comparison['ctr']


# ============================================================================
# Test Recommendation Generation
# ============================================================================

class TestRecommendationGeneration:
    """Test recommendation generation logic"""
    
    def test_generate_recommendations_structure(self, agent, sample_data):
        """Test that recommendations have correct structure"""
        result = agent.analyze(sample_data)
        
        assert 'recommendations' in result
        assert isinstance(result['recommendations'], list)
    
    def test_recommendations_from_creative_fatigue(self, agent, mock_patterns):
        """Test recommendations generated from creative fatigue"""
        insights = {'pattern_insights': []}
        recommendations = agent._generate_recommendations(
            insights, mock_patterns, None, None, None
        )
        
        # Should have recommendation for creative fatigue
        creative_rec = next((r for r in recommendations 
                            if r.get('category') == 'Creative'), None)
        
        assert creative_rec is not None
        assert creative_rec['priority'] == 'high'
        assert 'creative' in creative_rec['recommendation'].lower()
    
    def test_recommendation_priority_assignment(self, agent, mock_patterns):
        """Test that recommendations have proper priority levels"""
        insights = {'pattern_insights': []}
        recommendations = agent._generate_recommendations(
            insights, mock_patterns, None, None, None
        )
        
        for rec in recommendations:
            assert 'priority' in rec
            assert rec['priority'] in ['critical', 'high', 'medium', 'low', 'optional']
    
    def test_recommendation_has_expected_impact(self, agent, mock_patterns):
        """Test that recommendations include expected impact"""
        insights = {'pattern_insights': []}
        recommendations = agent._generate_recommendations(
            insights, mock_patterns, None, None, None
        )
        
        for rec in recommendations:
            assert 'expected_impact' in rec
            if rec['expected_impact']:
                assert rec['expected_impact'] in ['high', 'medium', 'low']
    
    def test_recommendations_from_benchmarks(self, agent, mock_benchmarks):
        """Test recommendations generated from benchmark comparisons"""
        insights = {
            'pattern_insights': [],
            'benchmark_comparison': {
                'ctr': {'actual': 0.03, 'benchmark': 0.05, 'status': 'needs_work'},
                'cpc': {'actual': 2.5, 'benchmark': 2.0, 'status': 'needs_work'}
            }
        }
        
        patterns = {'trends': {'detected': False}}
        
        recommendations = agent._generate_recommendations(
            insights, patterns, mock_benchmarks, None, None
        )
        
        # Should have recommendations for CTR and CPC improvements
        assert len(recommendations) > 0
        
        # Check for CTR recommendation
        ctr_rec = next((r for r in recommendations 
                       if 'ctr' in r.get('category', '').lower()), None)
        assert ctr_rec is not None


# ============================================================================
# Test RAG Integration
# ============================================================================

class TestRAGIntegration:
    """Test RAG retrieval for optimization strategies"""
    
    def test_build_optimization_query(self, agent, mock_patterns):
        """Test RAG query building from patterns"""
        insights = {}
        query = agent._build_optimization_query(insights, mock_patterns)
        
        assert isinstance(query, str)
        assert len(query) > 0
        
        # Should include pattern-specific terms
        if mock_patterns['creative_fatigue']['detected']:
            assert 'creative fatigue' in query.lower()
    
    @patch('src.engine.agents.enhanced_reasoning_agent.EnhancedReasoningAgent._generate_recommendations')
    def test_analyze_with_rag(self, mock_gen_recs, sample_data):
        """Test full analysis with RAG retriever"""
        # Create mock RAG retriever
        mock_rag = Mock()
        mock_rag.retrieve = Mock(return_value={
            'results': [
                {'content': 'Optimization strategy 1'},
                {'content': 'Optimization strategy 2'}
            ]
        })
        
        agent = EnhancedReasoningAgent(rag_retriever=mock_rag)
        mock_gen_recs.return_value = []
        
        result = agent.analyze(sample_data)
        
        # RAG should have been called if patterns detected
        # (may not be called if no patterns detected)
        assert 'optimization_context' in result


# ============================================================================
# Test End-to-End Analysis
# ============================================================================

class TestEndToEndAnalysis:
    """Test complete analysis workflow"""
    
    def test_full_analysis_workflow(self, agent, sample_data):
        """Test complete analysis from data to recommendations"""
        result = agent.analyze(sample_data)
        
        # Check all expected components
        assert 'insights' in result
        assert 'patterns' in result
        assert 'recommendations' in result
        assert 'analysis_timestamp' in result
        
        # Verify timestamp format
        timestamp = result['analysis_timestamp']
        datetime.fromisoformat(timestamp)  # Should not raise
    
    def test_analysis_with_channel_insights(self, agent, sample_data):
        """Test analysis with channel-specific insights"""
        channel_insights = {
            'platform': 'Google Ads',
            'insights': ['High quality score', 'Good ad relevance']
        }
        
        result = agent.analyze(sample_data, channel_insights=channel_insights)
        
        assert 'insights' in result
        assert 'channel_insights' in result['insights']
        assert result['insights']['channel_insights'] == channel_insights
    
    def test_analysis_handles_empty_data(self, agent):
        """Test that analysis handles edge cases gracefully"""
        empty_data = pd.DataFrame()
        
        # Should not crash
        result = agent.analyze(empty_data)
        
        assert 'insights' in result
        assert 'patterns' in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
