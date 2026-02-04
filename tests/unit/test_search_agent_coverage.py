"""
Tests for SearchChannelAgent (Phase C.2).
Verifies search-specific metrics like Quality Score and Match Types.
"""

import pytest
import pandas as pd
import numpy as np
from src.engine.agents.channel_specialists.search_agent import SearchChannelAgent, SearchBenchmarks

@pytest.fixture
def search_data():
    """Create mock search data for Google Ads."""
    return pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=14),
        'Platform': ['Google Ads'] * 14,
        'Spend': [100.0] * 14,
        'Impressions': [1000] * 14,
        'Clicks': [50] * 14,
        'Conversions': [5] * 14,
        'CTR': [0.05] * 14,
        'CPC': [2.0] * 14,
        'Quality_Score': [7.0] * 14,
        'Impression_Share': [0.8] * 14,
        'Keyword': ['keyword a'] * 14,
        'Match_Type': ['Exact'] * 14
    })

class TestSearchChannelAgent:
    """Unit tests for SearchChannelAgent."""

    def test_search_benchmarks(self):
        """Verify benchmark lookups."""
        assert SearchBenchmarks.get_benchmark("quality_score") == 7.0
        assert SearchBenchmarks.get_benchmark("ctr") == 0.035

    def test_analyze_quality_scores_low(self, search_data):
        """Verify detection of low Quality Scores."""
        agent = SearchChannelAgent()
        data = search_data.copy()
        data['Quality_Score'] = 4.0 # Threshold < 5
        
        result = agent._analyze_quality_scores(data)
        assert result['status'] == 'poor'
        assert "Low Quality Scores detected" in result['findings'][0]

    def test_analyze_auction_metrics_high_cpc(self, search_data):
        """Verify detection of high CPC vs benchmark."""
        agent = SearchChannelAgent()
        data = search_data.copy()
        # Benchmark is 2.50. Let's make it 4.0
        data['CPC'] = 4.0
        
        result = agent._analyze_auction_metrics(data)
        assert "above benchmark" in result['findings'][0]
        assert result['cpc_vs_benchmark'] > 0

    def test_analyze_keywords_concentration(self, search_data):
        """Verify keyword spend concentration detection."""
        agent = SearchChannelAgent()
        # Create 11 keywords, 1 takes 90% spend
        data = pd.DataFrame({
            'Keyword': [f'kw {i}' for i in range(11)],
            'Spend': [90] + [1]*10,
            'CTR': [0.05] * 11
        })
        result = agent._analyze_keywords(data)
        assert result['top_10_spend_concentration'] > 80
        assert "high concentration risk" in result['findings'][0]

    def test_analyze_match_types_broad_bias(self, search_data):
        """Verify warning for excessive broad match usage."""
        agent = SearchChannelAgent()
        data = search_data.copy()
        data['Match_Type'] = 'Broad'
        
        result = agent._analyze_match_types(data)
        assert result['distribution']['Broad'] == 1.0
        assert "broad match" in result['findings'][0]

    def test_overall_health_calculation(self):
        """Verify health score aggregation."""
        agent = SearchChannelAgent()
        insights = {
            'quality_score_analysis': {'status': 'excellent'},
            'impression_share_gaps': {'status': 'good'}
        }
        health = agent._calculate_overall_health(insights)
        assert health in ['excellent', 'good']
