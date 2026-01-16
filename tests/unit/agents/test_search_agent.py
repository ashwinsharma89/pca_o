import pytest
import pandas as pd
import numpy as np
from src.engine.agents.channel_specialists.search_agent import SearchChannelAgent

class TestSearchChannelAgent:
    @pytest.fixture
    def agent(self):
        return SearchChannelAgent()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'Campaign_ID': ['C1', 'C2', 'C3'],
            'Campaign_Name': ['Search_Brand', 'Search_Generic', 'Search_Competitor'],
            'Platform': ['Google Ads', 'Google Ads', 'Google Ads'],
            'Spend': [1000, 2000, 500],
            'Impressions': [10000, 20000, 5000],
            'Clicks': [500, 600, 100],
            'Conversions': [50, 20, 5],
            'CTR': [0.05, 0.03, 0.02],
            'CPC': [2.0, 3.33, 5.0],
            'Quality_Score': [8, 5, 3],
            'Impression_Share': [0.9, 0.6, 0.4],
            'Match_Type': ['Exact', 'Phrase', 'Broad']
        })

    def test_analyze_structure(self, agent, sample_data):
        """Test that analysis returns expected keys."""
        results = agent.analyze(sample_data)
        assert 'quality_score_analysis' in results
        assert 'auction_insights' in results
        assert 'keyword_performance' in results
        assert 'overall_health' in results
        assert 'recommendations' in results

    def test_quality_score_analysis(self, agent, sample_data):
        """Test quality score logic."""
        qs_analysis = agent._analyze_quality_scores(sample_data)
        assert qs_analysis['average_score'] == 5.33
        assert 'low_qs_count' in qs_analysis
        assert qs_analysis['low_qs_count'] == 1 # Only index 2 has QS 3 < 5

    def test_calculate_overall_health(self, agent):
        """Test health calculation logic."""
        insights = {
            'a': {'status': 'excellent'},
            'b': {'status': 'good'}
        }
        health = agent._calculate_overall_health(insights)
        assert health == 'excellent'

        insights['c'] = {'status': 'poor'}
        health = agent._calculate_overall_health(insights)
        assert health in ['good', 'average'] # (4+3+1)/3 = 2.66 -> good

    def test_analyze_match_types(self, agent, sample_data):
        """Test match type distribution."""
        mt_analysis = agent._analyze_match_types(sample_data)
        assert 'Exact' in mt_analysis['distribution']
        assert mt_analysis['distribution']['Exact'] == pytest.approx(1/3)
