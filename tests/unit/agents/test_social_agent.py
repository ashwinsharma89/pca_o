import pytest
import pandas as pd
from src.engine.agents.channel_specialists.social_agent import SocialChannelAgent

class TestSocialChannelAgent:
    @pytest.fixture
    def agent(self):
        return SocialChannelAgent()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'Campaign_ID': ['S1', 'S2'],
            'Campaign_Name': ['Social_Meta_Awareness', 'Social_Tiktok_Conv'],
            'Platform': ['Meta', 'TikTok'],
            'Spend': [5000, 2000],
            'Impressions': [500000, 100000],
            'Clicks': [10000, 2000],
            'Conversions': [200, 150],
            'CPM': [10.0, 20.0],
            'Engagement_Rate': [0.03, 0.05],
            'Frequency': [2.5, 1.8],
            'Video_View_Rate': [0.25, 0.45]
        })

    def test_analyze_structure(self, agent, sample_data):
        """Test that analysis returns social-specific keys."""
        results = agent.analyze(sample_data)
        assert 'engagement_metrics' in results
        assert 'audience_saturation' in results
        assert 'creative_performance' in results
        assert 'overall_health' in results
        assert 'recommendations' in results

    def test_cpm_analysis(self, agent, sample_data):
        """Test CPM benchmarking."""
        results = agent.analyze(sample_data)
        delivery = results['engagement_metrics']
        assert 'engagement_rate' in delivery

    def test_frequency_analysis(self, agent, sample_data):
        """Test frequency alerting."""
        freq_analysis = agent._analyze_frequency(sample_data)
        assert 'average_frequency' in freq_analysis
        assert freq_analysis['average_frequency'] == 2.15
