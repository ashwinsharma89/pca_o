"""
Tests for SocialChannelAgent (Phase C.2).
Verifies social-specific metrics and fatigue detection.
"""

import pytest
import pandas as pd
import numpy as np
from src.engine.agents.channel_specialists.social_agent import SocialChannelAgent, SocialBenchmarks

@pytest.fixture
def social_data():
    """Create mock social data for Meta."""
    return pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=14),
        'Platform': ['Meta Ads'] * 14,
        'Spend': [100.0] * 14,
        'Impressions': [10000] * 14,
        'Clicks': [100] * 14,
        'Likes': [50] * 14,
        'Comments': [10] * 14,
        'Shares': [5] * 14,
        'Frequency': [2.0] * 14,
        'CTR': [0.01] * 14,
        'CPM': [10.0] * 14,
        'Creative': ['Asset A'] * 14
    })

class TestSocialChannelAgent:
    """Unit tests for SocialChannelAgent."""

    def test_social_benchmarks(self):
        """Verify benchmark lookups."""
        assert SocialBenchmarks.get_benchmark("Meta", "ctr") == 0.009
        assert SocialBenchmarks.get_benchmark("TikTok", "ctr") == 0.016
        assert SocialBenchmarks.get_benchmark("Unknown", "ctr") == 0.009 # Default meta

    def test_detect_creative_fatigue_severe(self, social_data):
        """Verify severe fatigue detection (high CTR decline)."""
        agent = SocialChannelAgent()
        data = social_data.copy()
        # Decline CTR from 0.02 to 0.01 over 14 days
        data['CTR'] = np.linspace(0.02, 0.01, 14)
        
        result = agent._detect_creative_fatigue(data)
        assert result['status'] == 'severe'
        assert result['ctr_decline_pct'] < -30

    def test_analyze_frequency_saturated(self, social_data):
        """Verify audience saturation detection."""
        agent = SocialChannelAgent()
        data = social_data.copy()
        data['Frequency'] = 6.0 # Threshold 5
        
        result = agent._analyze_frequency(data)
        assert result['status'] == 'saturated'
        assert "saturation detected" in result['findings'][0]

    def test_analyze_engagement(self, social_data):
        """Verify engagement rate calculation."""
        agent = SocialChannelAgent()
        result = agent._analyze_engagement(social_data)
        
        # 65 engagements / 140000 impressions
        expected_rate = (65 / 140000) * 100
        assert pytest.approx(result['engagement_rate'], 0.001) == expected_rate

    def test_analyze_delivery_high_cpm(self, social_data):
        """Verify delivery status for high CPM."""
        agent = SocialChannelAgent()
        data = social_data.copy()
        # Bench for meta is 7.19. Let's make it 15.0
        data['Spend'] = 150.0
        data['Impressions'] = 10000
        
        result = agent._analyze_delivery(data)
        assert result['status'] == 'poor'
        assert result['cpm'] == 15.0

    def test_overall_health_calculation(self):
        """Verify health score aggregation."""
        agent = SocialChannelAgent()
        insights = {
            'creative_fatigue': {'status': 'healthy'},
            'audience_saturation': {'status': 'optimal'},
            'engagement_metrics': {'status': 'good'}
        }
        health = agent._calculate_overall_health(insights)
        assert health in ['excellent', 'good']
