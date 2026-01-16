"""
Unit tests for src/agents/channel_specialists/
Covers: ChannelRouter, SearchChannelAgent, SocialChannelAgent, ProgrammaticAgent
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from src.engine.agents.channel_specialists import (
    ChannelRouter,
    SearchChannelAgent,
    SocialChannelAgent,
    ProgrammaticAgent,
    BaseChannelSpecialist
)


# ============================================================================
# Channel Router Tests
# ============================================================================

class TestChannelRouter:
    """Tests for ChannelRouter."""
    
    @pytest.fixture
    def router(self):
        """Create channel router instance."""
        return ChannelRouter()
    
    def test_router_initialization(self, router):
        """Router should initialize with all specialists."""
        assert router is not None
        assert 'search' in router.specialists
        assert 'social' in router.specialists
        assert 'programmatic' in router.specialists
    
    def test_detect_google_ads_as_search(self, router):
        """Google Ads should be detected as search channel."""
        df = pd.DataFrame({
            'Platform': ['Google Ads', 'Google Ads'],
            'Spend': [1000, 2000],
            'Clicks': [100, 200]
        })
        
        channel_type = router.detect_channel_type(df)
        assert channel_type == 'search'
    
    def test_detect_meta_as_social(self, router):
        """Meta should be detected as social channel."""
        df = pd.DataFrame({
            'Platform': ['Meta', 'Meta'],
            'Spend': [1000, 2000],
            'Impressions': [50000, 60000]
        })
        
        channel_type = router.detect_channel_type(df)
        assert channel_type == 'social'
    
    def test_detect_dv360_as_programmatic(self, router):
        """DV360 should be detected as programmatic channel."""
        df = pd.DataFrame({
            'Platform': ['DV360', 'DV360'],
            'Spend': [1000, 2000],
            'Viewability': [0.7, 0.8]
        })
        
        channel_type = router.detect_channel_type(df)
        assert channel_type == 'programmatic'
    
    def test_get_specialist_search(self, router):
        """Should return SearchChannelAgent for search."""
        specialist = router.get_specialist('search')
        assert isinstance(specialist, SearchChannelAgent)
    
    def test_get_specialist_social(self, router):
        """Should return SocialChannelAgent for social."""
        specialist = router.get_specialist('social')
        assert isinstance(specialist, SocialChannelAgent)
    
    def test_get_specialist_programmatic(self, router):
        """Should return ProgrammaticAgent for programmatic."""
        specialist = router.get_specialist('programmatic')
        assert isinstance(specialist, ProgrammaticAgent)
    
    def test_get_available_specialists(self, router):
        """Should list all available specialists."""
        available = router.get_available_specialists()
        
        assert 'search' in available
        assert 'social' in available
        assert 'programmatic' in available
    
    def test_route_and_analyze_empty_data(self, router):
        """Should handle empty data gracefully."""
        df = pd.DataFrame()
        
        result = router.route_and_analyze(df, 'search')
        
        assert result['status'] == 'error'
        # Message may say 'Empty' or 'No data'
        assert 'Empty' in result.get('error', '') or 'No data' in result.get('message', '')
    
    def test_infer_channel_from_metrics(self, router):
        """Should infer channel type from column names."""
        # Search indicators
        search_df = pd.DataFrame({
            'keyword': ['brand', 'generic'],
            'quality_score': [8, 6],
            'Spend': [1000, 2000]
        })
        
        channel = router.detect_channel_type(search_df)
        assert channel == 'search'
        
        # Social indicators
        social_df = pd.DataFrame({
            'engagement_rate': [0.05, 0.08],
            'frequency': [3.0, 2.5],
            'Spend': [1000, 2000]
        })
        
        channel = router.detect_channel_type(social_df)
        assert channel == 'social'


# ============================================================================
# Search Channel Agent Tests
# ============================================================================

class TestSearchChannelAgent:
    """Tests for SearchChannelAgent."""
    
    @pytest.fixture
    def agent(self):
        """Create search agent instance."""
        return SearchChannelAgent()
    
    @pytest.fixture
    def search_data(self):
        """Create sample search campaign data."""
        return pd.DataFrame({
            'Platform': ['Google Ads', 'Google Ads', 'Bing Ads'],
            'Campaign': ['Brand', 'Generic', 'Brand'],
            'Spend': [1000, 2000, 500],
            'Clicks': [200, 300, 100],
            'Impressions': [5000, 10000, 2500],
            'Conversions': [20, 15, 10],
            'CTR': [0.04, 0.03, 0.04],
            'CPC': [5.0, 6.67, 5.0],
            'Quality_Score': [8, 6, 7]
        })
    
    def test_agent_initialization(self, agent):
        """Agent should initialize correctly."""
        assert agent is not None
        assert isinstance(agent, BaseChannelSpecialist)
    
    def test_analyze_returns_dict(self, agent, search_data):
        """Analyze should return a dictionary."""
        result = agent.analyze(search_data)
        assert isinstance(result, dict)
    
    def test_analyze_includes_metrics(self, agent, search_data):
        """Analysis should include key metrics."""
        result = agent.analyze(search_data)
        
        # Should have some form of metrics or analysis
        assert 'overall_health' in result or 'metrics' in result or 'status' in result


# ============================================================================
# Social Channel Agent Tests
# ============================================================================

class TestSocialChannelAgent:
    """Tests for SocialChannelAgent."""
    
    @pytest.fixture
    def agent(self):
        """Create social agent instance."""
        return SocialChannelAgent()
    
    @pytest.fixture
    def social_data(self):
        """Create sample social campaign data."""
        return pd.DataFrame({
            'Platform': ['Meta', 'Meta', 'LinkedIn'],
            'Campaign': ['Awareness', 'Engagement', 'Lead Gen'],
            'Spend': [1500, 1000, 2000],
            'Impressions': [50000, 40000, 30000],
            'Clicks': [500, 400, 300],
            'Conversions': [25, 20, 30],
            'CTR': [0.01, 0.01, 0.01],
            'Frequency': [3.5, 2.8, 2.0],
            'Engagement_Rate': [0.05, 0.08, 0.03]
        })
    
    def test_agent_initialization(self, agent):
        """Agent should initialize correctly."""
        assert agent is not None
        assert isinstance(agent, BaseChannelSpecialist)
    
    def test_analyze_returns_dict(self, agent, social_data):
        """Analyze should return a dictionary."""
        result = agent.analyze(social_data)
        assert isinstance(result, dict)


# ============================================================================
# Programmatic Agent Tests
# ============================================================================

class TestProgrammaticAgent:
    """Tests for ProgrammaticAgent."""
    
    @pytest.fixture
    def agent(self):
        """Create programmatic agent instance."""
        return ProgrammaticAgent()
    
    @pytest.fixture
    def programmatic_data(self):
        """Create sample programmatic campaign data."""
        return pd.DataFrame({
            'Platform': ['DV360', 'DV360', 'TTD'],
            'Campaign': ['Prospecting', 'Retargeting', 'Awareness'],
            'Spend': [3000, 2000, 2500],
            'Impressions': [100000, 50000, 80000],
            'Clicks': [500, 400, 350],
            'Conversions': [25, 40, 20],
            'Viewability': [0.70, 0.75, 0.68],
            'CTR': [0.005, 0.008, 0.004]
        })
    
    def test_agent_initialization(self, agent):
        """Agent should initialize correctly."""
        assert agent is not None
        assert isinstance(agent, BaseChannelSpecialist)
    
    def test_analyze_returns_dict(self, agent, programmatic_data):
        """Analyze should return a dictionary."""
        result = agent.analyze(programmatic_data)
        assert isinstance(result, dict)


# ============================================================================
# Multi-Channel Analysis Tests
# ============================================================================

class TestMultiChannelAnalysis:
    """Tests for cross-channel analysis."""
    
    @pytest.fixture
    def router(self):
        """Create channel router."""
        return ChannelRouter()
    
    def test_analyze_multi_channel(self, router):
        """Should analyze multiple channels."""
        campaigns = {
            'search': pd.DataFrame({
                'Platform': ['Google Ads'],
                'Spend': [1000],
                'Clicks': [100],
                'Conversions': [10]
            }),
            'social': pd.DataFrame({
                'Platform': ['Meta'],
                'Spend': [1500],
                'Impressions': [50000],
                'Conversions': [15]
            })
        }
        
        result = router.analyze_multi_channel(campaigns)
        
        assert 'channel_analyses' in result
        assert 'cross_channel_insights' in result
        assert result['total_channels'] == 2
    
    def test_cross_channel_insights_structure(self, router):
        """Cross-channel insights should have proper structure."""
        campaigns = {
            'search': pd.DataFrame({
                'Platform': ['Google Ads'],
                'Spend': [1000],
                'Clicks': [100]
            })
        }
        
        result = router.analyze_multi_channel(campaigns)
        insights = result['cross_channel_insights']
        
        assert 'overall_health' in insights
        assert 'needs_attention' in insights
        assert 'recommendations' in insights


# ============================================================================
# Platform Detection Tests
# ============================================================================

class TestPlatformDetection:
    """Tests for platform detection logic."""
    
    @pytest.fixture
    def router(self):
        return ChannelRouter()
    
    def test_detect_linkedin(self, router):
        """Should detect LinkedIn as social."""
        df = pd.DataFrame({'Platform': ['LinkedIn'], 'Spend': [1000]})
        assert router.detect_channel_type(df) == 'social'
    
    def test_detect_tiktok(self, router):
        """Should detect TikTok as social."""
        df = pd.DataFrame({'Platform': ['TikTok'], 'Spend': [1000]})
        assert router.detect_channel_type(df) == 'social'
    
    def test_detect_bing(self, router):
        """Should detect Bing as search."""
        df = pd.DataFrame({'Platform': ['Bing Ads'], 'Spend': [1000]})
        assert router.detect_channel_type(df) == 'search'
    
    def test_detect_amazon_dsp(self, router):
        """Should detect Amazon DSP as programmatic."""
        df = pd.DataFrame({'Platform': ['Amazon DSP'], 'Spend': [1000]})
        assert router.detect_channel_type(df) == 'programmatic'
    
    def test_unknown_platform_fallback(self, router):
        """Unknown platform should use metric inference."""
        df = pd.DataFrame({
            'Platform': ['Unknown Platform'],
            'Spend': [1000],
            'Clicks': [100]
        })
        
        # Should return 'unknown' or infer from metrics
        channel = router.detect_channel_type(df)
        assert channel in ['search', 'social', 'programmatic', 'unknown']
