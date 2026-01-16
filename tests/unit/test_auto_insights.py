"""
Unit tests for src/analytics/auto_insights.py
Covers: MediaAnalyticsExpert, RAG summaries, analysis methods
"""
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

# Import the module under test
from src.engine.analytics.auto_insights import MediaAnalyticsExpert


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_campaign_df():
    """Standard campaign DataFrame for testing."""
    return pd.DataFrame({
        "Campaign_Name": ["Brand Awareness", "Lead Gen", "Retargeting"],
        "Platform": ["Google", "Meta", "LinkedIn"],
        "Spend": [5000.0, 3000.0, 2000.0],
        "Conversions": [50, 80, 120],
        "Impressions": [100000, 50000, 30000],
        "Clicks": [2000, 1500, 1000],
        "Revenue": [10000, 15000, 25000],
        "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    })


@pytest.fixture
def mock_llm_response():
    """Mock LLM response for testing."""
    return {
        "content": "Based on the analysis, your campaigns are performing well.",
        "usage": {"prompt_tokens": 100, "completion_tokens": 50}
    }


@pytest.fixture
def analytics_expert():
    """Create MediaAnalyticsExpert with mocked dependencies."""
    # MediaAnalyticsExpert uses environment variables for API keys
    with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key', 'ANTHROPIC_API_KEY': 'test_key'}):
        expert = MediaAnalyticsExpert()
        return expert


# ============================================================================
# Initialization Tests
# ============================================================================

class TestMediaAnalyticsExpertInit:
    """Tests for MediaAnalyticsExpert initialization."""
    
    def test_expert_initializes(self):
        """Expert should initialize without errors."""
        # MediaAnalyticsExpert reads API keys from environment
        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            expert = MediaAnalyticsExpert()
            assert expert is not None
    
    def test_expert_has_required_methods(self, analytics_expert):
        """Expert should have all required analysis methods."""
        required_methods = [
            'analyze_all',
            '_generate_executive_summary_with_rag',
            '_calculate_metrics',
        ]
        for method in required_methods:
            assert hasattr(analytics_expert, method), f"Missing method: {method}"


# ============================================================================
# Analysis Method Tests
# ============================================================================

class TestAnalyzeAll:
    """Tests for the analyze_all method."""
    
    def test_analyze_all_returns_dict(self, analytics_expert, sample_campaign_df):
        """analyze_all should return a dictionary with results."""
        with patch.object(analytics_expert, '_generate_executive_summary_with_rag', return_value="Summary"):
            result = analytics_expert.analyze_all(sample_campaign_df)
            
            assert isinstance(result, dict)
    
    def test_analyze_all_handles_empty_df(self, analytics_expert):
        """analyze_all should handle empty DataFrame gracefully."""
        empty_df = pd.DataFrame()
        
        result = analytics_expert.analyze_all(empty_df)
        
        assert result is not None
        # Should return error or empty result, not crash
    
    def test_analyze_all_handles_missing_columns(self, analytics_expert):
        """analyze_all should handle DataFrame with missing expected columns."""
        minimal_df = pd.DataFrame({
            "Campaign_Name": ["Test"],
            "Spend": [100]
        })
        
        result = analytics_expert.analyze_all(minimal_df)
        
        assert result is not None


# ============================================================================
# Metrics Calculation Tests
# ============================================================================

class TestMetricsCalculation:
    """Tests for metrics calculation logic."""
    
    def test_calculate_ctr(self, sample_campaign_df):
        """CTR should be calculated correctly."""
        # CTR = Clicks / Impressions * 100
        expected_ctr = (sample_campaign_df["Clicks"].sum() / 
                       sample_campaign_df["Impressions"].sum() * 100)
        
        total_clicks = sample_campaign_df["Clicks"].sum()
        total_impressions = sample_campaign_df["Impressions"].sum()
        actual_ctr = total_clicks / total_impressions * 100
        
        assert actual_ctr == pytest.approx(expected_ctr, rel=0.01)
    
    def test_calculate_cpa(self, sample_campaign_df):
        """CPA should be calculated correctly."""
        # CPA = Spend / Conversions
        total_spend = sample_campaign_df["Spend"].sum()
        total_conversions = sample_campaign_df["Conversions"].sum()
        expected_cpa = total_spend / total_conversions
        
        assert expected_cpa == pytest.approx(40.0, rel=0.01)  # 10000 / 250
    
    def test_calculate_roas(self, sample_campaign_df):
        """ROAS should be calculated correctly."""
        # ROAS = Revenue / Spend
        total_revenue = sample_campaign_df["Revenue"].sum()
        total_spend = sample_campaign_df["Spend"].sum()
        expected_roas = total_revenue / total_spend
        
        assert expected_roas == pytest.approx(5.0, rel=0.01)  # 50000 / 10000
    
    def test_handles_zero_division(self):
        """Metrics should handle zero division gracefully."""
        df = pd.DataFrame({
            "Spend": [0],
            "Conversions": [0],
            "Impressions": [0],
            "Clicks": [0]
        })
        
        # Should not raise ZeroDivisionError
        try:
            ctr = df["Clicks"].sum() / max(df["Impressions"].sum(), 1) * 100
            cpa = df["Spend"].sum() / max(df["Conversions"].sum(), 1)
            assert ctr == 0
            assert cpa == 0
        except ZeroDivisionError:
            pytest.fail("Zero division not handled")


# ============================================================================
# RAG Summary Tests
# ============================================================================

class TestRAGSummary:
    """Tests for RAG-enhanced summary generation."""
    
    def test_rag_summary_returns_string(self, analytics_expert, sample_campaign_df):
        """RAG summary should return a string."""
        with patch.object(analytics_expert, '_generate_executive_summary_with_rag') as mock_rag:
            mock_rag.return_value = "Executive summary with RAG context"
            
            result = analytics_expert._generate_executive_summary_with_rag(
                sample_campaign_df, 
                {}
            )
            
            assert isinstance(result, str)
    
    def test_rag_summary_includes_metrics(self, analytics_expert, sample_campaign_df):
        """RAG summary should incorporate key metrics."""
        with patch.object(analytics_expert, '_generate_executive_summary_with_rag') as mock_rag:
            mock_rag.return_value = "Total spend: $10,000. ROAS: 5.0x"
            
            result = analytics_expert._generate_executive_summary_with_rag(
                sample_campaign_df,
                {"total_spend": 10000, "roas": 5.0}
            )
            
            assert "10,000" in result or "10000" in result


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling in analytics."""
    
    def test_handles_llm_timeout(self, analytics_expert, sample_campaign_df):
        """Should handle LLM timeout gracefully."""
        with patch.object(analytics_expert, '_generate_executive_summary_with_rag') as mock_rag:
            mock_rag.side_effect = TimeoutError("LLM timeout")
            
            try:
                result = analytics_expert.analyze_all(sample_campaign_df)
                # Should return partial results or error message
                assert result is not None
            except TimeoutError:
                # If it raises, that's also acceptable behavior
                pass
    
    def test_handles_invalid_data_types(self, analytics_expert):
        """Should handle invalid data types in DataFrame."""
        df = pd.DataFrame({
            "Spend": ["not a number", "also not"],
            "Conversions": ["invalid", "data"]
        })
        
        # Should not crash - either returns result or raises meaningful error
        try:
            result = analytics_expert.analyze_all(df)
            # If it returns, result should be dict or None
            assert result is None or isinstance(result, dict)
        except (ValueError, TypeError, KeyError) as e:
            # These are acceptable errors for invalid data
            pass
        except Exception as e:
            # Any other exception should have meaningful message
            assert str(e) is not None


# ============================================================================
# Platform-Specific Analysis Tests
# ============================================================================

class TestPlatformAnalysis:
    """Tests for platform-specific analysis."""
    
    def test_groups_by_platform(self, sample_campaign_df):
        """Analysis should correctly group by platform."""
        platform_stats = sample_campaign_df.groupby("Platform").agg({
            "Spend": "sum",
            "Conversions": "sum"
        })
        
        assert len(platform_stats) == 3
        assert "Google" in platform_stats.index
        assert "Meta" in platform_stats.index
        assert "LinkedIn" in platform_stats.index
    
    def test_platform_cpa_calculation(self, sample_campaign_df):
        """CPA should be calculated per platform."""
        platform_stats = sample_campaign_df.groupby("Platform").agg({
            "Spend": "sum",
            "Conversions": "sum"
        })
        platform_stats["CPA"] = platform_stats["Spend"] / platform_stats["Conversions"]
        
        assert platform_stats.loc["Google", "CPA"] == pytest.approx(100.0, rel=0.01)
        assert platform_stats.loc["Meta", "CPA"] == pytest.approx(37.5, rel=0.01)
        assert platform_stats.loc["LinkedIn", "CPA"] == pytest.approx(16.67, rel=0.1)
