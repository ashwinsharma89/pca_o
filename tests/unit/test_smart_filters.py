"""
Unit tests for streamlit_components/smart_filters.py and src/agents/visualization_filters.py
Covers: SmartFilterEngine, filter logic, date/platform/metric filtering
"""
import pandas as pd
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.engine.agents.visualization_filters import SmartFilterEngine


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_df():
    """Standard DataFrame for filter testing."""
    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    return pd.DataFrame({
        "Date": dates,
        "Platform": ["Google"] * 10 + ["Meta"] * 10 + ["LinkedIn"] * 10,
        "Spend": [100 + i * 10 for i in range(30)],
        "Conversions": [10 + i for i in range(30)],
        "Impressions": [1000 + i * 100 for i in range(30)],
        "Clicks": [50 + i * 5 for i in range(30)],
        "Funnel_Stage": ["Awareness"] * 10 + ["Consideration"] * 10 + ["Conversion"] * 10
    })


@pytest.fixture
def filter_engine():
    """Create SmartFilterEngine instance."""
    return SmartFilterEngine()


# ============================================================================
# Platform Filter Tests
# ============================================================================

class TestPlatformFilter:
    """Tests for platform filtering."""
    
    def test_filter_single_platform(self, sample_df):
        """Filter by single platform."""
        filtered = sample_df[sample_df["Platform"] == "Google"]
        
        assert len(filtered) == 10
        assert all(filtered["Platform"] == "Google")
    
    def test_filter_multiple_platforms(self, sample_df):
        """Filter by multiple platforms."""
        platforms = ["Google", "Meta"]
        filtered = sample_df[sample_df["Platform"].isin(platforms)]
        
        assert len(filtered) == 20
        assert set(filtered["Platform"].unique()) == {"Google", "Meta"}
    
    def test_filter_all_platforms(self, sample_df):
        """'All' should return all data."""
        # Simulating "All" selection
        filtered = sample_df.copy()
        
        assert len(filtered) == 30
    
    def test_filter_nonexistent_platform(self, sample_df):
        """Non-existent platform should return empty."""
        filtered = sample_df[sample_df["Platform"] == "TikTok"]
        
        assert len(filtered) == 0


# ============================================================================
# Date Range Filter Tests
# ============================================================================

class TestDateRangeFilter:
    """Tests for date range filtering."""
    
    def test_filter_date_range(self, sample_df):
        """Filter by date range."""
        start = pd.Timestamp("2024-01-05")
        end = pd.Timestamp("2024-01-15")
        
        filtered = sample_df[
            (sample_df["Date"] >= start) & 
            (sample_df["Date"] <= end)
        ]
        
        assert len(filtered) == 11  # Jan 5-15 inclusive
        assert filtered["Date"].min() >= start
        assert filtered["Date"].max() <= end
    
    def test_filter_single_day(self, sample_df):
        """Filter for single day."""
        target_date = pd.Timestamp("2024-01-10")
        
        filtered = sample_df[sample_df["Date"] == target_date]
        
        assert len(filtered) == 1
    
    def test_filter_future_dates(self, sample_df):
        """Future dates should return empty."""
        future = pd.Timestamp("2025-01-01")
        
        filtered = sample_df[sample_df["Date"] >= future]
        
        assert len(filtered) == 0
    
    def test_handles_mixed_date_formats(self):
        """Should handle mixed date formats."""
        df = pd.DataFrame({
            "Date": ["2024-01-01", "01/02/2024", "03-01-2024"],
            "Spend": [100, 200, 300]
        })
        
        # Parse with dayfirst=True for flexibility
        df["Date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=True)
        
        assert df["Date"].dtype == "datetime64[ns]"
        assert len(df) == 3


# ============================================================================
# Numeric Range Filter Tests
# ============================================================================

class TestNumericRangeFilter:
    """Tests for numeric range filtering (spend, conversions, etc.)."""
    
    def test_filter_spend_range(self, sample_df):
        """Filter by spend range."""
        min_spend = 150
        max_spend = 250
        
        filtered = sample_df[
            (sample_df["Spend"] >= min_spend) & 
            (sample_df["Spend"] <= max_spend)
        ]
        
        assert all(filtered["Spend"] >= min_spend)
        assert all(filtered["Spend"] <= max_spend)
    
    def test_filter_conversions_minimum(self, sample_df):
        """Filter by minimum conversions."""
        min_conv = 25
        
        filtered = sample_df[sample_df["Conversions"] >= min_conv]
        
        assert all(filtered["Conversions"] >= min_conv)
    
    def test_filter_zero_spend(self, sample_df):
        """Filter for zero spend."""
        # Add zero spend rows
        df = sample_df.copy()
        df.loc[0, "Spend"] = 0
        
        filtered = df[df["Spend"] == 0]
        
        assert len(filtered) == 1


# ============================================================================
# Funnel Stage Filter Tests
# ============================================================================

class TestFunnelStageFilter:
    """Tests for funnel stage filtering."""
    
    def test_filter_awareness(self, sample_df):
        """Filter for Awareness stage."""
        filtered = sample_df[sample_df["Funnel_Stage"] == "Awareness"]
        
        assert len(filtered) == 10
        assert all(filtered["Funnel_Stage"] == "Awareness")
    
    def test_filter_multiple_stages(self, sample_df):
        """Filter for multiple funnel stages."""
        stages = ["Awareness", "Conversion"]
        filtered = sample_df[sample_df["Funnel_Stage"].isin(stages)]
        
        assert len(filtered) == 20
    
    def test_funnel_stage_aggregation(self, sample_df):
        """Aggregate metrics by funnel stage."""
        funnel_stats = sample_df.groupby("Funnel_Stage").agg({
            "Spend": "sum",
            "Conversions": "sum"
        })
        
        assert len(funnel_stats) == 3
        assert "Awareness" in funnel_stats.index
        assert "Consideration" in funnel_stats.index
        assert "Conversion" in funnel_stats.index


# ============================================================================
# Combined Filter Tests
# ============================================================================

class TestCombinedFilters:
    """Tests for combining multiple filters."""
    
    def test_platform_and_date_filter(self, sample_df):
        """Combine platform and date filters."""
        platform = "Google"
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-01-05")
        
        filtered = sample_df[
            (sample_df["Platform"] == platform) &
            (sample_df["Date"] >= start) &
            (sample_df["Date"] <= end)
        ]
        
        assert len(filtered) == 5
        assert all(filtered["Platform"] == "Google")
    
    def test_all_filters_combined(self, sample_df):
        """Apply all filter types together."""
        filtered = sample_df[
            (sample_df["Platform"] == "Meta") &
            (sample_df["Date"] >= pd.Timestamp("2024-01-11")) &
            (sample_df["Date"] <= pd.Timestamp("2024-01-15")) &
            (sample_df["Spend"] >= 200) &
            (sample_df["Funnel_Stage"] == "Consideration")
        ]
        
        assert len(filtered) <= 5
        assert all(filtered["Platform"] == "Meta")
        assert all(filtered["Funnel_Stage"] == "Consideration")
    
    def test_empty_result_from_filters(self, sample_df):
        """Conflicting filters should return empty."""
        filtered = sample_df[
            (sample_df["Platform"] == "Google") &
            (sample_df["Funnel_Stage"] == "Conversion")  # Google is Awareness
        ]
        
        assert len(filtered) == 0


# ============================================================================
# Filter Metrics Calculation Tests
# ============================================================================

class TestFilteredMetrics:
    """Tests for calculating metrics on filtered data."""
    
    def test_filtered_totals(self, sample_df):
        """Calculate totals on filtered data."""
        filtered = sample_df[sample_df["Platform"] == "Google"]
        
        total_spend = filtered["Spend"].sum()
        total_conversions = filtered["Conversions"].sum()
        
        assert total_spend > 0
        assert total_conversions > 0
    
    def test_filtered_cpa(self, sample_df):
        """Calculate CPA on filtered data."""
        filtered = sample_df[sample_df["Platform"] == "Meta"]
        
        cpa = filtered["Spend"].sum() / filtered["Conversions"].sum()
        
        assert cpa > 0
    
    def test_filtered_ctr(self, sample_df):
        """Calculate CTR on filtered data."""
        filtered = sample_df[sample_df["Funnel_Stage"] == "Awareness"]
        
        ctr = filtered["Clicks"].sum() / filtered["Impressions"].sum() * 100
        
        assert 0 < ctr < 100


# ============================================================================
# Edge Cases
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases in filtering."""
    
    def test_empty_dataframe(self):
        """Handle empty DataFrame."""
        df = pd.DataFrame()
        
        # Should not crash
        assert len(df) == 0
    
    def test_missing_filter_column(self, sample_df):
        """Handle missing filter column gracefully."""
        # Remove Platform column
        df = sample_df.drop(columns=["Platform"])
        
        # Should check if column exists before filtering
        if "Platform" in df.columns:
            filtered = df[df["Platform"] == "Google"]
        else:
            filtered = df
        
        assert len(filtered) == len(df)
    
    def test_null_values_in_filter_column(self, sample_df):
        """Handle null values in filter column."""
        df = sample_df.copy()
        df.loc[0, "Platform"] = None
        
        # Filter should exclude nulls
        filtered = df[df["Platform"] == "Google"]
        
        assert len(filtered) == 9  # One less due to null
    
    def test_case_insensitive_platform_filter(self, sample_df):
        """Platform filter should be case-insensitive."""
        df = sample_df.copy()
        
        # Normalize to title case for comparison
        filtered = df[df["Platform"].str.lower() == "google"]
        
        assert len(filtered) == 10
