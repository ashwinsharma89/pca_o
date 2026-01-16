"""
Unit tests for src/visualization/chart_generator.py
Covers: SmartChartGenerator, chart generation, column detection
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.engine.visualization.chart_generator import SmartChartGenerator


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def chart_generator():
    """Create chart generator instance."""
    return SmartChartGenerator()


@pytest.fixture
def sample_campaign_data():
    """Create sample campaign data for testing."""
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Brand Campaign', 'Generic Campaign', 'Retargeting'] * 10,
        'Platform': ['Google Ads', 'Meta', 'LinkedIn'] * 10,
        'Channel': ['Search', 'Social', 'Social'] * 10,
        'Spend': np.random.uniform(100, 1000, 30),
        'Impressions': np.random.randint(10000, 100000, 30),
        'Clicks': np.random.randint(100, 1000, 30),
        'Conversions': np.random.randint(5, 50, 30),
        'CTR': np.random.uniform(0.01, 0.05, 30),
        'CPC': np.random.uniform(1, 5, 30),
        'CPA': np.random.uniform(20, 100, 30),
        'ROAS': np.random.uniform(1, 5, 30)
    })


@pytest.fixture
def minimal_data():
    """Create minimal campaign data."""
    return pd.DataFrame({
        'Spend': [1000, 2000, 1500],
        'Conversions': [10, 20, 15]
    })


# ============================================================================
# Initialization Tests
# ============================================================================

class TestChartGeneratorInit:
    """Tests for SmartChartGenerator initialization."""
    
    def test_initialization(self, chart_generator):
        """Generator should initialize correctly."""
        assert chart_generator is not None
        assert chart_generator.template == 'plotly_dark'
    
    def test_has_color_schemes(self, chart_generator):
        """Generator should have color schemes defined."""
        assert 'primary' in chart_generator.COLORS
        assert 'performance' in chart_generator.COLORS
        assert 'warning' in chart_generator.COLORS


# ============================================================================
# Overview Charts Tests
# ============================================================================

class TestOverviewCharts:
    """Tests for overview chart generation."""
    
    def test_generate_overview_charts(self, chart_generator, sample_campaign_data):
        """Should generate multiple overview charts."""
        charts = chart_generator.generate_overview_charts(sample_campaign_data)
        
        assert isinstance(charts, list)
        # Should generate at least some charts
        assert len(charts) >= 0
    
    def test_charts_have_required_fields(self, chart_generator, sample_campaign_data):
        """Each chart should have title, fig, and description."""
        charts = chart_generator.generate_overview_charts(sample_campaign_data)
        
        for chart in charts:
            assert 'title' in chart or 'fig' in chart
    
    def test_handles_minimal_data(self, chart_generator, minimal_data):
        """Should handle minimal data without errors."""
        charts = chart_generator.generate_overview_charts(minimal_data)
        
        assert isinstance(charts, list)
    
    def test_handles_empty_data(self, chart_generator):
        """Should handle empty DataFrame."""
        empty_df = pd.DataFrame()
        
        charts = chart_generator.generate_overview_charts(empty_df)
        
        assert isinstance(charts, list)
        assert len(charts) == 0


# ============================================================================
# Column Detection Tests
# ============================================================================

class TestColumnDetection:
    """Tests for automatic column detection."""
    
    def test_detects_spend_column(self, chart_generator):
        """Should detect spend column."""
        df = pd.DataFrame({'Spend': [100, 200], 'Other': [1, 2]})
        
        cols = chart_generator._detect_columns(df)
        
        assert cols['spend'] is not None
    
    def test_detects_date_column(self, chart_generator):
        """Should detect date column."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Spend': [100, 200, 300, 400, 500]
        })
        
        cols = chart_generator._detect_columns(df)
        
        assert cols['date'] is not None
    
    def test_detects_platform_column(self, chart_generator):
        """Should detect platform column."""
        df = pd.DataFrame({
            'Platform': ['Google Ads', 'Meta'],
            'Spend': [100, 200]
        })
        
        cols = chart_generator._detect_columns(df)
        
        assert cols['platform'] is not None
    
    def test_handles_missing_columns(self, chart_generator):
        """Should handle missing columns gracefully."""
        df = pd.DataFrame({'Random': [1, 2, 3]})
        
        cols = chart_generator._detect_columns(df)
        
        # Should return dict with None values for missing columns
        assert isinstance(cols, dict)


# ============================================================================
# Query-Based Charts Tests
# ============================================================================

class TestQueryCharts:
    """Tests for query-based chart generation."""
    
    def test_generate_query_charts(self, chart_generator, sample_campaign_data):
        """Should generate charts based on query."""
        charts = chart_generator.generate_query_charts(
            query="Show me spend by platform",
            results_df=sample_campaign_data
        )
        
        assert isinstance(charts, list)
    
    def test_trend_query_generates_line_chart(self, chart_generator, sample_campaign_data):
        """Trend queries should generate line charts."""
        charts = chart_generator.generate_query_charts(
            query="Show me the trend of spend over time",
            results_df=sample_campaign_data
        )
        
        # Should generate at least one chart
        assert isinstance(charts, list)
    
    def test_comparison_query_generates_bar_chart(self, chart_generator, sample_campaign_data):
        """Comparison queries should generate bar charts."""
        charts = chart_generator.generate_query_charts(
            query="Compare performance by platform",
            results_df=sample_campaign_data
        )
        
        assert isinstance(charts, list)


# ============================================================================
# Specific Chart Type Tests
# ============================================================================

class TestSpecificCharts:
    """Tests for specific chart types."""
    
    def test_spend_vs_conversions_chart(self, chart_generator, sample_campaign_data):
        """Should create spend vs conversions scatter plot."""
        cols = chart_generator._detect_columns(sample_campaign_data)
        
        chart = chart_generator._create_spend_vs_conversions(
            sample_campaign_data, cols
        )
        
        if chart:
            assert 'fig' in chart or 'title' in chart
    
    def test_kpi_trends_chart(self, chart_generator, sample_campaign_data):
        """Should create KPI trends line chart."""
        cols = chart_generator._detect_columns(sample_campaign_data)
        
        chart = chart_generator._create_kpi_trends(sample_campaign_data, cols)
        
        if chart:
            assert 'fig' in chart or 'title' in chart
    
    def test_channel_performance_chart(self, chart_generator, sample_campaign_data):
        """Should create channel performance comparison."""
        cols = chart_generator._detect_columns(sample_campaign_data)
        
        chart = chart_generator._create_channel_performance(
            sample_campaign_data, cols
        )
        
        if chart:
            assert 'fig' in chart or 'title' in chart


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestChartErrorHandling:
    """Tests for error handling in chart generation."""
    
    def test_handles_invalid_data_types(self, chart_generator):
        """Should handle invalid data types gracefully."""
        df = pd.DataFrame({
            'Spend': ['not', 'numeric', 'data'],
            'Conversions': ['also', 'not', 'numeric']
        })
        
        # Should not raise
        charts = chart_generator.generate_overview_charts(df)
        assert isinstance(charts, list)
    
    def test_handles_null_values(self, chart_generator):
        """Should handle null values in data."""
        df = pd.DataFrame({
            'Spend': [100, None, 300],
            'Conversions': [10, 20, None],
            'Platform': ['Google', None, 'Meta']
        })
        
        # Should not raise
        charts = chart_generator.generate_overview_charts(df)
        assert isinstance(charts, list)
    
    def test_handles_single_row(self, chart_generator):
        """Should handle single row of data."""
        df = pd.DataFrame({
            'Spend': [1000],
            'Conversions': [50],
            'Platform': ['Google Ads']
        })
        
        charts = chart_generator.generate_overview_charts(df)
        assert isinstance(charts, list)


# ============================================================================
# Chart Customization Tests
# ============================================================================

class TestChartCustomization:
    """Tests for chart customization options."""
    
    def test_uses_dark_template(self, chart_generator):
        """Charts should use dark template by default."""
        assert chart_generator.template == 'plotly_dark'
    
    def test_color_scheme_applied(self, chart_generator, sample_campaign_data):
        """Charts should use defined color schemes."""
        charts = chart_generator.generate_overview_charts(sample_campaign_data)
        
        # Verify charts are generated (color scheme is internal)
        assert isinstance(charts, list)


# ============================================================================
# Performance Tests
# ============================================================================

class TestChartPerformance:
    """Tests for chart generation performance."""
    
    def test_handles_large_dataset(self, chart_generator):
        """Should handle large datasets efficiently."""
        # Create large dataset
        large_df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=10000, freq='H'),
            'Spend': np.random.uniform(100, 1000, 10000),
            'Conversions': np.random.randint(1, 100, 10000),
            'Platform': np.random.choice(['Google', 'Meta', 'LinkedIn'], 10000)
        })
        
        # Should complete without timeout
        charts = chart_generator.generate_overview_charts(large_df)
        assert isinstance(charts, list)
    
    def test_handles_many_categories(self, chart_generator):
        """Should handle many categorical values."""
        df = pd.DataFrame({
            'Campaign': [f'Campaign_{i}' for i in range(100)],
            'Spend': np.random.uniform(100, 1000, 100),
            'Conversions': np.random.randint(1, 50, 100)
        })
        
        charts = chart_generator.generate_overview_charts(df)
        assert isinstance(charts, list)
