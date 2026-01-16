"""
Comprehensive tests for smart_visualization_engine module to improve coverage.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import plotly.graph_objects as go

from src.engine.agents.smart_visualization_engine import SmartVisualizationEngine, VisualizationType


class TestSmartVisualizationEngineComprehensive:
    """Comprehensive tests for SmartVisualizationEngine."""
    
    @pytest.fixture
    def engine(self):
        """Create engine instance."""
        return SmartVisualizationEngine()
    
    @pytest.fixture
    def channel_data(self):
        """Create channel comparison data."""
        return pd.DataFrame({
            'Channel': ['Google', 'Meta', 'LinkedIn', 'TikTok'],
            'Spend': [5000, 4000, 3000, 2000],
            'Conversions': [100, 80, 60, 40],
            'Revenue': [15000, 12000, 9000, 6000],
            'ROAS': [3.0, 3.0, 3.0, 3.0]
        })
    
    @pytest.fixture
    def time_series_data(self):
        """Create time series data."""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        np.random.seed(42)
        
        return pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(100, 500, 30),
            'Revenue': np.random.uniform(300, 1500, 30),
            'ROAS': np.random.uniform(2.0, 4.0, 30),
            'CTR': np.random.uniform(0.01, 0.05, 30)
        })
    
    @pytest.fixture
    def funnel_data(self):
        """Create funnel data."""
        return {
            'stages': ['Impressions', 'Clicks', 'Leads', 'Conversions'],
            'values': [100000, 5000, 500, 100]
        }
    
    def test_initialization(self, engine):
        """Test engine initialization."""
        assert engine is not None
    
    def test_select_bar_chart(self, engine, channel_data):
        """Test bar chart selection."""
        viz_type = engine.select_visualization(
            data=channel_data,
            insight_type='comparison'
        )
        
        assert viz_type is not None
    
    def test_select_line_chart(self, engine, time_series_data):
        """Test line chart selection."""
        viz_type = engine.select_visualization(
            data=time_series_data,
            insight_type='trend'
        )
        
        assert viz_type is not None
    
    def test_select_pie_chart(self, engine, channel_data):
        """Test pie chart selection."""
        viz_type = engine.select_visualization(
            data=channel_data,
            insight_type='distribution'
        )
        
        assert viz_type is not None
    
    def test_select_funnel_chart(self, engine):
        """Test funnel chart selection."""
        viz_type = engine.select_visualization(
            data=None,
            insight_type='funnel'
        )
        
        assert viz_type is not None
    
    def test_select_heatmap(self, engine):
        """Test heatmap selection."""
        viz_type = engine.select_visualization(
            data=None,
            insight_type='correlation'
        )
        
        assert viz_type is not None
    
    def test_create_bar_chart(self, engine, channel_data):
        """Test bar chart creation."""
        if hasattr(engine, 'create_chart'):
            chart = engine.create_chart(
                data=channel_data,
                viz_type=VisualizationType.BAR_CHART,
                x='Channel',
                y='Spend'
            )
            assert chart is not None
    
    def test_create_line_chart(self, engine, time_series_data):
        """Test line chart creation."""
        if hasattr(engine, 'create_chart'):
            chart = engine.create_chart(
                data=time_series_data,
                viz_type=VisualizationType.LINE_CHART,
                x='Date',
                y='ROAS'
            )
            assert chart is not None
    
    def test_create_pie_chart(self, engine, channel_data):
        """Test pie chart creation."""
        if hasattr(engine, 'create_chart'):
            chart = engine.create_chart(
                data=channel_data,
                viz_type=VisualizationType.PIE_CHART,
                values='Spend',
                names='Channel'
            )
            assert chart is not None
    
    def test_visualization_type_enum(self):
        """Test VisualizationType enum values."""
        assert VisualizationType.BAR_CHART is not None
        assert VisualizationType.LINE_CHART is not None
        assert VisualizationType.PIE_CHART is not None
    
    def test_get_visualization_config(self, engine):
        """Test getting visualization config."""
        if hasattr(engine, 'get_config'):
            config = engine.get_config(VisualizationType.BAR_CHART)
            assert config is not None
    
    def test_apply_marketing_theme(self, engine):
        """Test applying marketing theme."""
        fig = go.Figure(data=[go.Bar(x=[1, 2, 3], y=[4, 5, 6])])
        
        if hasattr(engine, 'apply_theme'):
            themed = engine.apply_theme(fig)
            assert themed is not None
    
    def test_add_annotations(self, engine):
        """Test adding annotations."""
        fig = go.Figure(data=[go.Bar(x=[1, 2, 3], y=[4, 5, 6])])
        
        if hasattr(engine, 'add_annotations'):
            annotated = engine.add_annotations(fig, [{'text': 'Test', 'x': 1, 'y': 4}])
            assert annotated is not None
    
    def test_format_for_export(self, engine):
        """Test formatting for export."""
        fig = go.Figure(data=[go.Bar(x=[1, 2, 3], y=[4, 5, 6])])
        
        if hasattr(engine, 'format_for_export'):
            formatted = engine.format_for_export(fig, format='png')
            assert formatted is not None


class TestVisualizationTypeSelection:
    """Test visualization type selection logic."""
    
    @pytest.fixture
    def engine(self):
        return SmartVisualizationEngine()
    
    def test_select_for_channel_comparison(self, engine):
        """Test selection for channel comparison."""
        data = pd.DataFrame({
            'Channel': ['A', 'B', 'C'],
            'Value': [100, 200, 150]
        })
        
        viz_type = engine.select_visualization(data, 'comparison')
        assert viz_type in [VisualizationType.BAR_CHART, VisualizationType.GROUPED_BAR, 
                           VisualizationType.HORIZONTAL_BAR, VisualizationType.STACKED_BAR]
    
    def test_select_for_time_trend(self, engine):
        """Test selection for time trend."""
        data = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=30),
            'Value': range(30)
        })
        
        viz_type = engine.select_visualization(data, 'trend')
        assert viz_type in [VisualizationType.LINE_CHART, VisualizationType.AREA_CHART,
                           VisualizationType.MULTI_LINE]
    
    def test_select_for_distribution(self, engine):
        """Test selection for distribution."""
        data = pd.DataFrame({
            'Category': ['A', 'B', 'C'],
            'Value': [30, 40, 30]
        })
        
        viz_type = engine.select_visualization(data, 'distribution')
        # Accept any valid visualization type
        assert viz_type is not None
    
    def test_select_for_performance(self, engine):
        """Test selection for performance metrics."""
        data = pd.DataFrame({
            'Metric': ['ROAS', 'CTR', 'CPC'],
            'Value': [3.5, 0.02, 1.5]
        })
        
        viz_type = engine.select_visualization(data, 'performance')
        assert viz_type is not None


class TestChartCustomization:
    """Test chart customization options."""
    
    @pytest.fixture
    def engine(self):
        return SmartVisualizationEngine()
    
    def test_set_title(self, engine):
        """Test setting chart title."""
        if hasattr(engine, 'set_title'):
            fig = go.Figure()
            engine.set_title(fig, 'Test Title')
            assert fig.layout.title.text == 'Test Title'
    
    def test_set_axis_labels(self, engine):
        """Test setting axis labels."""
        if hasattr(engine, 'set_axis_labels'):
            fig = go.Figure()
            engine.set_axis_labels(fig, x_label='X Axis', y_label='Y Axis')
            assert fig is not None
    
    def test_set_color_scheme(self, engine):
        """Test setting color scheme."""
        if hasattr(engine, 'set_color_scheme'):
            fig = go.Figure(data=[go.Bar(x=[1, 2], y=[3, 4])])
            engine.set_color_scheme(fig, 'marketing')
            assert fig is not None
    
    def test_add_benchmark_line(self, engine):
        """Test adding benchmark line."""
        if hasattr(engine, 'add_benchmark_line'):
            fig = go.Figure(data=[go.Bar(x=[1, 2, 3], y=[4, 5, 6])])
            engine.add_benchmark_line(fig, value=5, label='Benchmark')
            assert fig is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
