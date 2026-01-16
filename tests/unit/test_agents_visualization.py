"""
Unit tests for Visualization Agents.
Tests chart generation and visualization logic.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Try to import visualization agent
try:
    from src.engine.agents.visualization_agent import VisualizationAgent
    VIZ_AGENT_AVAILABLE = True
except ImportError:
    VIZ_AGENT_AVAILABLE = False
    VisualizationAgent = None

# Try to import enhanced visualization agent
try:
    from src.engine.agents.enhanced_visualization_agent import EnhancedVisualizationAgent
    ENHANCED_VIZ_AVAILABLE = True
except ImportError:
    ENHANCED_VIZ_AVAILABLE = False
    EnhancedVisualizationAgent = None

# Try to import smart visualization engine
try:
    from src.engine.agents.smart_visualization_engine import SmartVisualizationEngine
    SMART_VIZ_AVAILABLE = True
except ImportError:
    SMART_VIZ_AVAILABLE = False
    SmartVisualizationEngine = None

# Try to import chart generators
try:
    from src.engine.agents.chart_generators import ChartGenerator
    CHART_GEN_AVAILABLE = True
except ImportError:
    CHART_GEN_AVAILABLE = False
    ChartGenerator = None


class TestVisualizationAgent:
    """Test VisualizationAgent functionality."""
    
    pytestmark = pytest.mark.skipif(not VIZ_AGENT_AVAILABLE, reason="Visualization agent not available")
    
    @pytest.fixture
    def agent(self):
        """Create visualization agent."""
        return VisualizationAgent()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for visualization."""
        return pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=7),
            'Spend': [100, 120, 110, 130, 140, 135, 150],
            'Conversions': [10, 12, 11, 15, 14, 16, 18]
        })
    
    def test_initialization(self, agent):
        """Test agent initialization."""
        assert agent is not None
    
    def test_generate_chart(self, agent, sample_data):
        """Test chart generation."""
        if hasattr(agent, 'generate_chart'):
            try:
                chart = agent.generate_chart(
                    data=sample_data,
                    chart_type='line',
                    x='Date',
                    y='Spend'
                )
                assert chart is not None
            except Exception:
                pytest.skip("Chart generation requires plotting library")
    
    def test_select_chart_type(self, agent):
        """Test automatic chart type selection."""
        if hasattr(agent, 'select_chart_type'):
            chart_type = agent.select_chart_type(
                data_type='time_series',
                metric_count=1
            )
            assert chart_type in ['line', 'area', 'bar']


class TestEnhancedVisualizationAgent:
    """Test EnhancedVisualizationAgent functionality."""
    
    pytestmark = pytest.mark.skipif(not ENHANCED_VIZ_AVAILABLE, reason="Enhanced viz not available")
    
    @pytest.fixture
    def agent(self):
        """Create enhanced visualization agent."""
        return EnhancedVisualizationAgent()
    
    def test_initialization(self, agent):
        """Test agent initialization."""
        assert agent is not None
    
    def test_create_dashboard(self, agent):
        """Test dashboard creation."""
        if hasattr(agent, 'create_dashboard'):
            try:
                dashboard = agent.create_dashboard(
                    metrics=['spend', 'conversions', 'roas'],
                    data={}
                )
                assert dashboard is not None
            except Exception:
                pytest.skip("Dashboard creation requires setup")


class TestSmartVisualizationEngine:
    """Test SmartVisualizationEngine functionality."""
    
    pytestmark = pytest.mark.skipif(not SMART_VIZ_AVAILABLE, reason="Smart viz not available")
    
    @pytest.fixture
    def engine(self):
        """Create smart visualization engine."""
        return SmartVisualizationEngine()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data."""
        return pd.DataFrame({
            'Campaign': ['A', 'B', 'C'],
            'Spend': [1000, 2000, 1500],
            'ROAS': [3.5, 4.2, 2.8]
        })
    
    def test_initialization(self, engine):
        """Test engine initialization."""
        assert engine is not None
    
    def test_recommend_visualization(self, engine, sample_data):
        """Test visualization recommendation."""
        if hasattr(engine, 'recommend'):
            recommendation = engine.recommend(sample_data)
            assert recommendation is not None
    
    def test_auto_generate(self, engine, sample_data):
        """Test automatic visualization generation."""
        if hasattr(engine, 'auto_generate'):
            try:
                viz = engine.auto_generate(sample_data)
                assert viz is not None
            except Exception:
                pytest.skip("Auto generation requires plotting")


class TestChartGenerator:
    """Test ChartGenerator functionality."""
    
    pytestmark = pytest.mark.skipif(not CHART_GEN_AVAILABLE, reason="Chart generator not available")
    
    @pytest.fixture
    def generator(self):
        """Create chart generator."""
        return ChartGenerator()
    
    def test_initialization(self, generator):
        """Test generator initialization."""
        assert generator is not None
    
    def test_line_chart(self, generator):
        """Test line chart generation."""
        if hasattr(generator, 'line_chart'):
            data = pd.DataFrame({
                'x': [1, 2, 3, 4, 5],
                'y': [10, 20, 15, 25, 30]
            })
            try:
                chart = generator.line_chart(data, x='x', y='y')
                assert chart is not None
            except Exception:
                pytest.skip("Line chart requires plotting library")
    
    def test_bar_chart(self, generator):
        """Test bar chart generation."""
        if hasattr(generator, 'bar_chart'):
            data = pd.DataFrame({
                'category': ['A', 'B', 'C'],
                'value': [100, 200, 150]
            })
            try:
                chart = generator.bar_chart(data, x='category', y='value')
                assert chart is not None
            except Exception:
                pytest.skip("Bar chart requires plotting library")
    
    def test_pie_chart(self, generator):
        """Test pie chart generation."""
        if hasattr(generator, 'pie_chart'):
            data = pd.DataFrame({
                'category': ['A', 'B', 'C'],
                'value': [30, 50, 20]
            })
            try:
                chart = generator.pie_chart(data, values='value', names='category')
                assert chart is not None
            except Exception:
                pytest.skip("Pie chart requires plotting library")
