"""
Tests for EnhancedVisualizationAgent (Phase C.4).
Verifies categorization, chart routing, and dashboard generation.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.engine.agents.enhanced_visualization_agent import EnhancedVisualizationAgent
from src.engine.agents.smart_visualization_engine import VisualizationType

@pytest.fixture
def viz_agent():
    """Create agent with mocked underlying generators."""
    with patch('src.engine.agents.enhanced_visualization_agent.SmartChartGenerator') as mock_gen:
        agent = EnhancedVisualizationAgent()
        agent.chart_gen = MagicMock()
        return agent

class TestEnhancedVisualizationAgent:
    """Unit tests for EnhancedVisualizationAgent."""

    def test_categorize_insight(self, viz_agent):
        """Verify keyword-based insight categorization."""
        i1 = {"title": "ROAS trend over time", "description": "Daily performance"}
        cat1 = viz_agent._categorize_insight(i1)
        print(f"DEBUG: i1 text='{i1['title']} {i1['description']}' cat='{cat1}'")
        # If this fails, we want to see the cat.
        assert cat1 == "performance_trend"
        
        i2 = {"title": "Allocation", "description": "Spend breakdown"}
        cat2 = viz_agent._categorize_insight(i2)
        print(f"DEBUG: i2 cat='{cat2}'")
        assert cat2 == "budget_distribution"
        
        i3 = {"title": "Audience segment decay", "description": "Demographic dropoff"}
        cat3 = viz_agent._categorize_insight(i3)
        assert cat3 == "audience_performance"

    def test_generate_chart_routing(self, viz_agent):
        """Verify routing to correct chart generator methods."""
        # Gauge
        viz_agent._generate_chart(VisualizationType.GAUGE, {"actual": 5.0, "target": 10.0}, {}, title="Test Gauge")
        viz_agent.chart_gen.create_performance_gauge.assert_called_once()
        
        # Treemap
        viz_agent._generate_chart(VisualizationType.TREEMAP, {"labels": [], "parents": [], "values": []}, {})
        viz_agent.chart_gen.create_budget_treemap.assert_called_once()
        
        # Funnel
        viz_agent._generate_chart(VisualizationType.FUNNEL, [{"stage": "A", "value": 100}], {})
        viz_agent.chart_gen.create_conversion_funnel.assert_called_once()

    def test_create_visualizations_for_insights(self, viz_agent):
        """Verify full flow for multiple insights."""
        insights = [
            {"id": "i1", "title": "Trend", "data": {"dates": [], "metrics": {}}},
            {"id": "i2", "title": "Budget", "data": {"values": []}}
        ]
        
        results = viz_agent.create_visualizations_for_insights(insights)
        assert len(results) == 2
        assert results[0]["insight_id"] == "i1"
        assert results[1]["insight_id"] == "i2"

    def test_create_executive_dashboard(self, viz_agent):
        """Verify executive dashboard composition."""
        df = pd.DataFrame({
            'Channel': ['Meta', 'Google'],
            'Spend': [1000, 2000],
            'ROAS': [2.5, 3.0],
            'Date': pd.date_range(start='2024-01-01', periods=2),
            'Device': ['Mobile', 'Desktop'],
            'Conversions': [10, 20],
            'Campaign': ['C1', 'C2']
        })
        
        dashboard = viz_agent.create_executive_dashboard(insights=[], campaign_data=df)
        
        # Should contain Gauge, Channel Comparison, Treemap, Trend, Donut
        titles = [d["title"] for d in dashboard]
        assert any("Overall Campaign Performance" in t for t in titles)
        assert any("Channels Performance" in t for t in titles)
        assert any("Budget Allocation" in t for t in titles)
        assert len(dashboard) >= 4
