
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch, ANY
import plotly.graph_objects as go

from src.engine.agents.visualization_agent import VisualizationAgent
from src.engine.agents.enhanced_visualization_agent import EnhancedVisualizationAgent
from src.engine.agents.chart_generators import SmartChartGenerator
from src.platform.models.platform import PlatformType, MetricType
from src.platform.models.campaign import Campaign, ChannelPerformance, DateRange, CampaignObjective
from datetime import date

@pytest.fixture
def temp_output_dir(tmp_path):
    return tmp_path / "viz"

@pytest.fixture
def viz_agent(temp_output_dir):
    return VisualizationAgent(output_dir=temp_output_dir)

@pytest.fixture
def enhanced_viz_agent(temp_output_dir):
    return EnhancedVisualizationAgent(output_dir=temp_output_dir)

@pytest.fixture
def chart_gen():
    return SmartChartGenerator()

@pytest.fixture
def sample_channels():
    return [
        ChannelPerformance(
            platform=PlatformType.GOOGLE_ADS,
            platform_name="Google Ads",
            total_spend=1000.0,
            total_impressions=50000,
            total_clicks=2000,
            total_conversions=50,
            ctr=0.04,
            cpc=0.5,
            cpa=20.0,
            roas=3.0,
            performance_score=85.0
        ),
        ChannelPerformance(
            platform=PlatformType.META_ADS,
            platform_name="Meta Ads",
            total_spend=800.0,
            total_impressions=40000,
            total_clicks=1500,
            total_conversions=40,
            ctr=0.0375,
            cpc=0.53,
            cpa=20.0,
            roas=2.5,
            performance_score=80.0
        )
    ]

@pytest.fixture
def sample_campaign():
    return Campaign(
        campaign_id="c1",
        campaign_name="Test Campaign",
        objectives=[CampaignObjective.CONVERSION],
        date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 31)),
        normalized_metrics=[] # Populated in tests if needed
    )

# --- VisualizationAgent Tests ---

class TestVisualizationAgent:
    def test_init(self, temp_output_dir):
        agent = VisualizationAgent(output_dir=temp_output_dir)
        assert agent.output_dir == temp_output_dir
        assert temp_output_dir.exists()

    def test_generate_all_visualizations(self, viz_agent, sample_campaign, sample_channels):
        # Mock write_image to avoid overhead
        with patch("plotly.graph_objects.Figure.write_image"):
            # Mock create_funnel_chart to return None (testing it can handle it)
            with patch.object(viz_agent, "create_funnel_chart", return_value=None):
                vizes = viz_agent.generate_all_visualizations(sample_campaign, sample_channels)
                assert len(vizes) >= 4 # Comparison, Spend, ROAS, Efficiency, Radar

    def test_create_channel_comparison_chart(self, viz_agent, sample_channels):
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.create_channel_comparison_chart(sample_channels, "c1")
            assert res["type"] == "bar_chart"
            assert "channel_comparison.png" in res["filepath"]

    def test_create_spend_distribution_chart_empty(self, viz_agent):
        res = viz_agent.create_spend_distribution_chart([], "c1")
        assert res is None

    def test_create_roas_comparison_chart(self, viz_agent, sample_channels):
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.create_roas_comparison_chart(sample_channels, "c1")
            assert "roas_comparison" in res["filepath"]

    def test_create_efficiency_scatter(self, viz_agent, sample_channels):
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.create_efficiency_scatter(sample_channels, "c1")
            assert res["type"] == "scatter_plot"

    def test_create_performance_radar(self, viz_agent, sample_channels):
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.create_performance_radar(sample_channels, "c1")
            assert res["type"] == "radar_chart"

    def test_create_funnel_chart(self, viz_agent, sample_campaign):
        # Add metrics to campaign for funnel
        from src.platform.models.platform import NormalizedMetric
        sample_campaign.normalized_metrics = [
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.IMPRESSIONS, value=1000, source_snapshot_id="s1", original_metric_name="i"),
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.CLICKS, value=100, source_snapshot_id="s1", original_metric_name="c"),
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.CONVERSIONS, value=10, source_snapshot_id="s1", original_metric_name="conv")
        ]
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.create_funnel_chart(sample_campaign, "c1")
            assert res["type"] == "funnel_chart"

    def test_create_achievement_infographic(self, viz_agent):
        from src.platform.models.campaign import Achievement
        ach = [Achievement(achievement_type="milestone", title="T1", description="D1", impact_level="high")]
        res = viz_agent.create_achievement_infographic(ach, "c1")
        assert res["type"] == "infographic"

# --- EnhancedVisualizationAgent Tests ---

class TestEnhancedVisualizationAgent:
    def test_categorize_insight(self, enhanced_viz_agent):
        categories = {
            "channel comparison report": "channel_comparison",
            "performance trend": "performance_trend",
            "budget distribution": "budget_distribution",
            "audience segment": "audience_performance",
            "creative fatigue": "creative_decay",
            "attribution journey": "attribution_flow",
            "conversion funnel": "conversion_funnel",
            "quality score": "quality_score_components",
            "hour parting": "hourly_performance",
            "device breakdown": "device_breakdown",
            "geo performance": "geo_performance",
            "keyword efficiency": "keyword_efficiency",
            "frequency histogram": "frequency_analysis",
            "benchmark vs industry": "benchmark_comparison",
            "overall health": "campaign_health",
            "something unknown": "channel_comparison" # Default
        }
        for text, expected in categories.items():
            assert enhanced_viz_agent._categorize_insight({"title": text}) == expected

    def test_create_visualizations_for_insights(self, enhanced_viz_agent):
        insights = [
            {"id": "i1", "category": "channel_comparison", "title": "C1", "data": {"google": {"spend": 100}}}
        ]
        res = enhanced_viz_agent.create_visualizations_for_insights(insights)
        assert len(res) == 1
        assert res[0]["insight_id"] == "i1"

    def test_create_chart_for_category(self, enhanced_viz_agent):
        data = {"google": {"spend": 100}}
        res = enhanced_viz_agent.create_chart_for_category("channel_comparison", data, title="Title")
        assert res["category"] == "channel_comparison"
        assert res["title"] == "Title"

    def test_generate_chart_routing(self, enhanced_viz_agent):
        from src.engine.agents.smart_visualization_engine import VisualizationType
        # Test routing for different types
        types = [
            (VisualizationType.GROUPED_BAR, {"G": {"spend": 10}}),
            (VisualizationType.MULTI_LINE, {"dates": ["2024-01-01"], "metrics": {"ctr": [0.1]}}),
            (VisualizationType.SANKEY, {"paths": [{"path": ["A", "B"], "count":1}]}),
            (VisualizationType.GAUGE, {"actual": 10, "target": 100, "metric_name": "M"}),
            (VisualizationType.HEATMAP, {"values": np.zeros((7, 24))}),
            (VisualizationType.SCATTER_PLOT, {"keywords": [{"keyword": "K", "impressions": 1, "conversion_rate": 0.1, "spend": 1}]}),
            (VisualizationType.TREEMAP, {"labels": ["L"], "parents": [""], "values": [1], "performance": [1]}),
            (VisualizationType.FUNNEL, {"stages": ["S"], "values": [1]}),
            (VisualizationType.DONUT_CHART, {"devices": ["D"], "values": [1]}),
            (VisualizationType.HISTOGRAM, {"values": [1, 2, 3]}),
            (VisualizationType.BULLET_CHART, {"actual": 10, "target": 100})
        ]
        for vtype, vdata in types:
            try:
                chart = enhanced_viz_agent._generate_chart(vtype, vdata, {}, title="T")
                assert isinstance(chart, go.Figure)
            except Exception as e:
                print(f"FAILED for type {vtype}: {e}")
                raise e

    def test_dashboard_creation(self, enhanced_viz_agent):
        df = pd.DataFrame({
            "Channel": ["Google", "Meta", "LinkedIn", "Twitter", "TikTok", "Snapchat"],
            "Campaign": ["C1", "C2", "C3", "C4", "C5", "C6"],
            "Date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"],
            "Spend": [1000, 2000, 1500, 800, 1200, 2500],
            "Conversions": [10, 20, 15, 8, 12, 25],
            "ROAS": [2.5, 3.0, 2.0, 1.8, 2.2, 4.0],
            "CTR": [0.02, 0.03, 0.025, 0.015, 0.022, 0.04],
            "CPC": [1.0, 1.5, 1.2, 0.8, 1.1, 1.8],
            "Impressions": [10000, 20000, 15000, 8000, 12000, 25000],
            "Clicks": [200, 400, 300, 150, 250, 500],
            "Device": ["Desktop", "Mobile", "Tablet", "Mobile", "Desktop", "Mobile"],
            "Hour": [10, 14, 18, 9, 21, 12],
            "Day": [0, 1, 2, 3, 4, 5],
            "Frequency": [1.2, 2.5, 3.1, 1.8, 2.2, 4.5]
        })
        
        # Test basic dashboard
        dash = enhanced_viz_agent.create_dashboard_visualizations(df)
        assert len(dash) >= 3
        
        # Test executive dashboard
        insights = [{"id": "i1", "priority": 5, "title": "Top", "data": {"v": 1}}]
        exec_dash = enhanced_viz_agent.create_executive_dashboard(insights, df, {"target_roas": 3.0})
        assert len(exec_dash) > 0
        
        # Test analyst dashboard
        analyst_dash = enhanced_viz_agent.create_analyst_dashboard(insights, df)
        assert len(analyst_dash) > 0

    def test_visualization_agent_df_vs_list(self, viz_agent, temp_output_dir):
        """Test VisualizationAgent with formats"""
        # VisualizationAgent expects Campaign and ChannelPerformance objects
        from src.platform.models.campaign import Campaign, ChannelPerformance, DateRange, CampaignObjective
        from datetime import date
        
        campaign = Campaign(
            campaign_id="c1", 
            campaign_name="C1", 
            objectives=[CampaignObjective.CONVERSION],
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 31))
        )
        perfs = [ChannelPerformance(platform=PlatformType.GOOGLE_ADS, platform_name="Google Ads", spend=100, conversions=5)]
        
        with patch("plotly.graph_objects.Figure.write_image"):
            res = viz_agent.generate_all_visualizations(campaign, perfs)
            assert len(res) > 0

    def test_smart_chart_generator_edge_cases(self, chart_gen):
        """Test SmartChartGenerator edge cases for coverage"""
        
        # Empty keyword data
        fig = chart_gen.create_keyword_opportunity_scatter([])
        assert isinstance(fig, go.Figure)
        
        # Small trend data (test ma_window fix)
        small_data = {
            'dates': ['2024-01-01'],
            'metrics': {'m': [1.0]}
        }
        fig = chart_gen.create_performance_trend_chart(small_data, ['m'])
        assert isinstance(fig, go.Figure)
        
        # Gauge with benchmarks
        fig = chart_gen.create_performance_gauge(10, 100, "M", {"poor": 20, "good": 80, "excellent": 150})
        assert isinstance(fig, go.Figure)

    def test_enhanced_viz_agent_fallbacks(self, enhanced_viz_agent):
        """Test fallbacks in EnhancedVisualizationAgent"""
        from enum import Enum
        from src.engine.agents.smart_visualization_engine import VisualizationType
        
        # Unknown chart type
        class UnknownType(Enum):
            FOO = "foo"
        
        chart = enhanced_viz_agent._generate_chart(UnknownType.FOO, {"data": 1}, {})
        assert isinstance(chart, go.Figure)
        
        # Error in generation - should hit fallback
        error_chart = enhanced_viz_agent._generate_chart(VisualizationType.TREEMAP, ["not a dict"], {})
        assert isinstance(error_chart, go.Figure)
        
    def test_marketing_rules_adjust_for_data_coverage(self):
        """Test more branches in _adjust_for_data"""
        from src.engine.agents.marketing_visualization_rules import MarketingVisualizationRules
        from src.engine.agents.smart_visualization_engine import VisualizationType
        rules = MarketingVisualizationRules()
        config = rules._get_default_config()
        
        # Time granularity auto
        config["time_granularity"] = "auto"
        # Need date_range_days to trigger granularity logic
        data_with_dates = {"date_range": "30d", "date_range_days": 40}
        adj_config = rules._adjust_for_data(config, data_with_dates)
        assert adj_config["time_granularity"] == "weekly"
        
        # High cardinality bar -> adjust show_top_n
        config["chart_type"] = VisualizationType.BAR_CHART
        config["styling"] = {}
        data_high_card = {"cardinality": 25}
        adj_config = rules._adjust_for_data(config, data_high_card)
        assert adj_config["styling"]["show_top_n"] == 15

# --- SmartChartGenerator Tests ---

class TestSmartChartGenerator:
    def test_init(self):
        from src.engine.agents.chart_generators import SmartChartGenerator
        gen = SmartChartGenerator(brand_colors={"primary": "#000"})
        assert gen.brand_colors["primary"] == "#000"

    def test_create_performance_trend_with_anomalies(self, chart_gen):
        data = {
            "dates": [f"2024-01-{i:02d}" for i in range(1, 21)],
            "metrics": {
                "m": [10] * 19 + [100] # Anomaly at the end
            }
        }
        fig = chart_gen.create_performance_trend_chart(data, ["m"], show_anomalies=True)
        # Check if anomaly trace added
        trace_names = [t.name for t in fig.data]
        assert any("Anomalies" in name for name in trace_names)

    def test_create_attribution_sankey(self, chart_gen):
        data = {"paths": [{"path": ["A", "B", "Conversion"], "count": 10}]}
        fig = chart_gen.create_attribution_sankey(data)
        assert fig.data[0].type == "sankey"

    def test_create_keyword_opportunity_scatter(self, chart_gen):
        data = [
            {"keyword": "k1", "impressions": 100, "conversion_rate": 0.05, "spend": 10, "quality_score": 8},
            {"keyword": "k2", "impressions": 200, "conversion_rate": 0.02, "spend": 20, "quality_score": 5},
            {"keyword": "k3", "impressions": 300, "conversion_rate": 0.08, "spend": 30, "quality_score": 9},
            {"keyword": "k4", "impressions": 50, "conversion_rate": 0.01, "spend": 5, "quality_score": 3},
            {"keyword": "k5", "impressions": 1000, "conversion_rate": 0.1, "spend": 100, "quality_score": 10}
        ]
        fig = chart_gen.create_keyword_opportunity_scatter(data)
        assert fig.data[0].type == "scatter"

    def test_create_conversion_funnel_with_dropoff(self, chart_gen):
        data = {"stages": ["I", "C", "V"], "values": [100, 50, 10]}
        fig = chart_gen.create_conversion_funnel(data, show_percentages=True)
        assert len(fig.layout.annotations) >= 2 # Drop-off annotations

    def test_create_frequency_histogram(self, chart_gen):
        data = [1, 1, 2, 2, 2, 3, 4, 5, 2, 1]
        fig = chart_gen.create_frequency_histogram(data, optimal_range=(2, 4))
        assert fig.data[0].type == "histogram"
