"""
Marketing Visualization Rules
Domain-specific visualization rules for digital marketing contexts
"""

from typing import Dict, List, Any, Optional
from enum import Enum
from .smart_visualization_engine import VisualizationType
from loguru import logger


class MarketingInsightCategory(Enum):
    """Categories of marketing insights"""
    CHANNEL_COMPARISON = "channel_comparison"
    PERFORMANCE_TREND = "performance_trend"
    BUDGET_DISTRIBUTION = "budget_distribution"
    AUDIENCE_PERFORMANCE = "audience_performance"
    CREATIVE_DECAY = "creative_decay"
    ATTRIBUTION_FLOW = "attribution_flow"
    CONVERSION_FUNNEL = "conversion_funnel"
    QUALITY_SCORE = "quality_score_components"
    HOURLY_PERFORMANCE = "hourly_performance"
    DEVICE_BREAKDOWN = "device_breakdown"
    GEO_PERFORMANCE = "geo_performance"
    KEYWORD_EFFICIENCY = "keyword_efficiency"
    FREQUENCY_ANALYSIS = "frequency_analysis"
    BENCHMARK_COMPARISON = "benchmark_comparison"
    AD_PERFORMANCE = "ad_performance"
    CAMPAIGN_HEALTH = "campaign_health"


class MarketingVisualizationRules:
    """Domain-specific visualization rules for digital marketing"""
    
    def __init__(self):
        """Initialize marketing visualization rules"""
        self.rules = self._build_rules()
        logger.info("Initialized Marketing Visualization Rules")
    
    def get_visualization_for_insight(self, 
                                     insight_category: str,
                                     data: Optional[Dict] = None) -> Dict:
        """
        Map marketing insights to optimal visualizations
        
        Args:
            insight_category: Category of marketing insight
            data: Optional data to inform visualization choices
        
        Returns:
            Visualization config with:
            - chart_type
            - styling_rules
            - annotations
            - benchmarks_display
        """
        
        config = self.rules.get(insight_category, self._get_default_config())
        
        # Enhance config with data-specific adjustments
        if data:
            config = self._adjust_for_data(config, data)
        
        logger.info(f"Selected visualization config for {insight_category}: {config['chart_type']}")
        return config
    
    def _build_rules(self) -> Dict[str, Dict]:
        """Build the complete rule set"""
        
        return {
            # CHANNEL PERFORMANCE COMPARISON
            "channel_comparison": {
                "chart_type": VisualizationType.GROUPED_BAR,
                "metrics": ["spend", "conversions", "cpa", "roas"],
                "styling": {
                    "color_by": "efficiency",  # Green for good, red for poor
                    "sort_by": "roas",
                    "show_benchmarks": True,
                    "benchmark_style": "reference_line",
                    "color_scale": "RdYlGn_r"  # Red-Yellow-Green reversed
                },
                "annotations": {
                    "enabled": True,
                    "types": ["best_performer", "worst_performer"],
                    "show_values": True,
                    "show_variance": True
                },
                "layout": {
                    "height": 500,
                    "show_legend": True,
                    "x_axis_title": "Channel",
                    "y_axis_title": "Performance Metrics"
                }
            },
            
            # TIME-BASED PERFORMANCE
            "performance_trend": {
                "chart_type": VisualizationType.MULTI_LINE,
                "time_granularity": "auto",  # Auto-select based on date range
                "metrics": ["ctr", "cpc", "conversion_rate", "roas"],
                "styling": {
                    "highlight_anomalies": True,
                    "show_forecast": False,
                    "show_moving_average": True,
                    "ma_window": 7,  # 7-day moving average
                    "line_width": 2,
                    "opacity": 0.8
                },
                "annotations": {
                    "enabled": True,
                    "types": ["peaks", "valleys", "trend_changes"],
                    "anomaly_threshold": 2.5,  # Standard deviations
                    "show_events": True
                },
                "layout": {
                    "height": 500,
                    "show_legend": True,
                    "x_axis_title": "Date",
                    "y_axis_title": "Metric Value",
                    "show_range_selector": True
                }
            },
            
            # BUDGET ALLOCATION
            "budget_distribution": {
                "chart_type": VisualizationType.TREEMAP,
                "hierarchy": ["channel", "campaign", "ad_group"],
                "sizing_metric": "spend",
                "color_metric": "roas",
                "styling": {
                    "color_scale": "RdYlGn",  # Red-Yellow-Green
                    "show_percentage": True,
                    "show_values": True,
                    "border_width": 2,
                    "text_position": "middle center"
                },
                "annotations": {
                    "enabled": True,
                    "show_metric_name": True,
                    "show_percentage": True
                },
                "layout": {
                    "height": 600,
                    "title": "Budget Allocation & Efficiency"
                }
            },
            
            # AUDIENCE SEGMENTATION
            "audience_performance": {
                "chart_type": VisualizationType.BUBBLE_CHART,
                "x_axis": "reach",
                "y_axis": "engagement_rate",
                "bubble_size": "conversions",
                "color_by": "segment",
                "styling": {
                    "quadrant_lines": True,  # Show median lines
                    "highlight_outliers": True,
                    "bubble_scale": "sqrt",
                    "opacity": 0.6,
                    "show_labels": True
                },
                "annotations": {
                    "enabled": True,
                    "label_top_performers": 5,
                    "show_quadrant_labels": True
                },
                "layout": {
                    "height": 600,
                    "x_axis_title": "Reach",
                    "y_axis_title": "Engagement Rate",
                    "show_legend": True
                }
            },
            
            # CREATIVE FATIGUE
            "creative_decay": {
                "chart_type": VisualizationType.AREA_CHART,
                "metrics": ["ctr", "engagement_rate"],
                "overlay": "frequency",
                "styling": {
                    "color": "red_gradient",
                    "highlight_threshold": 7,  # Frequency threshold
                    "show_decay_rate": True,
                    "fill_opacity": 0.3,
                    "line_width": 2
                },
                "annotations": {
                    "enabled": True,
                    "show_threshold_line": True,
                    "threshold_label": "Fatigue Zone",
                    "show_decay_percentage": True
                },
                "layout": {
                    "height": 500,
                    "x_axis_title": "Time Period",
                    "y_axis_title": "Performance Metrics",
                    "show_legend": True
                }
            },
            
            # ATTRIBUTION JOURNEY
            "attribution_flow": {
                "chart_type": VisualizationType.SANKEY,
                "nodes": "touchpoints",
                "flows": "conversion_paths",
                "styling": {
                    "color_by_channel": True,
                    "show_conversion_rate": True,
                    "highlight_top_paths": 5,
                    "node_thickness": 20,
                    "link_opacity": 0.4
                },
                "annotations": {
                    "enabled": True,
                    "show_path_value": True,
                    "show_conversion_rate": True
                },
                "layout": {
                    "height": 700,
                    "title": "Multi-Touch Attribution Flow"
                }
            },
            
            # CONVERSION FUNNEL
            "conversion_funnel": {
                "chart_type": VisualizationType.FUNNEL,
                "stages": ["impression", "click", "landing", "conversion"],
                "styling": {
                    "show_drop_off_rate": True,
                    "highlight_biggest_drop": True,
                    "color_by_health": True,  # Green/yellow/red
                    "color_scale": {
                        "healthy": "#00C853",    # Green
                        "warning": "#FFD600",    # Yellow
                        "critical": "#D50000"    # Red
                    },
                    "text_position": "inside"
                },
                "annotations": {
                    "enabled": True,
                    "show_stage_values": True,
                    "show_drop_off_percentage": True,
                    "highlight_critical_drops": True
                },
                "layout": {
                    "height": 500,
                    "title": "Conversion Funnel Analysis"
                }
            },
            
            # QUALITY SCORE BREAKDOWN (Google Ads specific)
            "quality_score_components": {
                "chart_type": VisualizationType.BULLET_CHART,
                "components": ["expected_ctr", "ad_relevance", "landing_page"],
                "target": 7,  # Good QS benchmark
                "styling": {
                    "color_ranges": [
                        {"min": 0, "max": 5, "color": "#D50000", "label": "Poor"},
                        {"min": 5, "max": 7, "color": "#FFD600", "label": "Average"},
                        {"min": 7, "max": 10, "color": "#00C853", "label": "Good"}
                    ],
                    "show_target_line": True,
                    "bar_height": 30
                },
                "annotations": {
                    "enabled": True,
                    "show_component_scores": True,
                    "show_improvement_needed": True
                },
                "layout": {
                    "height": 400,
                    "title": "Quality Score Components"
                }
            },
            
            # DAYPARTING / HOUR OF DAY
            "hourly_performance": {
                "chart_type": VisualizationType.HEATMAP,
                "x_axis": "hour_of_day",
                "y_axis": "day_of_week",
                "value": "conversion_rate",
                "styling": {
                    "color_scale": "Viridis",
                    "annotate_values": True,
                    "highlight_best_times": True,
                    "show_scale": True,
                    "reverse_y_axis": False
                },
                "annotations": {
                    "enabled": True,
                    "show_values": True,
                    "value_format": ".2%",
                    "highlight_top_n": 5
                },
                "layout": {
                    "height": 500,
                    "x_axis_title": "Hour of Day",
                    "y_axis_title": "Day of Week",
                    "title": "Performance by Day & Hour"
                }
            },
            
            # DEVICE PERFORMANCE
            "device_breakdown": {
                "chart_type": VisualizationType.DONUT_CHART,
                "dimension": "device_type",
                "metric": "conversions",
                "styling": {
                    "show_percentages": True,
                    "inner_metric": "total_conversions",
                    "sort_by": "value",
                    "hole_size": 0.4,
                    "pull_largest": 0.1  # Pull out largest slice
                },
                "annotations": {
                    "enabled": True,
                    "show_values": True,
                    "show_percentages": True,
                    "center_text": "Total"
                },
                "layout": {
                    "height": 500,
                    "title": "Conversions by Device Type",
                    "show_legend": True
                }
            },
            
            # GEOGRAPHIC PERFORMANCE
            "geo_performance": {
                "chart_type": VisualizationType.HEATMAP,
                "dimension": "region",
                "metric": "roas",
                "styling": {
                    "color_scale": "RdYlGn",
                    "show_top_n": 10,
                    "annotate_values": True,
                    "sort_by": "value"
                },
                "annotations": {
                    "enabled": True,
                    "show_values": True,
                    "value_format": ".2f",
                    "highlight_outliers": True
                },
                "layout": {
                    "height": 600,
                    "title": "Geographic Performance (ROAS)",
                    "x_axis_title": "Region",
                    "y_axis_title": "ROAS"
                }
            },
            
            # KEYWORD PERFORMANCE (Search specific)
            "keyword_efficiency": {
                "chart_type": VisualizationType.SCATTER_PLOT,
                "x_axis": "impressions",
                "y_axis": "conversion_rate",
                "bubble_size": "spend",
                "color_by": "quality_score",
                "styling": {
                    "quadrant_lines": True,
                    "label_outliers": True,
                    "highlight_opportunities": True,  # High impressions, low CVR
                    "opacity": 0.6,
                    "size_scale": "sqrt"
                },
                "annotations": {
                    "enabled": True,
                    "label_top_keywords": 10,
                    "show_quadrant_labels": True,
                    "quadrant_labels": {
                        "top_right": "Stars",
                        "top_left": "Optimize",
                        "bottom_right": "Opportunities",
                        "bottom_left": "Underperformers"
                    }
                },
                "layout": {
                    "height": 600,
                    "x_axis_title": "Impressions",
                    "y_axis_title": "Conversion Rate",
                    "title": "Keyword Efficiency Matrix"
                }
            },
            
            # FREQUENCY DISTRIBUTION
            "frequency_analysis": {
                "chart_type": VisualizationType.HISTOGRAM,
                "metric": "frequency",
                "overlay": "conversion_rate",
                "styling": {
                    "bins": "auto",
                    "highlight_optimal_range": [3, 7],
                    "show_distribution_stats": True,
                    "bar_color": "#1f77b4",
                    "overlay_color": "#ff7f0e"
                },
                "annotations": {
                    "enabled": True,
                    "show_optimal_zone": True,
                    "show_mean": True,
                    "show_median": True,
                    "show_stats": ["mean", "median", "std"]
                },
                "layout": {
                    "height": 500,
                    "x_axis_title": "Frequency",
                    "y_axis_title": "Count / Conversion Rate",
                    "title": "Frequency Distribution Analysis"
                }
            },
            
            # PERFORMANCE VS BENCHMARK
            "benchmark_comparison": {
                "chart_type": VisualizationType.BULLET_CHART,
                "metrics": ["ctr", "cpc", "conversion_rate", "roas"],
                "benchmarks": "industry_average",
                "styling": {
                    "show_variance": True,
                    "color_by_performance": True,
                    "annotation_style": "percentage_diff",
                    "performance_colors": {
                        "above": "#00C853",
                        "at": "#FFD600",
                        "below": "#D50000"
                    }
                },
                "annotations": {
                    "enabled": True,
                    "show_variance_percentage": True,
                    "show_benchmark_value": True,
                    "show_actual_value": True
                },
                "layout": {
                    "height": 500,
                    "title": "Performance vs Industry Benchmarks"
                }
            },
            
            # AD PERFORMANCE
            "ad_performance": {
                "chart_type": VisualizationType.HORIZONTAL_BAR,
                "dimension": "ad_name",
                "metrics": ["ctr", "conversions", "roas"],
                "styling": {
                    "sort_by": "roas",
                    "show_top_n": 20,
                    "color_by": "performance",
                    "show_values": True
                },
                "annotations": {
                    "enabled": True,
                    "show_values": True,
                    "highlight_top_3": True
                },
                "layout": {
                    "height": 600,
                    "x_axis_title": "ROAS",
                    "y_axis_title": "Ad Name",
                    "title": "Top 20 Ads by ROAS"
                }
            },
            
            # CAMPAIGN HEALTH
            "campaign_health": {
                "chart_type": VisualizationType.GAUGE,
                "metric": "health_score",
                "target": 80,
                "styling": {
                    "color_ranges": [
                        {"min": 0, "max": 50, "color": "#D50000"},
                        {"min": 50, "max": 70, "color": "#FFD600"},
                        {"min": 70, "max": 100, "color": "#00C853"}
                    ],
                    "show_target": True,
                    "show_delta": True
                },
                "annotations": {
                    "enabled": True,
                    "show_score": True,
                    "show_status": True
                },
                "layout": {
                    "height": 400,
                    "title": "Campaign Health Score"
                }
            }
        }
    
    def _get_default_config(self) -> Dict:
        """Get default visualization config"""
        return {
            "chart_type": VisualizationType.BAR_CHART,
            "styling": {
                "default": True,
                "color_scale": "Blues"
            },
            "annotations": {
                "enabled": False
            },
            "layout": {
                "height": 500,
                "show_legend": True
            }
        }
    
    def _adjust_for_data(self, config: Dict, data: Dict) -> Dict:
        """Adjust configuration based on data characteristics"""
        
        # Adjust time granularity for trends
        if config.get("time_granularity") == "auto" and "date_range" in data:
            date_range_days = data.get("date_range_days", 30)
            
            if date_range_days <= 7:
                config["time_granularity"] = "hourly"
            elif date_range_days <= 31:
                config["time_granularity"] = "daily"
            elif date_range_days <= 90:
                config["time_granularity"] = "weekly"
            else:
                config["time_granularity"] = "monthly"
        
        # Adjust number of items shown based on data size
        if "cardinality" in data:
            cardinality = data["cardinality"]
            
            if cardinality > 50 and "show_top_n" not in config.get("styling", {}):
                config["styling"]["show_top_n"] = 20
            elif cardinality > 20:
                config["styling"]["show_top_n"] = 15
        
        return config
    
    def get_color_scheme_for_metric(self, metric: str) -> str:
        """Get appropriate color scheme for a metric"""
        
        # Metrics where higher is better
        positive_metrics = ["roas", "ctr", "conversion_rate", "quality_score", 
                          "engagement_rate", "reach", "impressions"]
        
        # Metrics where lower is better
        negative_metrics = ["cpc", "cpa", "cpm", "frequency", "bounce_rate"]
        
        if metric.lower() in positive_metrics:
            return "RdYlGn"  # Red-Yellow-Green (red=low, green=high)
        elif metric.lower() in negative_metrics:
            return "RdYlGn_r"  # Reversed (green=low, red=high)
        else:
            return "Blues"  # Default
    
    def get_benchmark_display_style(self, chart_type: VisualizationType) -> str:
        """Get appropriate benchmark display style for chart type"""
        
        benchmark_styles = {
            VisualizationType.BAR_CHART: "reference_line",
            VisualizationType.GROUPED_BAR: "reference_line",
            VisualizationType.LINE_CHART: "shaded_area",
            VisualizationType.MULTI_LINE: "reference_line",
            VisualizationType.BULLET_CHART: "target_marker",
            VisualizationType.GAUGE: "threshold_line",
            VisualizationType.SCATTER_PLOT: "quadrant_lines"
        }
        
        return benchmark_styles.get(chart_type, "reference_line")
    
    def get_annotation_config(self, insight_category: str) -> Dict:
        """Get annotation configuration for insight category"""
        
        config = self.rules.get(insight_category, {})
        return config.get("annotations", {"enabled": False})
    
    def should_show_benchmarks(self, insight_category: str) -> bool:
        """Determine if benchmarks should be shown for this insight"""
        
        benchmark_insights = [
            "channel_comparison",
            "benchmark_comparison",
            "quality_score_components",
            "campaign_health"
        ]
        
        return insight_category in benchmark_insights
    
    def get_layout_config(self, insight_category: str) -> Dict:
        """Get layout configuration for insight category"""
        
        config = self.rules.get(insight_category, {})
        return config.get("layout", {"height": 500, "show_legend": True})


class MarketingColorSchemes:
    """Predefined color schemes for marketing visualizations"""
    
    # Performance colors
    PERFORMANCE = {
        "excellent": "#00C853",   # Green
        "good": "#64DD17",        # Light Green
        "average": "#FFD600",     # Yellow
        "poor": "#FF6D00",        # Orange
        "critical": "#D50000"     # Red
    }
    
    # Channel colors
    CHANNELS = {
        "google_search": "#4285F4",    # Google Blue
        "google_display": "#34A853",   # Google Green
        "meta": "#1877F2",             # Facebook Blue
        "instagram": "#E4405F",        # Instagram Pink
        "linkedin": "#0A66C2",         # LinkedIn Blue
        "tiktok": "#000000",           # TikTok Black
        "twitter": "#1DA1F2",          # Twitter Blue
        "pinterest": "#E60023",        # Pinterest Red
        "snapchat": "#FFFC00",         # Snapchat Yellow
        "youtube": "#FF0000",          # YouTube Red
        "dv360": "#4285F4"             # DV360 Blue
    }
    
    # Device colors
    DEVICES = {
        "desktop": "#5E35B1",     # Purple
        "mobile": "#00897B",      # Teal
        "tablet": "#FB8C00"       # Orange
    }
    
    # Objective colors
    OBJECTIVES = {
        "awareness": "#42A5F5",       # Light Blue
        "consideration": "#66BB6A",   # Light Green
        "conversion": "#EF5350",      # Light Red
        "retention": "#AB47BC"        # Purple
    }
    
    @staticmethod
    def get_channel_color(channel: str) -> str:
        """Get color for a specific channel"""
        return MarketingColorSchemes.CHANNELS.get(
            channel.lower().replace(" ", "_"),
            "#757575"  # Default gray
        )
    
    @staticmethod
    def get_performance_color(value: float, 
                            benchmark: float,
                            higher_is_better: bool = True) -> str:
        """Get color based on performance vs benchmark"""
        
        if higher_is_better:
            ratio = value / benchmark if benchmark > 0 else 1
        else:
            ratio = benchmark / value if value > 0 else 1
        
        if ratio >= 1.2:
            return MarketingColorSchemes.PERFORMANCE["excellent"]
        elif ratio >= 1.0:
            return MarketingColorSchemes.PERFORMANCE["good"]
        elif ratio >= 0.8:
            return MarketingColorSchemes.PERFORMANCE["average"]
        elif ratio >= 0.6:
            return MarketingColorSchemes.PERFORMANCE["poor"]
        else:
            return MarketingColorSchemes.PERFORMANCE["critical"]
