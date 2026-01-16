"""
Marketing Visualization Rules - Integration Examples
Demonstrates domain-specific visualization rules for digital marketing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.marketing_visualization_rules import (
    MarketingVisualizationRules,
    MarketingInsightCategory,
    MarketingColorSchemes
)
from src.agents.smart_visualization_engine import SmartVisualizationEngine


def example_1_channel_comparison():
    """Example 1: Channel Performance Comparison"""
    
    print("=" * 70)
    print("Example 1: Channel Performance Comparison")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("channel_comparison")
    
    print(f"\n📊 Insight Category: Channel Comparison")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   Metrics: {', '.join(config['metrics'])}")
    print(f"   Sort By: {config['styling']['sort_by']}")
    print(f"   Show Benchmarks: {config['styling']['show_benchmarks']}")
    
    print(f"\n🎨 Styling:")
    print(f"   Color By: {config['styling']['color_by']}")
    print(f"   Color Scale: {config['styling']['color_scale']}")
    print(f"   Benchmark Style: {config['styling']['benchmark_style']}")
    
    print(f"\n💡 Annotations:")
    print(f"   Enabled: {config['annotations']['enabled']}")
    print(f"   Types: {', '.join(config['annotations']['types'])}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_2_performance_trend():
    """Example 2: Performance Trend Over Time"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Performance Trend Over Time")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config with data context
    data_context = {
        "date_range_days": 30,
        "cardinality": 30
    }
    
    config = rules.get_visualization_for_insight(
        "performance_trend",
        data=data_context
    )
    
    print(f"\n📊 Insight Category: Performance Trend")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   Time Granularity: {config['time_granularity']}")
    print(f"   Metrics: {', '.join(config['metrics'])}")
    
    print(f"\n🎨 Styling:")
    print(f"   Highlight Anomalies: {config['styling']['highlight_anomalies']}")
    print(f"   Show Moving Average: {config['styling']['show_moving_average']}")
    print(f"   MA Window: {config['styling']['ma_window']} days")
    
    print(f"\n💡 Annotations:")
    print(f"   Types: {', '.join(config['annotations']['types'])}")
    print(f"   Anomaly Threshold: {config['annotations']['anomaly_threshold']} std devs")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_3_creative_fatigue():
    """Example 3: Creative Fatigue Detection"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Creative Fatigue Detection")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("creative_decay")
    
    print(f"\n📊 Insight Category: Creative Decay")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   Metrics: {', '.join(config['metrics'])}")
    print(f"   Overlay: {config['overlay']}")
    
    print(f"\n🎨 Styling:")
    print(f"   Color: {config['styling']['color']}")
    print(f"   Fatigue Threshold: {config['styling']['highlight_threshold']} frequency")
    print(f"   Show Decay Rate: {config['styling']['show_decay_rate']}")
    
    print(f"\n💡 Annotations:")
    print(f"   Show Threshold Line: {config['annotations']['show_threshold_line']}")
    print(f"   Threshold Label: {config['annotations']['threshold_label']}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_4_conversion_funnel():
    """Example 4: Conversion Funnel Analysis"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Conversion Funnel Analysis")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("conversion_funnel")
    
    print(f"\n📊 Insight Category: Conversion Funnel")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   Stages: {', '.join(config['stages'])}")
    
    print(f"\n🎨 Styling:")
    print(f"   Show Drop-off Rate: {config['styling']['show_drop_off_rate']}")
    print(f"   Highlight Biggest Drop: {config['styling']['highlight_biggest_drop']}")
    print(f"   Color By Health: {config['styling']['color_by_health']}")
    
    print(f"\n🎨 Color Scale:")
    for status, color in config['styling']['color_scale'].items():
        print(f"   {status.title()}: {color}")
    
    print(f"\n💡 Annotations:")
    print(f"   Show Stage Values: {config['annotations']['show_stage_values']}")
    print(f"   Show Drop-off %: {config['annotations']['show_drop_off_percentage']}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_5_quality_score():
    """Example 5: Quality Score Components (Google Ads)"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Quality Score Components")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("quality_score_components")
    
    print(f"\n📊 Insight Category: Quality Score Components")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   Components: {', '.join(config['components'])}")
    print(f"   Target: {config['target']}")
    
    print(f"\n🎨 Color Ranges:")
    for range_config in config['styling']['color_ranges']:
        print(f"   {range_config['label']}: {range_config['min']}-{range_config['max']} ({range_config['color']})")
    
    print(f"\n💡 Annotations:")
    print(f"   Show Component Scores: {config['annotations']['show_component_scores']}")
    print(f"   Show Improvement Needed: {config['annotations']['show_improvement_needed']}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_6_dayparting():
    """Example 6: Hourly Performance Heatmap"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Hourly Performance (Day Parting)")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("hourly_performance")
    
    print(f"\n📊 Insight Category: Hourly Performance")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   X-Axis: {config['x_axis']}")
    print(f"   Y-Axis: {config['y_axis']}")
    print(f"   Value: {config['value']}")
    
    print(f"\n🎨 Styling:")
    print(f"   Color Scale: {config['styling']['color_scale']}")
    print(f"   Annotate Values: {config['styling']['annotate_values']}")
    print(f"   Highlight Best Times: {config['styling']['highlight_best_times']}")
    
    print(f"\n💡 Annotations:")
    print(f"   Value Format: {config['annotations']['value_format']}")
    print(f"   Highlight Top N: {config['annotations']['highlight_top_n']}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_7_keyword_efficiency():
    """Example 7: Keyword Efficiency Matrix"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Keyword Efficiency Matrix")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Get visualization config
    config = rules.get_visualization_for_insight("keyword_efficiency")
    
    print(f"\n📊 Insight Category: Keyword Efficiency")
    print(f"   Chart Type: {config['chart_type'].value}")
    print(f"   X-Axis: {config['x_axis']}")
    print(f"   Y-Axis: {config['y_axis']}")
    print(f"   Bubble Size: {config['bubble_size']}")
    print(f"   Color By: {config['color_by']}")
    
    print(f"\n🎨 Styling:")
    print(f"   Quadrant Lines: {config['styling']['quadrant_lines']}")
    print(f"   Label Outliers: {config['styling']['label_outliers']}")
    print(f"   Highlight Opportunities: {config['styling']['highlight_opportunities']}")
    
    print(f"\n💡 Quadrant Labels:")
    for position, label in config['annotations']['quadrant_labels'].items():
        print(f"   {position.replace('_', ' ').title()}: {label}")
    
    print(f"\n✅ Configuration retrieved successfully")


def example_8_color_schemes():
    """Example 8: Marketing Color Schemes"""
    
    print("\n\n" + "=" * 70)
    print("Example 8: Marketing Color Schemes")
    print("=" * 70)
    
    # Channel colors
    print(f"\n🎨 Channel Colors:")
    channels = ['google_search', 'meta', 'linkedin', 'tiktok', 'youtube']
    for channel in channels:
        color = MarketingColorSchemes.get_channel_color(channel)
        print(f"   {channel.replace('_', ' ').title()}: {color}")
    
    # Performance colors
    print(f"\n🎨 Performance Colors:")
    for level, color in MarketingColorSchemes.PERFORMANCE.items():
        print(f"   {level.title()}: {color}")
    
    # Device colors
    print(f"\n🎨 Device Colors:")
    for device, color in MarketingColorSchemes.DEVICES.items():
        print(f"   {device.title()}: {color}")
    
    # Performance-based coloring
    print(f"\n🎨 Performance-Based Coloring:")
    test_cases = [
        (120, 100, True, "Excellent (20% above)"),
        (105, 100, True, "Good (5% above)"),
        (95, 100, True, "Average (5% below)"),
        (75, 100, True, "Poor (25% below)"),
        (50, 100, True, "Critical (50% below)")
    ]
    
    for value, benchmark, higher_is_better, description in test_cases:
        color = MarketingColorSchemes.get_performance_color(value, benchmark, higher_is_better)
        print(f"   {description}: {color}")
    
    print(f"\n✅ Color schemes demonstrated successfully")


def example_9_benchmark_display():
    """Example 9: Benchmark Display Styles"""
    
    print("\n\n" + "=" * 70)
    print("Example 9: Benchmark Display Styles")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    # Test different chart types
    from src.agents.smart_visualization_engine import VisualizationType
    
    chart_types = [
        VisualizationType.BAR_CHART,
        VisualizationType.LINE_CHART,
        VisualizationType.BULLET_CHART,
        VisualizationType.GAUGE,
        VisualizationType.SCATTER_PLOT
    ]
    
    print(f"\n📊 Benchmark Display Styles by Chart Type:")
    for chart_type in chart_types:
        style = rules.get_benchmark_display_style(chart_type)
        print(f"   {chart_type.value.title()}: {style}")
    
    print(f"\n✅ Benchmark styles retrieved successfully")


def example_10_insight_categories():
    """Example 10: All Marketing Insight Categories"""
    
    print("\n\n" + "=" * 70)
    print("Example 10: All Marketing Insight Categories")
    print("=" * 70)
    
    # Initialize rules
    rules = MarketingVisualizationRules()
    
    print(f"\n📊 Available Marketing Insight Categories:")
    
    categories = [
        "channel_comparison",
        "performance_trend",
        "budget_distribution",
        "audience_performance",
        "creative_decay",
        "attribution_flow",
        "conversion_funnel",
        "quality_score_components",
        "hourly_performance",
        "device_breakdown",
        "geo_performance",
        "keyword_efficiency",
        "frequency_analysis",
        "benchmark_comparison",
        "ad_performance",
        "campaign_health"
    ]
    
    for i, category in enumerate(categories, 1):
        config = rules.get_visualization_for_insight(category)
        print(f"   {i:2d}. {category.replace('_', ' ').title()}")
        print(f"       Chart: {config['chart_type'].value}")
        print(f"       Benchmarks: {rules.should_show_benchmarks(category)}")
    
    print(f"\n✅ Total Categories: {len(categories)}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Marketing Visualization Rules - Integration Examples")
    print("=" * 70)
    
    # Run all examples
    example_1_channel_comparison()
    example_2_performance_trend()
    example_3_creative_fatigue()
    example_4_conversion_funnel()
    example_5_quality_score()
    example_6_dayparting()
    example_7_keyword_efficiency()
    example_8_color_schemes()
    example_9_benchmark_display()
    example_10_insight_categories()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities:")
    print("   • 16 marketing-specific insight categories")
    print("   • Domain-specific visualization rules")
    print("   • Context-aware configuration")
    print("   • Marketing color schemes")
    print("   • Benchmark display styles")
    print("   • Annotation configurations")
    print("   • Layout optimizations")
    print("=" * 70)
