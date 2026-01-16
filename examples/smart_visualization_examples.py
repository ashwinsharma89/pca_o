"""
Smart Visualization Engine - Integration Examples
Demonstrates intelligent visualization selection based on data and context
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.smart_visualization_engine import (
    SmartVisualizationEngine,
    VisualizationType,
    InsightType
)


def example_1_comparison_visualization():
    """Example 1: Comparison - Channel Performance"""
    
    print("=" * 70)
    print("Example 1: Comparison Visualization - Channel Performance")
    print("=" * 70)
    
    # Create sample data
    data = pd.DataFrame({
        'Channel': ['Google Search', 'Meta', 'LinkedIn', 'DV360', 'TikTok'],
        'Spend': [45000, 32000, 28000, 15000, 8000],
        'Conversions': [850, 620, 410, 180, 95],
        'ROAS': [3.2, 2.8, 4.1, 1.9, 2.3]
    })
    
    print(f"\n📊 Data: {len(data)} channels")
    print(data.to_string(index=False))
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for comparison
    viz_type = engine.select_visualization(
        data=data,
        insight_type="comparison",
        audience="executive"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: Few categories (5), single comparison → Bar chart optimal")
    
    # Create visualization
    fig = engine.create_visualization(
        data=data,
        viz_type=viz_type,
        title="Channel Performance Comparison",
        x='Channel',
        y='Spend'
    )
    
    print(f"   ✅ Visualization created successfully")


def example_2_trend_visualization():
    """Example 2: Trend - Performance Over Time"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Trend Visualization - Performance Over Time")
    print("=" * 70)
    
    # Create time series data
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    data = pd.DataFrame({
        'Date': dates,
        'CTR': np.random.uniform(0.03, 0.06, 30),
        'CPC': np.random.uniform(4, 8, 30),
        'Conv_Rate': np.random.uniform(0.02, 0.05, 30)
    })
    
    print(f"\n📊 Data: {len(data)} days, 3 metrics")
    print(data.head().to_string(index=False))
    print("...")
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for trend
    viz_type = engine.select_visualization(
        data=data,
        insight_type="trend",
        audience="analyst"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: Time series with 3 metrics → Multi-line chart optimal")
    
    # Create visualization
    fig = engine.create_visualization(
        data=data,
        viz_type=viz_type,
        title="Performance Trends - Last 30 Days"
    )
    
    print(f"   ✅ Visualization created successfully")


def example_3_composition_visualization():
    """Example 3: Composition - Budget Allocation"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Composition Visualization - Budget Allocation")
    print("=" * 70)
    
    # Create composition data
    data = pd.DataFrame({
        'Category': ['Search', 'Social', 'Display', 'Video', 'Native'],
        'Budget': [45000, 30000, 15000, 8000, 2000]
    })
    
    print(f"\n📊 Data: {len(data)} categories")
    print(data.to_string(index=False))
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for composition
    viz_type = engine.select_visualization(
        data=data,
        insight_type="composition",
        audience="executive"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: 5 categories, part-to-whole → Donut chart optimal")
    
    # Create visualization
    fig = engine.create_visualization(
        data=data,
        viz_type=viz_type,
        title="Budget Allocation by Category"
    )
    
    print(f"   ✅ Visualization created successfully")


def example_4_performance_visualization():
    """Example 4: Performance - KPI vs Target"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Performance Visualization - KPI vs Target")
    print("=" * 70)
    
    # Create performance data
    data = {
        'value': 85,
        'target': 100,
        'metric': 'Lead Quality Score'
    }
    
    print(f"\n📊 Data: Single KPI")
    print(f"   Metric: {data['metric']}")
    print(f"   Current: {data['value']}")
    print(f"   Target: {data['target']}")
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for performance
    viz_type = engine.select_visualization(
        data=data,
        insight_type="performance",
        audience="executive"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: Single KPI vs target, executive audience → Gauge optimal")
    
    # Create visualization
    fig = engine.create_visualization(
        data=data,
        viz_type=viz_type,
        title="Lead Quality Score"
    )
    
    print(f"   ✅ Visualization created successfully")


def example_5_relationship_visualization():
    """Example 5: Relationship - Correlation Analysis"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Relationship Visualization - Correlation Analysis")
    print("=" * 70)
    
    # Create correlation data
    np.random.seed(42)
    data = pd.DataFrame({
        'Frequency': np.random.uniform(2, 8, 50),
        'CTR': np.random.uniform(0.01, 0.05, 50),
        'Engagement_Rate': np.random.uniform(0.02, 0.08, 50)
    })
    
    # Add some correlation
    data['CTR'] = data['CTR'] - (data['Frequency'] - 5) * 0.003
    
    print(f"\n📊 Data: {len(data)} data points, 3 variables")
    print(data.head().to_string(index=False))
    print("...")
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for relationship
    viz_type = engine.select_visualization(
        data=data,
        insight_type="relationship",
        audience="analyst"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: 3 variables, relationship analysis → Bubble chart optimal")
    
    print(f"   ✅ Visualization type selected successfully")


def example_6_ranking_visualization():
    """Example 6: Ranking - Top Performing Campaigns"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Ranking Visualization - Top Performing Campaigns")
    print("=" * 70)
    
    # Create ranking data
    campaigns = [f"Campaign {i}" for i in range(1, 21)]
    roas = sorted(np.random.uniform(1.5, 5.0, 20), reverse=True)
    
    data = pd.DataFrame({
        'Campaign': campaigns,
        'ROAS': roas
    })
    
    print(f"\n📊 Data: {len(data)} campaigns")
    print(data.head(10).to_string(index=False))
    print("...")
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Select visualization for ranking
    viz_type = engine.select_visualization(
        data=data,
        insight_type="ranking",
        audience="analyst"
    )
    
    print(f"\n🎨 Selected Visualization: {viz_type.value}")
    print(f"   Reason: Many items (20), ranking → Horizontal bar optimal")
    
    # Create visualization
    fig = engine.create_visualization(
        data=data,
        viz_type=viz_type,
        title="Top 20 Campaigns by ROAS"
    )
    
    print(f"   ✅ Visualization created successfully")


def example_7_context_aware_selection():
    """Example 7: Context-Aware Selection"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Context-Aware Visualization Selection")
    print("=" * 70)
    
    # Same data, different contexts
    data = pd.DataFrame({
        'Metric': ['CTR', 'CPC', 'Conv Rate', 'ROAS'],
        'Actual': [0.045, 5.5, 0.06, 3.2],
        'Benchmark': [0.04, 6.0, 0.05, 2.5]
    })
    
    print(f"\n📊 Data: 4 metrics with benchmarks")
    print(data.to_string(index=False))
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Context 1: Executive audience
    print(f"\n🎯 Context 1: Executive Audience")
    viz_type_exec = engine.select_visualization(
        data=data,
        insight_type="performance",
        audience="executive",
        context={'show_variance': False}
    )
    print(f"   Selected: {viz_type_exec.value}")
    print(f"   Reason: Multiple KPIs, executive → Grouped bar for quick comparison")
    
    # Context 2: Analyst audience with variance
    print(f"\n🎯 Context 2: Analyst Audience (Variance Analysis)")
    viz_type_analyst = engine.select_visualization(
        data=data,
        insight_type="performance",
        audience="analyst",
        context={'show_variance': True}
    )
    print(f"   Selected: {viz_type_analyst.value}")
    print(f"   Reason: Variance analysis requested → Waterfall chart shows deltas")


def example_8_data_profiling():
    """Example 8: Data Profiling and Selection Logic"""
    
    print("\n\n" + "=" * 70)
    print("Example 8: Data Profiling and Selection Logic")
    print("=" * 70)
    
    # Initialize engine
    engine = SmartVisualizationEngine()
    
    # Test different data profiles
    test_cases = [
        {
            'name': 'Few categories, single metric',
            'data': pd.DataFrame({
                'Category': ['A', 'B', 'C', 'D'],
                'Value': [100, 150, 120, 90]
            }),
            'insight_type': 'comparison'
        },
        {
            'name': 'Many categories, single metric',
            'data': pd.DataFrame({
                'Category': [f'Cat{i}' for i in range(15)],
                'Value': np.random.randint(50, 200, 15)
            }),
            'insight_type': 'comparison'
        },
        {
            'name': 'Multiple metrics, few categories',
            'data': pd.DataFrame({
                'Channel': ['Search', 'Social', 'Display'],
                'Metric1': [100, 150, 120],
                'Metric2': [80, 90, 110],
                'Metric3': [120, 140, 100]
            }),
            'insight_type': 'comparison'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📊 Test Case {i}: {test_case['name']}")
        
        # Profile data
        profile = engine._profile_data(test_case['data'])
        print(f"   Cardinality: {profile['cardinality']}")
        print(f"   Num Metrics: {profile['num_metrics']}")
        
        # Select visualization
        viz_type = engine.select_visualization(
            data=test_case['data'],
            insight_type=test_case['insight_type'],
            audience='analyst'
        )
        
        print(f"   Selected: {viz_type.value}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Smart Visualization Engine - Integration Examples")
    print("=" * 70)
    
    # Run all examples
    example_1_comparison_visualization()
    example_2_trend_visualization()
    example_3_composition_visualization()
    example_4_performance_visualization()
    example_5_relationship_visualization()
    example_6_ranking_visualization()
    example_7_context_aware_selection()
    example_8_data_profiling()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities:")
    print("   • Automatic visualization type selection")
    print("   • Context-aware decision making")
    print("   • Audience-specific optimization")
    print("   • Data profiling and analysis")
    print("   • 15+ visualization types supported")
    print("   • Intelligent defaults and fallbacks")
    print("=" * 70)
