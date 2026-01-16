"""
Smart Chart Generator - Integration Examples
Demonstrates publication-ready chart generation for digital marketing
"""

import numpy as np
from datetime import datetime, timedelta
from src.agents.chart_generators import SmartChartGenerator


def example_1_channel_comparison():
    """Example 1: Channel Performance Comparison"""
    
    print("=" * 70)
    print("Example 1: Channel Performance Comparison")
    print("=" * 70)
    
    # Sample data
    data = {
        'Google Ads': {
            'spend': 45000,
            'conversions': 850,
            'cpa': 52.94,
            'roas': 3.2
        },
        'Meta': {
            'spend': 32000,
            'conversions': 620,
            'cpa': 51.61,
            'roas': 2.8
        },
        'LinkedIn': {
            'spend': 28000,
            'conversions': 410,
            'cpa': 68.29,
            'roas': 4.1
        },
        'DV360': {
            'spend': 15000,
            'conversions': 180,
            'cpa': 83.33,
            'roas': 1.9
        }
    }
    
    metrics = ['spend', 'conversions', 'cpa', 'roas']
    
    benchmarks = {
        'cpa': 60.0,
        'roas': 2.5
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_channel_comparison_chart(data, metrics, benchmarks)
    
    print(f"\n📊 Chart Created: Channel Performance Comparison")
    print(f"   Channels: {len(data)}")
    print(f"   Metrics: {', '.join(metrics)}")
    print(f"   Benchmarks: {len(benchmarks)} metrics")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()  # Uncomment to display
    return fig


def example_2_performance_trend():
    """Example 2: Performance Trend Over Time"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Performance Trend Over Time")
    print("=" * 70)
    
    # Generate sample time series data
    dates = [(datetime.now() - timedelta(days=x)).strftime('%Y-%m-%d') 
             for x in range(30, 0, -1)]
    
    # Simulate metrics with trends and anomalies
    np.random.seed(42)
    base_ctr = 0.035
    ctr_values = base_ctr + np.random.normal(0, 0.005, 30) + np.linspace(0, 0.01, 30)
    ctr_values[15] = 0.055  # Anomaly
    
    base_cpc = 5.5
    cpc_values = base_cpc + np.random.normal(0, 0.5, 30) - np.linspace(0, 0.5, 30)
    cpc_values[20] = 8.0  # Anomaly
    
    data = {
        'dates': dates,
        'metrics': {
            'ctr': ctr_values.tolist(),
            'cpc': cpc_values.tolist()
        }
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_performance_trend_chart(
        data,
        metrics=['ctr', 'cpc'],
        show_anomalies=True
    )
    
    print(f"\n📊 Chart Created: Performance Trend")
    print(f"   Time Period: {len(dates)} days")
    print(f"   Metrics: CTR, CPC")
    print(f"   Features: Moving average, anomaly detection")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_3_attribution_sankey():
    """Example 3: Attribution Flow (Sankey Diagram)"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Attribution Flow (Sankey Diagram)")
    print("=" * 70)
    
    # Sample attribution paths
    touchpoint_data = {
        'paths': [
            {'path': ['Google', 'LinkedIn', 'Conversion'], 'count': 150},
            {'path': ['Meta', 'Google', 'Conversion'], 'count': 120},
            {'path': ['Google', 'Conversion'], 'count': 200},
            {'path': ['LinkedIn', 'Google', 'Conversion'], 'count': 80},
            {'path': ['Meta', 'LinkedIn', 'Conversion'], 'count': 60},
            {'path': ['Meta', 'Conversion'], 'count': 90},
            {'path': ['LinkedIn', 'Conversion'], 'count': 70}
        ]
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_attribution_sankey(touchpoint_data)
    
    total_conversions = sum(p['count'] for p in touchpoint_data['paths'])
    
    print(f"\n📊 Chart Created: Attribution Flow")
    print(f"   Total Paths: {len(touchpoint_data['paths'])}")
    print(f"   Total Conversions: {total_conversions}")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_4_performance_gauge():
    """Example 4: Performance Gauge (KPI)"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Performance Gauge (KPI)")
    print("=" * 70)
    
    # Sample KPI data
    actual = 0.045  # 4.5% CTR
    target = 0.04   # 4.0% target
    
    benchmarks = {
        'poor': 0.025,
        'good': 0.04,
        'excellent': 0.06
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_performance_gauge(
        actual=actual,
        target=target,
        metric_name="Click-Through Rate (CTR)",
        benchmarks=benchmarks
    )
    
    print(f"\n📊 Chart Created: Performance Gauge")
    print(f"   Metric: CTR")
    print(f"   Actual: {actual:.2%}")
    print(f"   Target: {target:.2%}")
    print(f"   Performance: {((actual/target - 1) * 100):.1f}% above target")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_5_hourly_heatmap():
    """Example 5: Hourly Performance Heatmap"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Hourly Performance Heatmap")
    print("=" * 70)
    
    # Generate sample heatmap data (7 days x 24 hours)
    np.random.seed(42)
    
    # Simulate day parting patterns
    base_rate = 0.03
    data = []
    
    for day in range(7):
        day_data = []
        for hour in range(24):
            # Higher conversion rates during business hours
            if 9 <= hour <= 17 and day < 5:  # Weekday business hours
                rate = base_rate + np.random.normal(0.01, 0.005)
            elif 18 <= hour <= 21:  # Evening
                rate = base_rate + np.random.normal(0.005, 0.003)
            else:
                rate = base_rate + np.random.normal(-0.005, 0.003)
            
            day_data.append(max(0, rate))
        data.append(day_data)
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_hourly_heatmap(np.array(data))
    
    print(f"\n📊 Chart Created: Hourly Performance Heatmap")
    print(f"   Dimensions: 7 days x 24 hours")
    print(f"   Metric: Conversion Rate")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_6_keyword_scatter():
    """Example 6: Keyword Opportunity Matrix"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Keyword Opportunity Matrix")
    print("=" * 70)
    
    # Sample keyword data
    np.random.seed(42)
    
    keyword_data = [
        {'keyword': 'digital marketing', 'impressions': 50000, 'conversion_rate': 0.045, 'spend': 5000, 'quality_score': 8},
        {'keyword': 'ppc agency', 'impressions': 8000, 'conversion_rate': 0.065, 'spend': 1200, 'quality_score': 9},
        {'keyword': 'google ads', 'impressions': 120000, 'conversion_rate': 0.025, 'spend': 8000, 'quality_score': 6},
        {'keyword': 'social media marketing', 'impressions': 35000, 'conversion_rate': 0.038, 'spend': 3500, 'quality_score': 7},
        {'keyword': 'seo services', 'impressions': 15000, 'conversion_rate': 0.055, 'spend': 2000, 'quality_score': 8},
        {'keyword': 'content marketing', 'impressions': 25000, 'conversion_rate': 0.042, 'spend': 2800, 'quality_score': 7},
        {'keyword': 'email marketing', 'impressions': 18000, 'conversion_rate': 0.048, 'spend': 2200, 'quality_score': 8},
        {'keyword': 'marketing automation', 'impressions': 12000, 'conversion_rate': 0.058, 'spend': 1800, 'quality_score': 9},
        {'keyword': 'brand awareness', 'impressions': 80000, 'conversion_rate': 0.015, 'spend': 4000, 'quality_score': 5},
        {'keyword': 'lead generation', 'impressions': 45000, 'conversion_rate': 0.052, 'spend': 4500, 'quality_score': 8}
    ]
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_keyword_opportunity_scatter(keyword_data)
    
    print(f"\n📊 Chart Created: Keyword Opportunity Matrix")
    print(f"   Keywords: {len(keyword_data)}")
    print(f"   Quadrants: Stars, Niche Winners, Opportunities, Underperformers")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_7_budget_treemap():
    """Example 7: Budget Allocation Treemap"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Budget Allocation Treemap")
    print("=" * 70)
    
    # Sample hierarchical budget data
    budget_data = {
        'labels': [
            'Google', 'Google-Search', 'Google-Display',
            'Meta', 'Meta-Facebook', 'Meta-Instagram',
            'LinkedIn', 'LinkedIn-Sponsored', 'LinkedIn-InMail'
        ],
        'parents': [
            '', 'Google', 'Google',
            '', 'Meta', 'Meta',
            '', 'LinkedIn', 'LinkedIn'
        ],
        'values': [
            50000, 35000, 15000,
            32000, 22000, 10000,
            28000, 20000, 8000
        ],
        'performance': [
            3.0, 3.2, 2.5,
            2.8, 2.9, 2.6,
            4.1, 4.3, 3.5
        ]
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_budget_treemap(budget_data)
    
    total_budget = sum([v for v, p in zip(budget_data['values'], budget_data['parents']) if p == ''])
    
    print(f"\n📊 Chart Created: Budget Allocation Treemap")
    print(f"   Total Budget: ${total_budget:,.0f}")
    print(f"   Channels: 3 (Google, Meta, LinkedIn)")
    print(f"   Campaigns: {len([p for p in budget_data['parents'] if p != ''])}")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_8_conversion_funnel():
    """Example 8: Conversion Funnel"""
    
    print("\n\n" + "=" * 70)
    print("Example 8: Conversion Funnel")
    print("=" * 70)
    
    # Sample funnel data
    funnel_data = {
        'stages': ['Impressions', 'Clicks', 'Leads', 'Conversions'],
        'values': [100000, 5000, 500, 100]
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_conversion_funnel(funnel_data, show_percentages=True)
    
    # Calculate conversion rates
    click_rate = (funnel_data['values'][1] / funnel_data['values'][0]) * 100
    lead_rate = (funnel_data['values'][2] / funnel_data['values'][1]) * 100
    conv_rate = (funnel_data['values'][3] / funnel_data['values'][2]) * 100
    
    print(f"\n📊 Chart Created: Conversion Funnel")
    print(f"   Stages: {len(funnel_data['stages'])}")
    print(f"   Click Rate: {click_rate:.2f}%")
    print(f"   Lead Rate: {lead_rate:.2f}%")
    print(f"   Conversion Rate: {conv_rate:.2f}%")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_9_frequency_histogram():
    """Example 9: Frequency Distribution"""
    
    print("\n\n" + "=" * 70)
    print("Example 9: Frequency Distribution")
    print("=" * 70)
    
    # Generate sample frequency data
    np.random.seed(42)
    frequency_data = np.random.gamma(3, 2, 1000).tolist()  # Gamma distribution
    
    optimal_range = (3, 7)
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_frequency_histogram(frequency_data, optimal_range)
    
    mean_freq = np.mean(frequency_data)
    median_freq = np.median(frequency_data)
    
    print(f"\n📊 Chart Created: Frequency Distribution")
    print(f"   Data Points: {len(frequency_data)}")
    print(f"   Mean Frequency: {mean_freq:.2f}")
    print(f"   Median Frequency: {median_freq:.2f}")
    print(f"   Optimal Range: {optimal_range[0]}-{optimal_range[1]}")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


def example_10_device_donut():
    """Example 10: Device Breakdown Donut Chart"""
    
    print("\n\n" + "=" * 70)
    print("Example 10: Device Breakdown Donut Chart")
    print("=" * 70)
    
    # Sample device data
    device_data = {
        'devices': ['Desktop', 'Mobile', 'Tablet'],
        'values': [5000, 8000, 2000]
    }
    
    # Create chart
    generator = SmartChartGenerator()
    fig = generator.create_device_donut(device_data)
    
    total = sum(device_data['values'])
    mobile_pct = (device_data['values'][1] / total) * 100
    
    print(f"\n📊 Chart Created: Device Breakdown")
    print(f"   Total Conversions: {total:,}")
    print(f"   Mobile Share: {mobile_pct:.1f}%")
    print(f"   ✅ Chart ready for display")
    
    # fig.show()
    return fig


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Smart Chart Generator - Integration Examples")
    print("=" * 70)
    
    # Run all examples
    example_1_channel_comparison()
    example_2_performance_trend()
    example_3_attribution_sankey()
    example_4_performance_gauge()
    example_5_hourly_heatmap()
    example_6_keyword_scatter()
    example_7_budget_treemap()
    example_8_conversion_funnel()
    example_9_frequency_histogram()
    example_10_device_donut()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities:")
    print("   • 10 publication-ready chart types")
    print("   • Intelligent defaults and styling")
    print("   • Marketing-specific color schemes")
    print("   • Automatic anomaly detection")
    print("   • Benchmark integration")
    print("   • Interactive hover templates")
    print("   • Responsive layouts")
    print("=" * 70)
