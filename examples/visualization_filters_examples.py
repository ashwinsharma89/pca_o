"""
Smart Filter System - Complete Examples
Demonstrates intelligent filtering for visualizations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.visualization_filters import SmartFilterEngine, FilterType


def create_sample_campaign_data():
    """Create comprehensive sample data for filter testing"""
    
    np.random.seed(42)
    dates = pd.date_range(start='2024-01-01', periods=90, freq='D')
    
    data = []
    channels = ['Google Ads', 'Meta', 'LinkedIn', 'TikTok']
    campaigns = ['Brand Awareness', 'Lead Gen', 'Conversion', 'Retargeting']
    devices = ['Desktop', 'Mobile', 'Tablet']
    
    for date in dates:
        for channel in channels:
            for campaign in campaigns[:2]:  # 2 campaigns per channel
                data.append({
                    'date': date,
                    'channel': channel,
                    'campaign': campaign,
                    'device': np.random.choice(devices, p=[0.35, 0.55, 0.1]),
                    'spend': np.random.uniform(500, 3000),
                    'impressions': np.random.randint(5000, 50000),
                    'clicks': np.random.randint(200, 2000),
                    'conversions': np.random.randint(20, 200),
                    'ctr': np.random.uniform(0.02, 0.06),
                    'cpc': np.random.uniform(2, 8),
                    'cpa': np.random.uniform(20, 100),
                    'roas': np.random.uniform(1.5, 5.0),
                    'conversion_rate': np.random.uniform(0.01, 0.08)
                })
    
    return pd.DataFrame(data)


def example_1_filter_suggestions():
    """Example 1: Get Smart Filter Suggestions"""
    
    print("=" * 70)
    print("Example 1: Smart Filter Suggestions")
    print("=" * 70)
    
    # Create data
    data = create_sample_campaign_data()
    
    print(f"\n📊 Campaign Data:")
    print(f"   Rows: {len(data)}")
    print(f"   Columns: {', '.join(data.columns)}")
    print(f"   Date Range: {data['date'].min().date()} to {data['date'].max().date()}")
    
    # Initialize filter engine
    filter_engine = SmartFilterEngine()
    
    # Get suggestions
    context = {
        'business_model': 'B2B',
        'has_ab_test': False,
        'benchmarks': {'ctr': 0.035, 'cpc': 4.5, 'roas': 2.5}
    }
    
    suggestions = filter_engine.suggest_filters_for_data(data, context)
    
    print(f"\n🔍 Filter Suggestions ({len(suggestions)} total):")
    
    for idx, suggestion in enumerate(suggestions, 1):
        print(f"\n   {idx}. {suggestion['label']} (Priority: {suggestion['priority']})")
        print(f"      Type: {suggestion['type'].value}")
        print(f"      Reasoning: {suggestion['reasoning']}")
        
        if 'options' in suggestion:
            if isinstance(suggestion['options'], list) and len(suggestion['options']) <= 5:
                print(f"      Options: {', '.join(map(str, suggestion['options']))}")
            elif isinstance(suggestion['options'], list):
                print(f"      Options: {len(suggestion['options'])} available")
    
    print(f"\n✅ Filter suggestions generated successfully")


def example_2_date_filters():
    """Example 2: Apply Date Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Date Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    
    # Test 1: Last 30 days
    print(f"\n🔍 Test 1: Last 30 Days Filter")
    filters = {
        'date_filter': {
            'type': FilterType.DATE_PRESET,
            'preset': 'last_30_days'
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows")
    print(f"   Date Range: {filtered_data['date'].min().date()} to {filtered_data['date'].max().date()}")
    
    # Test 2: Specific date range
    print(f"\n🔍 Test 2: Specific Date Range")
    filters = {
        'date_range': {
            'type': FilterType.DATE_RANGE,
            'start_date': '2024-02-01',
            'end_date': '2024-02-29'
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows")
    print(f"   Date Range: {filtered_data['date'].min().date()} to {filtered_data['date'].max().date()}")
    
    print(f"\n✅ Date filters applied successfully")


def example_3_channel_filters():
    """Example 3: Channel and Dimensional Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Channel and Dimensional Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    print(f"   Channels: {', '.join(data['channel'].unique())}")
    
    # Filter to specific channels
    print(f"\n🔍 Filter: Google Ads and Meta only")
    filters = {
        'channel_filter': {
            'type': FilterType.CHANNEL,
            'column': 'channel',
            'values': ['Google Ads', 'Meta']
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows")
    print(f"   Channels: {', '.join(filtered_data['channel'].unique())}")
    
    # Multiple dimensional filters
    print(f"\n🔍 Filter: Google Ads + Mobile devices")
    filters = {
        'channel_filter': {
            'type': FilterType.CHANNEL,
            'column': 'channel',
            'values': ['Google Ads']
        },
        'device_filter': {
            'type': FilterType.DEVICE,
            'column': 'device',
            'values': ['Mobile']
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows")
    print(f"   Channel: {filtered_data['channel'].unique()[0]}")
    print(f"   Device: {filtered_data['device'].unique()[0]}")
    
    print(f"\n✅ Dimensional filters applied successfully")


def example_4_performance_tier_filters():
    """Example 4: Performance Tier Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Performance Tier Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    print(f"   ROAS Range: {data['roas'].min():.2f} to {data['roas'].max():.2f}")
    print(f"   ROAS Mean: {data['roas'].mean():.2f}")
    
    # Top performers
    print(f"\n🔍 Filter: Top Performers (Top 20% by ROAS)")
    filters = {
        'performance_tier': {
            'type': FilterType.PERFORMANCE_TIER,
            'tier': 'top',
            'metric': 'roas'
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   ROAS Range: {filtered_data['roas'].min():.2f} to {filtered_data['roas'].max():.2f}")
    print(f"   ROAS Mean: {filtered_data['roas'].mean():.2f}")
    
    # Bottom performers
    print(f"\n🔍 Filter: Bottom Performers (Bottom 20% by ROAS)")
    filters = {
        'performance_tier': {
            'type': FilterType.PERFORMANCE_TIER,
            'tier': 'bottom',
            'metric': 'roas'
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   ROAS Range: {filtered_data['roas'].min():.2f} to {filtered_data['roas'].max():.2f}")
    print(f"   ROAS Mean: {filtered_data['roas'].mean():.2f}")
    
    print(f"\n✅ Performance tier filters applied successfully")


def example_5_metric_threshold_filters():
    """Example 5: Metric Threshold Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Metric Threshold Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    
    # Single threshold
    print(f"\n🔍 Filter: CTR > 4%")
    filters = {
        'ctr_threshold': {
            'type': FilterType.METRIC_THRESHOLD,
            'conditions': [
                {'metric': 'ctr', 'operator': '>', 'value': 0.04}
            ]
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   CTR Range: {filtered_data['ctr'].min():.4f} to {filtered_data['ctr'].max():.4f}")
    
    # Multiple thresholds
    print(f"\n🔍 Filter: CTR > 4% AND Spend > $1000")
    filters = {
        'multi_threshold': {
            'type': FilterType.METRIC_THRESHOLD,
            'conditions': [
                {'metric': 'ctr', 'operator': '>', 'value': 0.04},
                {'metric': 'spend', 'operator': '>', 'value': 1000}
            ]
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   CTR Range: {filtered_data['ctr'].min():.4f} to {filtered_data['ctr'].max():.4f}")
    print(f"   Spend Range: ${filtered_data['spend'].min():.0f} to ${filtered_data['spend'].max():.0f}")
    
    # Between range
    print(f"\n🔍 Filter: CPA between $30 and $60")
    filters = {
        'cpa_range': {
            'type': FilterType.METRIC_THRESHOLD,
            'conditions': [
                {'metric': 'cpa', 'operator': 'between', 'value': (30, 60)}
            ]
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   CPA Range: ${filtered_data['cpa'].min():.2f} to ${filtered_data['cpa'].max():.2f}")
    
    print(f"\n✅ Metric threshold filters applied successfully")


def example_6_benchmark_filters():
    """Example 6: Benchmark Relative Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Benchmark Relative Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    benchmarks = {'ctr': 0.035, 'roas': 2.5}
    
    print(f"\n📊 Original Data: {len(data)} rows")
    print(f"   Benchmarks: CTR = {benchmarks['ctr']:.3f}, ROAS = {benchmarks['roas']:.1f}")
    
    # Above benchmark
    print(f"\n🔍 Filter: Performance Above Benchmark")
    filters = {
        'above_benchmark': {
            'type': FilterType.BENCHMARK_RELATIVE,
            'comparison': 'above',
            'benchmarks': benchmarks
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   CTR Mean: {filtered_data['ctr'].mean():.4f} (benchmark: {benchmarks['ctr']:.3f})")
    print(f"   ROAS Mean: {filtered_data['roas'].mean():.2f} (benchmark: {benchmarks['roas']:.1f})")
    
    # Below benchmark
    print(f"\n🔍 Filter: Performance Below Benchmark")
    filters = {
        'below_benchmark': {
            'type': FilterType.BENCHMARK_RELATIVE,
            'comparison': 'below',
            'benchmarks': benchmarks
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   CTR Mean: {filtered_data['ctr'].mean():.4f} (benchmark: {benchmarks['ctr']:.3f})")
    print(f"   ROAS Mean: {filtered_data['roas'].mean():.2f} (benchmark: {benchmarks['roas']:.1f})")
    
    print(f"\n✅ Benchmark filters applied successfully")


def example_7_filter_impact_analysis():
    """Example 7: Filter Impact Analysis"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Filter Impact Analysis")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    # Apply aggressive filters
    print(f"\n📊 Applying Aggressive Filters...")
    filters = {
        'date_filter': {
            'type': FilterType.DATE_PRESET,
            'preset': 'last_7_days'
        },
        'channel_filter': {
            'type': FilterType.CHANNEL,
            'column': 'channel',
            'values': ['Google Ads']
        },
        'performance_tier': {
            'type': FilterType.PERFORMANCE_TIER,
            'tier': 'top',
            'metric': 'roas'
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    
    # Get impact summary
    impact = filter_engine.get_filter_impact_summary()
    
    print(f"\n📊 Filter Impact Summary:")
    print(f"   Original Rows: {impact['rows_original']}")
    print(f"   Filtered Rows: {impact['rows_filtered']}")
    print(f"   Rows Removed: {impact['rows_removed']}")
    print(f"   Reduction: {impact['reduction_percentage']:.1f}%")
    print(f"   Filters Applied: {impact['filters_applied']}")
    
    if impact.get('warnings'):
        print(f"\n⚠️ Warnings:")
        for warning in impact['warnings']:
            print(f"   [{warning['severity'].upper()}] {warning['message']}")
            print(f"   Suggestion: {warning['suggestion']}")
    
    print(f"\n✅ Filter impact analyzed successfully")


def example_8_combined_filters():
    """Example 8: Combined Complex Filters"""
    
    print("\n\n" + "=" * 70)
    print("Example 8: Combined Complex Filters")
    print("=" * 70)
    
    data = create_sample_campaign_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    
    # Complex filter combination
    print(f"\n🔍 Complex Filter Combination:")
    print(f"   • Last 30 days")
    print(f"   • Google Ads or Meta")
    print(f"   • Mobile devices")
    print(f"   • CTR > 3.5%")
    print(f"   • ROAS > 2.5")
    
    filters = {
        'date': {
            'type': FilterType.DATE_PRESET,
            'preset': 'last_30_days'
        },
        'channels': {
            'type': FilterType.CHANNEL,
            'column': 'channel',
            'values': ['Google Ads', 'Meta']
        },
        'device': {
            'type': FilterType.DEVICE,
            'column': 'device',
            'values': ['Mobile']
        },
        'metrics': {
            'type': FilterType.METRIC_THRESHOLD,
            'conditions': [
                {'metric': 'ctr', 'operator': '>', 'value': 0.035},
                {'metric': 'roas', 'operator': '>', 'value': 2.5}
            ]
        }
    }
    
    filtered_data = filter_engine.apply_filters(data, filters)
    
    print(f"\n📊 Filtered Data: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
    print(f"   Channels: {', '.join(filtered_data['channel'].unique())}")
    print(f"   Devices: {', '.join(filtered_data['device'].unique())}")
    print(f"   CTR Range: {filtered_data['ctr'].min():.4f} to {filtered_data['ctr'].max():.4f}")
    print(f"   ROAS Range: {filtered_data['roas'].min():.2f} to {filtered_data['roas'].max():.2f}")
    
    print(f"\n✅ Complex filters applied successfully")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Smart Filter System - Complete Examples")
    print("=" * 70)
    
    # Run all examples
    example_1_filter_suggestions()
    example_2_date_filters()
    example_3_channel_filters()
    example_4_performance_tier_filters()
    example_5_metric_threshold_filters()
    example_6_benchmark_filters()
    example_7_filter_impact_analysis()
    example_8_combined_filters()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities Demonstrated:")
    print("   • Smart filter suggestions based on data")
    print("   • Date filters (presets and ranges)")
    print("   • Dimensional filters (channel, device, campaign)")
    print("   • Performance tier filters (top/middle/bottom)")
    print("   • Metric threshold filters")
    print("   • Benchmark relative filters")
    print("   • Filter impact analysis")
    print("   • Complex filter combinations")
    print("\n🎯 Filter Types Available:")
    print("   • Temporal: Date range, presets, comparisons")
    print("   • Dimensional: Channel, campaign, device, geography")
    print("   • Performance: Thresholds, tiers, benchmarks")
    print("   • Advanced: Statistical, anomaly detection")
    print("=" * 70)
