"""
Filter Presets - Complete Examples
Demonstrates comprehensive filter presets for common analysis scenarios
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.filter_presets import FilterPresets
from src.agents.visualization_filters import SmartFilterEngine


def create_sample_data():
    """Create comprehensive sample data"""
    np.random.seed(42)
    dates = pd.date_range(start='2024-01-01', periods=90, freq='D')
    
    data = []
    for date in dates:
        for channel in ['Google Ads', 'Meta', 'LinkedIn', 'TikTok']:
            for campaign in ['Brand', 'Performance']:
                data.append({
                    'date': date,
                    'channel': channel,
                    'campaign': campaign,
                    'funnel_stage': np.random.choice(['awareness', 'consideration', 'conversion']),
                    'device': np.random.choice(['Desktop', 'Mobile', 'Tablet'], p=[0.35, 0.55, 0.1]),
                    'spend': np.random.uniform(100, 3000),
                    'impressions': np.random.randint(1000, 50000),
                    'clicks': np.random.randint(50, 2000),
                    'conversions': np.random.randint(5, 200),
                    'ctr': np.random.uniform(0.01, 0.08),
                    'cpc': np.random.uniform(1, 10),
                    'cpa': np.random.uniform(10, 150),
                    'roas': np.random.uniform(0.5, 6.0),
                    'conversion_rate': np.random.uniform(0.005, 0.10),
                    'lead_quality_score': np.random.uniform(0.3, 0.95)
                })
    
    return pd.DataFrame(data)


def example_1_list_all_presets():
    """Example 1: List All Available Presets"""
    
    print("=" * 70)
    print("Example 1: List All Available Presets")
    print("=" * 70)
    
    # Get all presets
    all_presets = FilterPresets.PRESETS
    
    print(f"\n📊 Total Presets Available: {len(all_presets)}")
    
    # Group by category
    categories = FilterPresets.get_categories()
    
    for category in categories:
        presets_in_category = FilterPresets.get_presets_by_category(category)
        print(f"\n📁 {category} ({len(presets_in_category)} presets):")
        
        for preset_name, preset_data in presets_in_category.items():
            print(f"   • {preset_data['name']}")
            print(f"     {preset_data['description']}")
    
    print(f"\n✅ Listed all presets by category")


def example_2_performance_analysis_presets():
    """Example 2: Performance Analysis Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Performance Analysis Presets")
    print("=" * 70)
    
    data = create_sample_data()
    filter_engine = SmartFilterEngine()
    
    print(f"\n📊 Original Data: {len(data)} rows")
    
    # Test performance presets
    performance_presets = ['top_performers', 'bottom_performers', 'recent_top_performers']
    
    for preset_name in performance_presets:
        preset = FilterPresets.get_preset(preset_name)
        
        print(f"\n🔍 Preset: {preset['name']}")
        print(f"   Description: {preset['description']}")
        print(f"   Use Case: {preset['use_case']}")
        
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"   Result: {len(filtered_data)} rows ({len(filtered_data)/len(data)*100:.1f}%)")
        print(f"   Avg ROAS: {filtered_data['roas'].mean():.2f}")
    
    print(f"\n✅ Performance analysis presets demonstrated")


def example_3_optimization_presets():
    """Example 3: Optimization Opportunity Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Optimization Opportunity Presets")
    print("=" * 70)
    
    data = create_sample_data()
    filter_engine = SmartFilterEngine()
    
    # Context with benchmarks
    context = {
        'benchmarks': {
            'ctr': 0.035,
            'roas': 2.5,
            'cpc': 4.5
        }
    }
    
    optimization_presets = ['opportunities', 'high_spend_low_roas', 'low_ctr_high_spend']
    
    for preset_name in optimization_presets:
        preset = FilterPresets.get_preset(preset_name, context=context)
        
        print(f"\n💡 Preset: {preset['name']}")
        print(f"   Description: {preset['description']}")
        
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"   Result: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Total Spend: ${filtered_data['spend'].sum():,.0f}")
            print(f"   Avg ROAS: {filtered_data['roas'].mean():.2f}")
            print(f"   Potential Savings: ${filtered_data['spend'].sum() * 0.2:,.0f} (if optimized)")
    
    print(f"\n✅ Optimization presets demonstrated")


def example_4_device_and_channel_presets():
    """Example 4: Device and Channel Specific Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Device and Channel Specific Presets")
    print("=" * 70)
    
    data = create_sample_data()
    filter_engine = SmartFilterEngine()
    
    # Device presets
    print(f"\n📱 Device-Specific Presets:")
    device_presets = ['mobile_performance', 'mobile_high_ctr', 'desktop_performance']
    
    for preset_name in device_presets:
        preset = FilterPresets.get_preset(preset_name)
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"\n   {preset['name']}: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Devices: {', '.join(filtered_data['device'].unique())}")
            print(f"   Avg CTR: {filtered_data['ctr'].mean():.2%}")
    
    # Channel presets
    print(f"\n📺 Channel-Specific Presets:")
    channel_presets = ['paid_search_only', 'social_media_only']
    
    for preset_name in channel_presets:
        preset = FilterPresets.get_preset(preset_name)
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"\n   {preset['name']}: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Channels: {', '.join(filtered_data['channel'].unique())}")
    
    print(f"\n✅ Device and channel presets demonstrated")


def example_5_funnel_based_presets():
    """Example 5: Funnel-Based Analysis Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Funnel-Based Analysis Presets")
    print("=" * 70)
    
    data = create_sample_data()
    filter_engine = SmartFilterEngine()
    
    funnel_presets = ['high_intent', 'awareness_stage', 'consideration_stage']
    
    for preset_name in funnel_presets:
        preset = FilterPresets.get_preset(preset_name)
        
        print(f"\n🎯 Preset: {preset['name']}")
        print(f"   Description: {preset['description']}")
        
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"   Result: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Funnel Stages: {', '.join(filtered_data['funnel_stage'].unique())}")
            print(f"   Avg Conversion Rate: {filtered_data['conversion_rate'].mean():.2%}")
    
    print(f"\n✅ Funnel-based presets demonstrated")


def example_6_search_presets():
    """Example 6: Search Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Search Presets")
    print("=" * 70)
    
    # Search by term
    search_results = FilterPresets.search_presets("mobile")
    
    print(f"\n🔍 Search Results for 'mobile': {len(search_results)} presets")
    for preset_name, preset_data in search_results.items():
        print(f"   • {preset_data['name']}")
        print(f"     {preset_data['description']}")
    
    # Search by another term
    search_results = FilterPresets.search_presets("high")
    
    print(f"\n🔍 Search Results for 'high': {len(search_results)} presets")
    for preset_name, preset_data in list(search_results.items())[:5]:
        print(f"   • {preset_data['name']}")
    
    print(f"\n✅ Preset search demonstrated")


def example_7_recommended_presets():
    """Example 7: Context-Based Recommendations"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Context-Based Recommendations")
    print("=" * 70)
    
    # B2B context
    b2b_context = {
        'business_model': 'B2B',
        'benchmarks': {'roas': 3.0}
    }
    
    b2b_recommendations = FilterPresets.get_recommended_presets(b2b_context)
    
    print(f"\n👔 B2B Context Recommendations:")
    for preset_name in b2b_recommendations:
        preset = FilterPresets.get_preset(preset_name)
        print(f"   • {preset['name']}")
        print(f"     {preset['description']}")
    
    # B2C context
    b2c_context = {
        'business_model': 'B2C',
        'benchmarks': {'roas': 2.5}
    }
    
    b2c_recommendations = FilterPresets.get_recommended_presets(b2c_context)
    
    print(f"\n🛍️ B2C Context Recommendations:")
    for preset_name in b2c_recommendations:
        preset = FilterPresets.get_preset(preset_name)
        print(f"   • {preset['name']}")
        print(f"     {preset['description']}")
    
    print(f"\n✅ Context-based recommendations demonstrated")


def example_8_quality_and_budget_presets():
    """Example 8: Quality and Budget Analysis Presets"""
    
    print("\n\n" + "=" * 70)
    print("Example 8: Quality and Budget Analysis Presets")
    print("=" * 70)
    
    data = create_sample_data()
    filter_engine = SmartFilterEngine()
    
    # Quality presets
    print(f"\n✨ Quality Analysis Presets:")
    quality_presets = ['high_quality_traffic', 'low_quality_traffic']
    
    for preset_name in quality_presets:
        preset = FilterPresets.get_preset(preset_name)
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"\n   {preset['name']}: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Avg CTR: {filtered_data['ctr'].mean():.2%}")
            print(f"   Avg Conversion Rate: {filtered_data['conversion_rate'].mean():.2%}")
    
    # Budget presets
    print(f"\n💰 Budget Analysis Presets:")
    budget_presets = ['high_budget_campaigns', 'low_budget_high_roas']
    
    for preset_name in budget_presets:
        preset = FilterPresets.get_preset(preset_name)
        filtered_data = filter_engine.apply_filters(data, preset['filters'])
        
        print(f"\n   {preset['name']}: {len(filtered_data)} rows")
        if len(filtered_data) > 0:
            print(f"   Total Spend: ${filtered_data['spend'].sum():,.0f}")
            print(f"   Avg ROAS: {filtered_data['roas'].mean():.2f}")
    
    print(f"\n✅ Quality and budget presets demonstrated")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Filter Presets - Complete Examples")
    print("=" * 70)
    
    # Run all examples
    example_1_list_all_presets()
    example_2_performance_analysis_presets()
    example_3_optimization_presets()
    example_4_device_and_channel_presets()
    example_5_funnel_based_presets()
    example_6_search_presets()
    example_7_recommended_presets()
    example_8_quality_and_budget_presets()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities Demonstrated:")
    print("   • 25+ predefined filter presets")
    print("   • 8 preset categories")
    print("   • Performance analysis presets")
    print("   • Optimization opportunity presets")
    print("   • Device and channel specific presets")
    print("   • Funnel-based analysis presets")
    print("   • Quality and budget presets")
    print("   • Search functionality")
    print("   • Context-based recommendations")
    print("\n🎯 Preset Categories:")
    categories = FilterPresets.get_categories()
    for cat in categories:
        count = len(FilterPresets.get_presets_by_category(cat))
        print(f"   • {cat}: {count} presets")
    print("=" * 70)
