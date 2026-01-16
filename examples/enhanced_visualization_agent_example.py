"""
Enhanced Visualization Agent - Integration Example
Demonstrates the complete integrated visualization system
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.enhanced_visualization_agent import EnhancedVisualizationAgent


def create_sample_campaign_data():
    """Create sample campaign data"""
    
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    data = []
    for date in dates:
        for channel in ['Google Ads', 'Meta', 'LinkedIn']:
            data.append({
                'Date': date,
                'Channel': channel,
                'Platform': channel,
                'Spend': np.random.uniform(1000, 3000),
                'Impressions': np.random.randint(10000, 50000),
                'Clicks': np.random.randint(500, 2000),
                'Conversions': np.random.randint(50, 200),
                'CTR': np.random.uniform(0.02, 0.05),
                'CPC': np.random.uniform(2, 8),
                'ROAS': np.random.uniform(1.5, 4.0),
                'Device': np.random.choice(['Desktop', 'Mobile', 'Tablet'], p=[0.4, 0.5, 0.1])
            })
    
    return pd.DataFrame(data)


def example_1_automatic_insight_visualization():
    """Example 1: Automatically create visualizations from insights"""
    
    print("=" * 70)
    print("Example 1: Automatic Insight Visualization")
    print("=" * 70)
    
    # Sample insights from reasoning agent
    insights = [
        {
            'id': 'insight_1',
            'title': 'Channel Performance Comparison',
            'description': 'Google Ads outperforming other channels in ROAS',
            'data': {
                'Google Ads': {'spend': 45000, 'conversions': 850, 'roas': 3.2},
                'Meta': {'spend': 32000, 'conversions': 620, 'roas': 2.8},
                'LinkedIn': {'spend': 28000, 'conversions': 410, 'roas': 4.1}
            },
            'benchmarks': {'roas': 2.5}
        },
        {
            'id': 'insight_2',
            'title': 'Performance Trend Over Time',
            'description': 'CTR improving over the last 30 days',
            'data': {
                'dates': [(datetime.now() - timedelta(days=x)).strftime('%Y-%m-%d') 
                         for x in range(30, 0, -1)],
                'metrics': {
                    'ctr': (0.03 + np.random.normal(0, 0.003, 30) + np.linspace(0, 0.01, 30)).tolist(),
                    'cpc': (5.5 + np.random.normal(0, 0.3, 30) - np.linspace(0, 0.5, 30)).tolist()
                }
            }
        },
        {
            'id': 'insight_3',
            'title': 'Device Performance Breakdown',
            'description': 'Mobile driving majority of conversions',
            'data': {
                'devices': ['Desktop', 'Mobile', 'Tablet'],
                'values': [5000, 8000, 2000]
            }
        }
    ]
    
    # Initialize enhanced visualization agent
    viz_agent = EnhancedVisualizationAgent()
    
    # Create visualizations
    visualizations = viz_agent.create_visualizations_for_insights(insights)
    
    print(f"\n📊 Created {len(visualizations)} visualizations:")
    for viz in visualizations:
        print(f"\n   • {viz['title']}")
        print(f"     Chart Type: {viz['chart_type']}")
        print(f"     Category: {viz['category']}")
        print(f"     Description: {viz['description']}")
    
    print(f"\n✅ All visualizations created successfully")
    
    # Uncomment to display
    # for viz in visualizations:
    #     viz['chart'].show()
    
    return visualizations


def example_2_category_specific_charts():
    """Example 2: Create charts for specific categories"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Category-Specific Chart Creation")
    print("=" * 70)
    
    viz_agent = EnhancedVisualizationAgent()
    
    # Test different categories
    categories = [
        {
            'category': 'channel_comparison',
            'data': {
                'Google': {'spend': 45000, 'conversions': 850, 'roas': 3.2},
                'Meta': {'spend': 32000, 'conversions': 620, 'roas': 2.8}
            },
            'title': 'Channel Performance'
        },
        {
            'category': 'conversion_funnel',
            'data': {
                'stages': ['Impressions', 'Clicks', 'Leads', 'Conversions'],
                'values': [100000, 5000, 500, 100]
            },
            'title': 'Conversion Funnel'
        },
        {
            'category': 'campaign_health',
            'data': {
                'value': 85,
                'target': 80,
                'metric_name': 'Campaign Health Score'
            },
            'title': 'Campaign Health'
        }
    ]
    
    print(f"\n📊 Creating charts for {len(categories)} categories:")
    
    for cat_config in categories:
        result = viz_agent.create_chart_for_category(
            category=cat_config['category'],
            data=cat_config['data'],
            title=cat_config['title']
        )
        
        print(f"\n   • {cat_config['title']}")
        print(f"     Category: {cat_config['category']}")
        print(f"     Chart Type: {result['chart_type']}")
    
    print(f"\n✅ All category-specific charts created")


def example_3_dashboard_creation():
    """Example 3: Create complete dashboard from campaign data"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Complete Dashboard Creation")
    print("=" * 70)
    
    # Create sample data
    campaign_data = create_sample_campaign_data()
    
    print(f"\n📊 Campaign Data:")
    print(f"   Rows: {len(campaign_data)}")
    print(f"   Date Range: {campaign_data['Date'].min()} to {campaign_data['Date'].max()}")
    print(f"   Channels: {', '.join(campaign_data['Channel'].unique())}")
    
    # Initialize agent
    viz_agent = EnhancedVisualizationAgent()
    
    # Create dashboard
    dashboard = viz_agent.create_dashboard_visualizations(campaign_data)
    
    print(f"\n📊 Dashboard Created with {len(dashboard)} visualizations:")
    for key, viz_data in dashboard.items():
        print(f"\n   • {viz_data['title']}")
        print(f"     Category: {key}")
        print(f"     Chart Type: {viz_data['chart_type']}")
    
    print(f"\n✅ Complete dashboard ready")
    
    # Uncomment to display
    # for viz_data in dashboard.values():
    #     viz_data['chart'].show()
    
    return dashboard


def example_4_color_schemes():
    """Example 4: Using marketing color schemes"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Marketing Color Schemes")
    print("=" * 70)
    
    viz_agent = EnhancedVisualizationAgent()
    
    # Get channel colors
    print(f"\n🎨 Channel Colors:")
    channels = ['Google Ads', 'Meta', 'LinkedIn', 'TikTok', 'YouTube']
    for channel in channels:
        color = viz_agent.get_color_for_channel(channel)
        print(f"   {channel}: {color}")
    
    # Get performance colors
    print(f"\n🎨 Performance-Based Colors:")
    test_cases = [
        (120, 100, True, "Excellent (20% above)"),
        (105, 100, True, "Good (5% above)"),
        (95, 100, True, "Average (5% below)"),
        (75, 100, True, "Poor (25% below)")
    ]
    
    for value, benchmark, higher_is_better, description in test_cases:
        color = viz_agent.get_performance_color(value, benchmark, higher_is_better)
        print(f"   {description}: {color}")
    
    print(f"\n✅ Color schemes demonstrated")


def example_5_insight_categorization():
    """Example 5: Automatic insight categorization"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Automatic Insight Categorization")
    print("=" * 70)
    
    viz_agent = EnhancedVisualizationAgent()
    
    # Test insights with different descriptions
    test_insights = [
        {'title': 'Channel Performance Analysis', 'description': 'Comparing Google and Meta'},
        {'title': 'Daily Trends', 'description': 'Performance over time showing improvement'},
        {'title': 'Budget Analysis', 'description': 'Budget allocation across campaigns'},
        {'title': 'Creative Performance', 'description': 'Creative fatigue detected in ads'},
        {'title': 'Customer Journey', 'description': 'Attribution path analysis'},
        {'title': 'Conversion Analysis', 'description': 'Funnel showing drop-off rates'},
        {'title': 'Quality Metrics', 'description': 'Quality score components breakdown'},
        {'title': 'Time Analysis', 'description': 'Hourly performance patterns'},
        {'title': 'Device Report', 'description': 'Mobile vs desktop performance'},
        {'title': 'Keyword Analysis', 'description': 'Search term efficiency matrix'}
    ]
    
    print(f"\n🔍 Categorizing {len(test_insights)} insights:")
    
    for insight in test_insights:
        category = viz_agent._categorize_insight(insight)
        print(f"\n   • \"{insight['title']}\"")
        print(f"     → Category: {category}")
    
    print(f"\n✅ All insights categorized")


def example_6_complete_workflow():
    """Example 6: Complete end-to-end workflow"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Complete End-to-End Workflow")
    print("=" * 70)
    
    # Step 1: Create campaign data
    print(f"\n📊 Step 1: Creating campaign data...")
    campaign_data = create_sample_campaign_data()
    print(f"   ✅ Created {len(campaign_data)} rows of data")
    
    # Step 2: Generate insights (simulated)
    print(f"\n🤖 Step 2: Generating insights...")
    insights = [
        {
            'id': 'insight_1',
            'title': 'Channel Performance Comparison',
            'description': 'Multi-channel performance analysis',
            'category': 'channel_comparison',
            'data': {
                'Google Ads': {
                    'spend': campaign_data[campaign_data['Channel'] == 'Google Ads']['Spend'].sum(),
                    'conversions': campaign_data[campaign_data['Channel'] == 'Google Ads']['Conversions'].sum(),
                    'roas': campaign_data[campaign_data['Channel'] == 'Google Ads']['ROAS'].mean()
                },
                'Meta': {
                    'spend': campaign_data[campaign_data['Channel'] == 'Meta']['Spend'].sum(),
                    'conversions': campaign_data[campaign_data['Channel'] == 'Meta']['Conversions'].sum(),
                    'roas': campaign_data[campaign_data['Channel'] == 'Meta']['ROAS'].mean()
                },
                'LinkedIn': {
                    'spend': campaign_data[campaign_data['Channel'] == 'LinkedIn']['Spend'].sum(),
                    'conversions': campaign_data[campaign_data['Channel'] == 'LinkedIn']['Conversions'].sum(),
                    'roas': campaign_data[campaign_data['Channel'] == 'LinkedIn']['ROAS'].mean()
                }
            }
        }
    ]
    print(f"   ✅ Generated {len(insights)} insights")
    
    # Step 3: Create visualizations
    print(f"\n🎨 Step 3: Creating visualizations...")
    viz_agent = EnhancedVisualizationAgent()
    visualizations = viz_agent.create_visualizations_for_insights(insights, campaign_data)
    print(f"   ✅ Created {len(visualizations)} visualizations")
    
    # Step 4: Create dashboard
    print(f"\n📊 Step 4: Creating dashboard...")
    dashboard = viz_agent.create_dashboard_visualizations(campaign_data)
    print(f"   ✅ Created dashboard with {len(dashboard)} charts")
    
    # Summary
    print(f"\n✅ Complete workflow executed successfully!")
    print(f"\n📊 Summary:")
    print(f"   Data Rows: {len(campaign_data)}")
    print(f"   Insights: {len(insights)}")
    print(f"   Insight Visualizations: {len(visualizations)}")
    print(f"   Dashboard Charts: {len(dashboard)}")
    print(f"   Total Visualizations: {len(visualizations) + len(dashboard)}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Enhanced Visualization Agent - Integration Example")
    print("=" * 70)
    
    # Run all examples
    example_1_automatic_insight_visualization()
    example_2_category_specific_charts()
    example_3_dashboard_creation()
    example_4_color_schemes()
    example_5_insight_categorization()
    example_6_complete_workflow()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities Demonstrated:")
    print("   • Automatic visualization from insights")
    print("   • Category-specific chart creation")
    print("   • Complete dashboard generation")
    print("   • Marketing color schemes")
    print("   • Automatic insight categorization")
    print("   • End-to-end workflow")
    print("\n🎨 Integrated Layers:")
    print("   1. Smart Visualization Engine - Automatic selection")
    print("   2. Marketing Visualization Rules - Domain expertise")
    print("   3. Smart Chart Generators - Publication quality")
    print("   4. Enhanced Visualization Agent - Complete integration")
    print("=" * 70)
