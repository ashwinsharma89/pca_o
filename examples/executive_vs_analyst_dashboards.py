"""
Executive vs Analyst Dashboards - Complete Example
Demonstrates audience-specific visualization sets
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.enhanced_visualization_agent import EnhancedVisualizationAgent


def create_comprehensive_campaign_data():
    """Create comprehensive campaign data with all fields"""
    
    np.random.seed(42)
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    data = []
    campaigns = ['Brand Awareness', 'Lead Gen', 'Conversion', 'Retargeting']
    
    for date in dates:
        for channel in ['Google Ads', 'Meta', 'LinkedIn']:
            for campaign in campaigns[:2]:  # 2 campaigns per channel
                data.append({
                    'Date': date,
                    'Channel': channel,
                    'Platform': channel,
                    'Campaign': campaign,
                    'Spend': np.random.uniform(500, 2000),
                    'Impressions': np.random.randint(5000, 30000),
                    'Clicks': np.random.randint(200, 1000),
                    'Conversions': np.random.randint(20, 100),
                    'CTR': np.random.uniform(0.02, 0.06),
                    'CPC': np.random.uniform(2, 8),
                    'ROAS': np.random.uniform(1.5, 4.5),
                    'Device': np.random.choice(['Desktop', 'Mobile', 'Tablet'], p=[0.35, 0.55, 0.1]),
                    'Frequency': np.random.uniform(2, 10)
                })
    
    return pd.DataFrame(data)


def create_sample_insights():
    """Create sample insights from reasoning agent"""
    
    return [
        {
            'id': 'insight_1',
            'title': 'LinkedIn Outperforming Other Channels',
            'description': 'LinkedIn showing 45% higher ROAS than average',
            'priority': 10,
            'category': 'channel_comparison',
            'data': {
                'Google Ads': {'spend': 45000, 'conversions': 850, 'roas': 3.2},
                'Meta': {'spend': 32000, 'conversions': 620, 'roas': 2.8},
                'LinkedIn': {'spend': 28000, 'conversions': 410, 'roas': 4.6}
            }
        },
        {
            'id': 'insight_2',
            'title': 'Performance Improving Over Time',
            'description': 'ROAS trending upward by 15% over last 30 days',
            'priority': 8,
            'category': 'performance_trend',
            'data': {
                'dates': [(datetime.now() - timedelta(days=x)).strftime('%Y-%m-%d') 
                         for x in range(30, 0, -1)],
                'metrics': {
                    'roas': (2.5 + np.random.normal(0, 0.2, 30) + np.linspace(0, 0.5, 30)).tolist()
                }
            }
        },
        {
            'id': 'insight_3',
            'title': 'Mobile Driving Majority of Conversions',
            'description': 'Mobile accounts for 55% of all conversions',
            'priority': 6,
            'category': 'device_breakdown',
            'data': {
                'devices': ['Desktop', 'Mobile', 'Tablet'],
                'values': [3500, 5500, 1000]
            }
        },
        {
            'id': 'insight_4',
            'title': 'Budget Allocation Opportunity',
            'description': 'Shift budget from underperforming channels',
            'priority': 7,
            'category': 'budget_distribution',
            'data': {}
        }
    ]


def example_1_executive_dashboard():
    """Example 1: Executive Dashboard (High-Level)"""
    
    print("=" * 70)
    print("Example 1: Executive Dashboard")
    print("=" * 70)
    
    # Create data
    campaign_data = create_comprehensive_campaign_data()
    insights = create_sample_insights()
    
    print(f"\n📊 Campaign Data:")
    print(f"   Rows: {len(campaign_data)}")
    print(f"   Channels: {', '.join(campaign_data['Channel'].unique())}")
    print(f"   Date Range: {campaign_data['Date'].min().date()} to {campaign_data['Date'].max().date()}")
    
    # Initialize agent
    viz_agent = EnhancedVisualizationAgent()
    
    # Create executive dashboard
    context = {'target_roas': 3.0}
    executive_viz = viz_agent.create_executive_dashboard(
        insights=insights,
        campaign_data=campaign_data,
        context=context
    )
    
    print(f"\n🎯 Executive Dashboard Created:")
    print(f"   Total Charts: {len(executive_viz)} (optimized for executives)")
    print(f"\n   Charts Included:")
    for idx, viz in enumerate(executive_viz, 1):
        print(f"   {idx}. {viz['title']}")
        print(f"      Type: {viz['chart_type']}")
        print(f"      Description: {viz['description']}")
    
    print(f"\n✨ Executive Dashboard Features:")
    print(f"   • High-level overview (5-7 charts max)")
    print(f"   • Visual impact (gauges, donuts, simple bars)")
    print(f"   • Top insights only")
    print(f"   • Action-oriented")
    print(f"   • Big numbers, clear trends")
    
    print(f"\n✅ Executive dashboard ready for presentation")
    
    # Uncomment to display
    # for viz in executive_viz:
    #     viz['chart'].show()
    
    return executive_viz


def example_2_analyst_dashboard():
    """Example 2: Analyst Dashboard (Detailed)"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Analyst Dashboard")
    print("=" * 70)
    
    # Create data
    campaign_data = create_comprehensive_campaign_data()
    insights = create_sample_insights()
    
    print(f"\n📊 Campaign Data:")
    print(f"   Rows: {len(campaign_data)}")
    print(f"   Channels: {', '.join(campaign_data['Channel'].unique())}")
    print(f"   Campaigns: {', '.join(campaign_data['Campaign'].unique())}")
    
    # Initialize agent
    viz_agent = EnhancedVisualizationAgent()
    
    # Create analyst dashboard
    analyst_viz = viz_agent.create_analyst_dashboard(
        insights=insights,
        campaign_data=campaign_data
    )
    
    print(f"\n🔬 Analyst Dashboard Created:")
    print(f"   Total Charts: {len(analyst_viz)} (comprehensive analysis)")
    
    # Group by section
    sections = {}
    for viz in analyst_viz:
        section = viz.get('section', 'other')
        if section not in sections:
            sections[section] = []
        sections[section].append(viz)
    
    print(f"\n   Charts by Section:")
    for section, charts in sections.items():
        print(f"\n   📊 {section.replace('_', ' ').title()} ({len(charts)} charts):")
        for chart in charts:
            print(f"      • {chart['title']} ({chart['chart_type']})")
    
    print(f"\n✨ Analyst Dashboard Features:")
    print(f"   • Comprehensive coverage (15-20 charts)")
    print(f"   • Detailed breakdowns")
    print(f"   • All insights visualized")
    print(f"   • Granular data")
    print(f"   • Exploratory analysis")
    print(f"   • Anomaly detection")
    
    print(f"\n✅ Analyst dashboard ready for deep-dive analysis")
    
    # Uncomment to display
    # for viz in analyst_viz:
    #     viz['chart'].show()
    
    return analyst_viz


def example_3_side_by_side_comparison():
    """Example 3: Side-by-Side Comparison"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Executive vs Analyst - Side-by-Side Comparison")
    print("=" * 70)
    
    # Create data
    campaign_data = create_comprehensive_campaign_data()
    insights = create_sample_insights()
    
    # Initialize agent
    viz_agent = EnhancedVisualizationAgent()
    
    # Create both dashboards
    context = {'target_roas': 3.0}
    executive_viz = viz_agent.create_executive_dashboard(insights, campaign_data, context)
    analyst_viz = viz_agent.create_analyst_dashboard(insights, campaign_data)
    
    print(f"\n📊 Comparison:")
    print(f"\n   {'Aspect':<30} {'Executive':<20} {'Analyst':<20}")
    print(f"   {'-'*70}")
    print(f"   {'Number of Charts':<30} {len(executive_viz):<20} {len(analyst_viz):<20}")
    print(f"   {'Chart Complexity':<30} {'Simple':<20} {'Detailed':<20}")
    print(f"   {'Anomaly Detection':<30} {'No':<20} {'Yes':<20}")
    print(f"   {'All Insights Shown':<30} {'Top Only':<20} {'All':<20}")
    print(f"   {'Target Audience':<30} {'C-Suite':<20} {'Data Team':<20}")
    
    print(f"\n   Executive Dashboard Charts:")
    for idx, viz in enumerate(executive_viz, 1):
        print(f"   {idx}. {viz['title']} ({viz['chart_type']})")
    
    print(f"\n   Analyst Dashboard Sections:")
    sections = set(viz.get('section', 'other') for viz in analyst_viz)
    for section in sections:
        count = len([v for v in analyst_viz if v.get('section') == section])
        print(f"   • {section.replace('_', ' ').title()}: {count} charts")
    
    print(f"\n✅ Both dashboards created successfully")


def example_4_audience_specific_features():
    """Example 4: Audience-Specific Features"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Audience-Specific Features")
    print("=" * 70)
    
    print(f"\n🎯 Executive Dashboard Features:")
    print(f"\n   Chart Selection:")
    print(f"   • Performance Gauge - Overall health at a glance")
    print(f"   • Top 5 Channels - Focus on what matters")
    print(f"   • Budget Treemap - Visual budget allocation")
    print(f"   • ROAS Trend - Simple 30-day trend")
    print(f"   • Device Donut - Clear distribution")
    print(f"   • Top Insight - Key recommendation")
    
    print(f"\n   Optimization:")
    print(f"   • Limited to 5-7 charts maximum")
    print(f"   • No anomaly detection (too technical)")
    print(f"   • Simplified metrics (ROAS focus)")
    print(f"   • Action-oriented titles")
    print(f"   • Big numbers, clear visuals")
    
    print(f"\n🔬 Analyst Dashboard Features:")
    print(f"\n   Chart Selection:")
    print(f"   • All Insights - Complete coverage")
    print(f"   • Comprehensive Channel Analysis - All channels, all metrics")
    print(f"   • Detailed Trends - Multiple metrics with anomalies")
    print(f"   • Hourly Heatmap - Day parting analysis")
    print(f"   • Frequency Distribution - Statistical analysis")
    print(f"   • Hierarchical Treemap - Channel > Campaign drill-down")
    print(f"   • Conversion Funnel - Stage-by-stage analysis")
    
    print(f"\n   Optimization:")
    print(f"   • 15-20 charts for comprehensive view")
    print(f"   • Anomaly detection enabled")
    print(f"   • All available metrics shown")
    print(f"   • Technical depth")
    print(f"   • Exploratory analysis")


def example_5_use_case_scenarios():
    """Example 5: Use Case Scenarios"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Use Case Scenarios")
    print("=" * 70)
    
    print(f"\n📊 Scenario 1: Executive Board Meeting")
    print(f"   Audience: C-Suite, Board Members")
    print(f"   Dashboard: Executive")
    print(f"   Focus: High-level performance, ROI, key decisions")
    print(f"   Charts: 5-7 visual, impactful charts")
    print(f"   Time: 15-minute presentation")
    
    print(f"\n📊 Scenario 2: Weekly Performance Review")
    print(f"   Audience: Marketing Team, Analysts")
    print(f"   Dashboard: Analyst")
    print(f"   Focus: Detailed performance, optimization opportunities")
    print(f"   Charts: 15-20 comprehensive charts")
    print(f"   Time: 1-hour deep-dive")
    
    print(f"\n📊 Scenario 3: Client Presentation")
    print(f"   Audience: Client Stakeholders")
    print(f"   Dashboard: Executive")
    print(f"   Focus: Results, recommendations, next steps")
    print(f"   Charts: 5-7 clear, actionable charts")
    print(f"   Time: 30-minute presentation")
    
    print(f"\n📊 Scenario 4: Campaign Optimization Workshop")
    print(f"   Audience: Media Buyers, Analysts")
    print(f"   Dashboard: Analyst")
    print(f"   Focus: Granular insights, testing opportunities")
    print(f"   Charts: 15-20 detailed charts")
    print(f"   Time: 2-hour workshop")


def example_6_complete_workflow():
    """Example 6: Complete Workflow with Both Dashboards"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Complete Workflow - Both Dashboards")
    print("=" * 70)
    
    # Step 1: Data preparation
    print(f"\n📊 Step 1: Data Preparation")
    campaign_data = create_comprehensive_campaign_data()
    insights = create_sample_insights()
    print(f"   ✅ Campaign data loaded: {len(campaign_data)} rows")
    print(f"   ✅ Insights generated: {len(insights)} insights")
    
    # Step 2: Initialize agent
    print(f"\n🤖 Step 2: Initialize Enhanced Visualization Agent")
    viz_agent = EnhancedVisualizationAgent()
    print(f"   ✅ Agent initialized with all 4 layers")
    
    # Step 3: Create executive dashboard
    print(f"\n🎯 Step 3: Create Executive Dashboard")
    context = {'target_roas': 3.0}
    executive_viz = viz_agent.create_executive_dashboard(insights, campaign_data, context)
    print(f"   ✅ Executive dashboard created: {len(executive_viz)} charts")
    
    # Step 4: Create analyst dashboard
    print(f"\n🔬 Step 4: Create Analyst Dashboard")
    analyst_viz = viz_agent.create_analyst_dashboard(insights, campaign_data)
    print(f"   ✅ Analyst dashboard created: {len(analyst_viz)} charts")
    
    # Step 5: Summary
    print(f"\n📊 Step 5: Summary")
    print(f"   Executive Dashboard:")
    print(f"   • Charts: {len(executive_viz)}")
    print(f"   • Focus: High-level, actionable")
    print(f"   • Audience: C-Suite, Stakeholders")
    
    print(f"\n   Analyst Dashboard:")
    print(f"   • Charts: {len(analyst_viz)}")
    print(f"   • Focus: Detailed, comprehensive")
    print(f"   • Audience: Analysts, Media Buyers")
    
    print(f"\n✅ Complete workflow executed successfully!")
    print(f"   Both dashboards ready for their respective audiences")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Executive vs Analyst Dashboards - Complete Example")
    print("=" * 70)
    
    # Run all examples
    example_1_executive_dashboard()
    example_2_analyst_dashboard()
    example_3_side_by_side_comparison()
    example_4_audience_specific_features()
    example_5_use_case_scenarios()
    example_6_complete_workflow()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Takeaways:")
    print("\n   Executive Dashboard:")
    print("   • 5-7 charts maximum")
    print("   • Simple, visual charts (gauges, donuts, bars)")
    print("   • Top insights only")
    print("   • Action-oriented")
    print("   • No anomaly detection")
    print("   • Focus on ROAS and key metrics")
    
    print("\n   Analyst Dashboard:")
    print("   • 15-20 comprehensive charts")
    print("   • Detailed charts (scatter, heatmaps, trends)")
    print("   • All insights visualized")
    print("   • Exploratory analysis")
    print("   • Anomaly detection enabled")
    print("   • All available metrics")
    
    print("\n🎯 Use Cases:")
    print("   • Executive: Board meetings, client presentations")
    print("   • Analyst: Performance reviews, optimization workshops")
    
    print("=" * 70)
