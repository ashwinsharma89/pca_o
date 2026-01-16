"""
Enhanced Reasoning Agent Integration Example
Demonstrates pattern recognition and contextual analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.agents.enhanced_reasoning_agent import EnhancedReasoningAgent, PatternDetector
from src.knowledge.benchmark_engine import DynamicBenchmarkEngine
from src.models.campaign import CampaignContext, BusinessModel


def create_sample_data_with_fatigue():
    """Create sample data showing creative fatigue"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Simulate declining CTR over time (creative fatigue)
    base_ctr = 0.045
    ctr_decline = np.linspace(0, -0.20, 14)  # 20% decline over 2 weeks
    ctrs = base_ctr * (1 + ctr_decline) + np.random.normal(0, 0.002, 14)
    
    data = pd.DataFrame({
        'Date': dates,
        'Campaign_Name': ['Creative Fatigue Test'] * 14,
        'Platform': ['Meta'] * 14,
        'Impressions': np.random.randint(80000, 120000, 14),
        'Clicks': [int(imp * ctr) for imp, ctr in zip(np.random.randint(80000, 120000, 14), ctrs)],
        'Conversions': np.random.randint(150, 250, 14),
        'Spend': np.random.uniform(1800, 2200, 14),
        'CTR': ctrs,
        'CPC': np.random.uniform(1.8, 2.5, 14),
        'Frequency': np.linspace(3, 8.5, 14)  # Increasing frequency
    })
    
    return data


def create_sample_data_with_anomalies():
    """Create sample data with anomalies"""
    dates = pd.date_range(start='2024-01-01', periods=20, freq='D')
    
    # Normal data with some anomalies
    normal_cpc = np.random.normal(5.5, 0.5, 20)
    normal_cpc[7] = 15.0  # Anomaly
    normal_cpc[14] = 18.0  # Anomaly
    
    data = pd.DataFrame({
        'Date': dates,
        'Campaign_Name': ['Anomaly Test'] * 20,
        'Platform': ['Google Ads'] * 20,
        'Impressions': np.random.randint(50000, 70000, 20),
        'Clicks': np.random.randint(1500, 2500, 20),
        'Conversions': np.random.randint(100, 200, 20),
        'Spend': np.random.uniform(8000, 12000, 20),
        'CTR': np.random.uniform(0.03, 0.05, 20),
        'CPC': normal_cpc
    })
    
    return data


def create_sample_data_with_day_parting():
    """Create sample data with day parting opportunities"""
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    # Simulate day-of-week patterns
    data_list = []
    for date in dates:
        day_of_week = date.dayofweek
        
        # Better performance on weekdays
        if day_of_week < 5:  # Monday-Friday
            base_conv_rate = 0.05
        else:  # Weekend
            base_conv_rate = 0.03
        
        impressions = np.random.randint(40000, 60000)
        clicks = int(impressions * np.random.uniform(0.03, 0.05))
        conversions = int(clicks * base_conv_rate)
        
        data_list.append({
            'Date': date,
            'Campaign_Name': 'Day Parting Test',
            'Platform': 'LinkedIn',
            'Impressions': impressions,
            'Clicks': clicks,
            'Conversions': conversions,
            'Spend': np.random.uniform(3000, 4000),
            'CTR': clicks / impressions,
            'CPC': np.random.uniform(6, 10)
        })
    
    return pd.DataFrame(data_list)


def example_creative_fatigue_detection():
    """Example: Detect creative fatigue"""
    
    print("=" * 70)
    print("Example 1: Creative Fatigue Detection")
    print("=" * 70)
    
    # Create data with fatigue
    data = create_sample_data_with_fatigue()
    
    print(f"\n📊 Campaign Data: {len(data)} days")
    print(f"   Platform: {data['Platform'].iloc[0]}")
    print(f"   Initial CTR: {data['CTR'].iloc[0]:.2%}")
    print(f"   Final CTR: {data['CTR'].iloc[-1]:.2%}")
    print(f"   Average Frequency: {data['Frequency'].mean():.1f}")
    
    # Initialize pattern detector
    detector = PatternDetector()
    
    # Detect patterns
    patterns = detector.detect_all(data)
    
    # Check creative fatigue
    fatigue = patterns.get('creative_fatigue', {})
    
    print(f"\n🎨 Creative Fatigue Analysis:")
    if fatigue.get('detected'):
        print(f"   Status: ⚠️ DETECTED")
        print(f"   Severity: {fatigue.get('severity', 'unknown').upper()}")
        evidence = fatigue.get('evidence', {})
        print(f"   Frequency: {evidence.get('frequency', 0):.1f}")
        print(f"   CTR Decline: {evidence.get('ctr_decline', 0):.1%}")
        print(f"   💡 Recommendation: {evidence.get('recommendation', 'N/A')}")
    else:
        print(f"   Status: ✅ No fatigue detected")


def example_anomaly_detection():
    """Example: Detect anomalies in performance"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: Anomaly Detection")
    print("=" * 70)
    
    # Create data with anomalies
    data = create_sample_data_with_anomalies()
    
    print(f"\n📊 Campaign Data: {len(data)} days")
    print(f"   Platform: {data['Platform'].iloc[0]}")
    print(f"   Average CPC: ${data['CPC'].mean():.2f}")
    print(f"   CPC Range: ${data['CPC'].min():.2f} - ${data['CPC'].max():.2f}")
    
    # Initialize pattern detector
    detector = PatternDetector()
    
    # Detect patterns
    patterns = detector.detect_all(data)
    
    # Check anomalies
    anomalies = patterns.get('anomalies', {})
    
    print(f"\n⚠️ Anomaly Analysis:")
    if anomalies.get('detected'):
        print(f"   Status: DETECTED")
        print(f"   Description: {anomalies.get('description', 'N/A')}")
        for anomaly in anomalies.get('anomalies', []):
            print(f"   • {anomaly['metric']}: {anomaly['count']} outliers ({anomaly['severity']} severity)")
    else:
        print(f"   Status: ✅ No anomalies detected")


def example_day_parting_opportunities():
    """Example: Find day parting opportunities"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: Day Parting Opportunities")
    print("=" * 70)
    
    # Create data with day patterns
    data = create_sample_data_with_day_parting()
    
    print(f"\n📊 Campaign Data: {len(data)} days")
    print(f"   Platform: {data['Platform'].iloc[0]}")
    
    # Initialize pattern detector
    detector = PatternDetector()
    
    # Detect patterns
    patterns = detector.detect_all(data)
    
    # Check day parting
    daypart = patterns.get('day_parting_opportunities', {})
    
    print(f"\n⏰ Day Parting Analysis:")
    if daypart.get('detected'):
        print(f"   Status: OPPORTUNITY FOUND")
        if daypart.get('type') == 'day_of_week':
            print(f"   Type: Day of Week Pattern")
            print(f"   Best Days: {', '.join(daypart.get('best_days', []))}")
            print(f"   Worst Days: {', '.join(daypart.get('worst_days', []))}")
        print(f"   💡 Recommendation: {daypart.get('recommendation', 'N/A')}")
    else:
        print(f"   Status: No significant patterns found")


def example_full_enhanced_reasoning():
    """Example: Full enhanced reasoning with all components"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Full Enhanced Reasoning Analysis")
    print("=" * 70)
    
    # Create data with multiple patterns
    data = create_sample_data_with_fatigue()
    
    # Create campaign context
    context = CampaignContext(
        business_model=BusinessModel.B2C,
        industry_vertical="E-commerce",
        average_order_value=85.0,
        purchase_frequency="monthly",
        customer_lifetime_value=425.0,
        target_cac=35.0
    )
    
    print(f"\n📊 Campaign Context: {context.get_context_summary()}")
    
    # Initialize components
    benchmark_engine = DynamicBenchmarkEngine()
    reasoning_agent = EnhancedReasoningAgent(
        rag_retriever=None,  # No RAG for this example
        benchmark_engine=benchmark_engine
    )
    
    # Run full analysis
    print(f"\n🤖 Running Enhanced Reasoning Analysis...")
    analysis = reasoning_agent.analyze(
        campaign_data=data,
        channel_insights=None,
        campaign_context=context
    )
    
    # Display results
    print(f"\n📈 Performance Summary:")
    perf_summary = analysis['insights']['performance_summary']
    print(f"   Total Spend: ${perf_summary.get('total_spend', 0):,.2f}")
    print(f"   Total Impressions: {perf_summary.get('total_impressions', 0):,}")
    print(f"   Total Clicks: {perf_summary.get('total_clicks', 0):,}")
    print(f"   Overall CTR: {perf_summary.get('overall_ctr', 0):.2%}")
    
    print(f"\n🔍 Pattern Insights:")
    for insight in analysis['insights']['pattern_insights']:
        print(f"   {insight}")
    
    print(f"\n💡 Recommendations ({len(analysis['recommendations'])} total):")
    for rec in analysis['recommendations'][:5]:
        priority_emoji = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(rec['priority'], '⚪')
        print(f"   {priority_emoji} [{rec['priority'].upper()}] {rec['category']}")
        print(f"      Issue: {rec['issue']}")
        print(f"      Action: {rec['recommendation']}")
        print()


def example_trend_detection():
    """Example: Detect performance trends"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Trend Detection")
    print("=" * 70)
    
    # Create data with improving trend
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Simulate improving CTR
    base_ctr = 0.03
    ctr_improvement = np.linspace(0, 0.30, 14)  # 30% improvement
    ctrs = base_ctr * (1 + ctr_improvement) + np.random.normal(0, 0.001, 14)
    
    data = pd.DataFrame({
        'Date': dates,
        'Campaign_Name': ['Trend Test'] * 14,
        'Platform': ['Google Ads'] * 14,
        'Impressions': np.random.randint(50000, 70000, 14),
        'Clicks': [int(imp * ctr) for imp, ctr in zip(np.random.randint(50000, 70000, 14), ctrs)],
        'Conversions': np.random.randint(150, 250, 14),
        'Spend': np.random.uniform(5000, 7000, 14),
        'CTR': ctrs,
        'CPC': np.random.uniform(4, 6, 14)
    })
    
    print(f"\n📊 Campaign Data: {len(data)} days")
    print(f"   Initial CTR: {data['CTR'].iloc[0]:.2%}")
    print(f"   Final CTR: {data['CTR'].iloc[-1]:.2%}")
    print(f"   Change: {((data['CTR'].iloc[-1] / data['CTR'].iloc[0]) - 1):.1%}")
    
    # Detect trends
    detector = PatternDetector()
    patterns = detector.detect_all(data)
    
    trends = patterns.get('trends', {})
    
    print(f"\n📈 Trend Analysis:")
    if trends.get('detected'):
        print(f"   Status: TREND DETECTED")
        print(f"   Direction: {trends.get('direction', 'unknown').upper()}")
        print(f"   Description: {trends.get('description', 'N/A')}")
        
        if 'metrics' in trends:
            print(f"\n   Metric Details:")
            for metric, details in trends['metrics'].items():
                print(f"      {metric.upper()}:")
                print(f"         Direction: {details['direction']}")
                print(f"         R²: {details['r_squared']:.3f}")
    else:
        print(f"   Status: No significant trends detected")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Enhanced Reasoning Agent - Integration Examples")
    print("=" * 70)
    
    # Run all examples
    example_creative_fatigue_detection()
    example_anomaly_detection()
    example_day_parting_opportunities()
    example_trend_detection()
    example_full_enhanced_reasoning()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Capabilities:")
    print("   • Creative fatigue detection with severity levels")
    print("   • Anomaly detection using statistical methods")
    print("   • Day parting opportunity identification")
    print("   • Performance trend analysis")
    print("   • Contextual recommendations with benchmarks")
    print("   • Integration with B2B/B2C context and dynamic benchmarks")
    print("=" * 70)
