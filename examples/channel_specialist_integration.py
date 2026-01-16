"""
Channel Specialist Integration Example
Demonstrates how to integrate channel-specific intelligence into your analysis pipeline
"""

import pandas as pd
from src.agents.channel_specialists import (
    ChannelRouter,
    SearchChannelAgent,
    SocialChannelAgent,
    ProgrammaticAgent
)


def example_single_channel_analysis():
    """Example: Analyze a single channel campaign"""
    
    # Load campaign data (example)
    campaign_data = pd.DataFrame({
        'Campaign_Name': ['Search Campaign 1', 'Search Campaign 2'],
        'Platform': ['Google Ads', 'Google Ads'],
        'Impressions': [100000, 150000],
        'Clicks': [3500, 4200],
        'Spend': [5000, 6500],
        'Conversions': [140, 168],
        'CTR': [0.035, 0.028],
        'CPC': [1.43, 1.55],
        'Quality_Score': [7.5, 6.8],
        'Impression_Share': [0.65, 0.58]
    })
    
    # Initialize router
    router = ChannelRouter()
    
    # Analyze (auto-detects channel type)
    results = router.route_and_analyze(campaign_data)
    
    print("=== Single Channel Analysis ===")
    print(f"Channel Type: {results['channel_type']}")
    print(f"Platform: {results['platform']}")
    print(f"Overall Health: {results['overall_health']}")
    print(f"\nKey Findings:")
    
    # Print findings from each analysis area
    for key, value in results.items():
        if isinstance(value, dict) and 'findings' in value:
            print(f"\n{value['metric']}:")
            for finding in value['findings']:
                print(f"  - {finding}")
    
    # Print recommendations
    if 'recommendations' in results:
        print(f"\n=== Recommendations ===")
        for rec in results['recommendations']:
            print(f"Priority: {rec.get('priority', 'N/A')}")
            print(f"Area: {rec.get('area', 'N/A')}")
            print(f"Recommendation: {rec.get('recommendation', 'N/A')}")
            print()


def example_multi_channel_analysis():
    """Example: Analyze multiple channels simultaneously"""
    
    # Search campaign data
    search_data = pd.DataFrame({
        'Campaign_Name': ['Brand Search', 'Generic Search'],
        'Platform': ['Google Ads', 'Google Ads'],
        'Impressions': [50000, 100000],
        'Clicks': [2500, 3000],
        'Spend': [3000, 5000],
        'Conversions': [125, 120],
        'Quality_Score': [8.5, 6.5],
        'Impression_Share': [0.85, 0.55]
    })
    
    # Social campaign data
    social_data = pd.DataFrame({
        'Campaign_Name': ['Meta Awareness', 'Meta Conversion'],
        'Platform': ['Meta', 'Meta'],
        'Impressions': [500000, 300000],
        'Clicks': [4500, 3600],
        'Spend': [4000, 3500],
        'Conversions': [90, 108],
        'Frequency': [2.8, 3.5],
        'Engagement_Rate': [0.06, 0.05]
    })
    
    # Programmatic campaign data
    programmatic_data = pd.DataFrame({
        'Campaign_Name': ['Display Awareness', 'Video Campaign'],
        'Platform': ['DV360', 'DV360'],
        'Impressions': [1000000, 500000],
        'Clicks': [4600, 8000],
        'Spend': [2800, 5000],
        'Conversions': [46, 80],
        'Viewability': [0.72, 0.68],
        'Brand_Safety_Score': [0.96, 0.94]
    })
    
    # Initialize router
    router = ChannelRouter()
    
    # Analyze all channels
    campaigns_by_channel = {
        'search': search_data,
        'social': social_data,
        'programmatic': programmatic_data
    }
    
    results = router.analyze_multi_channel(campaigns_by_channel)
    
    print("=== Multi-Channel Analysis ===")
    print(f"Channels Analyzed: {results['channels_analyzed']}")
    print(f"Overall Health: {results['cross_channel_insights']['overall_health']}")
    
    # Print each channel's health
    print(f"\n=== Channel Health Summary ===")
    for channel, analysis in results['channel_analyses'].items():
        if isinstance(analysis, dict) and 'overall_health' in analysis:
            print(f"{channel.capitalize()}: {analysis['overall_health']}")
    
    # Print channels needing attention
    needs_attention = results['cross_channel_insights']['needs_attention']
    if needs_attention:
        print(f"\n⚠️ Channels Needing Attention: {', '.join(needs_attention)}")
    
    # Print cross-channel recommendations
    print(f"\n=== Cross-Channel Recommendations ===")
    for rec in results['cross_channel_insights']['recommendations']:
        print(f"Priority: {rec['priority']}")
        print(f"Recommendation: {rec['recommendation']}")
        print()


def example_direct_specialist_usage():
    """Example: Use a specific specialist directly"""
    
    # Create search campaign data
    search_data = pd.DataFrame({
        'Campaign_Name': ['Brand', 'Generic', 'Competitor'],
        'Keyword': ['brand shoes', 'running shoes', 'nike shoes'],
        'Match_Type': ['Exact', 'Phrase', 'Phrase'],
        'Impressions': [10000, 50000, 30000],
        'Clicks': [800, 1500, 900],
        'Spend': [1200, 3000, 2400],
        'Conversions': [40, 60, 36],
        'Quality_Score': [9, 7, 5],
        'Impression_Share': [0.90, 0.60, 0.40],
        'CTR': [0.08, 0.03, 0.03]
    })
    
    # Initialize search specialist directly
    search_agent = SearchChannelAgent()
    
    # Perform analysis
    analysis = search_agent.analyze(search_data)
    
    print("=== Direct Search Specialist Analysis ===")
    print(f"Platform: {analysis['platform']}")
    print(f"Overall Health: {analysis['overall_health']}")
    
    # Print Quality Score analysis
    if 'quality_score_analysis' in analysis:
        qs_analysis = analysis['quality_score_analysis']
        print(f"\n=== Quality Score Analysis ===")
        print(f"Average QS: {qs_analysis.get('average_score', 'N/A')}")
        print(f"Status: {qs_analysis.get('status', 'N/A')}")
        for finding in qs_analysis.get('findings', []):
            print(f"  - {finding}")
    
    # Print Impression Share analysis
    if 'impression_share_gaps' in analysis:
        is_analysis = analysis['impression_share_gaps']
        print(f"\n=== Impression Share Analysis ===")
        print(f"Average IS: {is_analysis.get('average_impression_share', 'N/A')}%")
        print(f"Status: {is_analysis.get('status', 'N/A')}")
        for finding in is_analysis.get('findings', []):
            print(f"  - {finding}")


def example_with_rag_integration():
    """Example: Use specialists with RAG for enhanced insights"""
    
    # Note: This requires RAG system to be set up
    # For demonstration, we'll show the structure
    
    try:
        from src.mcp.rag_integration import RAGIntegration
        
        # Initialize RAG
        rag = RAGIntegration()
        
        # Initialize router with RAG
        router = ChannelRouter(rag_retriever=rag)
        
        # Campaign data
        campaign_data = pd.DataFrame({
            'Campaign_Name': ['Social Campaign 1'],
            'Platform': ['Meta'],
            'Impressions': [500000],
            'Frequency': [4.2],
            'CTR': [0.015],
            'Engagement_Rate': [0.04]
        })
        
        # Analyze with RAG-enhanced insights
        results = router.route_and_analyze(campaign_data)
        
        print("=== RAG-Enhanced Analysis ===")
        print(f"Channel: {results['channel_type']}")
        print(f"Health: {results['overall_health']}")
        
        # RAG retrieves relevant best practices automatically
        print("\nRecommendations include context from knowledge base:")
        for rec in results.get('recommendations', []):
            print(f"  - {rec.get('recommendation', 'N/A')}")
    
    except ImportError:
        print("RAG integration not available. Install required dependencies.")


def example_custom_benchmarks():
    """Example: Override default benchmarks with custom values"""
    
    from src.agents.channel_specialists.search_agent import SearchBenchmarks
    
    # Override benchmarks for your industry
    SearchBenchmarks.BENCHMARKS['ctr'] = 0.045  # 4.5% CTR target
    SearchBenchmarks.BENCHMARKS['quality_score'] = 8.0  # Higher QS target
    SearchBenchmarks.BENCHMARKS['roas'] = 5.0  # 5:1 ROAS target
    
    # Now use the specialist
    search_agent = SearchChannelAgent()
    
    # Campaign data
    campaign_data = pd.DataFrame({
        'Campaign_Name': ['Campaign 1'],
        'Impressions': [100000],
        'Clicks': [4200],
        'CTR': [0.042],
        'Quality_Score': [7.5],
        'Spend': [5000],
        'Revenue': [22000]
    })
    
    analysis = search_agent.analyze(campaign_data)
    
    print("=== Analysis with Custom Benchmarks ===")
    print(f"Custom CTR Benchmark: {SearchBenchmarks.BENCHMARKS['ctr']}")
    print(f"Actual CTR: {campaign_data['CTR'][0]}")
    print(f"Performance: {analysis.get('overall_health', 'N/A')}")


if __name__ == "__main__":
    print("=" * 60)
    print("Channel Specialist Integration Examples")
    print("=" * 60)
    
    print("\n\n1. Single Channel Analysis")
    print("-" * 60)
    example_single_channel_analysis()
    
    print("\n\n2. Multi-Channel Analysis")
    print("-" * 60)
    example_multi_channel_analysis()
    
    print("\n\n3. Direct Specialist Usage")
    print("-" * 60)
    example_direct_specialist_usage()
    
    print("\n\n4. Custom Benchmarks")
    print("-" * 60)
    example_custom_benchmarks()
    
    print("\n\n5. RAG Integration")
    print("-" * 60)
    example_with_rag_integration()
    
    print("\n\n" + "=" * 60)
    print("Examples Complete!")
    print("=" * 60)
