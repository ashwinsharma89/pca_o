"""
B2B Specialist Agent Integration Example
Demonstrates how to use B2B/B2C context-aware analysis
"""

import pandas as pd
from src.agents.b2b_specialist_agent import B2BSpecialistAgent
from src.models.campaign import CampaignContext, BusinessModel, TargetAudienceLevel
from src.analytics import MediaAnalyticsExpert


def example_b2b_saas_campaign():
    """Example: B2B SaaS campaign analysis"""
    
    print("=" * 60)
    print("Example 1: B2B SaaS Campaign")
    print("=" * 60)
    
    # Create campaign data
    campaign_data = pd.DataFrame({
        'Campaign_Name': ['LinkedIn Lead Gen', 'Google Search - Demo', 'Content Syndication'],
        'Platform': ['LinkedIn', 'Google Ads', 'Third-Party'],
        'Impressions': [500000, 250000, 300000],
        'Clicks': [4000, 7500, 3000],
        'Conversions': [120, 225, 90],  # Leads
        'Spend': [15000, 18000, 12000],
        'CTR': [0.008, 0.03, 0.01],
        'CPC': [3.75, 2.40, 4.00]
    })
    
    # Define B2B context
    b2b_context = CampaignContext(
        business_model=BusinessModel.B2B,
        industry_vertical="SaaS",
        sales_cycle_length=60,  # 60-day sales cycle
        average_deal_size=25000,  # $25K ACV
        target_audience_level=TargetAudienceLevel.VP_DIRECTOR,
        customer_lifetime_value=75000,  # $75K LTV
        target_cac=5000,  # $5K target CAC
        geographic_focus=["North America", "Europe"],
        competitive_intensity="high",
        brand_maturity="growth"
    )
    
    print(f"\n📊 Campaign Context: {b2b_context.get_context_summary()}\n")
    
    # Run base analysis
    expert = MediaAnalyticsExpert()
    base_analysis = expert.analyze_all(campaign_data)
    
    # Enhance with B2B specialist
    b2b_specialist = B2BSpecialistAgent()
    enhanced_analysis = b2b_specialist.enhance_analysis(
        base_insights=base_analysis,
        campaign_context=b2b_context,
        campaign_data=campaign_data
    )
    
    # Display B2B-specific insights
    b2b_insights = enhanced_analysis.get('business_model_analysis', {})
    
    print("🎯 Lead Quality Analysis:")
    lead_analysis = b2b_insights.get('lead_quality_analysis', {})
    for finding in lead_analysis.get('findings', []):
        print(f"  {finding}")
    print(f"  Total Leads: {lead_analysis.get('total_leads', 0)}")
    print(f"  Est. MQLs: {lead_analysis.get('estimated_mqls', 0)}")
    print(f"  Est. SQLs: {lead_analysis.get('estimated_sqls', 0)}")
    print(f"  Cost per SQL: {lead_analysis.get('cost_per_sql', 'N/A')}")
    
    print("\n💰 Pipeline Impact:")
    pipeline_analysis = b2b_insights.get('pipeline_contribution', {})
    for finding in pipeline_analysis.get('findings', []):
        print(f"  {finding}")
    
    print("\n📅 Sales Cycle Alignment:")
    cycle_analysis = b2b_insights.get('sales_cycle_alignment', {})
    for finding in cycle_analysis.get('findings', []):
        print(f"  {finding}")
    if cycle_analysis.get('recommendation'):
        print(f"  💡 {cycle_analysis['recommendation']}")
    
    print("\n👔 Audience Seniority Analysis:")
    seniority_analysis = b2b_insights.get('audience_seniority_analysis', {})
    for finding in seniority_analysis.get('findings', []):
        print(f"  {finding}")
    if seniority_analysis.get('recommendation'):
        print(f"  💡 {seniority_analysis['recommendation']}")
    
    print("\n📋 B2B Recommendations:")
    for rec in enhanced_analysis.get('recommendations', [])[:5]:
        if isinstance(rec, dict):
            print(f"  [{rec.get('priority', 'medium').upper()}] {rec.get('recommendation', rec)}")
        else:
            print(f"  {rec}")


def example_b2c_ecommerce_campaign():
    """Example: B2C E-commerce campaign analysis"""
    
    print("\n\n" + "=" * 60)
    print("Example 2: B2C E-commerce Campaign")
    print("=" * 60)
    
    # Create campaign data
    campaign_data = pd.DataFrame({
        'Campaign_Name': ['Meta Prospecting', 'Meta Retargeting', 'Google Shopping'],
        'Platform': ['Meta', 'Meta', 'Google Ads'],
        'Impressions': [2000000, 500000, 800000],
        'Clicks': [30000, 15000, 24000],
        'Conversions': [900, 750, 960],  # Purchases
        'Spend': [15000, 7500, 12000],
        'CTR': [0.015, 0.03, 0.03],
        'CPC': [0.50, 0.50, 0.50]
    })
    
    # Define B2C context
    b2c_context = CampaignContext(
        business_model=BusinessModel.B2C,
        industry_vertical="E-commerce - Fashion",
        average_order_value=85,  # $85 AOV
        purchase_frequency="monthly",
        customer_lifetime_value=425,  # $425 LTV (5 purchases)
        target_cac=35,  # $35 target CAC
        geographic_focus=["United States"],
        competitive_intensity="high",
        seasonality_factor="high",
        brand_maturity="mature"
    )
    
    print(f"\n📊 Campaign Context: {b2c_context.get_context_summary()}\n")
    
    # Run base analysis
    expert = MediaAnalyticsExpert()
    base_analysis = expert.analyze_all(campaign_data)
    
    # Enhance with B2B specialist (handles B2C too)
    b2b_specialist = B2BSpecialistAgent()
    enhanced_analysis = b2b_specialist.enhance_analysis(
        base_insights=base_analysis,
        campaign_context=b2c_context,
        campaign_data=campaign_data
    )
    
    # Display B2C-specific insights
    b2c_insights = enhanced_analysis.get('business_model_analysis', {})
    
    print("🛍️ Purchase Behavior Analysis:")
    purchase_analysis = b2c_insights.get('purchase_behavior_analysis', {})
    for finding in purchase_analysis.get('findings', []):
        print(f"  {finding}")
    
    print("\n💵 CAC Efficiency:")
    cac_analysis = b2c_insights.get('customer_acquisition_efficiency', {})
    for finding in cac_analysis.get('findings', []):
        print(f"  {finding}")
    print(f"  Actual CAC: {cac_analysis.get('actual_cac', 'N/A')}")
    print(f"  Target CAC: ${b2c_context.target_cac:.2f}")
    
    print("\n📊 Lifetime Value Analysis:")
    ltv_analysis = b2c_insights.get('lifetime_value_analysis', {})
    for finding in ltv_analysis.get('findings', []):
        print(f"  {finding}")
    print(f"  LTV:CAC Ratio: {ltv_analysis.get('ltv_cac_ratio', 'N/A')}")
    
    print("\n🔄 Conversion Funnel:")
    funnel_analysis = b2c_insights.get('conversion_funnel_analysis', {})
    for finding in funnel_analysis.get('findings', []):
        print(f"  {finding}")
    if funnel_analysis.get('bottleneck'):
        print(f"  ⚠️ Bottleneck: {funnel_analysis['bottleneck']}")
    
    print("\n📋 B2C Recommendations:")
    for rec in enhanced_analysis.get('recommendations', [])[:5]:
        if isinstance(rec, dict):
            print(f"  [{rec.get('priority', 'medium').upper()}] {rec.get('recommendation', rec)}")
        else:
            print(f"  {rec}")


def example_b2b2c_marketplace():
    """Example: B2B2C Marketplace (hybrid model)"""
    
    print("\n\n" + "=" * 60)
    print("Example 3: B2B2C Marketplace (Hybrid)")
    print("=" * 60)
    
    # Create campaign data
    campaign_data = pd.DataFrame({
        'Campaign_Name': ['B2B LinkedIn', 'B2C Meta', 'B2B Google Search'],
        'Platform': ['LinkedIn', 'Meta', 'Google Ads'],
        'Impressions': [300000, 1500000, 400000],
        'Clicks': [2400, 22500, 12000],
        'Conversions': [72, 675, 360],
        'Spend': [9600, 11250, 18000],
        'CTR': [0.008, 0.015, 0.03],
        'CPC': [4.00, 0.50, 1.50]
    })
    
    # Define hybrid context
    hybrid_context = CampaignContext(
        business_model=BusinessModel.B2B2C,
        industry_vertical="Marketplace Platform",
        sales_cycle_length=30,  # B2B side
        average_deal_size=5000,  # B2B side
        average_order_value=150,  # B2C side
        purchase_frequency="quarterly",  # B2C side
        customer_lifetime_value=2000,  # Blended
        target_cac=200,  # Blended
        geographic_focus=["Global"],
        competitive_intensity="high",
        brand_maturity="growth"
    )
    
    print(f"\n📊 Campaign Context: {hybrid_context.get_context_summary()}\n")
    
    # Run analysis
    expert = MediaAnalyticsExpert()
    base_analysis = expert.analyze_all(campaign_data)
    
    # Enhance with specialist
    specialist = B2BSpecialistAgent()
    enhanced_analysis = specialist.enhance_analysis(
        base_insights=base_analysis,
        campaign_context=hybrid_context,
        campaign_data=campaign_data
    )
    
    # Display hybrid insights
    hybrid_insights = enhanced_analysis.get('business_model_analysis', {})
    
    print("🔀 Hybrid Model Analysis:")
    print(f"  Business Model: {hybrid_insights.get('business_model', 'N/A')}")
    
    if 'b2b_analysis' in hybrid_insights:
        print("\n  📊 B2B Side:")
        b2b_side = hybrid_insights['b2b_analysis'].get('business_model_analysis', {})
        lead_analysis = b2b_side.get('lead_quality_analysis', {})
        print(f"    Total Leads: {lead_analysis.get('total_leads', 0)}")
        print(f"    Cost per SQL: {lead_analysis.get('cost_per_sql', 'N/A')}")
    
    if 'b2c_analysis' in hybrid_insights:
        print("\n  🛍️ B2C Side:")
        b2c_side = hybrid_insights['b2c_analysis'].get('business_model_analysis', {})
        cac_analysis = b2c_side.get('customer_acquisition_efficiency', {})
        print(f"    Actual CAC: {cac_analysis.get('actual_cac', 'N/A')}")
        ltv_analysis = b2c_side.get('lifetime_value_analysis', {})
        print(f"    LTV:CAC Ratio: {ltv_analysis.get('ltv_cac_ratio', 'N/A')}")
    
    print("\n📋 Hybrid Recommendations:")
    hybrid_recs = hybrid_insights.get('hybrid_recommendations', [])
    for rec in hybrid_recs[:5]:
        if isinstance(rec, dict):
            print(f"  [{rec.get('priority', 'medium').upper()}] {rec.get('category', 'General')}: {rec.get('recommendation', rec)}")
        else:
            print(f"  {rec}")


def example_benchmark_comparison():
    """Example: Compare B2B vs B2C benchmarks"""
    
    print("\n\n" + "=" * 60)
    print("Example 4: B2B vs B2C Benchmark Comparison")
    print("=" * 60)
    
    specialist = B2BSpecialistAgent()
    
    print("\n📊 B2B Benchmarks:")
    b2b_benchmarks = specialist.b2b_benchmarks
    print("\n  LinkedIn:")
    print(f"    CTR: {b2b_benchmarks['linkedin']['ctr']}")
    print(f"    CPC: {b2b_benchmarks['linkedin']['cpc']}")
    print(f"    Lead Quality Rate: {b2b_benchmarks['linkedin']['lead_quality_rate']}")
    
    print("\n  Google Search (B2B):")
    print(f"    CTR: {b2b_benchmarks['google_search_b2b']['ctr']}")
    print(f"    CPC: {b2b_benchmarks['google_search_b2b']['cpc']}")
    
    print("\n📊 B2C Benchmarks:")
    b2c_benchmarks = specialist.b2c_benchmarks
    print("\n  Meta:")
    print(f"    CTR: {b2c_benchmarks['meta']['ctr']}")
    print(f"    CPC: {b2c_benchmarks['meta']['cpc']}")
    
    print("\n  Google Search (B2C):")
    print(f"    CTR: {b2c_benchmarks['google_search_b2c']['ctr']}")
    print(f"    CPC: {b2c_benchmarks['google_search_b2c']['cpc']}")
    
    print("\n💡 Key Differences:")
    print("  • B2B typically has lower CTR but higher CPC")
    print("  • B2B focuses on lead quality over volume")
    print("  • B2C prioritizes conversion rate and CAC efficiency")
    print("  • B2B has longer sales cycles requiring nurture")
    print("  • B2C emphasizes LTV:CAC ratio and repeat purchases")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("B2B/B2C Specialist Agent Integration Examples")
    print("=" * 60)
    
    # Run all examples
    example_b2b_saas_campaign()
    example_b2c_ecommerce_campaign()
    example_b2b2c_marketplace()
    example_benchmark_comparison()
    
    print("\n\n" + "=" * 60)
    print("Examples Complete!")
    print("=" * 60)
