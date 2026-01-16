"""
Simple Automated Knowledge Base Ingestion Script
Works directly with Streamlit session state
Run this while Streamlit app is running
"""

import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime

# All sources in optimal order (Priority 1 only - 46 critical sources)
PRIORITY_1_SOURCES = [
    # Terminology & Glossaries (INGEST FIRST)
    ("https://www.impressiondigital.com/blog/digital-marketing-glossary/", "Terminology", "Digital marketing glossary"),
    ("https://www.marketingterms.com/glossary/", "Terminology", "Marketing terms glossary"),
    ("https://www.mtu.edu/umc/services/websites/terminology/", "Terminology", "Website terminology"),
    
    # Campaign Analytics Fundamentals
    ("https://supermetrics.com/blog/how-to-analyze-campaigns", "Campaign Analytics", "How to analyze campaigns"),
    ("https://improvado.io/blog/campaign-analytics", "Campaign Analytics", "Campaign analytics guide"),
    ("https://supermetrics.com/blog/growth-marketing-analytics", "Campaign Analytics", "Growth marketing analytics"),
    ("https://supermetrics.com/blog/marketing-data-management", "Campaign Analytics", "Marketing data management"),
    
    # KPIs & Metrics Core
    ("https://www.dashthis.com/kpi-examples/", "KPIs", "Ultimate KPI examples guide"),
    ("https://www.klipfolio.com/resources/kpi-examples/digital-marketing", "KPIs", "Digital marketing KPI examples"),
    ("https://www.simplekpi.com/Resources/Articles/what-are-kpis", "KPIs", "What are KPIs"),
    
    # Funnel Analytics
    ("https://advertising.amazon.com/library/guides/marketing-funnel", "Funnel", "Marketing funnel guide"),
    ("https://www.semrush.com/blog/marketing-funnel/", "Funnel", "Marketing funnel stages"),
    
    # Campaign Hierarchy
    ("https://analyticsliv.com/blogs/dv360-campaign-structure-best-practices", "Hierarchy", "DV360 campaign structure"),
    ("https://blog.adnabu.com/google-ads/google-ads-reporting/", "Hierarchy", "Google Ads reporting"),
    
    # Google Ads
    ("https://www.factors.ai/blog/google-ads-101-types-benefits", "Google Ads", "Google Ads 101"),
    ("https://www.semrush.com/blog/search-engine-marketing/", "SEM", "Search engine marketing"),
    ("https://zapier.com/blog/types-of-google-ads/", "Google Ads", "Types of Google Ads - Zapier"),
    ("https://www.demandcurve.com/blog/types-of-google-ads", "Google Ads", "Types of Google Ads - Demand Curve"),
    ("https://support.google.com/google-ads/answer/2580383?hl=en", "Google Ads", "Google Ads Help"),
    
    # Bing Ads
    ("https://www.technadigital.com/bing-ads-effective-campaign-guide/", "Bing Ads", "Bing Ads campaign guide"),
    ("https://www.datafeedwatch.com/blog/microsoft-bing-ads-guide", "Bing Ads", "Microsoft Bing Ads guide"),
    
    # Meta Ads
    ("https://magicbrief.com/post/understanding-meta-ads-reporting-a-complete-guide", "Meta Ads", "Meta Ads reporting guide"),
    ("https://leadsbridge.com/blog/what-are-meta-ads/", "Meta Ads", "What are Meta Ads"),
    ("https://support.agorapulse.com/en/articles/10137532-meta-ads-report-explained", "Meta Ads", "Meta Ads report explained"),
    ("https://www.facebook.com/business/help/1153577308409919?id=1858550721111595", "Meta Ads", "Facebook audience targeting"),
    
    # LinkedIn Ads
    ("https://www.sprinklr.com/help/articles/getting-started/linkedin-ads-campaign-structure-roles/645234780d27fc559bbe4b62", "LinkedIn", "LinkedIn Ads structure"),
    ("https://www.linkedin.com/help/lms/answer/a424171", "LinkedIn", "LinkedIn Ads targeting"),
    
    # Social Media General
    ("https://digitalbrolly.com/social-media-platforms-for-marketing/", "Social", "Social media platforms"),
    ("https://www.singlegrain.com/blog/ms/best-social-media-platforms-for-advertising/", "Social", "Best social platforms"),
    
    # Audience & Targeting
    ("https://www.criteo.com/glossary/audience-targeting/", "Targeting", "Audience targeting glossary"),
    ("https://www.brafton.com/blog/strategy/audience-targeting-examples/", "Targeting", "Audience targeting examples"),
    ("https://www.taboola.com/help/en/articles/10695578-audience-insights-demographics", "Demographics", "Audience insights demographics"),
    ("https://metricswatch.com/insights/best-practices-for-social-media-demographic-reports", "Demographics", "Demographic reports best practices"),
    
    # Display & Programmatic
    ("https://improvado.io/blog/gdn-vs-dv360-ad-platform-comparison", "Display", "GDN vs DV360 comparison"),
    ("https://www.m3.agency/news-insights/ultimate-guide-to-display-marketing-gdn-vs-dsps", "Display", "Display marketing guide"),
    ("https://agencyanalytics.com/blog/what-is-programmatic-advertising", "Programmatic", "What is programmatic advertising"),
    ("https://improvado.io/blog/the-trade-desk-guide", "Programmatic", "The Trade Desk guide"),
    
    # Analytics & Visualization
    ("https://improvado.io/blog/looker-vs-tableau-vs-power-bi", "Analytics", "Looker vs Tableau vs Power BI"),
    ("https://improvado.io/blog/adobe-analytics", "Analytics", "Adobe Analytics guide"),
    ("https://improvado.io/blog/marketing-data-visualization-techniques", "Visualization", "Data visualization techniques"),
    ("https://blog.coupler.io/data-visualization-guide/", "Visualization", "Data visualization guide"),
    
    # Case Studies
    ("https://supermetrics.com/customers/shopee", "Case Study", "Shopee case study"),
    ("https://supermetrics.com/customers/vodafoneziggo", "Case Study", "VodafoneZiggo case study"),
    ("https://supermetrics.com/customers/pattrns", "Case Study", "Pattrns case study"),
    ("https://supermetrics.com/customers/orange-line", "Case Study", "Orange Line case study"),
]


def print_header():
    """Print script header"""
    print("=" * 80)
    print("AUTOMATED KNOWLEDGE BASE INGESTION")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Sources (Priority 1): {len(PRIORITY_1_SOURCES)}")
    print("=" * 80)
    print()


def print_source_list():
    """Print all sources to be ingested"""
    print("\n📋 SOURCES TO INGEST:\n")
    
    current_category = None
    for i, (url, category, description) in enumerate(PRIORITY_1_SOURCES, 1):
        if category != current_category:
            print(f"\n{category}:")
            current_category = category
        print(f"  {i}. {description}")
        print(f"     {url}")
    
    print("\n" + "=" * 80)


def create_ingestion_instructions():
    """Create instructions for manual ingestion via Streamlit UI"""
    instructions = """
================================================================================
AUTOMATED INGESTION INSTRUCTIONS
================================================================================

Since the knowledge base requires the Streamlit app to be running, please follow
these steps to ingest all sources:

OPTION 1: Manual Ingestion via UI (Recommended)
------------------------------------------------
1. Open the Streamlit app: streamlit run streamlit_apps/app.py
2. Navigate to "📚 Knowledge Base" page
3. For each URL below, paste it and click "Learn from URL"
4. Wait for extraction and quality validation
5. Move to next URL

OPTION 2: Use the Streamlit API (If available)
-----------------------------------------------
If your Streamlit app exposes an API endpoint for ingestion, you can automate
this process. Otherwise, manual ingestion is required.

SOURCES LIST (In Order):
================================================================================
"""
    
    for i, (url, category, description) in enumerate(PRIORITY_1_SOURCES, 1):
        instructions += f"\n{i}. [{category}] {description}\n"
        instructions += f"   URL: {url}\n"
    
    instructions += """
================================================================================
TIPS FOR FASTER INGESTION:
================================================================================
1. Copy all URLs to a text file
2. Open Knowledge Base page in Streamlit
3. Paste URLs one by one (Ctrl+V)
4. Click "Learn from URL" for each
5. Average time: 1-2 minutes per URL
6. Total time: ~90-120 minutes for all 46 sources

QUALITY TARGETS:
- Aim for 70+ quality score on each source
- Re-ingest sources with <60 quality score
- Review validation warnings

PROGRESS TRACKING:
- Check knowledge_ingestion.log for details
- Monitor quality scores in Streamlit UI
- Test with sample queries after every 10 sources

================================================================================
"""
    
    return instructions


def save_sources_to_file():
    """Save sources to a file for easy reference"""
    # Save as JSON
    sources_json = []
    for url, category, description in PRIORITY_1_SOURCES:
        sources_json.append({
            "url": url,
            "category": category,
            "description": description,
            "priority": 1
        })
    
    with open('knowledge_sources_priority1.json', 'w') as f:
        json.dump(sources_json, f, indent=2)
    
    print("✅ Saved sources to: knowledge_sources_priority1.json")
    
    # Save as text file
    with open('knowledge_sources_priority1.txt', 'w') as f:
        f.write("PRIORITY 1 KNOWLEDGE SOURCES (46 sources)\n")
        f.write("="*80 + "\n\n")
        
        current_category = None
        for i, (url, category, description) in enumerate(PRIORITY_1_SOURCES, 1):
            if category != current_category:
                f.write(f"\n{category}:\n")
                f.write("-" * 40 + "\n")
                current_category = category
            f.write(f"{i}. {description}\n")
            f.write(f"   {url}\n\n")
    
    print("✅ Saved sources to: knowledge_sources_priority1.txt")


def main():
    """Main function"""
    print_header()
    print_source_list()
    
    print("\n📝 Generating ingestion instructions...")
    instructions = create_ingestion_instructions()
    
    # Save instructions to file
    with open('INGESTION_INSTRUCTIONS.txt', 'w') as f:
        f.write(instructions)
    
    print("✅ Saved instructions to: INGESTION_INSTRUCTIONS.txt")
    
    # Save sources to files
    print("\n📝 Saving sources to files...")
    save_sources_to_file()
    
    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print("1. Open INGESTION_INSTRUCTIONS.txt for detailed steps")
    print("2. Open knowledge_sources_priority1.txt for source list")
    print("3. Start Streamlit app: streamlit run streamlit_apps/app.py")
    print("4. Navigate to Knowledge Base page")
    print("5. Begin ingesting sources one by one")
    print("\nEstimated time: 90-120 minutes for all 46 Priority 1 sources")
    print("=" * 80)


if __name__ == "__main__":
    main()
