"""
Generate ordered source list for ingestion
No dependencies required
"""

import json
from datetime import datetime

# All sources in optimal order (Priority 1 - 46 critical sources)
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


def main():
    print("=" * 80)
    print("GENERATING KNOWLEDGE SOURCE FILES")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Priority 1 Sources: {len(PRIORITY_1_SOURCES)}")
    print("=" * 80)
    
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
    
    print("\n[OK] Saved: knowledge_sources_priority1.json")
    
    # Save as text file with URLs only (easy to copy-paste)
    with open('knowledge_sources_urls_only.txt', 'w') as f:
        for url, _, _ in PRIORITY_1_SOURCES:
            f.write(url + '\n')
    
    print("[OK] Saved: knowledge_sources_urls_only.txt (URLs only)")
    
    # Save detailed text file
    with open('knowledge_sources_priority1.txt', 'w') as f:
        f.write("PRIORITY 1 KNOWLEDGE SOURCES - ORDERED FOR INGESTION\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Sources: {len(PRIORITY_1_SOURCES)}\n")
        f.write("="*80 + "\n\n")
        
        current_category = None
        for i, (url, category, description) in enumerate(PRIORITY_1_SOURCES, 1):
            if category != current_category:
                f.write(f"\n{'='*80}\n")
                f.write(f"{category.upper()}\n")
                f.write(f"{'='*80}\n\n")
                current_category = category
            f.write(f"{i}. {description}\n")
            f.write(f"   URL: {url}\n\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("INGESTION INSTRUCTIONS\n")
        f.write("="*80 + "\n\n")
        f.write("1. Start Streamlit app: streamlit run streamlit_apps/app.py\n")
        f.write("2. Navigate to 'Knowledge Base' page\n")
        f.write("3. For each URL above:\n")
        f.write("   a. Copy the URL\n")
        f.write("   b. Paste in the URL input field\n")
        f.write("   c. Click 'Learn from URL'\n")
        f.write("   d. Wait for extraction (30-60 seconds)\n")
        f.write("   e. Check quality score (aim for 70+)\n")
        f.write("   f. Move to next URL\n\n")
        f.write("Estimated Time: 90-120 minutes for all 46 sources\n")
        f.write("Average per source: 2-3 minutes\n\n")
        f.write("="*80 + "\n")
    
    print("[OK] Saved: knowledge_sources_priority1.txt (Detailed list)")
    
    print("\n" + "=" * 80)
    print("FILES GENERATED SUCCESSFULLY!")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Open 'knowledge_sources_priority1.txt' for full details")
    print("2. Open 'knowledge_sources_urls_only.txt' for quick copy-paste")
    print("3. Start Streamlit app and navigate to Knowledge Base page")
    print("4. Begin ingesting sources in order")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
