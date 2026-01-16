"""
Automated Knowledge Base Ingestion Script
Ingests all curated sources into the PCA Agent knowledge base
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.platform.knowledge.knowledge_ingestion import KnowledgeIngestion
from src.platform.knowledge.vector_store import VectorStoreBuilder

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('knowledge_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# All sources in optimal order
SOURCES = [
    # PHASE 1: FOUNDATION - Critical Sources (Priority 1)
    
    # Terminology & Glossaries (INGEST FIRST)
    {
        "url": "https://www.impressiondigital.com/blog/digital-marketing-glossary/",
        "category": "Terminology",
        "priority": 1,
        "description": "Comprehensive digital marketing glossary"
    },
    {
        "url": "https://www.marketingterms.com/glossary/",
        "category": "Terminology",
        "priority": 1,
        "description": "Marketing terms glossary"
    },
    {
        "url": "https://www.mtu.edu/umc/services/websites/terminology/",
        "category": "Terminology",
        "priority": 1,
        "description": "Website terminology"
    },
    
    # Campaign Analytics Fundamentals
    {
        "url": "https://supermetrics.com/blog/how-to-analyze-campaigns",
        "category": "Campaign Analytics",
        "priority": 1,
        "description": "How to analyze campaigns - Supermetrics"
    },
    {
        "url": "https://improvado.io/blog/campaign-analytics",
        "category": "Campaign Analytics",
        "priority": 1,
        "description": "Campaign analytics guide - Improvado"
    },
    {
        "url": "https://supermetrics.com/blog/growth-marketing-analytics",
        "category": "Campaign Analytics",
        "priority": 1,
        "description": "Growth marketing analytics"
    },
    {
        "url": "https://supermetrics.com/blog/marketing-data-management",
        "category": "Campaign Analytics",
        "priority": 1,
        "description": "Marketing data management"
    },
    
    # KPIs & Metrics Core
    {
        "url": "https://www.dashthis.com/kpi-examples/",
        "category": "KPIs",
        "priority": 1,
        "description": "Ultimate KPI examples guide - DashThis"
    },
    {
        "url": "https://www.klipfolio.com/resources/kpi-examples/digital-marketing",
        "category": "KPIs",
        "priority": 1,
        "description": "Digital marketing KPI examples - Klipfolio"
    },
    {
        "url": "https://www.simplekpi.com/Resources/Key-Performance-Indicators",
        "category": "KPIs",
        "priority": 1,
        "description": "What are KPIs - SimpleKPI"
    },
    
    # Funnel Analytics
    {
        "url": "https://advertising.amazon.com/library/guides/marketing-funnel",
        "category": "Funnel",
        "priority": 1,
        "description": "Marketing funnel guide - Amazon Advertising"
    },
    {
        "url": "https://www.semrush.com/blog/marketing-funnel/",
        "category": "Funnel",
        "priority": 1,
        "description": "Marketing funnel stages - Semrush"
    },
    
    # Campaign Hierarchy
    {
        "url": "https://analyticsliv.com/blogs/dv360-campaign-structure-best-practices",
        "category": "Hierarchy",
        "priority": 1,
        "description": "DV360 campaign structure best practices"
    },
    {
        "url": "https://blog.adnabu.com/google-ads/google-ads-reporting/",
        "category": "Hierarchy",
        "priority": 1,
        "description": "Google Ads reporting structure"
    },
    
    # PHASE 2: PLATFORMS - Search & Social
    
    # Google Ads
    {
        "url": "https://www.factors.ai/blog/google-ads-101-types-benefits",
        "category": "Google Ads",
        "priority": 1,
        "description": "Google Ads 101: Types & Benefits"
    },
    {
        "url": "https://www.semrush.com/blog/search-engine-marketing/",
        "category": "SEM",
        "priority": 1,
        "description": "Search engine marketing guide - Semrush"
    },
    {
        "url": "https://zapier.com/blog/types-of-google-ads/",
        "category": "Google Ads",
        "priority": 1,
        "description": "Types of Google Ads - Zapier"
    },
    {
        "url": "https://www.demandcurve.com/blog/types-of-google-ads",
        "category": "Google Ads",
        "priority": 1,
        "description": "Types of Google Ads - Demand Curve"
    },
    {
        "url": "https://support.google.com/google-ads/answer/2580383?hl=en",
        "category": "Google Ads",
        "priority": 1,
        "description": "Google Ads Help - Reporting"
    },
    
    # Bing Ads
    {
        "url": "https://www.technadigital.com/bing-ads-effective-campaign-guide/",
        "category": "Bing Ads",
        "priority": 1,
        "description": "Bing Ads effective campaign guide"
    },
    {
        "url": "https://www.datafeedwatch.com/blog/microsoft-bing-ads-guide",
        "category": "Bing Ads",
        "priority": 1,
        "description": "Microsoft Bing Ads guide"
    },
    
    # Meta Ads
    {
        "url": "https://magicbrief.com/post/understanding-meta-ads-reporting-a-complete-guide",
        "category": "Meta Ads",
        "priority": 1,
        "description": "Understanding Meta Ads reporting - Complete guide"
    },
    {
        "url": "https://leadsbridge.com/blog/what-are-meta-ads/",
        "category": "Meta Ads",
        "priority": 1,
        "description": "What are Meta Ads - LeadsBridge"
    },
    {
        "url": "https://support.agorapulse.com/en/articles/10137532-meta-ads-report-explained",
        "category": "Meta Ads",
        "priority": 1,
        "description": "Meta Ads report explained - Agorapulse"
    },
    {
        "url": "https://www.facebook.com/business/help/1153577308409919?id=1858550721111595",
        "category": "Meta Ads",
        "priority": 1,
        "description": "Facebook audience targeting help"
    },
    
    # LinkedIn Ads
    {
        "url": "https://www.sprinklr.com/help/articles/getting-started/linkedin-ads-campaign-structure-roles/645234780d27fc559bbe4b62",
        "category": "LinkedIn",
        "priority": 1,
        "description": "LinkedIn Ads campaign structure & roles"
    },
    {
        "url": "https://www.linkedin.com/help/lms/answer/a424171",
        "category": "LinkedIn",
        "priority": 1,
        "description": "LinkedIn Ads targeting help"
    },
    
    # Social Media General
    {
        "url": "https://digitalbrolly.com/social-media-platforms-for-marketing/",
        "category": "Social",
        "priority": 1,
        "description": "Social media platforms for marketing"
    },
    {
        "url": "https://www.singlegrain.com/blog/ms/best-social-media-platforms-for-advertising/",
        "category": "Social",
        "priority": 1,
        "description": "Best social media platforms for advertising"
    },
    
    # PHASE 3: AUDIENCE & TARGETING
    
    {
        "url": "https://www.criteo.com/digital-advertising-glossary/?_gl=1*g6j076*_up*MQ..*_ga*MTE2ODQ4NjQ3My4xNzYzODY5NzQz*_ga_W0WYERL9HS*czE3NjM4Njk3NDIkbzEkZzAkdDE3NjM4Njk3NDIkajYwJGwwJGg4NDc1MjQ4NDQ.",
        "category": "Targeting",
        "priority": 1,
        "description": "Audience targeting glossary - Criteo"
    },
    {
        "url": "https://www.criteo.com/ultimate-guide-to-audience-targeting-for-advertising/",
        "category": "Targeting",
        "priority": 1,
        "description": "Ultimate guide to audience targeting"
    },
    {
        "url": "https://www.brafton.com/blog/strategy/audience-targeting-examples/",
        "category": "Targeting",
        "priority": 1,
        "description": "Audience targeting examples - Brafton"
    },
    {
        "url": "https://www.brafton.com/blog/strategy/audience-segmentation/",
        "category": "Targeting",
        "priority": 1,
        "description": "Audience segmentation guide"
    },
    {
        "url": "https://www.brafton.com.au/blog/strategy/audience-interest/",
        "category": "Targeting",
        "priority": 1,
        "description": "Audience interest analysis"
    },
    {
        "url": "https://www.brafton.com/blog/strategy/6-real-life-target-audience-examples-to-help-you-define-your-own-b2b-and-b2c/",
        "category": "Targeting",
        "priority": 1,
        "description": "Real-life target audience examples"
    },
    {
        "url": "https://metricswatch.com/insights/best-practices-for-social-media-demographic-reports",
        "category": "Demographics",
        "priority": 1,
        "description": "Demographic reports best practices"
    },
    
    # PHASE 4: DISPLAY & PROGRAMMATIC
    
    {
        "url": "https://improvado.io/blog/gdn-vs-dv360-ad-platform-comparison",
        "category": "Display",
        "priority": 1,
        "description": "GDN vs DV360 comparison - Improvado"
    },
    {
        "url": "https://www.m3.agency/news-insights/ultimate-guide-to-display-marketing-gdn-vs-dsps",
        "category": "Display",
        "priority": 1,
        "description": "Ultimate guide to display marketing"
    },
    {
        "url": "https://agencyanalytics.com/blog/what-is-programmatic-advertising",
        "category": "Programmatic",
        "priority": 1,
        "description": "What is programmatic advertising"
    },
    {
        "url": "https://improvado.io/blog/the-trade-desk-guide",
        "category": "Programmatic",
        "priority": 1,
        "description": "The Trade Desk guide - Improvado"
    },
    
    # PHASE 5: ANALYTICS & VISUALIZATION
    
    {
        "url": "https://improvado.io/blog/looker-vs-tableau-vs-power-bi",
        "category": "Analytics",
        "priority": 1,
        "description": "Looker vs Tableau vs Power BI"
    },
    {
        "url": "https://improvado.io/blog/adobe-analytics",
        "category": "Analytics",
        "priority": 1,
        "description": "Adobe Analytics guide - Improvado"
    },
    {
        "url": "https://improvado.io/blog/marketing-data-visualization-techniques",
        "category": "Visualization",
        "priority": 1,
        "description": "Marketing data visualization techniques"
    },
    {
        "url": "https://blog.coupler.io/data-visualization-guide/",
        "category": "Visualization",
        "priority": 1,
        "description": "Data visualization guide - Coupler.io"
    },
    
    # PHASE 6: CASE STUDIES
    
    {
        "url": "https://supermetrics.com/case-studies/shopee",
        "category": "Case Study",
        "priority": 1,
        "description": "Shopee case study - E-commerce"
    },
    {
        "url": "https://supermetrics.com/case-studies/found-uk",
        "category": "Case Study",
        "priority": 1,
        "description": "Found UK case study"
    },
    {
        "url": "https://supermetrics.com/case-studies/pattrns",
        "category": "Case Study",
        "priority": 1,
        "description": "Pattrns case study - Marketing agency"
    },
    {
        "url": "https://supermetrics.com/case-studies/orange-line",
        "category": "Case Study",
        "priority": 1,
        "description": "Orange Line case study - Marketing agency"
    },
]

# Add Priority 2 sources (can be extended)
PRIORITY_2_SOURCES = [
    # Campaign Analytics Extended
    {
        "url": "https://improvado.io/blog/ai-marketing-agents",
        "category": "Campaign Analytics",
        "priority": 2,
        "description": "AI marketing agents"
    },
    {
        "url": "https://supermetrics.com/blog/marketing-data-extraction",
        "category": "Campaign Analytics",
        "priority": 2,
        "description": "Marketing data extraction"
    },
    {
        "url": "https://supermetrics.com/blog/marketing-data-transformation",
        "category": "Campaign Analytics",
        "priority": 2,
        "description": "Marketing data transformation"
    },
    {
        "url": "https://supermetrics.com/blog/marketing-data-loading",
        "category": "Campaign Analytics",
        "priority": 2,
        "description": "Marketing data loading"
    },
    {
        "url": "https://improvado.io/blog/marketing-data-pipeline",
        "category": "Campaign Analytics",
        "priority": 2,
        "description": "Marketing data pipeline"
    },
    # Add more Priority 2 sources as needed
]

# Load pending URLs (if available)
PENDING_FILE = Path('data/pending_urls.json')
pending_sources = []
if PENDING_FILE.exists():
    try:
        pending_data = json.loads(PENDING_FILE.read_text(encoding='utf-8'))
        for entry in pending_data:
            pending_sources.append({
                "url": entry['url'],
                "category": entry.get('category', 'Pending'),
                "priority": entry.get('priority', 2),
                "description": entry.get('description', f"Pending source from {entry.get('source_file', 'unknown')}")
            })
    except json.JSONDecodeError:
        logger.error("Could not load pending_urls.json; continuing without it")

# Combine sources: prioritize curated sources first, then pending backlog
ALL_SOURCES = SOURCES + PRIORITY_2_SOURCES + pending_sources


def ingest_source(knowledge_engine, source_info, index, total):
    """Ingest a single source with error handling"""
    url = source_info["url"]
    category = source_info["category"]
    description = source_info["description"]
    priority = source_info["priority"]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"[{index}/{total}] Ingesting: {description}")
    logger.info(f"URL: {url}")
    logger.info(f"Category: {category} | Priority: {priority}")
    logger.info(f"{'='*80}")
    
    try:
        result = knowledge_engine.ingest_from_url(url)
        
        if result['success']:
            quality_score = result.get('quality_score', 0)
            chunk_count = result.get('chunk_count', 0)
            
            logger.info("SUCCESS")
            logger.info(f"   Quality Score: {quality_score}/100")
            logger.info(f"   Chunks: {chunk_count}")
            logger.info(f"   Title: {result.get('title', 'N/A')}")
            
            # Log warnings if any
            validation = result.get('validation', {})
            warnings = validation.get('warnings', [])
            if warnings:
                logger.warning(f"   Warnings: {len(warnings)}")
                for warning in warnings:
                    logger.warning(f"      - {warning}")
            
            result['category'] = category
            result['priority'] = priority
            result['description'] = description

            return {
                'status': 'success',
                'url': url,
                'category': category,
                'quality_score': quality_score,
                'chunk_count': chunk_count
            }
        else:
            error = result.get('error', 'Unknown error')
            logger.error(f"FAILED: {error}")
            
            return {
                'status': 'failed',
                'url': url,
                'category': category,
                'error': error
            }
            
    except Exception as e:
        logger.error(f"EXCEPTION: {str(e)}")
        return {
            'status': 'exception',
            'url': url,
            'category': category,
            'error': str(e)
        }


def main():
    """Main ingestion function"""
    logger.info("="*80)
    logger.info("AUTOMATED KNOWLEDGE BASE INGESTION")
    logger.info("="*80)
    logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total Sources: {len(ALL_SOURCES)}")
    logger.info("="*80)
    
    # Initialize knowledge engine
    logger.info("\nInitializing Knowledge Ingestion Engine...")
    knowledge_engine = KnowledgeIngestion(
        chunk_size=1000,
        chunk_overlap=200,
        min_content_length=100
    )
    logger.info("✅ Engine initialized")
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'exception': []
    }
    
    start_time = time.time()
    
    # Ingest each source
    for index, source_info in enumerate(ALL_SOURCES, 1):
        result = ingest_source(knowledge_engine, source_info, index, len(ALL_SOURCES))
        results[result['status']].append(result)
        
        # Small delay to avoid rate limiting
        time.sleep(2)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("INGESTION COMPLETE!")
    logger.info("="*80)
    logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {duration/60:.2f} minutes ({duration:.0f} seconds)")
    logger.info(f"\nResults:")
    logger.info(f"  Successful: {len(results['success'])}")
    logger.info(f"  Failed: {len(results['failed'])}")
    logger.info(f"  Exceptions: {len(results['exception'])}")
    logger.info(f"  📊 Success Rate: {len(results['success'])/len(ALL_SOURCES)*100:.1f}%")
    
    # Quality statistics
    if results['success']:
        quality_scores = [r['quality_score'] for r in results['success']]
        avg_quality = sum(quality_scores) / len(quality_scores)
        logger.info(f"\nQuality Statistics:")
        logger.info(f"  Average Quality Score: {avg_quality:.1f}/100")
        logger.info(f"  Excellent (80-100): {sum(1 for s in quality_scores if s >= 80)}")
        logger.info(f"  Good (70-79): {sum(1 for s in quality_scores if 70 <= s < 80)}")
        logger.info(f"  Fair (60-69): {sum(1 for s in quality_scores if 60 <= s < 70)}")
        logger.info(f"  Poor (<60): {sum(1 for s in quality_scores if s < 60)}")
    
    # Failed sources
    if results['failed']:
        logger.warning(f"\nFailed Sources ({len(results['failed'])}):")
        for result in results['failed']:
            logger.warning(f"  - {result['url']}")
            logger.warning(f"    Error: {result['error']}")
    
    # Exception sources
    if results['exception']:
        logger.error(f"\nException Sources ({len(results['exception'])}):")
        for result in results['exception']:
            logger.error(f"  - {result['url']}")
            logger.error(f"    Error: {result['error']}")
    
    logger.info("\n" + "="*80)
    logger.info("Knowledge base ingestion complete!")
    logger.info("Check 'knowledge_ingestion.log' for detailed logs")
    logger.info("="*80)
    
    # Save knowledge base status
    summary = knowledge_engine.get_knowledge_summary()
    if summary:
        logger.info("\nKnowledge Base Summary:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")

    # Persist knowledge base and rebuild vector store
    try:
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        knowledge_path = data_dir / 'knowledge_base.json'
        knowledge_path.write_text(
            json.dumps(knowledge_engine.knowledge_base, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
        logger.info(f"Knowledge base saved to {knowledge_path}")

        if knowledge_engine.knowledge_base:
            logger.info("Building vector store (FAISS)...")
            VectorStoreBuilder().build_from_documents(knowledge_engine.knowledge_base)
            logger.info("Vector store built successfully")
        else:
            logger.warning("No knowledge documents available to build vector store")
    except Exception as exc:
        logger.error(f"Failed to persist knowledge base or build vector store: {exc}")


if __name__ == "__main__":
    main()
