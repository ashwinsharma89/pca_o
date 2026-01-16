#!/usr/bin/env python3
"""
KG-RAG ETL Pipeline

Orchestrates the full ETL process from DuckDB to Neo4j.

Usage:
    python scripts/kg_rag/run_etl.py --source /path/to/campaigns.duckdb
    python scripts/kg_rag/run_etl.py --source /path/to/campaigns.duckdb --table daily_metrics
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import date

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.kg_rag.etl.extractors.duckdb_extractor import DuckDBExtractor
from src.kg_rag.etl.transformers.campaign_transformer import CampaignTransformer
from src.kg_rag.etl.transformers.metric_transformer import MetricTransformer
from src.kg_rag.etl.transformers.targeting_transformer import TargetingTransformer
from src.kg_rag.etl.loaders.neo4j_loader import Neo4jLoader
from src.kg_rag.client.connection import get_neo4j_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_etl(
    source_path: str,
    campaign_table: str = "campaigns",
    metric_table: str = None,
    batch_size: int = 1000,
    platform_filter: str = None,
    date_from: date = None,
    date_to: date = None
) -> dict:
    """
    Run the full ETL pipeline.
    
    Args:
        source_path: Path to DuckDB file
        campaign_table: Campaign table name
        metric_table: Metrics table name (optional)
        batch_size: Batch size for processing
        platform_filter: Optional platform filter
        date_from: Optional start date
        date_to: Optional end date
        
    Returns:
        Summary statistics
    """
    stats = {
        "campaigns_extracted": 0,
        "campaigns_loaded": 0,
        "metrics_extracted": 0,
        "metrics_loaded": 0,
        "targeting_loaded": 0,
    }
    
    # Initialize components
    logger.info(f"Starting ETL from {source_path}")
    
    extractor = DuckDBExtractor(source_path)
    campaign_transformer = CampaignTransformer()
    metric_transformer = MetricTransformer()
    targeting_transformer = TargetingTransformer()
    loader = Neo4jLoader()
    
    # Detect platform from data
    detected_platform = extractor.detect_platform(campaign_table)
    if detected_platform:
        logger.info(f"Detected platform: {detected_platform}")
        campaign_transformer.default_platform = detected_platform
        targeting_transformer.platform = detected_platform
    
    # Extract and load campaigns
    logger.info("Extracting campaigns...")
    
    for batch in extractor.extract_campaigns(
        table=campaign_table,
        batch_size=batch_size,
        platform_filter=platform_filter,
        date_from=date_from,
        date_to=date_to
    ):
        stats["campaigns_extracted"] += len(batch)
        
        # Transform campaigns
        campaigns = campaign_transformer.transform(batch)
        
        # Load campaigns
        result = loader.load_campaigns(campaigns)
        stats["campaigns_loaded"] += result.get("records", 0)
        
        # Create platform relationships
        loader.load_campaign_platform_relationships(campaigns)
        
        # Extract and load targeting
        targeting_records = [targeting_transformer.extract_from_campaign(c) for c in batch]
        targeting_records = [t for t in targeting_records if t]
        if targeting_records:
            result = loader.load_targeting(targeting_records)
            stats["targeting_loaded"] += result.get("records", 0)
    
    # Extract and load metrics if table exists
    if metric_table:
        tables = extractor.get_tables()
        if metric_table in tables:
            logger.info("Extracting metrics...")
            
            for batch in extractor.extract_metrics(
                table=metric_table,
                batch_size=batch_size,
                date_from=date_from,
                date_to=date_to
            ):
                stats["metrics_extracted"] += len(batch)
                
                # Transform
                metrics = metric_transformer.transform(batch)
                
                # Load
                result = loader.load_metrics(metrics)
                stats["metrics_loaded"] += result.get("records", 0)
            
            # Update campaign totals
            logger.info("Updating campaign totals...")
            updated = loader.update_all_campaign_totals()
            stats["campaigns_updated"] = updated
    
    # Get final stats
    logger.info("Getting final graph statistics...")
    graph_stats = loader.get_stats()
    stats.update(graph_stats)
    
    extractor.close()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Run KG-RAG ETL Pipeline")
    parser.add_argument(
        "--source",
        required=True,
        help="Path to DuckDB file"
    )
    parser.add_argument(
        "--campaign-table",
        default="campaigns",
        help="Campaign table name"
    )
    parser.add_argument(
        "--metric-table",
        default=None,
        help="Metrics table name"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size"
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Filter by platform"
    )
    parser.add_argument(
        "--date-from",
        default=None,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="End date (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    # Parse dates
    date_from = None
    date_to = None
    if args.date_from:
        from datetime import datetime
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
    if args.date_to:
        from datetime import datetime
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    
    # Verify Neo4j connection
    try:
        neo4j = get_neo4j_connection()
        health = neo4j.health_check()
        if not health["connected"]:
            logger.error(f"Cannot connect to Neo4j: {health.get('error')}")
            sys.exit(1)
        logger.info(f"Connected to Neo4j at {health['uri']}")
    except Exception as e:
        logger.error(f"Neo4j connection failed: {e}")
        sys.exit(1)
    
    # Run ETL
    try:
        stats = run_etl(
            source_path=args.source,
            campaign_table=args.campaign_table,
            metric_table=args.metric_table,
            batch_size=args.batch_size,
            platform_filter=args.platform,
            date_from=date_from,
            date_to=date_to
        )
        
        # Print summary
        print("\n" + "=" * 50)
        print("ETL COMPLETE")
        print("=" * 50)
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("=" * 50 + "\n")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
