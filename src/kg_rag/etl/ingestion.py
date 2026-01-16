
import logging
import pandas as pd
import hashlib
from typing import Dict, Any, List

from src.kg_rag.etl.transformers.campaign_transformer import CampaignTransformer
from src.kg_rag.etl.transformers.metric_transformer import MetricTransformer
from src.kg_rag.etl.transformers.targeting_transformer import TargetingTransformer
from src.kg_rag.etl.loaders.neo4j_loader import Neo4jLoader

logger = logging.getLogger(__name__)

def ingest_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Ingest a DataFrame into the Knowledge Graph.
    
    Args:
        df: Pandas DataFrame with campaign data
        
    Returns:
        Summary of ingestion stats
    """
    # Convert DataFrame to list of dictionaries
    # Handle NaN values and normalize keys
    raw_records = df.where(pd.notnull(df), None).to_dict(orient="records")
    records = [_normalize_record(r) for r in raw_records]

    logger.info(f"Starting ingestion of {len(records)} records into Knowledge Graph")

    try:
        # Initialize transformers
        campaign_transformer = CampaignTransformer()
        metric_transformer = MetricTransformer()
        targeting_transformer = TargetingTransformer()

        # Transform data
        logger.info("Transforming records...")
        campaigns = campaign_transformer.transform(records)
        metrics = metric_transformer.transform(records)
        targeting = targeting_transformer.transform(records)
        
        logger.info(f"Transformed: {len(campaigns)} campaigns, {len(metrics)} metrics, {len(targeting)} targeting")

        # Initialize loader
        loader = Neo4jLoader()

        # Load data in dependency order
        
        # 1. Campaigns
        logger.info("Loading campaigns...")
        c_stats = loader.load_campaigns(campaigns)
        loader.load_campaign_platform_relationships(campaigns)
        
        # 2. Targeting
        if targeting:
            logger.info("Loading targeting...")
            t_stats = loader.load_targeting(targeting)
        else:
            t_stats = {"nodes_created": 0}
            
        # 3. Metrics
        if metrics:
            logger.info("Loading metrics...")
            m_stats = loader.load_metrics(metrics)
            
            # Update campaign totals based on loaded metrics
            logger.info("Updating campaign totals...")
            loader.update_all_campaign_totals()
        else:
            m_stats = {"nodes_created": 0}

        summary = {
            "campaigns": c_stats,
            "targeting": t_stats,
            "metrics": m_stats,
            "total_records_processed": len(records)
        }
        
        logger.info(f"Ingestion completed: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize record keys and calculate derived fields."""
    normalized = {}
    
    # Key normalization mapping
    key_map = {
        "campaign_name_full": "campaign_name",
        "total_spent": "spend",
        "site_visit": "conversions", # Mapped based on user feedback/column analysis
        "device_type": "device_types", 
        "campaign_objective": "objective",
        "age_group": "age_range",
        "geographic_region": "geo_countries",
        "start_date": "start_date", # date column?
        "date": "date"
    }
    
    for k, v in record.items():
        k_norm = k.lower().replace(" ", "_")
        target_key = key_map.get(k_norm, k_norm)
        normalized[target_key] = v
        
    # Handle composite fields
    # Revenue
    rev = 0.0
    if "revenue_2024" in normalized and normalized["revenue_2024"]:
        try:
            rev += float(normalized["revenue_2024"])
        except: pass
    if "revenue_2025" in normalized and normalized["revenue_2025"]:
        try:
            rev += float(normalized["revenue_2025"])
        except: pass
    normalized["revenue"] = rev
    
    # Targeting formatting
    if "device_types" in normalized and isinstance(normalized["device_types"], str):
        normalized["device_types"] = [normalized["device_types"]]
        
    if "geo_countries" in normalized and isinstance(normalized["geo_countries"], str):
        normalized["geo_countries"] = [normalized["geo_countries"]]
        
    # Normalize dates to YYYY-MM-DD string
    for date_field in ["date", "start_date", "end_date"]:
        if date_field in normalized and normalized[date_field]:
            val = normalized[date_field]
            if hasattr(val, "date"): # Timestamp/datetime
                try:
                    normalized[date_field] = val.date().isoformat()
                except:
                    normalized[date_field] = None
            elif isinstance(val, str):
                # Handle NaT string
                if val in ("NaT", "nat", "None", "none", ""):
                    normalized[date_field] = None
                # Handle 2024-01-01T00:00:00 or 2024-01-01 00:00:00
                elif "T" in val:
                    normalized[date_field] = val.split("T")[0]
                elif " " in val:
                    normalized[date_field] = val.split(" ")[0]
        
    # Fallback: construct date from year and month columns if date is missing
    if not normalized.get("date"):
        year = normalized.get("year")
        month = normalized.get("month")
        if year and month and str(year).isdigit() and str(month).isdigit():
            # Use first day of the month
            try:
                y = int(year)
                m = int(month)
                if 1 <= m <= 12 and 2000 <= y <= 2100:
                    normalized["date"] = f"{y:04d}-{m:02d}-01"
            except:
                pass
        
    # Generate campaign_id if missing (critical for linking metrics/targeting)
    if not normalized.get("campaign_id"):
        name = normalized.get("campaign_name")
        platform = normalized.get("platform")
        if name and platform:
            hash_input = f"{platform}:{name}"
            normalized["campaign_id"] = hashlib.md5(hash_input.encode()).hexdigest()[:12]

    return normalized
