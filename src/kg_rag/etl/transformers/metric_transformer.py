"""
Metric Transformer for KG-RAG ETL

Transforms raw performance metrics into graph-ready format.
Stores only raw additive metrics - calculated metrics computed at query time.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import date, datetime
import math


logger = logging.getLogger(__name__)


class MetricTransformer:
    """
    Transform metric records for Neo4j ingestion.
    
    Stores raw additive metrics only. Calculated metrics (CTR, CPC, ROAS, etc.)
    are computed at aggregate level during query time.
    """
    
    # Raw metrics to store (additive)
    RAW_METRICS = [
        "impressions", "clicks", "spend", "conversions", "revenue",
        "reach", "video_plays", "video_25", "video_50", "video_75", "video_completes",
        "engagements", "likes", "comments", "shares",
        "pc_conversions", "pv_conversions"
    ]
    
    # Dimension fields to store on Metric node
    DIMENSIONS = [
        "channel", "platform", "funnel", "ad_type", "placement",
        "device_types", "age_range", "geo_countries", "gender_targeting",
        "audience_segment", "creative_format", "targeting_type",
        "campaign_objective", "bid_strategy"
    ]
    
    def __init__(self):
        """Initialize transformer."""
        pass
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform metric records for Neo4j.
        
        Args:
            records: Raw metric records (with canonical column names)
            
        Returns:
            List of transformed Metric node properties
        """
        transformed = []
        import hashlib
        
        for record in records:
            try:
                metric = self._transform_record(record, hashlib)
                if metric:
                    transformed.append(metric)
            except Exception as e:
                logger.warning(f"Failed to transform metric: {e}")
                continue
        
        return transformed
    
    def _transform_record(self, record: Dict[str, Any], hashlib_mod) -> Optional[Dict[str, Any]]:
        """Transform a single metric record."""
        # Require campaign_id and date
        campaign_id = record.get("campaign_id")
        metric_date = self._to_date(record.get("date"))
        
        if not campaign_id or not metric_date:
            logger.warning(f"Metric missing campaign_id or date: {record}")
            return None
        
        # Extract dimensions for ID generation
        dim_values = []
        dims_dict = {}
        
        for dim in self.DIMENSIONS:
            val = record.get(dim)
            if val is not None:
                # Handle lists (e.g. device_types) for hashing
                if isinstance(val, list):
                    val_str = ",".join(sorted([str(v) for v in val]))
                    dim_values.append(f"{dim}:{val_str}")
                else:
                    dim_values.append(f"{dim}:{str(val)}")
                dims_dict[dim] = val
        
        # Generate granular unique ID
        # Hash(CampaignID + Date + Dimensions)
        dims_hash_input = "|".join(sorted(dim_values))
        hash_input = f"{campaign_id}_{metric_date}_{dims_hash_input}"
        suffix = hashlib_mod.md5(hash_input.encode()).hexdigest()[:8]
        
        metric_id = f"{campaign_id}_{metric_date}_{suffix}"
        
        # Build node properties with raw metrics only
        metric = {
            "id": metric_id,
            "campaign_id": str(campaign_id),
            "date": metric_date,
            
            # Dimensions
            **dims_dict,
            
            # Core metrics (always try to get)
            "impressions": self._to_int(record.get("impressions")),
            "clicks": self._to_int(record.get("clicks")),
            "spend": self._to_float(record.get("spend")),
            
            # Optional metrics
            "conversions": self._to_float(record.get("conversions")),
            "revenue": self._to_float(record.get("revenue")),
            "reach": self._to_int(record.get("reach")),
            
            # Video metrics
            "video_plays": self._to_int(record.get("video_plays")),
            "video_25": self._to_int(record.get("video_25")),
            "video_50": self._to_int(record.get("video_50")),
            "video_75": self._to_int(record.get("video_75")),
            "video_completes": self._to_int(record.get("video_completes")),
            
            # Engagement
            "engagements": self._to_int(record.get("engagements")),
            "likes": self._to_int(record.get("likes")),
            "comments": self._to_int(record.get("comments")),
            "shares": self._to_int(record.get("shares")),
            
            # Attribution
            "pc_conversions": self._to_float(record.get("pc_conversions")),
            "pv_conversions": self._to_float(record.get("pv_conversions")),
        }
        
        # Remove None values to save space
        metric = {k: v for k, v in metric.items() if v is not None}
        
        # Ensure required fields are present
        metric.setdefault("impressions", 0)
        metric.setdefault("clicks", 0)
        metric.setdefault("spend", 0.0)
        
        return metric
    
    def aggregate_for_campaign(
        self,
        metrics: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Aggregate metrics for campaign totals update.
        
        Args:
            metrics: List of daily metric records
            
        Returns:
            Aggregated totals
        """
        totals = {
            "impressions_total": 0,
            "clicks_total": 0,
            "spend_total": 0.0,
            "conversions_total": 0.0,
            "revenue_total": 0.0,
        }
        
        for m in metrics:
            totals["impressions_total"] += m.get("impressions", 0) or 0
            totals["clicks_total"] += m.get("clicks", 0) or 0
            totals["spend_total"] += m.get("spend", 0.0) or 0.0
            totals["conversions_total"] += m.get("conversions", 0.0) or 0.0
            totals["revenue_total"] += m.get("revenue", 0.0) or 0.0
        
        return totals
    
    def _to_int(self, value: Any) -> Optional[int]:
        """Convert value to int."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _to_float(self, value: Any) -> Optional[float]:
        """Convert value to float."""
        if value is None:
            return None
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return round(f, 4)
        except (ValueError, TypeError):
            return None
    
    def _to_date(self, value: Any) -> Optional[str]:
        """Convert value to date string (ISO format)."""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            # Try parsing common formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    continue
            # Return as-is if looks like ISO format
            if len(value) == 10 and value[4] == '-' and value[7] == '-':
                return value
        
        return None
