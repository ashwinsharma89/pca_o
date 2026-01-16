"""
Data Extraction and Normalization Agent.
Normalizes extracted data across platforms into a unified schema.
"""
from typing import List, Dict, Optional
from collections import defaultdict

from loguru import logger

from src.platform.models.platform import (
    PlatformType,
    PlatformSnapshot,
    NormalizedMetric,
    ExtractedMetric,
    MetricType,
    PLATFORM_CONFIGS
)
from src.platform.models.campaign import Campaign


class ExtractionAgent:
    """Agent for normalizing and validating extracted data across platforms."""
    
    def __init__(self):
        """Initialize Extraction Agent."""
        logger.info("Initialized ExtractionAgent")
    
    def normalize_campaign_data(self, campaign: Campaign) -> Campaign:
        """
        Normalize all extracted data from campaign snapshots.
        
        Args:
            campaign: Campaign with extracted snapshot data
            
        Returns:
            Campaign with normalized metrics
        """
        logger.info(f"Normalizing data for campaign {campaign.campaign_id}")
        
        normalized_metrics = []
        
        for snapshot in campaign.snapshots:
            if snapshot.processing_status != "completed":
                logger.warning(f"Skipping snapshot {snapshot.snapshot_id} - not completed")
                continue
            
            # Normalize metrics from this snapshot
            snapshot_metrics = self._normalize_snapshot_metrics(snapshot)
            normalized_metrics.extend(snapshot_metrics)
        
        campaign.normalized_metrics = normalized_metrics
        logger.info(f"Normalized {len(normalized_metrics)} metrics across {len(campaign.snapshots)} snapshots")
        
        # Validate data consistency
        validation_results = self._validate_metrics(normalized_metrics)
        if validation_results["warnings"]:
            for warning in validation_results["warnings"]:
                logger.warning(warning)
                campaign.processing_logs.append(f"WARNING: {warning}")
        
        return campaign
    
    def _normalize_snapshot_metrics(
        self,
        snapshot: PlatformSnapshot
    ) -> List[NormalizedMetric]:
        """Normalize metrics from a single snapshot."""
        normalized = []
        
        for metric in snapshot.extracted_metrics:
            # Skip if we couldn't map to a standard type
            if not metric.metric_type:
                logger.debug(f"Skipping unmapped metric: {metric.metric_name}")
                continue
            
            normalized_metric = NormalizedMetric(
                platform=snapshot.platform,
                metric_type=metric.metric_type,
                value=metric.value,
                unit=metric.unit,
                currency=metric.currency,
                date_range=metric.date_range or snapshot.date_range,
                confidence=metric.confidence,
                source_snapshot_id=snapshot.snapshot_id,
                original_metric_name=metric.metric_name
            )
            
            normalized.append(normalized_metric)
        
        return normalized
    
    def _validate_metrics(self, metrics: List[NormalizedMetric]) -> Dict[str, List[str]]:
        """
        Validate normalized metrics for consistency and anomalies.
        
        Returns:
            Dictionary with 'warnings' and 'errors' lists
        """
        warnings = []
        errors = []
        
        # Group metrics by platform and type
        grouped = defaultdict(list)
        for metric in metrics:
            key = (metric.platform, metric.metric_type)
            grouped[key].append(metric)
        
        # Check for duplicates
        for (platform, metric_type), metric_list in grouped.items():
            if len(metric_list) > 1:
                values = [m.value for m in metric_list]
                if len(set(values)) > 1:
                    warnings.append(
                        f"Multiple different values for {platform.value}/{metric_type.value}: {values}"
                    )
        
        # Check for anomalies
        for metric in metrics:
            # CTR should be between 0-100%
            if metric.metric_type == MetricType.CTR:
                if metric.value < 0 or metric.value > 100:
                    warnings.append(
                        f"Unusual CTR value: {metric.value}% for {metric.platform.value}"
                    )
            
            # ROAS should be positive
            if metric.metric_type == MetricType.ROAS:
                if metric.value < 0:
                    errors.append(
                        f"Negative ROAS: {metric.value} for {metric.platform.value}"
                    )
            
            # Spend should be positive
            if metric.metric_type == MetricType.SPEND:
                if metric.value < 0:
                    errors.append(
                        f"Negative spend: {metric.value} for {metric.platform.value}"
                    )
        
        return {"warnings": warnings, "errors": errors}
    
    def aggregate_metrics(
        self,
        metrics: List[NormalizedMetric],
        by: str = "platform"
    ) -> Dict[str, Dict[MetricType, float]]:
        """
        Aggregate metrics by platform or metric type.
        
        Args:
            metrics: List of normalized metrics
            by: Aggregation key ('platform' or 'metric_type')
            
        Returns:
            Aggregated metrics dictionary
        """
        aggregated = defaultdict(lambda: defaultdict(float))
        counts = defaultdict(lambda: defaultdict(int))
        
        for metric in metrics:
            if by == "platform":
                key = metric.platform.value
            elif by == "metric_type":
                key = metric.metric_type.value
            else:
                raise ValueError(f"Invalid aggregation key: {by}")
            
            # For most metrics, we sum
            if metric.metric_type in [
                MetricType.IMPRESSIONS,
                MetricType.CLICKS,
                MetricType.CONVERSIONS,
                MetricType.SPEND,
                MetricType.LIKES,
                MetricType.SHARES,
                MetricType.COMMENTS,
                MetricType.VIDEO_VIEWS,
                MetricType.REACH
            ]:
                aggregated[key][metric.metric_type] += metric.value
            
            # For rates and ratios, we average
            elif metric.metric_type in [
                MetricType.CTR,
                MetricType.CONVERSION_RATE,
                MetricType.CPC,
                MetricType.CPM,
                MetricType.CPA,
                MetricType.ROAS,
                MetricType.QUALITY_SCORE,
                MetricType.RELEVANCE_SCORE,
                MetricType.VIEWABILITY,
                MetricType.VIDEO_COMPLETION_RATE,
                MetricType.FREQUENCY
            ]:
                aggregated[key][metric.metric_type] += metric.value
                counts[key][metric.metric_type] += 1
        
        # Calculate averages for rate metrics
        for key in aggregated:
            for metric_type in counts[key]:
                if counts[key][metric_type] > 0:
                    aggregated[key][metric_type] /= counts[key][metric_type]
        
        return dict(aggregated)
    
    def calculate_derived_metrics(
        self,
        metrics: List[NormalizedMetric]
    ) -> List[NormalizedMetric]:
        """
        Calculate derived metrics from base metrics.
        
        Examples:
        - CTR = (Clicks / Impressions) * 100
        - CPC = Spend / Clicks
        - Conversion Rate = (Conversions / Clicks) * 100
        
        Args:
            metrics: List of normalized metrics
            
        Returns:
            List of derived metrics
        """
        derived = []
        
        # Group by platform
        by_platform = defaultdict(dict)
        for metric in metrics:
            by_platform[metric.platform][metric.metric_type] = metric
        
        for platform, platform_metrics in by_platform.items():
            # Calculate CTR if we have clicks and impressions
            if MetricType.CLICKS in platform_metrics and MetricType.IMPRESSIONS in platform_metrics:
                clicks = platform_metrics[MetricType.CLICKS].value
                impressions = platform_metrics[MetricType.IMPRESSIONS].value
                
                if impressions > 0 and MetricType.CTR not in platform_metrics:
                    ctr = (clicks / impressions) * 100
                    derived.append(NormalizedMetric(
                        platform=platform,
                        metric_type=MetricType.CTR,
                        value=round(ctr, 2),
                        unit="%",
                        confidence=0.95,
                        source_snapshot_id="calculated",
                        original_metric_name="Calculated CTR"
                    ))
            
            # Calculate CPC if we have spend and clicks
            if MetricType.SPEND in platform_metrics and MetricType.CLICKS in platform_metrics:
                spend = platform_metrics[MetricType.SPEND].value
                clicks = platform_metrics[MetricType.CLICKS].value
                
                if clicks > 0 and MetricType.CPC not in platform_metrics:
                    cpc = spend / clicks
                    derived.append(NormalizedMetric(
                        platform=platform,
                        metric_type=MetricType.CPC,
                        value=round(cpc, 2),
                        currency=platform_metrics[MetricType.SPEND].currency,
                        confidence=0.95,
                        source_snapshot_id="calculated",
                        original_metric_name="Calculated CPC"
                    ))
            
            # Calculate CPA if we have spend and conversions
            if MetricType.SPEND in platform_metrics and MetricType.CONVERSIONS in platform_metrics:
                spend = platform_metrics[MetricType.SPEND].value
                conversions = platform_metrics[MetricType.CONVERSIONS].value
                
                if conversions > 0 and MetricType.CPA not in platform_metrics:
                    cpa = spend / conversions
                    derived.append(NormalizedMetric(
                        platform=platform,
                        metric_type=MetricType.CPA,
                        value=round(cpa, 2),
                        currency=platform_metrics[MetricType.SPEND].currency,
                        confidence=0.95,
                        source_snapshot_id="calculated",
                        original_metric_name="Calculated CPA"
                    ))
            
            # Calculate Conversion Rate if we have conversions and clicks
            if MetricType.CONVERSIONS in platform_metrics and MetricType.CLICKS in platform_metrics:
                conversions = platform_metrics[MetricType.CONVERSIONS].value
                clicks = platform_metrics[MetricType.CLICKS].value
                
                if clicks > 0 and MetricType.CONVERSION_RATE not in platform_metrics:
                    conv_rate = (conversions / clicks) * 100
                    derived.append(NormalizedMetric(
                        platform=platform,
                        metric_type=MetricType.CONVERSION_RATE,
                        value=round(conv_rate, 2),
                        unit="%",
                        confidence=0.95,
                        source_snapshot_id="calculated",
                        original_metric_name="Calculated Conversion Rate"
                    ))
        
        logger.info(f"Calculated {len(derived)} derived metrics")
        return derived
    
    def get_platform_summary(
        self,
        metrics: List[NormalizedMetric],
        platform: PlatformType
    ) -> Dict[str, float]:
        """
        Get summary metrics for a specific platform.
        
        Args:
            metrics: List of normalized metrics
            platform: Platform to summarize
            
        Returns:
            Dictionary of metric summaries
        """
        platform_metrics = [m for m in metrics if m.platform == platform]
        
        summary = {}
        for metric in platform_metrics:
            summary[metric.metric_type.value] = metric.value
        
        return summary
