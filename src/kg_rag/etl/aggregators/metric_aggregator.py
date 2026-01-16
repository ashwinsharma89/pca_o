"""
Metric Aggregator for KG-RAG

Computes calculated metrics at aggregate level.
CTR, CPC, CPM, CPA, ROAS, CVR, VTR are calculated from raw metrics.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class AggregatedMetrics:
    """Container for aggregated metrics."""
    impressions: int
    clicks: int
    spend: float
    conversions: float
    revenue: float
    reach: Optional[int] = None
    video_plays: Optional[int] = None
    video_completes: Optional[int] = None
    
    @property
    def ctr(self) -> float:
        """Click-through rate (%)."""
        if self.impressions > 0:
            return round((self.clicks / self.impressions) * 100, 4)
        return 0.0
    
    @property
    def cpc(self) -> float:
        """Cost per click."""
        if self.clicks > 0:
            return round(self.spend / self.clicks, 4)
        return 0.0
    
    @property
    def cpm(self) -> float:
        """Cost per mille (1000 impressions)."""
        if self.impressions > 0:
            return round((self.spend / self.impressions) * 1000, 4)
        return 0.0
    
    @property
    def cpa(self) -> float:
        """Cost per acquisition."""
        if self.conversions > 0:
            return round(self.spend / self.conversions, 4)
        return 0.0
    
    @property
    def roas(self) -> float:
        """Return on ad spend."""
        if self.spend > 0:
            return round(self.revenue / self.spend, 4)
        return 0.0
    
    @property
    def cvr(self) -> float:
        """Conversion rate (%)."""
        if self.clicks > 0:
            return round((self.conversions / self.clicks) * 100, 4)
        return 0.0
    
    @property
    def vtr(self) -> float:
        """Video through rate (%)."""
        if self.video_plays and self.video_plays > 0 and self.video_completes:
            return round((self.video_completes / self.video_plays) * 100, 4)
        return 0.0
    
    @property
    def frequency(self) -> float:
        """Average frequency."""
        if self.reach and self.reach > 0:
            return round(self.impressions / self.reach, 2)
        return 0.0
    
    def to_dict(self, include_calculated: bool = True) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Args:
            include_calculated: Include calculated metrics
            
        Returns:
            Dict with raw and optionally calculated metrics
        """
        result = {
            "impressions": self.impressions,
            "clicks": self.clicks,
            "spend": self.spend,
            "conversions": self.conversions,
            "revenue": self.revenue,
        }
        
        if self.reach is not None:
            result["reach"] = self.reach
        if self.video_plays is not None:
            result["video_plays"] = self.video_plays
        if self.video_completes is not None:
            result["video_completes"] = self.video_completes
        
        if include_calculated:
            result.update({
                "ctr": self.ctr,
                "cpc": self.cpc,
                "cpm": self.cpm,
                "cpa": self.cpa,
                "roas": self.roas,
                "cvr": self.cvr,
            })
            
            if self.video_plays:
                result["vtr"] = self.vtr
            if self.reach:
                result["frequency"] = self.frequency
        
        return result


class MetricAggregator:
    """
    Compute calculated metrics at aggregate level.
    
    All calculated metrics (CTR, CPC, CPM, CPA, ROAS, VTR, CVR) are
    computed from raw aggregated metrics, NOT stored per-row.
    
    Usage:
        aggregator = MetricAggregator()
        
        # From raw totals
        result = aggregator.calculate({
            'impressions': 10000,
            'clicks': 500,
            'spend': 1000.0,
            'conversions': 50,
            'revenue': 5000.0
        }, calculations=['ctr', 'cpc', 'roas'])
        
        # From metric list
        metrics = [{'impressions': 1000, 'clicks': 50, ...}, ...]
        agg = aggregator.aggregate_metrics(metrics)
        print(agg.ctr, agg.roas)
    """
    
    FORMULAS = {
        'ctr': lambda m: (m['clicks'] / m['impressions'] * 100) if m['impressions'] > 0 else 0,
        'cpc': lambda m: (m['spend'] / m['clicks']) if m['clicks'] > 0 else 0,
        'cpm': lambda m: (m['spend'] / m['impressions'] * 1000) if m['impressions'] > 0 else 0,
        'cpa': lambda m: (m['spend'] / m['conversions']) if m['conversions'] > 0 else 0,
        'roas': lambda m: (m['revenue'] / m['spend']) if m['spend'] > 0 else 0,
        'cvr': lambda m: (m['conversions'] / m['clicks'] * 100) if m['clicks'] > 0 else 0,
        'vtr': lambda m: (m.get('video_completes', 0) / m.get('video_plays', 1) * 100) if m.get('video_plays', 0) > 0 else 0,
        'frequency': lambda m: (m['impressions'] / m.get('reach', 1)) if m.get('reach', 0) > 0 else 0,
    }
    
    @classmethod
    def calculate(
        cls,
        metrics: Dict[str, Any],
        calculations: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Calculate derived metrics from raw aggregates.
        
        Args:
            metrics: Dict with raw metrics (impressions, clicks, spend, etc.)
            calculations: List of metrics to calculate (default: all)
            
        Returns:
            Dict with calculated metrics
        """
        if calculations is None:
            calculations = ['ctr', 'cpc', 'cpm', 'cpa', 'roas', 'cvr']
        
        # Ensure required fields
        m = {
            'impressions': metrics.get('impressions', 0) or 0,
            'clicks': metrics.get('clicks', 0) or 0,
            'spend': metrics.get('spend', 0) or 0,
            'conversions': metrics.get('conversions', 0) or 0,
            'revenue': metrics.get('revenue', 0) or 0,
            'reach': metrics.get('reach'),
            'video_plays': metrics.get('video_plays'),
            'video_completes': metrics.get('video_completes'),
        }
        
        results = {}
        for calc in calculations:
            if calc in cls.FORMULAS:
                try:
                    results[calc] = round(cls.FORMULAS[calc](m), 4)
                except (ZeroDivisionError, TypeError):
                    results[calc] = 0.0
        
        return results
    
    @classmethod
    def aggregate_metrics(
        cls,
        metrics: List[Dict[str, Any]]
    ) -> AggregatedMetrics:
        """
        Aggregate a list of metric records.
        
        Args:
            metrics: List of daily metric records
            
        Returns:
            AggregatedMetrics object with calculated properties
        """
        totals = {
            'impressions': 0,
            'clicks': 0,
            'spend': 0.0,
            'conversions': 0.0,
            'revenue': 0.0,
            'reach': None,
            'video_plays': None,
            'video_completes': None,
        }
        
        has_reach = False
        has_video = False
        
        for m in metrics:
            totals['impressions'] += m.get('impressions', 0) or 0
            totals['clicks'] += m.get('clicks', 0) or 0
            totals['spend'] += m.get('spend', 0) or 0
            totals['conversions'] += m.get('conversions', 0) or 0
            totals['revenue'] += m.get('revenue', 0) or 0
            
            if m.get('reach'):
                if totals['reach'] is None:
                    totals['reach'] = 0
                totals['reach'] = max(totals['reach'], m['reach'])  # MAX for reach
                has_reach = True
            
            if m.get('video_plays'):
                if totals['video_plays'] is None:
                    totals['video_plays'] = 0
                totals['video_plays'] += m['video_plays']
                has_video = True
            
            if m.get('video_completes'):
                if totals['video_completes'] is None:
                    totals['video_completes'] = 0
                totals['video_completes'] += m['video_completes']
        
        return AggregatedMetrics(
            impressions=totals['impressions'],
            clicks=totals['clicks'],
            spend=round(totals['spend'], 2),
            conversions=round(totals['conversions'], 2),
            revenue=round(totals['revenue'], 2),
            reach=totals['reach'] if has_reach else None,
            video_plays=totals['video_plays'] if has_video else None,
            video_completes=totals['video_completes'] if has_video else None,
        )
    
    @classmethod
    def aggregate_by_dimension(
        cls,
        metrics: List[Dict[str, Any]],
        dimension: str
    ) -> Dict[str, AggregatedMetrics]:
        """
        Aggregate metrics grouped by a dimension.
        
        Args:
            metrics: List of metric records
            dimension: Field to group by (e.g., 'platform_id', 'device')
            
        Returns:
            Dict mapping dimension value to aggregated metrics
        """
        groups: Dict[str, List[Dict[str, Any]]] = {}
        
        for m in metrics:
            key = str(m.get(dimension, 'unknown'))
            if key not in groups:
                groups[key] = []
            groups[key].append(m)
        
        return {
            key: cls.aggregate_metrics(records)
            for key, records in groups.items()
        }
