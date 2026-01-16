"""Data models package."""
from .platform import (
    PlatformType,
    MetricType,
    ChartType,
    ExtractedMetric,
    ExtractedChart,
    ExtractedTable,
    PlatformSnapshot,
    NormalizedMetric,
    PLATFORM_CONFIGS
)
from .campaign import (
    Campaign,
    CampaignObjective,
    CampaignStatus,
    DateRange,
    ChannelPerformance,
    CrossChannelInsight,
    Achievement,
    ConsolidatedReport,
    ReportTemplate,
    ReportConfig
)

__all__ = [
    "PlatformType",
    "MetricType",
    "ChartType",
    "ExtractedMetric",
    "ExtractedChart",
    "ExtractedTable",
    "PlatformSnapshot",
    "NormalizedMetric",
    "PLATFORM_CONFIGS",
    "Campaign",
    "CampaignObjective",
    "CampaignStatus",
    "DateRange",
    "ChannelPerformance",
    "CrossChannelInsight",
    "Achievement",
    "ConsolidatedReport",
    "ReportTemplate",
    "ReportConfig"
]
