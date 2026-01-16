"""
Data models for advertising platform metrics and configurations.
"""
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field


class PlatformType(str, Enum):
    """Supported advertising platforms."""
    GOOGLE_ADS = "google_ads"
    CM360 = "cm360"
    DV360 = "dv360"
    META_ADS = "meta_ads"
    SNAPCHAT_ADS = "snapchat_ads"
    LINKEDIN_ADS = "linkedin_ads"


class MetricType(str, Enum):
    """Common metric types across platforms."""
    # Performance Metrics
    IMPRESSIONS = "impressions"
    CLICKS = "clicks"
    CTR = "ctr"
    CONVERSIONS = "conversions"
    CONVERSION_RATE = "conversion_rate"
    
    # Cost Metrics
    SPEND = "spend"
    CPC = "cpc"
    CPM = "cpm"
    CPA = "cpa"
    ROAS = "roas"
    
    # Engagement Metrics
    LIKES = "likes"
    SHARES = "shares"
    COMMENTS = "comments"
    VIDEO_VIEWS = "video_views"
    VIDEO_COMPLETION_RATE = "video_completion_rate"
    
    # Reach Metrics
    REACH = "reach"
    FREQUENCY = "frequency"
    UNIQUE_USERS = "unique_users"
    
    # Quality Metrics
    QUALITY_SCORE = "quality_score"
    RELEVANCE_SCORE = "relevance_score"
    VIEWABILITY = "viewability"


class ChartType(str, Enum):
    """Types of charts detected in dashboards."""
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    PIE_CHART = "pie_chart"
    AREA_CHART = "area_chart"
    SCATTER_PLOT = "scatter_plot"
    HEATMAP = "heatmap"
    TABLE = "table"
    FUNNEL = "funnel"


class ExtractedMetric(BaseModel):
    """A single metric extracted from a dashboard."""
    metric_name: str = Field(..., description="Name of the metric")
    metric_type: Optional[MetricType] = Field(None, description="Standardized metric type")
    value: float = Field(..., description="Numeric value")
    unit: Optional[str] = Field(None, description="Unit (%, $, etc.)")
    currency: Optional[str] = Field(None, description="Currency code (USD, EUR, etc.)")
    date_range: Optional[str] = Field(None, description="Date range for this metric")
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Extraction confidence")
    source_location: Optional[Dict[str, int]] = Field(
        None,
        description="Bounding box in image (x, y, width, height)"
    )


class ExtractedChart(BaseModel):
    """A chart extracted from a dashboard."""
    chart_type: ChartType = Field(..., description="Type of chart")
    title: Optional[str] = Field(None, description="Chart title")
    data_points: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Extracted data points"
    )
    x_axis_label: Optional[str] = Field(None, description="X-axis label")
    y_axis_label: Optional[str] = Field(None, description="Y-axis label")
    legend: Optional[List[str]] = Field(None, description="Legend items")
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Extraction confidence")
    source_location: Optional[Dict[str, int]] = Field(
        None,
        description="Bounding box in image"
    )


class ExtractedTable(BaseModel):
    """A table extracted from a dashboard."""
    headers: List[str] = Field(..., description="Column headers")
    rows: List[List[str]] = Field(..., description="Table rows")
    title: Optional[str] = Field(None, description="Table title")
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Extraction confidence")


class PlatformSnapshot(BaseModel):
    """A dashboard snapshot from a specific platform."""
    snapshot_id: str = Field(..., description="Unique snapshot ID")
    platform: PlatformType = Field(..., description="Advertising platform")
    campaign_id: str = Field(..., description="Parent campaign ID")
    file_path: str = Field(..., description="Path to snapshot image")
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Extracted Data
    detected_platform: Optional[PlatformType] = Field(
        None,
        description="Auto-detected platform from visual branding"
    )
    extracted_metrics: List[ExtractedMetric] = Field(
        default_factory=list,
        description="Metrics extracted from snapshot"
    )
    extracted_charts: List[ExtractedChart] = Field(
        default_factory=list,
        description="Charts extracted from snapshot"
    )
    extracted_tables: List[ExtractedTable] = Field(
        default_factory=list,
        description="Tables extracted from snapshot"
    )
    
    # Metadata
    date_range: Optional[str] = Field(None, description="Campaign date range from snapshot")
    campaign_name: Optional[str] = Field(None, description="Campaign name from snapshot")
    processing_status: str = Field("pending", description="Processing status")
    processing_error: Optional[str] = Field(None, description="Error message if failed")
    
    # Vision Model Output
    raw_vision_output: Optional[str] = Field(None, description="Raw VLM response")
    vision_model_used: Optional[str] = Field(None, description="VLM model used")


class PlatformConfig(BaseModel):
    """Configuration for a specific platform."""
    platform: PlatformType
    display_name: str
    color: str = Field(..., description="Brand color for visualizations")
    icon: Optional[str] = Field(None, description="Icon/logo path")
    
    # Platform-specific metric mappings
    metric_mappings: Dict[str, MetricType] = Field(
        default_factory=dict,
        description="Map platform-specific metric names to standard types"
    )
    
    # Expected metrics for this platform
    expected_metrics: List[MetricType] = Field(
        default_factory=list,
        description="Metrics typically available on this platform"
    )


# Platform configurations
PLATFORM_CONFIGS = {
    PlatformType.GOOGLE_ADS: PlatformConfig(
        platform=PlatformType.GOOGLE_ADS,
        display_name="Google Ads",
        color="#4285F4",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.CLICKS,
            MetricType.CTR,
            MetricType.CONVERSIONS,
            MetricType.SPEND,
            MetricType.CPC,
            MetricType.CPA,
            MetricType.QUALITY_SCORE,
        ],
        metric_mappings={
            "impr.": MetricType.IMPRESSIONS,
            "cost": MetricType.SPEND,
            "conv.": MetricType.CONVERSIONS,
            "conv. rate": MetricType.CONVERSION_RATE,
        }
    ),
    PlatformType.CM360: PlatformConfig(
        platform=PlatformType.CM360,
        display_name="Campaign Manager 360",
        color="#EA4335",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.CLICKS,
            MetricType.CTR,
            MetricType.REACH,
            MetricType.FREQUENCY,
            MetricType.VIEWABILITY,
        ]
    ),
    PlatformType.DV360: PlatformConfig(
        platform=PlatformType.DV360,
        display_name="Display & Video 360",
        color="#FBBC04",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.CLICKS,
            MetricType.CTR,
            MetricType.SPEND,
            MetricType.CPM,
            MetricType.VIEWABILITY,
            MetricType.VIDEO_COMPLETION_RATE,
        ]
    ),
    PlatformType.META_ADS: PlatformConfig(
        platform=PlatformType.META_ADS,
        display_name="Meta Ads",
        color="#1877F2",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.REACH,
            MetricType.CLICKS,
            MetricType.CTR,
            MetricType.SPEND,
            MetricType.CPC,
            MetricType.CPA,
            MetricType.ROAS,
            MetricType.LIKES,
            MetricType.SHARES,
            MetricType.COMMENTS,
        ],
        metric_mappings={
            "amount spent": MetricType.SPEND,
            "link clicks": MetricType.CLICKS,
            "cost per result": MetricType.CPA,
        }
    ),
    PlatformType.SNAPCHAT_ADS: PlatformConfig(
        platform=PlatformType.SNAPCHAT_ADS,
        display_name="Snapchat Ads",
        color="#FFFC00",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.REACH,
            MetricType.CLICKS,
            MetricType.SPEND,
            MetricType.VIDEO_VIEWS,
            MetricType.VIDEO_COMPLETION_RATE,
        ]
    ),
    PlatformType.LINKEDIN_ADS: PlatformConfig(
        platform=PlatformType.LINKEDIN_ADS,
        display_name="LinkedIn Ads",
        color="#0A66C2",
        expected_metrics=[
            MetricType.IMPRESSIONS,
            MetricType.CLICKS,
            MetricType.CTR,
            MetricType.CONVERSIONS,
            MetricType.SPEND,
            MetricType.CPC,
            MetricType.CPA,
            MetricType.LIKES,
            MetricType.SHARES,
            MetricType.COMMENTS,
        ]
    ),
}


class NormalizedMetric(BaseModel):
    """Normalized metric across all platforms."""
    platform: PlatformType
    metric_type: MetricType
    value: float
    unit: Optional[str] = None
    currency: Optional[str] = None
    date_range: Optional[str] = None
    confidence: float = 1.0
    
    # Source tracking
    source_snapshot_id: str
    original_metric_name: str
