"""
Data models for campaign analysis and reporting.
"""
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime, date
from pydantic import BaseModel, Field
from .platform import PlatformType, PlatformSnapshot, NormalizedMetric


class BusinessModel(str, Enum):
    """Business model types."""
    B2B = "B2B"
    B2C = "B2C"
    B2B2C = "B2B2C"


class TargetAudienceLevel(str, Enum):
    """Target audience seniority levels (primarily for B2B)."""
    C_SUITE = "C-suite"
    VP_DIRECTOR = "VP/Director"
    MANAGER = "Manager"
    INDIVIDUAL_CONTRIBUTOR = "Individual Contributor"
    MIXED = "Mixed"
    CONSUMER = "Consumer"  # For B2C


class CampaignObjective(str, Enum):
    """Campaign objectives."""
    AWARENESS = "awareness"
    CONSIDERATION = "consideration"
    CONVERSION = "conversion"
    ENGAGEMENT = "engagement"
    TRAFFIC = "traffic"
    LEAD_GENERATION = "lead_generation"
    APP_INSTALLS = "app_installs"


class CampaignStatus(str, Enum):
    """Campaign analysis status."""
    CREATED = "created"
    UPLOADING = "uploading"
    PROCESSING = "processing"
    EXTRACTING = "extracting"
    REASONING = "reasoning"
    GENERATING_REPORT = "generating_report"
    COMPLETED = "completed"
    FAILED = "failed"


class DateRange(BaseModel):
    """Campaign date range."""
    start: date = Field(..., description="Start date")
    end: date = Field(..., description="End date")
    
    @property
    def duration_days(self) -> int:
        """Calculate duration in days."""
        return (self.end - self.start).days + 1


class CampaignContext(BaseModel):
    """Business context for campaign analysis."""
    business_model: BusinessModel = Field(..., description="Business model type (B2B, B2C, B2B2C)")
    industry_vertical: str = Field(..., description="Industry vertical (e.g., SaaS, E-commerce, Healthcare)")
    
    # B2B-Specific Fields
    sales_cycle_length: Optional[int] = Field(None, description="Average sales cycle in days")
    average_deal_size: Optional[float] = Field(None, description="Average deal size in dollars")
    target_audience_level: Optional[TargetAudienceLevel] = Field(None, description="Target audience seniority level")
    
    # B2C-Specific Fields
    average_order_value: Optional[float] = Field(None, description="Average order value (B2C)")
    purchase_frequency: Optional[str] = Field(None, description="Purchase frequency (e.g., 'weekly', 'monthly', 'yearly')")
    
    # Common Fields
    customer_lifetime_value: Optional[float] = Field(None, description="Customer lifetime value")
    target_cac: Optional[float] = Field(None, description="Target customer acquisition cost")
    geographic_focus: Optional[List[str]] = Field(default_factory=list, description="Primary geographic markets")
    
    # Additional Context
    competitive_intensity: Optional[str] = Field(None, description="Competitive intensity (low, medium, high)")
    seasonality_factor: Optional[str] = Field(None, description="Seasonality impact (none, low, medium, high)")
    brand_maturity: Optional[str] = Field(None, description="Brand maturity (startup, growth, mature, enterprise)")
    
    def is_b2b(self) -> bool:
        """Check if campaign is B2B."""
        return self.business_model in [BusinessModel.B2B, BusinessModel.B2B2C]
    
    def is_b2c(self) -> bool:
        """Check if campaign is B2C."""
        return self.business_model in [BusinessModel.B2C, BusinessModel.B2B2C]
    
    def get_context_summary(self) -> str:
        """Get a human-readable summary of the campaign context."""
        summary_parts = [
            f"{self.business_model.value} {self.industry_vertical}"
        ]
        
        if self.is_b2b() and self.sales_cycle_length:
            summary_parts.append(f"{self.sales_cycle_length}-day sales cycle")
        
        if self.average_deal_size:
            summary_parts.append(f"${self.average_deal_size:,.0f} avg deal")
        elif self.average_order_value:
            summary_parts.append(f"${self.average_order_value:,.0f} AOV")
        
        if self.target_audience_level:
            summary_parts.append(f"targeting {self.target_audience_level.value}")
        
        return " | ".join(summary_parts)


class Campaign(BaseModel):
    """Campaign analysis request."""
    campaign_id: str = Field(..., description="Unique campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    objectives: List[CampaignObjective] = Field(..., description="Campaign objectives")
    date_range: DateRange = Field(..., description="Campaign date range")
    
    # Business Context
    campaign_context: Optional[CampaignContext] = Field(None, description="Business context for analysis")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    status: CampaignStatus = Field(default=CampaignStatus.CREATED)
    
    # Snapshots
    snapshots: List[PlatformSnapshot] = Field(
        default_factory=list,
        description="Uploaded dashboard snapshots"
    )
    
    # Extracted Data
    normalized_metrics: List[NormalizedMetric] = Field(
        default_factory=list,
        description="Normalized metrics across all platforms"
    )
    
    # Analysis Results
    insights: Optional[Dict[str, Any]] = Field(None, description="AI-generated insights")
    achievements: Optional[List[Dict[str, Any]]] = Field(None, description="Key achievements")
    recommendations: Optional[List[str]] = Field(None, description="Recommendations")
    
    # Report
    report_path: Optional[str] = Field(None, description="Path to generated report")
    report_generated_at: Optional[datetime] = Field(None, description="Report generation timestamp")
    
    # Processing
    processing_error: Optional[str] = Field(None, description="Error message if failed")
    processing_logs: List[str] = Field(default_factory=list, description="Processing logs")


class ChannelPerformance(BaseModel):
    """Performance analysis for a single channel."""
    platform: PlatformType
    platform_name: str
    
    # Key Metrics
    total_impressions: Optional[float] = None
    total_clicks: Optional[float] = None
    total_conversions: Optional[float] = None
    total_spend: Optional[float] = None
    
    # Calculated Metrics
    ctr: Optional[float] = Field(None, description="Click-through rate")
    cpc: Optional[float] = Field(None, description="Cost per click")
    cpa: Optional[float] = Field(None, description="Cost per acquisition")
    roas: Optional[float] = Field(None, description="Return on ad spend")
    conversion_rate: Optional[float] = Field(None, description="Conversion rate")
    
    # Rankings
    performance_score: Optional[float] = Field(
        None,
        ge=0.0,
        le=100.0,
        description="Overall performance score (0-100)"
    )
    efficiency_rank: Optional[int] = Field(None, description="Efficiency ranking")
    
    # Insights
    strengths: List[str] = Field(default_factory=list, description="Channel strengths")
    weaknesses: List[str] = Field(default_factory=list, description="Channel weaknesses")
    opportunities: List[str] = Field(default_factory=list, description="Opportunities")
    
    # Top Performers
    top_creative: Optional[str] = Field(None, description="Best performing creative")
    top_audience: Optional[str] = Field(None, description="Best performing audience")
    top_placement: Optional[str] = Field(None, description="Best performing placement")


class CrossChannelInsight(BaseModel):
    """Cross-channel analysis insight."""
    insight_type: str = Field(..., description="Type of insight (synergy, attribution, etc.)")
    title: str = Field(..., description="Insight title")
    description: str = Field(..., description="Detailed description")
    affected_platforms: List[PlatformType] = Field(..., description="Platforms involved")
    impact_score: float = Field(..., ge=0.0, le=10.0, description="Impact score (0-10)")
    supporting_data: Optional[Dict[str, Any]] = Field(None, description="Supporting data")


class Achievement(BaseModel):
    """Campaign achievement."""
    achievement_type: str = Field(..., description="Type of achievement")
    title: str = Field(..., description="Achievement title")
    description: str = Field(..., description="Detailed description")
    metric_value: Optional[float] = Field(None, description="Associated metric value")
    metric_name: Optional[str] = Field(None, description="Associated metric name")
    platform: Optional[PlatformType] = Field(None, description="Platform (if channel-specific)")
    impact_level: str = Field(..., description="Impact level (high, medium, low)")
    visual_data: Optional[Dict[str, Any]] = Field(None, description="Data for visualization")


class ConsolidatedReport(BaseModel):
    """Consolidated campaign report data."""
    campaign: Campaign
    
    # Summary
    executive_summary: str = Field(..., description="Executive summary")
    total_spend: float = Field(..., description="Total spend across all channels")
    total_conversions: float = Field(..., description="Total conversions")
    overall_roas: Optional[float] = Field(None, description="Overall ROAS")
    
    # Channel Performance
    channel_performances: List[ChannelPerformance] = Field(
        default_factory=list,
        description="Performance by channel"
    )
    
    # Cross-Channel Analysis
    cross_channel_insights: List[CrossChannelInsight] = Field(
        default_factory=list,
        description="Cross-channel insights"
    )
    
    # Achievements
    achievements: List[Achievement] = Field(
        default_factory=list,
        description="Key achievements"
    )
    
    # Recommendations
    recommendations: List[str] = Field(
        default_factory=list,
        description="Strategic recommendations"
    )
    
    # Attribution
    attribution_model: Optional[str] = Field(None, description="Attribution model used")
    attribution_data: Optional[Dict[str, Any]] = Field(None, description="Attribution data")
    
    # Visualizations
    visualizations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Generated visualizations metadata"
    )


class ReportTemplate(str, Enum):
    """Available report templates."""
    CORPORATE = "corporate"
    CREATIVE = "creative"
    MINIMAL = "minimal"
    DETAILED = "detailed"


class ReportConfig(BaseModel):
    """Report generation configuration."""
    template: ReportTemplate = Field(default=ReportTemplate.CORPORATE)
    include_raw_data: bool = Field(default=False, description="Include raw data appendix")
    include_methodology: bool = Field(default=True, description="Include methodology section")
    brand_color: Optional[str] = Field(None, description="Override brand color")
    company_name: Optional[str] = Field(None, description="Override company name")
    company_logo_path: Optional[str] = Field(None, description="Override company logo")
    
    # Content Options
    include_executive_summary: bool = Field(default=True)
    include_channel_breakdown: bool = Field(default=True)
    include_cross_channel_analysis: bool = Field(default=True)
    include_achievements: bool = Field(default=True)
    include_recommendations: bool = Field(default=True)
    include_visualizations: bool = Field(default=True)
    
    # Format Options
    output_format: str = Field(default="pptx", description="Output format (pptx, pdf)")
    page_size: str = Field(default="16:9", description="Slide aspect ratio")
