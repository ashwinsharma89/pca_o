"""
Response Models for API Validation

Ensures consistent response structure and prevents data leakage.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime


# ============================================================================
# STANDARD RESPONSE WRAPPERS
# ============================================================================

class SuccessResponse(BaseModel):
    """Standard success response"""
    success: bool = True
    message: str
    data: Optional[Dict[str, Any]] = None


class ErrorResponse(BaseModel):
    """Standard error response"""
    success: bool = False
    error: Dict[str, Any] = Field(..., description="Error details")
    path: Optional[str] = None
    method: Optional[str] = None


class PaginatedResponse(BaseModel):
    """Paginated response wrapper"""
    success: bool = True
    data: List[Dict[str, Any]]
    total: int = Field(..., description="Total number of items")
    limit: int = Field(..., description="Items per page")
    offset: int = Field(..., description="Current offset")
    has_more: bool = Field(..., description="Whether more items exist")


# ============================================================================
# CAMPAIGN RESPONSES
# ============================================================================

class CampaignUploadResponse(BaseModel):
    """Response for campaign data upload"""
    success: bool = True
    message: str
    rows_imported: int = Field(..., ge=0, description="Number of rows imported")
    file_size_mb: float = Field(..., ge=0, description="File size in MB")
    processing_time_seconds: float = Field(..., ge=0, description="Processing time")
    parquet_path: str = Field(..., description="Path to stored Parquet file")


class ChatResponse(BaseModel):
    """Response for chat/Q&A queries"""
    success: bool
    query: Optional[str] = None
    sql: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    data: Optional[List[Dict[str, Any]]] = None
    summary: Optional[str] = None
    chart: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    knowledge: Optional[Dict[str, Any]] = None
    retrieved: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


class AnalysisResponse(BaseModel):
    """Response for campaign analysis"""
    success: bool = True
    insights: List[Dict[str, Any]] = Field(default_factory=list)
    recommendations: List[Dict[str, Any]] = Field(default_factory=list)
    summary: Optional[str] = None
    benchmarks: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# VISUALIZATION RESPONSES
# ============================================================================

class ChartDataPoint(BaseModel):
    """Single data point for charts"""
    label: str
    value: Union[float, int]
    category: Optional[str] = None


class VisualizationResponse(BaseModel):
    """Response for visualization endpoints"""
    success: bool = True
    chart_type: str = Field(..., description="Chart type (bar, line, pie, etc.)")
    metric: str = Field(..., description="Primary metric displayed")
    data: List[Dict[str, Any]] = Field(..., description="Chart data points")
    title: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DashboardStatsResponse(BaseModel):
    """Response for dashboard statistics"""
    success: bool = True
    total_spend: float = Field(..., ge=0)
    total_impressions: int = Field(..., ge=0)
    total_clicks: int = Field(..., ge=0)
    total_conversions: int = Field(..., ge=0)
    avg_ctr: float = Field(..., ge=0)
    avg_cpc: float = Field(..., ge=0)
    avg_cpm: float = Field(..., ge=0)
    roas: Optional[float] = None
    period: Optional[Dict[str, str]] = None


class FilterOptionsResponse(BaseModel):
    """Response for filter options"""
    success: bool = True
    platforms: List[str] = Field(default_factory=list)
    channels: List[str] = Field(default_factory=list)
    funnels: List[str] = Field(default_factory=list)
    objectives: List[str] = Field(default_factory=list)
    regions: List[str] = Field(default_factory=list)
    devices: List[str] = Field(default_factory=list)
    ad_types: List[str] = Field(default_factory=list)
    placements: List[str] = Field(default_factory=list)
    date_range: Optional[Dict[str, str]] = None


class FunnelStatsResponse(BaseModel):
    """Response for funnel statistics"""
    success: bool = True
    stages: List[Dict[str, Any]] = Field(default_factory=list)
    conversion_rates: Dict[str, float] = Field(default_factory=dict)
    drop_off_points: Optional[List[Dict[str, Any]]] = None


class AudienceStatsResponse(BaseModel):
    """Response for audience statistics"""
    success: bool = True
    demographics: Dict[str, Any] = Field(default_factory=dict)
    segments: List[Dict[str, Any]] = Field(default_factory=list)
    top_performing: Optional[List[Dict[str, Any]]] = None



class GlobalMetricsResponse(BaseModel):
    """Response model for global metrics aggregation"""
    total_spend: float = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_conversions: int = 0
    avg_ctr: float = 0
    avg_cpc: float = 0
    avg_cpa: float = 0
    conversion_rate: float = 0


class VisualizationSeries(BaseModel):
    """Generic model for time-series or categorical data points"""
    name: Optional[str] = None
    date: Optional[str] = None
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    revenue: float = 0
    reach: int = 0
    ctr: float = 0
    cpc: float = 0
    cpa: float = 0
    cpm: float = 0
    roas: float = 0
    
    # Allow extra fields for dynamic dimensions if needed
    model_config = {"extra": "allow"}


class VisualizationsResponse(BaseModel):
    """Response model for /visualizations endpoint"""
    trend: List[VisualizationSeries] = Field(default_factory=list)
    device: List[VisualizationSeries] = Field(default_factory=list)
    platform: List[VisualizationSeries] = Field(default_factory=list)
    channel: List[VisualizationSeries] = Field(default_factory=list)
    region: List[VisualizationSeries] = Field(default_factory=list)
    audience: List[VisualizationSeries] = Field(default_factory=list)
    age: List[VisualizationSeries] = Field(default_factory=list)
    ad_type: List[VisualizationSeries] = Field(default_factory=list)
    objective: List[VisualizationSeries] = Field(default_factory=list)
    targeting: List[VisualizationSeries] = Field(default_factory=list)


class DashboardGroupsResponse(BaseModel):
    """Summary groups for dashboard stats"""
    pass  # Allow arbitrary structure for now if complex, or define strict
    model_config = {"extra": "allow"}

class DashboardStatsResponse(BaseModel):
    """Response for /dashboard-stats endpoint"""
    summary_groups: Dict[str, Any] = Field(default_factory=dict)
    monthly_performance: List[VisualizationSeries] = Field(default_factory=list)
    platform_performance: List[VisualizationSeries] = Field(default_factory=list)


class RegressionResult(BaseModel):
    """Single regression result"""
    metric: str
    coefficient: float
    p_value: float
    confidence_interval: Optional[List[float]] = None
    significant: bool = False


class RegressionResponse(BaseModel):
    """Response for regression analysis"""
    success: bool = True
    model_type: str = Field(default="linear", description="Regression model type")
    r_squared: float = Field(..., ge=0, le=1, description="R-squared value")
    adjusted_r_squared: Optional[float] = None
    coefficients: List[RegressionResult] = Field(default_factory=list)
    predictions: Optional[List[Dict[str, Any]]] = None
    shap_summary: Optional[Dict[str, Any]] = None
    recommendations: Optional[List[Dict[str, Any]]] = None


class AnalyticsSnapshotResponse(BaseModel):
    """Response for analytics snapshot"""
    success: bool = True
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    summary: Dict[str, Any] = Field(default_factory=dict)
    trends: List[Dict[str, Any]] = Field(default_factory=list)
    anomalies: Optional[List[Dict[str, Any]]] = None
    insights: Optional[List[str]] = None


class KPIComparisonResponse(BaseModel):
    """Response for KPI comparison"""
    success: bool = True
    kpis: List[str] = Field(..., description="KPIs compared")
    dimension: str = Field(..., description="Comparison dimension")
    data: List[Dict[str, Any]] = Field(default_factory=list)
    summary: Optional[Dict[str, Any]] = None
    chart_config: Optional[Dict[str, Any]] = None


class ChartDataResponse(BaseModel):
    """Response for chart data endpoint"""
    success: bool = True
    chart_type: str
    labels: List[str] = Field(default_factory=list)
    datasets: List[Dict[str, Any]] = Field(default_factory=list)
    options: Optional[Dict[str, Any]] = None


class SuggestedQuestionsResponse(BaseModel):
    """Response for suggested questions"""
    success: bool = True
    questions: List[str] = Field(default_factory=list, max_length=20)
    categories: Optional[Dict[str, List[str]]] = None


class CampaignDetailResponse(BaseModel):
    """Response for single campaign details"""
    success: bool = True
    campaign: Dict[str, Any] = Field(..., description="Campaign details")
    metrics: Optional[Dict[str, Any]] = None
    insights: Optional[List[str]] = None


class CampaignListResponse(BaseModel):
    """Response for campaign list"""
    success: bool = True
    campaigns: List[Dict[str, Any]] = Field(default_factory=list)
    total: int = Field(..., ge=0)
    page: int = Field(..., ge=1)
    per_page: int = Field(..., ge=1)
    has_next: bool = False


# ============================================================================
# AUTH RESPONSES
# ============================================================================

class LoginResponse(BaseModel):
    """Response for login"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(..., description="Token expiry in seconds")
    user: Dict[str, Any] = Field(..., description="User information")
    requires_mfa: Optional[bool] = False


class MFASetupResponse(BaseModel):
    """Response for MFA setup"""
    secret: str = Field(..., description="MFA secret key")
    qr_code: str = Field(..., description="QR code for authenticator app")
    backup_codes: List[str] = Field(..., description="Backup recovery codes")


# ============================================================================
# WEBHOOK RESPONSES
# ============================================================================

class WebhookResponse(BaseModel):
    """Response for webhook operations"""
    id: str
    url: str
    events: List[str]
    description: Optional[str]
    is_active: bool
    created_at: datetime
    last_triggered_at: Optional[datetime]
    success_count: int = Field(..., ge=0)
    failure_count: int = Field(..., ge=0)


# ============================================================================
# VALIDATION ERROR RESPONSE
# ============================================================================

class ValidationErrorDetail(BaseModel):
    """Detail for a single validation error"""
    loc: List[str] = Field(..., description="Location of error (field path)")
    msg: str = Field(..., description="Error message")
    type: str = Field(..., description="Error type")


class ValidationErrorResponse(BaseModel):
    """Response for validation errors"""
    success: bool = False
    error: Dict[str, Any] = Field(
        ...,
        description="Validation error details",
        examples=[{
            "code": "VALIDATION_ERROR",
            "message": "Request validation failed",
            "details": []
        }]
    )
    detail: List[ValidationErrorDetail] = Field(..., description="Detailed validation errors")


# ============================================================================
# HEALTH CHECK RESPONSES
# ============================================================================

class HealthCheckResponse(BaseModel):
    """Response for health check endpoint"""
    status: str = Field(..., description="Health status")
    version: str = Field(..., description="API version")
    uptime_seconds: Optional[float] = None
    components: Optional[Dict[str, str]] = None


class DetailedHealthResponse(BaseModel):
    """Response for detailed health check"""
    status: str
    version: str
    components: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    metrics: Optional[Dict[str, Any]] = None

