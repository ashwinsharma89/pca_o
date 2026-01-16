"""
Pydantic schemas for agent output validation.

Ensures all agent outputs are properly structured and validated
to prevent malformed data from breaking downstream components.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime


class ConfidenceLevel(str, Enum):
    """Confidence level for insights and recommendations"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class PriorityLevel(int, Enum):
    """Priority level for recommendations (1=highest, 5=lowest)"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    OPTIONAL = 5


class MetricType(str, Enum):
    """Types of metrics that can be extracted"""
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    COUNT = "count"
    RATE = "rate"
    DURATION = "duration"


class PatternType(str, Enum):
    """Types of patterns that can be detected"""
    TREND = "trend"
    ANOMALY = "anomaly"
    SEASONALITY = "seasonality"
    CREATIVE_FATIGUE = "creative_fatigue"
    AUDIENCE_SATURATION = "audience_saturation"
    DAY_PARTING = "day_parting"
    BUDGET_PACING = "budget_pacing"
    PERFORMANCE_CLUSTER = "performance_cluster"


class AgentInsight(BaseModel):
    """Validated insight output from an agent"""
    text: str = Field(..., min_length=10, max_length=1000, description="Insight text")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0-1")
    confidence_level: Optional[ConfidenceLevel] = None
    supporting_data: Optional[Dict[str, Any]] = Field(default=None, description="Data supporting this insight")
    pattern_type: Optional[PatternType] = Field(default=None, description="Type of pattern detected")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @model_validator(mode='after')
    def set_confidence_level(self):
        """Auto-set confidence level based on confidence score"""
        if self.confidence_level is None:
            if self.confidence >= 0.8:
                self.confidence_level = ConfidenceLevel.HIGH
            elif self.confidence >= 0.5:
                self.confidence_level = ConfidenceLevel.MEDIUM
            else:
                self.confidence_level = ConfidenceLevel.LOW
        return self
    
    class Config:
        use_enum_values = True


class AgentRecommendation(BaseModel):
    """Validated recommendation output from an agent"""
    action: str = Field(..., min_length=10, max_length=500, description="Recommended action")
    rationale: str = Field(..., min_length=20, max_length=1000, description="Why this action is recommended")
    priority: PriorityLevel = Field(default=PriorityLevel.MEDIUM, description="Priority level 1-5")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence in this recommendation")
    expected_impact: Optional[str] = Field(default=None, description="Expected impact of this action")
    estimated_effort: Optional[str] = Field(default=None, description="Effort required to implement")
    category: Optional[str] = Field(default=None, description="Category of recommendation")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @field_validator('priority', mode='before')
    @classmethod
    def validate_priority(cls, v):
        """Ensure priority is valid"""
        if isinstance(v, int):
            if 1 <= v <= 5:
                return PriorityLevel(v)
            raise ValueError("Priority must be between 1 and 5")
        return v
    
    class Config:
        use_enum_values = True


class DetectedPattern(BaseModel):
    """Validated pattern detection output"""
    pattern_type: PatternType
    description: str = Field(..., min_length=10)
    confidence: float = Field(..., ge=0.0, le=1.0)
    severity: Optional[str] = Field(default=None, description="Severity if negative pattern")
    metrics: Optional[Dict[str, float]] = Field(default=None, description="Metrics related to pattern")
    time_range: Optional[Dict[str, str]] = Field(default=None, description="Time range of pattern")
    
    class Config:
        use_enum_values = True


class AgentMetadata(BaseModel):
    """Metadata about the agent execution"""
    agent_name: str
    agent_version: Optional[str] = "1.0.0"
    execution_time_ms: Optional[float] = None
    data_points_analyzed: Optional[int] = None
    platform: Optional[str] = None
    objective: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AgentOutput(BaseModel):
    """Complete validated agent output"""
    insights: List[AgentInsight] = Field(default_factory=list, description="Generated insights")
    recommendations: List[AgentRecommendation] = Field(default_factory=list, description="Generated recommendations")
    patterns: Optional[List[DetectedPattern]] = Field(default=None, description="Detected patterns")
    metadata: AgentMetadata
    overall_confidence: float = Field(..., ge=0.0, le=1.0, description="Overall confidence in output")
    warnings: Optional[List[str]] = Field(default=None, description="Any warnings or caveats")
    
    @field_validator('insights')
    @classmethod
    def validate_insights(cls, v):
        """Ensure at least some output is provided"""
        # Allow empty insights if there are recommendations or patterns
        return v
    
    @model_validator(mode='after')
    def validate_has_content(self):
        """Ensure output has at least insights, recommendations, or patterns"""
        if not self.insights and not self.recommendations and not self.patterns:
            raise ValueError("Agent output must contain at least one insight, recommendation, or pattern")
        return self
    
    @field_validator('overall_confidence', mode='before')
    @classmethod
    def calculate_overall_confidence(cls, v, info):
        """Calculate overall confidence if not provided"""
        if v is not None:
            return v
        
        # Calculate from insights and recommendations
        if info.data:
            insights = info.data.get('insights', [])
            recommendations = info.data.get('recommendations', [])
            
            all_confidences = []
            all_confidences.extend([i.confidence for i in insights])
            all_confidences.extend([r.confidence for r in recommendations])
            
            if all_confidences:
                return sum(all_confidences) / len(all_confidences)
        
        return 0.5  # Default medium confidence


class VisionAgentMetric(BaseModel):
    """Validated metric extracted by vision agent"""
    name: str
    value: Union[float, int, str]
    metric_type: MetricType
    confidence: float = Field(..., ge=0.0, le=1.0)
    unit: Optional[str] = None
    
    class Config:
        use_enum_values = True


class VisionAgentChart(BaseModel):
    """Validated chart data extracted by vision agent"""
    chart_type: str
    title: Optional[str] = None
    data_points: List[Dict[str, Any]]
    confidence: float = Field(..., ge=0.0, le=1.0)


class VisionAgentTable(BaseModel):
    """Validated table data extracted by vision agent"""
    headers: List[str]
    rows: List[List[Any]]
    confidence: float = Field(..., ge=0.0, le=1.0)


class VisionAgentOutput(BaseModel):
    """Complete validated vision agent output"""
    platform: str
    metrics: List[VisionAgentMetric] = Field(default_factory=list)
    charts: List[VisionAgentChart] = Field(default_factory=list)
    tables: List[VisionAgentTable] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    overall_confidence: float = Field(..., ge=0.0, le=1.0)
    
    @field_validator('metrics')
    @classmethod
    def validate_metrics(cls, v):
        """Ensure at least some metrics were extracted"""
        if not v:
            raise ValueError("Vision agent must extract at least one metric")
        return v


# Utility functions for filtering by confidence
def filter_high_confidence_insights(output: AgentOutput, threshold: float = 0.8) -> List[AgentInsight]:
    """Filter insights by confidence threshold"""
    return [i for i in output.insights if i.confidence >= threshold]


def filter_high_confidence_recommendations(output: AgentOutput, threshold: float = 0.8) -> List[AgentRecommendation]:
    """Filter recommendations by confidence threshold"""
    return [r for r in output.recommendations if r.confidence >= threshold]


def filter_by_priority(output: AgentOutput, max_priority: int = 3) -> List[AgentRecommendation]:
    """Filter recommendations by priority level"""
    return [r for r in output.recommendations if r.priority <= max_priority]
