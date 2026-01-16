"""
Event types for the PCA system.

Defines all event types used in the event-driven architecture.
Events enable loose coupling between components.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
import uuid


# ============================================================================
# Event Priority
# ============================================================================

class EventPriority(str, Enum):
    """Event priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


# ============================================================================
# Base Event
# ============================================================================

@dataclass
class BaseEvent:
    """
    Base class for all events.
    
    All events inherit from this and add specific fields.
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = field(default="base_event")
    timestamp: datetime = field(default_factory=datetime.utcnow)
    priority: EventPriority = EventPriority.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp.isoformat(),
            'priority': self.priority.value,
            'metadata': self.metadata
        }


# ============================================================================
# Agent Events
# ============================================================================

@dataclass
class AgentAnalysisRequested(BaseEvent):
    """Event: Agent analysis has been requested."""
    event_type: str = "agent.analysis.requested"
    
    campaign_data: Any = None
    platform: Optional[str] = None
    objective: Optional[str] = None
    channel_insights: Optional[Dict[str, Any]] = None
    campaign_context: Optional[Any] = None
    request_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'platform': self.platform,
            'objective': self.objective,
            'request_id': self.request_id,
            'has_channel_insights': self.channel_insights is not None,
            'has_campaign_context': self.campaign_context is not None
        })
        return base


@dataclass
class AgentAnalysisCompleted(BaseEvent):
    """Event: Agent analysis has completed."""
    event_type: str = "agent.analysis.completed"
    
    request_id: Optional[str] = None
    agent_name: str = "unknown"
    result: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    confidence: float = 0.0
    insights_count: int = 0
    recommendations_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'request_id': self.request_id,
            'agent_name': self.agent_name,
            'execution_time_ms': self.execution_time_ms,
            'confidence': self.confidence,
            'insights_count': self.insights_count,
            'recommendations_count': self.recommendations_count
        })
        return base


@dataclass
class AgentAnalysisFailed(BaseEvent):
    """Event: Agent analysis has failed."""
    event_type: str = "agent.analysis.failed"
    priority: EventPriority = EventPriority.HIGH
    
    request_id: Optional[str] = None
    agent_name: str = "unknown"
    error_type: str = "unknown"
    error_message: str = ""
    fallback_used: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'request_id': self.request_id,
            'agent_name': self.agent_name,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'fallback_used': self.fallback_used
        })
        return base


# ============================================================================
# Pattern Detection Events
# ============================================================================

@dataclass
class PatternDetected(BaseEvent):
    """Event: A pattern has been detected in campaign data."""
    event_type: str = "pattern.detected"
    
    pattern_type: str = "unknown"
    description: str = ""
    confidence: float = 0.0
    severity: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    campaign_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'pattern_type': self.pattern_type,
            'description': self.description,
            'confidence': self.confidence,
            'severity': self.severity,
            'metrics': self.metrics,
            'campaign_id': self.campaign_id
        })
        return base


@dataclass
class AnomalyDetected(BaseEvent):
    """Event: An anomaly has been detected."""
    event_type: str = "pattern.anomaly.detected"
    priority: EventPriority = EventPriority.HIGH
    
    metric_name: str = ""
    expected_value: float = 0.0
    actual_value: float = 0.0
    deviation_percentage: float = 0.0
    campaign_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'metric_name': self.metric_name,
            'expected_value': self.expected_value,
            'actual_value': self.actual_value,
            'deviation_percentage': self.deviation_percentage,
            'campaign_id': self.campaign_id
        })
        return base


# ============================================================================
# Recommendation Events
# ============================================================================

@dataclass
class RecommendationGenerated(BaseEvent):
    """Event: A recommendation has been generated."""
    event_type: str = "recommendation.generated"
    
    recommendation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action: str = ""
    rationale: str = ""
    rec_priority: str = "medium"  # Renamed to avoid conflict with BaseEvent.priority
    confidence: float = 0.0
    expected_impact: Optional[str] = None
    campaign_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'recommendation_id': self.recommendation_id,
            'action': self.action,
            'rationale': self.rationale,
            'rec_priority': self.rec_priority,
            'confidence': self.confidence,
            'expected_impact': self.expected_impact,
            'campaign_id': self.campaign_id
        })
        return base


@dataclass
class RecommendationApplied(BaseEvent):
    """Event: A recommendation has been applied."""
    event_type: str = "recommendation.applied"
    
    recommendation_id: str = ""
    applied_by: str = "system"
    result: str = "success"
    campaign_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'recommendation_id': self.recommendation_id,
            'applied_by': self.applied_by,
            'result': self.result,
            'campaign_id': self.campaign_id
        })
        return base


# ============================================================================
# Benchmark Events
# ============================================================================

@dataclass
class BenchmarkUpdated(BaseEvent):
    """Event: Benchmark data has been updated."""
    event_type: str = "benchmark.updated"
    
    platform: str = ""
    objective: str = ""
    metrics: Dict[str, float] = field(default_factory=dict)
    source: str = "system"
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'platform': self.platform,
            'objective': self.objective,
            'metrics': self.metrics,
            'source': self.source
        })
        return base


@dataclass
class BenchmarkComparisonCompleted(BaseEvent):
    """Event: Benchmark comparison has been completed."""
    event_type: str = "benchmark.comparison.completed"
    
    campaign_id: Optional[str] = None
    platform: str = ""
    objective: str = ""
    results: Dict[str, Any] = field(default_factory=dict)
    below_benchmark_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'campaign_id': self.campaign_id,
            'platform': self.platform,
            'objective': self.objective,
            'below_benchmark_count': self.below_benchmark_count
        })
        return base


# ============================================================================
# Knowledge Base Events
# ============================================================================

@dataclass
class KnowledgeQueryRequested(BaseEvent):
    """Event: Knowledge base query has been requested."""
    event_type: str = "knowledge.query.requested"
    
    query: str = ""
    filters: Dict[str, Any] = field(default_factory=dict)
    top_k: int = 5
    request_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'query': self.query,
            'filters': self.filters,
            'top_k': self.top_k,
            'request_id': self.request_id
        })
        return base


@dataclass
class KnowledgeQueryCompleted(BaseEvent):
    """Event: Knowledge base query has been completed."""
    event_type: str = "knowledge.query.completed"
    
    request_id: Optional[str] = None
    results_count: int = 0
    execution_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'request_id': self.request_id,
            'results_count': self.results_count,
            'execution_time_ms': self.execution_time_ms
        })
        return base


# ============================================================================
# System Events
# ============================================================================

@dataclass
class SystemHealthCheck(BaseEvent):
    """Event: System health check performed."""
    event_type: str = "system.health.check"
    
    component: str = "system"
    status: str = "healthy"
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'component': self.component,
            'status': self.status,
            'details': self.details
        })
        return base


@dataclass
class SystemError(BaseEvent):
    """Event: System error occurred."""
    event_type: str = "system.error"
    priority: EventPriority = EventPriority.CRITICAL
    
    component: str = "unknown"
    error_type: str = "unknown"
    error_message: str = ""
    stack_trace: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'component': self.component,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'has_stack_trace': self.stack_trace is not None
        })
        return base


# ============================================================================
# Event Type Registry
# ============================================================================

EVENT_TYPES = {
    # Agent events
    'agent.analysis.requested': AgentAnalysisRequested,
    'agent.analysis.completed': AgentAnalysisCompleted,
    'agent.analysis.failed': AgentAnalysisFailed,
    
    # Pattern events
    'pattern.detected': PatternDetected,
    'pattern.anomaly.detected': AnomalyDetected,
    
    # Recommendation events
    'recommendation.generated': RecommendationGenerated,
    'recommendation.applied': RecommendationApplied,
    
    # Benchmark events
    'benchmark.updated': BenchmarkUpdated,
    'benchmark.comparison.completed': BenchmarkComparisonCompleted,
    
    # Knowledge events
    'knowledge.query.requested': KnowledgeQueryRequested,
    'knowledge.query.completed': KnowledgeQueryCompleted,
    
    # System events
    'system.health.check': SystemHealthCheck,
    'system.error': SystemError,
}
