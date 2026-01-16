"""
Events package for event-driven architecture.

Provides:
- Event types for all system operations
- Event bus for publish/subscribe
- Event-driven agent wrappers
- Event listeners for monitoring
"""

from src.core.events.event_types import (
    BaseEvent,
    EventPriority,
    AgentAnalysisRequested,
    AgentAnalysisCompleted,
    AgentAnalysisFailed,
    PatternDetected,
    AnomalyDetected,
    RecommendationGenerated,
    RecommendationApplied,
    BenchmarkUpdated,
    BenchmarkComparisonCompleted,
    KnowledgeQueryRequested,
    KnowledgeQueryCompleted,
    SystemHealthCheck,
    SystemError,
    EVENT_TYPES
)

from src.core.events.event_bus import (
    EventBus,
    get_event_bus,
    reset_event_bus
)

from src.core.events.event_driven_agent import EventDrivenAgent

from src.core.events.event_listeners import (
    LoggingListener,
    MetricsListener,
    PrometheusMetricsListener,
    AlertListener,
    EventHistoryListener
)

__all__ = [
    # Event types
    'BaseEvent',
    'EventPriority',
    'AgentAnalysisRequested',
    'AgentAnalysisCompleted',
    'AgentAnalysisFailed',
    'PatternDetected',
    'AnomalyDetected',
    'RecommendationGenerated',
    'RecommendationApplied',
    'BenchmarkUpdated',
    'BenchmarkComparisonCompleted',
    'KnowledgeQueryRequested',
    'KnowledgeQueryCompleted',
    'SystemHealthCheck',
    'SystemError',
    'EVENT_TYPES',
    
    # Event bus
    'EventBus',
    'get_event_bus',
    'reset_event_bus',
    
    # Event-driven components
    'EventDrivenAgent',
    
    # Listeners
    'LoggingListener',
    'MetricsListener',
    'PrometheusMetricsListener',
    'AlertListener',
    'EventHistoryListener',
]
