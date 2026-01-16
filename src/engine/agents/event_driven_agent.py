"""
Event-driven agent wrapper.

Wraps agents to publish events for all operations, enabling:
- Loose coupling between components
- Event-driven monitoring
- Audit trail of all agent actions
- Easy integration with event listeners
"""

from typing import Any, Dict, Optional
import time
from datetime import datetime

from src.core.interfaces.protocols import IReasoningAgent
from src.core.events.event_bus import get_event_bus
from src.core.events.event_types import (
    AgentAnalysisRequested,
    AgentAnalysisCompleted,
    AgentAnalysisFailed,
    PatternDetected,
    RecommendationGenerated,
    EventPriority
)
from loguru import logger


class EventDrivenAgent:
    """
    Event-driven wrapper for reasoning agents.
    
    Wraps any IReasoningAgent and publishes events for:
    - Analysis requests
    - Analysis completion
    - Analysis failures
    - Pattern detection
    - Recommendation generation
    
    Example:
        # Wrap existing agent
        base_agent = EnhancedReasoningAgent(...)
        event_agent = EventDrivenAgent(base_agent, name="ReasoningAgent")
        
        # Subscribe to events
        def on_complete(event):
            print(f"Analysis done: {event.insights_count} insights")
        
        get_event_bus().subscribe('agent.analysis.completed', on_complete)
        
        # Use agent normally - events published automatically
        result = event_agent.analyze(data)
    """
    
    def __init__(
        self,
        agent: IReasoningAgent,
        name: str = "Agent",
        event_bus: Optional[Any] = None
    ):
        """
        Initialize event-driven agent.
        
        Args:
            agent: Base agent to wrap
            name: Agent name for events
            event_bus: Event bus to use (default: global)
        """
        self._agent = agent
        self._name = name
        self._event_bus = event_bus or get_event_bus()
        
        logger.info(f"Initialized event-driven agent: {name}")
    
    def analyze(
        self,
        campaign_data: Any,
        channel_insights: Optional[Dict[str, Any]] = None,
        campaign_context: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Analyze campaign data with event publishing.
        
        Args:
            campaign_data: Campaign data to analyze
            channel_insights: Optional channel-specific insights
            campaign_context: Optional campaign context
            
        Returns:
            Analysis result
        """
        import uuid
        request_id = str(uuid.uuid4())
        
        # Publish request event
        request_event = AgentAnalysisRequested(
            request_id=request_id,
            platform=getattr(campaign_data, 'Platform', [None])[0] if hasattr(campaign_data, 'Platform') else None,
            channel_insights=channel_insights,
            campaign_context=campaign_context,
            metadata={'agent_name': self._name}
        )
        self._event_bus.publish(request_event)
        
        # Execute analysis
        start_time = time.time()
        try:
            result = self._agent.analyze(
                campaign_data=campaign_data,
                channel_insights=channel_insights,
                campaign_context=campaign_context
            )
            
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Extract metrics from result
            insights = result.get('insights', {})
            pattern_insights = insights.get('pattern_insights', [])
            recommendations = result.get('recommendations', [])
            
            # Calculate confidence
            confidence = self._calculate_confidence(result)
            
            # Publish completion event
            completion_event = AgentAnalysisCompleted(
                request_id=request_id,
                agent_name=self._name,
                result=result,
                execution_time_ms=execution_time_ms,
                confidence=confidence,
                insights_count=len(pattern_insights),
                recommendations_count=len(recommendations),
                metadata={'agent_name': self._name}
            )
            self._event_bus.publish(completion_event)
            
            # Publish pattern events
            self._publish_pattern_events(pattern_insights)
            
            # Publish recommendation events
            self._publish_recommendation_events(recommendations)
            
            return result
            
        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Publish failure event
            failure_event = AgentAnalysisFailed(
                request_id=request_id,
                agent_name=self._name,
                error_type=type(e).__name__,
                error_message=str(e),
                fallback_used=False,
                metadata={'agent_name': self._name, 'execution_time_ms': execution_time_ms}
            )
            self._event_bus.publish(failure_event)
            
            # Re-raise
            raise
    
    def _calculate_confidence(self, result: Dict[str, Any]) -> float:
        """Calculate overall confidence from result."""
        try:
            # Try to get confidence from result
            if 'confidence' in result:
                return float(result['confidence'])
            
            # Calculate from patterns
            patterns = result.get('patterns', {})
            if patterns:
                detected_count = sum(1 for v in patterns.values() if v.get('detected', False))
                total_count = len(patterns)
                return detected_count / total_count if total_count > 0 else 0.5
            
            # Default
            return 0.7
        except:
            return 0.5
    
    def _publish_pattern_events(self, pattern_insights: list) -> None:
        """Publish events for detected patterns."""
        for insight in pattern_insights:
            try:
                event = PatternDetected(
                    pattern_type=insight.get('type', 'unknown'),
                    description=insight.get('description', ''),
                    confidence=insight.get('confidence', 0.5),
                    severity=insight.get('severity'),
                    metrics=insight.get('metrics', {}),
                    metadata={'agent_name': self._name}
                )
                self._event_bus.publish(event)
            except Exception as e:
                logger.warning(f"Failed to publish pattern event: {e}")
    
    def _publish_recommendation_events(self, recommendations: list) -> None:
        """Publish events for generated recommendations."""
        for rec in recommendations:
            try:
                event = RecommendationGenerated(
                    action=rec.get('action', ''),
                    rationale=rec.get('rationale', ''),
                    rec_priority=rec.get('priority', 'medium'),
                    confidence=rec.get('confidence', 0.5),
                    expected_impact=rec.get('expected_impact'),
                    metadata={'agent_name': self._name}
                )
                self._event_bus.publish(event)
            except Exception as e:
                logger.warning(f"Failed to publish recommendation event: {e}")
    
    @property
    def name(self) -> str:
        """Get agent name."""
        return self._name
    
    @property
    def base_agent(self) -> IReasoningAgent:
        """Get the wrapped base agent."""
        return self._agent
