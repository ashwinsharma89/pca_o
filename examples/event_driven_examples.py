"""
Comprehensive example of event-driven architecture.

Demonstrates:
1. Setting up event bus
2. Creating event-driven agents
3. Subscribing to events
4. Publishing custom events
5. Monitoring with event listeners
"""

import pandas as pd
import numpy as np
from datetime import datetime

# Import event system
from src.events import (
    get_event_bus,
    EventDrivenAgent,
    LoggingListener,
    MetricsListener,
    AlertListener,
    AgentAnalysisCompleted,
    PatternDetected,
    RecommendationGenerated
)

# Import DI system
from src.di.containers import get_reasoning_agent


# ============================================================================
# Example 1: Basic Event Publishing
# ============================================================================

def example_1_basic_events():
    """Basic event publishing and subscription."""
    print("=" * 60)
    print("Example 1: Basic Event Publishing")
    print("=" * 60)
    
    # Get event bus
    bus = get_event_bus()
    
    # Create a simple event handler
    def on_analysis_complete(event):
        print(f"✅ Analysis completed!")
        print(f"   Agent: {event.agent_name}")
        print(f"   Execution time: {event.execution_time_ms:.2f}ms")
        print(f"   Insights: {event.insights_count}")
        print(f"   Recommendations: {event.recommendations_count}")
    
    # Subscribe to events
    bus.subscribe('agent.analysis.completed', on_analysis_complete)
    
    # Publish a test event
    event = AgentAnalysisCompleted(
        agent_name="TestAgent",
        execution_time_ms=150.5,
        confidence=0.85,
        insights_count=5,
        recommendations_count=3
    )
    bus.publish(event)
    
    print()


# ============================================================================
# Example 2: Event-Driven Agent
# ============================================================================

def example_2_event_driven_agent():
    """Use event-driven agent wrapper."""
    print("=" * 60)
    print("Example 2: Event-Driven Agent")
    print("=" * 60)
    
    # Get base agent from DI container
    base_agent = get_reasoning_agent()
    
    if not base_agent:
        print("⚠️  Agent not available")
        return
    
    # Wrap with event-driven wrapper
    event_agent = EventDrivenAgent(base_agent, name="EnhancedReasoning")
    
    # Subscribe to events
    bus = get_event_bus()
    
    events_received = []
    
    def track_event(event):
        events_received.append(event.event_type)
        print(f"📢 Event: {event.event_type}")
    
    bus.subscribe('*', track_event)
    
    # Create test data
    dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
    data = pd.DataFrame({
        'Date': dates,
        'Platform': ['meta'] * 10,
        'Spend': np.random.uniform(1000, 2000, 10),
        'Impressions': np.random.uniform(10000, 20000, 10),
        'Clicks': np.random.uniform(500, 1000, 10),
        'Conversions': np.random.uniform(50, 100, 10),
        'CTR': np.random.uniform(0.04, 0.06, 10),
        'CPC': np.random.uniform(1.5, 2.5, 10),
        'Campaign': ['Campaign_A'] * 10
    })
    
    # Run analysis - events published automatically
    print("\nRunning analysis...")
    result = event_agent.analyze(data)
    
    print(f"\n✅ Analysis complete!")
    print(f"   Events published: {len(events_received)}")
    print(f"   Event types: {set(events_received)}")
    
    print()


# ============================================================================
# Example 3: Multiple Event Listeners
# ============================================================================

def example_3_multiple_listeners():
    """Use multiple event listeners."""
    print("=" * 60)
    print("Example 3: Multiple Event Listeners")
    print("=" * 60)
    
    bus = get_event_bus()
    
    # Setup listeners
    logging_listener = LoggingListener(log_level="INFO")
    metrics_listener = MetricsListener()
    
    def custom_alert(message):
        print(f"🚨 ALERT: {message}")
    
    alert_listener = AlertListener(alert_callback=custom_alert)
    
    # Subscribe listeners
    bus.subscribe('*', logging_listener.on_event)
    bus.subscribe('*', metrics_listener.on_event)
    bus.subscribe('*', alert_listener.on_event)
    
    # Publish various events
    print("\nPublishing events...\n")
    
    # Analysis completed
    bus.publish(AgentAnalysisCompleted(
        agent_name="Agent1",
        execution_time_ms=200,
        confidence=0.9,
        insights_count=10,
        recommendations_count=5
    ))
    
    # Pattern detected
    bus.publish(PatternDetected(
        pattern_type="trend",
        description="Upward trend in CTR",
        confidence=0.85
    ))
    
    # Recommendation generated
    bus.publish(RecommendationGenerated(
        action="Increase budget",
        rationale="Strong performance trend",
        confidence=0.8
    ))
    
    # Get metrics
    print("\n" + "=" * 60)
    print("Metrics Summary")
    print("=" * 60)
    stats = metrics_listener.get_stats()
    print(f"Total events: {stats['total_events']}")
    print(f"Event types: {list(stats['event_counts'].keys())}")
    print(f"Pattern detections: {stats['total_patterns']}")
    
    print()


# ============================================================================
# Example 4: Event History and Querying
# ============================================================================

def example_4_event_history():
    """Query event history."""
    print("=" * 60)
    print("Example 4: Event History")
    print("=" * 60)
    
    bus = get_event_bus()
    
    # Publish some events
    for i in range(5):
        bus.publish(AgentAnalysisCompleted(
            agent_name=f"Agent{i}",
            execution_time_ms=100 + i * 10,
            confidence=0.7 + i * 0.05,
            insights_count=i + 1,
            recommendations_count=i
        ))
    
    # Get history
    history = bus.get_history(limit=10)
    
    print(f"\nEvent history ({len(history)} events):")
    for event in history[-5:]:
        print(f"  - {event.event_type} at {event.timestamp.strftime('%H:%M:%S')}")
    
    # Get specific event type
    analysis_events = bus.get_history(event_type='agent.analysis.completed')
    print(f"\nAnalysis events: {len(analysis_events)}")
    
    # Get stats
    stats = bus.get_stats()
    print(f"\nBus stats:")
    print(f"  Total subscribers: {stats['total_subscribers']}")
    print(f"  History size: {stats['history_size']}")
    print(f"  Event types: {list(stats['event_type_counts'].keys())}")
    
    print()


# ============================================================================
# Example 5: Custom Event Handlers
# ============================================================================

def example_5_custom_handlers():
    """Create custom event handlers."""
    print("=" * 60)
    print("Example 5: Custom Event Handlers")
    print("=" * 60)
    
    bus = get_event_bus()
    
    # Performance monitor
    class PerformanceMonitor:
        def __init__(self):
            self.slow_threshold_ms = 500
        
        def on_analysis_complete(self, event):
            if event.execution_time_ms > self.slow_threshold_ms:
                print(f"⚠️  Slow analysis detected!")
                print(f"   Agent: {event.agent_name}")
                print(f"   Time: {event.execution_time_ms:.2f}ms")
    
    # Low confidence monitor
    class ConfidenceMonitor:
        def __init__(self):
            self.min_confidence = 0.7
        
        def on_analysis_complete(self, event):
            if event.confidence < self.min_confidence:
                print(f"⚠️  Low confidence analysis!")
                print(f"   Agent: {event.agent_name}")
                print(f"   Confidence: {event.confidence:.2f}")
    
    # Create monitors
    perf_monitor = PerformanceMonitor()
    conf_monitor = ConfidenceMonitor()
    
    # Subscribe
    bus.subscribe('agent.analysis.completed', perf_monitor.on_analysis_complete)
    bus.subscribe('agent.analysis.completed', conf_monitor.on_analysis_complete)
    
    # Test with various events
    print("\nPublishing test events...\n")
    
    # Fast, high confidence (no alerts)
    bus.publish(AgentAnalysisCompleted(
        agent_name="FastAgent",
        execution_time_ms=100,
        confidence=0.95,
        insights_count=5,
        recommendations_count=3
    ))
    
    # Slow execution (alert)
    bus.publish(AgentAnalysisCompleted(
        agent_name="SlowAgent",
        execution_time_ms=750,
        confidence=0.85,
        insights_count=5,
        recommendations_count=3
    ))
    
    # Low confidence (alert)
    bus.publish(AgentAnalysisCompleted(
        agent_name="UncertainAgent",
        execution_time_ms=200,
        confidence=0.55,
        insights_count=2,
        recommendations_count=1
    ))
    
    print()


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 12 + "Event-Driven Architecture Examples" + " " * 12 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        example_1_basic_events()
        example_2_event_driven_agent()
        example_3_multiple_listeners()
        example_4_event_history()
        example_5_custom_handlers()
        
        print("=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
