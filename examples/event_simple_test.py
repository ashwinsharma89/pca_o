"""Simple test of event system."""

from src.events import get_event_bus, AgentAnalysisCompleted, PatternDetected

# Get event bus
bus = get_event_bus()

# Track events
events_received = []

def track_event(event):
    events_received.append(event.event_type)
    print(f"✅ Received: {event.event_type}")

# Subscribe
bus.subscribe('*', track_event)

# Publish events
print("Publishing events...")
bus.publish(AgentAnalysisCompleted(
    agent_name="TestAgent",
    execution_time_ms=100,
    confidence=0.9,
    insights_count=5,
    recommendations_count=3
))

bus.publish(PatternDetected(
    pattern_type="trend",
    description="Upward trend",
    confidence=0.85
))

print(f"\n✅ Test complete! Received {len(events_received)} events")
print(f"Event types: {events_received}")
