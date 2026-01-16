# Deep Visibility: The Observability Guide

> *"You cannot optimize what you cannot measure."*

## 1. The Stack (The "All-Seeing Eye")

We use a multi-layered observability strategy to ensure we catch every query, error, and latency spike.

| Layer | Tool | Purpose | Best Practice |
|:---|:---|:---|:---|
| **Logs** | **Loguru** | Structured, color-coded event logging. | Use `logger.bind(request_id=...)` for context. Avoid `print()`. |
| **Metrics** | **Prometheus** | Time-series data (Latency, RPS, Memory). | Alert on `99th percentile` latency, not average. |
| **Traces** | **OpenTelemetry** | Request journey across services/agents. | Trace every Agent Graph execution to pin slow nodes. |
| **Errors** | **Sentry** | Crash reporting with full stack traces. | Tag errors with `agent_name` and `user_id`. |
| **Async** | **Flower** | Monitoring Celery background workers. | Watch for "Stuck" tasks in the queue. |
| **Viz** | **Grafana** | Unified dashboarding. | Create a "Red/Green" status board for Executives. |

## 2. Best Practices for Efficiency

### A. Contextual Logging (Loguru)
Don't just log strings. Log data structures.
```python
from loguru import logger

# BAD
logger.info("Starting analysis for user 123")

# GOOD
logger.bind(user_id=123, intent="campaign_analysis").info("Starting analysis")
```
*Why?* You can search logs by `json.user_id` later in Grafana/Loki.

### B. The "Red Method" for Metrics (Prometheus)
For every service (API, Orchestrator, Database), measure:
1.  **Rate**: Requests per second.
2.  **Errors**: Failed requests per second.
3.  **Duration**: Time to complete (p50, p90, p99).

### C. Tracing Agent Workflows (OpenTelemetry)
Spans are critical for AI Agents. Wrap your LangGraph nodes:
```python
with tracer.start_as_current_span("agent_reasoning") as span:
    span.set_attribute("user.id", user_id)
    span.set_attribute("ai.model", "gpt-4")
    result = llm.invoke(...)
    span.set_attribute("ai.tokens", result.usage)
```
*Why?* You will instantly see if the "Creative Analyzer" is the bottleneck vs the "Database".

## 4. The Anatomy of a Trace (Chart H)
*Distributed Tracing connects the dots across your async architecture.*

1.  **Root Span**: Starts at the API Gateway (e.g., `POST /query`).
2.  **Context Propagation**: The `X-Request-ID` is passed from API -> Orchestrator -> Agent -> Database.
3.  **Child Spans**: Each internal step (DB Query, LLM Call) is a distinct timed block nested under the parent.
4.  **Tags**: Critical metadata (e.g., `user_id`, `tokens_used`, `cache_hit`) attached to spans for filtering in Grafana/Jaeger.

## 5. Architecture Flow (Pipeline)
*See Chart G (Observability Pipeline) and Chart H (Trace Lifecycle) for visual details.*

1.  **Application** emits Logs/Traces/Metrics.
2.  **OpenTelemetry Collector** aggregates them.
3.  **Prometheus** scrapes metrics.
4.  **Sentry** captures exceptions.
5.  **Grafana** visualizes it all.
