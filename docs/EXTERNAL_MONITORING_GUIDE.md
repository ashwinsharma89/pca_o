# External Monitoring Integration Guide

## Overview

This guide explains how to integrate PCA Agent with Datadog and New Relic for comprehensive application performance monitoring (APM), error tracking, and custom dashboards.

---

## Why External Monitoring?

### Benefits

1. **Application Performance Monitoring (APM)**
   - Real-time performance metrics
   - Request/response tracking
   - Distributed tracing
   - Database query monitoring

2. **Error Tracking**
   - Automatic error detection
   - Stack trace capture
   - Error grouping and trends
   - Alert notifications

3. **Custom Dashboards**
   - Business metrics visualization
   - Real-time system health
   - Historical trend analysis
   - Custom alerts and SLOs

4. **Production Operations**
   - 24/7 monitoring
   - Incident management
   - Performance optimization
   - Capacity planning

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  PCA Agent Application                   │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │        External Monitoring System                  │ │
│  │  - Datadog Integration                             │ │
│  │  - New Relic Integration                           │ │
│  │  - Automatic Instrumentation                       │ │
│  └────────────────────────────────────────────────────┘ │
│                          │                               │
└──────────────────────────┼───────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
    ┌──────────────────┐      ┌──────────────────┐
    │     Datadog      │      │   New Relic      │
    │                  │      │                  │
    │  - APM Traces    │      │  - APM Traces    │
    │  - Metrics       │      │  - Metrics       │
    │  - Logs          │      │  - Errors        │
    │  - Events        │      │  - Events        │
    │  - Dashboards    │      │  - Dashboards    │
    └──────────────────┘      └──────────────────┘
```

---

## Setup

### 1. Datadog Setup

#### Install Datadog Agent

```bash
# Install Datadog Python library
pip install ddtrace

# Install Datadog Agent (Ubuntu/Debian)
DD_API_KEY=your_api_key bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"

# Or use Docker
docker run -d --name datadog-agent \
  -e DD_API_KEY=your_api_key \
  -e DD_SITE="datadoghq.com" \
  -e DD_APM_ENABLED=true \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v /proc/:/host/proc/:ro \
  -v /sys/fs/cgroup/:/host/sys/fs/cgroup:ro \
  datadog/agent:latest
```

#### Configure Environment Variables

```bash
export DATADOG_API_KEY=your_api_key
export DATADOG_APP_KEY=your_app_key
export ENVIRONMENT=production
export APP_VERSION=1.0.0
```

#### Run with Datadog APM

```bash
# Run application with Datadog tracing
ddtrace-run python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### 2. New Relic Setup

#### Install New Relic Agent

```bash
# Install New Relic Python library
pip install newrelic

# Generate configuration file
newrelic-admin generate-config your_license_key newrelic.ini
```

#### Configure Environment Variables

```bash
export NEW_RELIC_LICENSE_KEY=your_license_key
export NEW_RELIC_APP_NAME="PCA Agent"
export NEW_RELIC_ENVIRONMENT=production
```

#### Run with New Relic APM

```bash
# Run application with New Relic monitoring
NEW_RELIC_CONFIG_FILE=newrelic/newrelic.ini newrelic-admin run-program \
  python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### 3. Application Integration

#### Add Monitoring Middleware

```python
# In src/api/main.py
from fastapi import FastAPI
from src.api.middleware.monitoring_middleware import MonitoringMiddleware

app = FastAPI()

# Add monitoring middleware
app.add_middleware(MonitoringMiddleware)
```

#### Initialize Monitoring

```python
from src.observability.external_monitoring import external_monitoring

# Setup dashboards (run once)
external_monitoring.setup_dashboards()
```

---

## Usage

### Automatic Monitoring

All API requests are automatically monitored:

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/api/campaigns")
async def get_campaigns():
    # Automatically tracked:
    # - Response time
    # - Status code
    # - Error rate
    return {"campaigns": []}
```

### Manual Metric Tracking

```python
from src.observability.external_monitoring import external_monitoring

# Track custom metric
external_monitoring.send_metric(
    "campaigns.created",
    1,
    tags={"user_id": "user123"}
)

# Track event
external_monitoring.send_event(
    title="Campaign Created",
    text="User created new campaign",
    alert_type="info",
    tags={"user_id": "user123"}
)
```

### Function Monitoring

```python
from src.observability.external_monitoring import monitor

@monitor("process_campaign")
def process_campaign(campaign_id: str):
    # Function execution automatically monitored
    # - Duration
    # - Success/failure
    # - Errors
    pass
```

### Performance Tracking

```python
from src.observability.external_monitoring import track_performance

def analyze_data():
    with track_performance("data_analysis"):
        # Code execution tracked
        result = expensive_operation()
    
    return result
```

### Database Query Tracking

```python
from src.observability.external_monitoring import external_monitoring

def get_campaigns():
    start = time.time()
    
    try:
        result = db.query(Campaign).all()
        duration_ms = (time.time() - start) * 1000
        
        external_monitoring.track_database_query(
            query_type="SELECT",
            duration_ms=duration_ms,
            success=True
        )
        
        return result
    
    except Exception as e:
        duration_ms = (time.time() - start) * 1000
        external_monitoring.track_database_query(
            query_type="SELECT",
            duration_ms=duration_ms,
            success=False
        )
        raise
```

### LLM Call Tracking

```python
from src.observability.external_monitoring import external_monitoring

def call_llm(prompt: str):
    start = time.time()
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        
        duration_ms = (time.time() - start) * 1000
        tokens = response.usage.total_tokens
        cost = calculate_cost(tokens, "gpt-4")
        
        external_monitoring.track_llm_call(
            provider="openai",
            model="gpt-4",
            duration_ms=duration_ms,
            tokens_used=tokens,
            cost=cost,
            success=True
        )
        
        return response
    
    except Exception as e:
        duration_ms = (time.time() - start) * 1000
        external_monitoring.track_llm_call(
            provider="openai",
            model="gpt-4",
            duration_ms=duration_ms,
            tokens_used=0,
            cost=0,
            success=False
        )
        raise
```

### Error Tracking

```python
from src.observability.external_monitoring import external_monitoring

try:
    risky_operation()
except Exception as e:
    # Automatically track error
    external_monitoring.track_error(e, {
        "user_id": "user123",
        "operation": "risky_operation",
        "additional_context": "..."
    })
    raise
```

---

## Dashboards

### Datadog Dashboard

The system automatically creates a Datadog dashboard with:

1. **API Response Time** - Average response time over time
2. **Request Rate** - Requests per second
3. **Error Rate** - Errors per minute
4. **Cache Hit Rate** - Percentage of cache hits
5. **Database Query Time** - Average query duration
6. **LLM API Latency** - Latency by provider
7. **Active Users** - Current active users
8. **Memory Usage** - Application memory usage
9. **CPU Usage** - Application CPU usage
10. **Top Errors** - Most common errors

Access: https://app.datadoghq.com/dashboard/

### New Relic Dashboard

New Relic automatically provides:

1. **APM Overview** - Application performance summary
2. **Transactions** - Request/response details
3. **Databases** - Database query performance
4. **External Services** - Third-party API calls
5. **Errors** - Error tracking and analysis
6. **JVM/Runtime** - Runtime metrics

Access: https://one.newrelic.com/

---

## Metrics Reference

### API Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `pca_agent.api.response_time` | API response time in ms | Gauge |
| `pca_agent.api.requests` | Total API requests | Count |
| `pca_agent.api.errors` | API errors | Count |

### Database Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `pca_agent.database.query_time` | Query duration in ms | Gauge |
| `pca_agent.database.queries` | Total queries | Count |

### Cache Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `pca_agent.cache.hits` | Cache hits | Count |
| `pca_agent.cache.misses` | Cache misses | Count |
| `pca_agent.cache.operation_time` | Cache operation time | Gauge |

### LLM Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `pca_agent.llm.latency` | LLM API latency in ms | Gauge |
| `pca_agent.llm.tokens` | Tokens used | Count |
| `pca_agent.llm.cost` | API call cost | Gauge |
| `pca_agent.llm.calls` | Total LLM calls | Count |

### Error Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `pca_agent.errors.count` | Total errors | Count |

---

## Alerting

### Datadog Alerts

Create alerts in Datadog for:

1. **High Error Rate**
   - Condition: `sum:pca_agent.errors.count{*}.as_rate() > 10`
   - Alert: When error rate exceeds 10/min

2. **Slow Response Time**
   - Condition: `avg:pca_agent.api.response_time{*} > 2000`
   - Alert: When average response time > 2s

3. **Low Cache Hit Rate**
   - Condition: `(sum:pca_agent.cache.hits{*} / (sum:pca_agent.cache.hits{*} + sum:pca_agent.cache.misses{*})) * 100 < 70`
   - Alert: When cache hit rate < 70%

### New Relic Alerts

Create alerts in New Relic for:

1. **Apdex Score**
   - Condition: Apdex score < 0.8
   - Alert: When user satisfaction drops

2. **Error Rate**
   - Condition: Error rate > 5%
   - Alert: When error rate is high

3. **Response Time**
   - Condition: 95th percentile > 3s
   - Alert: When response time degrades

---

## Best Practices

### 1. Tag Everything

Use consistent tags for better filtering:

```python
external_monitoring.send_metric(
    "custom.metric",
    value,
    tags={
        "environment": "production",
        "service": "pca-agent",
        "version": "1.0.0",
        "feature": "campaigns"
    }
)
```

### 2. Monitor Business Metrics

Track business-relevant metrics:

```python
# Track campaign creation
external_monitoring.send_metric("campaigns.created", 1)

# Track revenue
external_monitoring.send_metric("revenue.generated", revenue_amount)

# Track user signups
external_monitoring.send_metric("users.signups", 1)
```

### 3. Set Up SLOs

Define Service Level Objectives:

- API response time P95 < 500ms
- Error rate < 1%
- Availability > 99.9%

### 4. Use Distributed Tracing

Enable distributed tracing for microservices:

```python
# Datadog automatically traces across services
# New Relic automatically traces across services
```

### 5. Monitor Dependencies

Track external service performance:

```python
external_monitoring.track_llm_call(
    provider="openai",
    model="gpt-4",
    duration_ms=duration,
    tokens_used=tokens,
    cost=cost
)
```

---

## Troubleshooting

### Metrics Not Appearing

1. **Check API Keys**
```bash
echo $DATADOG_API_KEY
echo $NEW_RELIC_LICENSE_KEY
```

2. **Verify Agent Status**
```bash
# Datadog
sudo datadog-agent status

# New Relic
newrelic-admin validate-config newrelic.ini
```

3. **Check Logs**
```bash
# Datadog
tail -f /var/log/datadog/agent.log

# New Relic
tail -f /var/log/newrelic/newrelic-python-agent.log
```

### High Overhead

If monitoring causes performance issues:

1. **Reduce Sampling Rate**
```python
# Datadog
DD_TRACE_SAMPLE_RATE=0.1  # Sample 10% of requests

# New Relic
transaction_tracer.transaction_threshold = 2.0  # Only trace slow requests
```

2. **Disable Detailed Tracing**
```python
# Disable SQL query capture
transaction_tracer.record_sql = off
```

---

## Cost Optimization

### Datadog

- Use metric aggregation
- Set appropriate retention periods
- Use log sampling for high-volume logs
- Archive old data to S3

### New Relic

- Use data ingest governance
- Set data retention policies
- Use sampling for high-throughput apps
- Monitor data usage dashboard

---

## Conclusion

External monitoring with Datadog and New Relic provides:

- ✅ Real-time performance monitoring
- ✅ Automatic error tracking
- ✅ Custom business dashboards
- ✅ Production operations visibility
- ✅ Proactive alerting
- ✅ Historical trend analysis

**Status**: Production-ready external monitoring integration!
