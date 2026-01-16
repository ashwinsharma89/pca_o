# Log Aggregation Guide

## Overview

The PCA Agent log aggregation system provides centralized logging with ELK Stack (Elasticsearch, Logstash, Kibana) and Splunk integration for comprehensive log management, search, analysis, and alerting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  PCA Agent Application                   │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Loguru Logger                         │ │
│  │  - Application logs                                │ │
│  │  - Error logs                                      │ │
│  │  - Access logs                                     │ │
│  └────────────────────────────────────────────────────┘ │
│                          │                               │
│                          ▼                               │
│  ┌────────────────────────────────────────────────────┐ │
│  │          Log Aggregation System                    │ │
│  │  - Elasticsearch Shipper                           │ │
│  │  - Splunk Shipper                                  │ │
│  │  - Log Enrichment                                  │ │
│  └────────────────────────────────────────────────────┘ │
│                          │                               │
└──────────────────────────┼───────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
    ┌──────────────────┐      ┌──────────────────┐
    │   ELK Stack      │      │     Splunk       │
    │                  │      │                  │
    │  Elasticsearch   │      │  HTTP Event      │
    │  Logstash        │      │  Collector       │
    │  Kibana          │      │                  │
    │  Filebeat        │      │                  │
    └──────────────────┘      └──────────────────┘
              │                         │
              ▼                         ▼
    ┌──────────────────┐      ┌──────────────────┐
    │  Log Search &    │      │  Log Analysis &  │
    │  Visualization   │      │  Dashboards      │
    └──────────────────┘      └──────────────────┘
```

---

## Features

### 1. Centralized Logging
- All logs from all services in one place
- Unified search across all log sources
- Consistent log format and structure

### 2. Log Enrichment
- Automatic metadata addition (timestamp, hostname, service, environment)
- Correlation IDs for distributed tracing
- GeoIP lookup for IP addresses
- User agent parsing

### 3. Log Processing
- JSON parsing
- Pattern matching and tagging
- Error extraction
- Performance metrics extraction

### 4. Alerting
- Pattern-based alerts
- Threshold-based alerts
- Real-time notifications
- Automatic incident creation

### 5. Retention & Lifecycle
- Hot/Warm/Cold/Delete lifecycle
- Automatic index rollover
- Configurable retention periods
- Cost-optimized storage

---

## Setup

### 1. Start ELK Stack

```bash
# Start all ELK services
docker-compose -f docker-compose.elk.yml up -d

# Check service health
docker-compose -f docker-compose.elk.yml ps

# View logs
docker-compose -f docker-compose.elk.yml logs -f
```

### 2. Verify Services

```bash
# Elasticsearch
curl http://localhost:9200/_cluster/health

# Logstash
curl http://localhost:9600

# Kibana
curl http://localhost:5601/api/status
```

### 3. Configure Application

```bash
# Set environment variables
export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200
export SPLUNK_HOST=your-splunk-host
export SPLUNK_PORT=8088
export SPLUNK_HEC_TOKEN=your-token
export ENVIRONMENT=production
```

### 4. Initialize Index Templates

```python
from src.observability.log_aggregation import log_aggregator

# Create Elasticsearch index template
log_aggregator.elasticsearch.create_index_template()

# Create lifecycle policy
log_aggregator.elasticsearch.create_lifecycle_policy()
```

---

## Usage

### Basic Logging

```python
from loguru import logger

# Logs are automatically shipped to ELK and Splunk
logger.info("Application started")
logger.warning("High memory usage detected")
logger.error("Database connection failed")
```

### Structured Logging

```python
from loguru import logger

# Add context to logs
logger.bind(
    user_id="user123",
    request_id="req456",
    correlation_id="corr789"
).info("User logged in")

# Log with metrics
logger.bind(
    duration_ms=1250,
    status_code=200,
    endpoint="/api/campaigns"
).info("API request completed")
```

### Error Logging

```python
from loguru import logger

try:
    # Some operation
    result = risky_operation()
except Exception as e:
    logger.exception("Operation failed")
    # Exception details automatically included
```

### Direct Log Shipping

```python
from src.observability.log_aggregation import log_aggregator

# Ship structured log directly
log_aggregator.ship_structured_log(
    level="INFO",
    message="Custom event",
    user_id="user123",
    action="campaign_created",
    campaign_id="camp456"
)
```

---

## Kibana Setup

### 1. Access Kibana

Open browser: http://localhost:5601

### 2. Create Index Pattern

1. Go to **Management** → **Stack Management** → **Index Patterns**
2. Click **Create index pattern**
3. Enter pattern: `pca-agent-logs-*`
4. Select time field: `@timestamp`
5. Click **Create index pattern**

### 3. Explore Logs

1. Go to **Discover**
2. Select `pca-agent-logs-*` index pattern
3. Search and filter logs

### 4. Create Visualizations

**Example: Error Rate Over Time**
1. Go to **Visualize** → **Create visualization**
2. Select **Line chart**
3. Select `pca-agent-logs-*` index
4. Y-axis: Count
5. X-axis: Date Histogram on `@timestamp`
6. Add filter: `level: ERROR`
7. Save visualization

**Example: Top Error Types**
1. Create **Pie chart**
2. Slice by: `error_type.keyword`
3. Add filter: `level: ERROR`
4. Save visualization

### 5. Create Dashboard

1. Go to **Dashboard** → **Create dashboard**
2. Add saved visualizations
3. Arrange and resize
4. Save dashboard

---

## Log Queries

### Search Examples

**Find all errors:**
```
level: ERROR
```

**Find errors for specific user:**
```
level: ERROR AND user_id: "user123"
```

**Find slow requests:**
```
duration_ms: >5000
```

**Find database errors:**
```
message: "database" AND level: ERROR
```

**Find by correlation ID:**
```
correlation_id: "corr789"
```

**Find security events:**
```
tags: security
```

### Advanced Queries

**Errors in last hour:**
```
level: ERROR AND @timestamp: [now-1h TO now]
```

**High error rate:**
```
level: ERROR AND @timestamp: [now-5m TO now]
```

**Specific error type:**
```
error_type: "DatabaseConnectionError"
```

---

## Alerting

### Built-in Alerts

The system includes pre-configured alerts:

1. **Critical Errors** - Immediate alert on CRITICAL/FATAL logs
2. **Database Failures** - 3+ failures in 5 minutes
3. **LLM API Failures** - 5+ failures in 5 minutes
4. **High Error Rate** - 50+ errors in 5 minutes
5. **Authentication Failures** - 10+ failures in 5 minutes
6. **Slow Requests** - 10+ slow requests in 5 minutes
7. **Memory Warnings** - 3+ warnings in 5 minutes
8. **Security Events** - Immediate alert on security violations

### Custom Alerts

```python
from src.observability.log_alerts import log_alert_manager, LogAlert

# Create custom alert
custom_alert = LogAlert(
    name="Custom Alert",
    pattern=r"your_pattern_here",
    severity="high",
    threshold=5,
    time_window_minutes=10,
    action=lambda matches: print(f"Alert triggered: {len(matches)} matches")
)

# Add to manager
log_alert_manager.add_alert(custom_alert)
```

### Kibana Alerts

1. Go to **Observability** → **Alerts**
2. Click **Create alert**
3. Select alert type
4. Configure conditions
5. Set actions (email, Slack, PagerDuty)
6. Save alert

---

## Log Retention

### Lifecycle Policy

Logs follow this lifecycle:

- **Hot** (0-7 days): Active indexing and searching
- **Warm** (7-30 days): Read-only, optimized storage
- **Cold** (30-90 days): Frozen, minimal resources
- **Delete** (>90 days): Automatically deleted

### Modify Retention

```python
# Update lifecycle policy
policy = {
    "policy": {
        "phases": {
            "hot": {"min_age": "0ms", "actions": {"rollover": {"max_age": "7d"}}},
            "warm": {"min_age": "7d", "actions": {"shrink": {"number_of_shards": 1}}},
            "cold": {"min_age": "30d", "actions": {"freeze": {}}},
            "delete": {"min_age": "180d", "actions": {"delete": {}}}  # 6 months
        }
    }
}

# Apply policy
requests.put(
    "http://localhost:9200/_ilm/policy/logs-policy",
    json=policy
)
```

---

## Performance Optimization

### 1. Index Settings

```json
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "refresh_interval": "30s",
    "index.codec": "best_compression"
  }
}
```

### 2. Bulk Shipping

```python
# Ship logs in batches
from src.observability.log_aggregation import log_aggregator

logs = [
    {"level": "INFO", "message": "Log 1"},
    {"level": "INFO", "message": "Log 2"},
    # ... more logs
]

for log in logs:
    log_aggregator.ship(log)
```

### 3. Sampling

```python
# Sample high-volume logs
import random

if random.random() < 0.1:  # 10% sampling
    logger.debug("High volume debug log")
```

---

## Troubleshooting

### Logs Not Appearing

1. Check Elasticsearch health:
```bash
curl http://localhost:9200/_cluster/health
```

2. Check Logstash pipeline:
```bash
curl http://localhost:9600/_node/stats/pipelines
```

3. Check Filebeat status:
```bash
docker logs pca-filebeat
```

4. Verify log shipping:
```python
from src.observability.log_aggregation import log_aggregator
log_aggregator.ship({"level": "INFO", "message": "Test log"})
```

### High Memory Usage

1. Reduce Elasticsearch heap size:
```yaml
environment:
  - "ES_JAVA_OPTS=-Xms256m -Xmx256m"
```

2. Reduce Logstash heap size:
```yaml
environment:
  - "LS_JAVA_OPTS=-Xmx128m -Xms128m"
```

### Slow Queries

1. Add index to frequently searched fields
2. Use field data types appropriately
3. Limit search time range
4. Use filters instead of queries when possible

---

## Best Practices

### 1. Structured Logging
Always use structured logs with consistent fields:
```python
logger.bind(
    user_id=user_id,
    action=action,
    resource=resource
).info("Action performed")
```

### 2. Correlation IDs
Use correlation IDs to trace requests across services:
```python
from src.middleware.correlation_id import get_correlation_id

logger.bind(correlation_id=get_correlation_id()).info("Processing request")
```

### 3. Log Levels
Use appropriate log levels:
- **DEBUG**: Detailed information for debugging
- **INFO**: General informational messages
- **WARNING**: Warning messages
- **ERROR**: Error messages
- **CRITICAL**: Critical issues requiring immediate attention

### 4. Sensitive Data
Never log sensitive data:
```python
# Bad
logger.info(f"User password: {password}")

# Good
logger.info(f"User authenticated: {user_id}")
```

### 5. Performance
Avoid expensive operations in log messages:
```python
# Bad
logger.debug(f"Data: {expensive_serialization(data)}")

# Good
if logger.level("DEBUG").no >= logger.level("DEBUG").no:
    logger.debug(f"Data: {expensive_serialization(data)}")
```

---

## Monitoring

### Key Metrics

Monitor these metrics in Kibana:

1. **Log Volume**: Logs per second
2. **Error Rate**: Errors per minute
3. **Response Time**: P95, P99 latencies
4. **Top Errors**: Most common error types
5. **User Activity**: Active users, actions

### Dashboards

Create dashboards for:

1. **Application Health**: Error rates, response times
2. **User Activity**: Logins, actions, sessions
3. **Performance**: Slow requests, database queries
4. **Security**: Failed logins, suspicious activity
5. **Infrastructure**: Resource usage, service health

---

## Integration

### Slack Notifications

```python
def send_slack_alert(message: str):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(webhook_url, json={"text": message})

# Use in alert action
alert = LogAlert(
    name="Critical Alert",
    pattern=r"CRITICAL",
    severity="critical",
    threshold=1,
    action=lambda matches: send_slack_alert(f"Critical error: {matches[0]['message']}")
)
```

### PagerDuty Integration

```python
def send_pagerduty_alert(severity: str, message: str):
    api_key = os.getenv("PAGERDUTY_API_KEY")
    # PagerDuty API call
    pass
```

---

## Conclusion

The log aggregation system provides comprehensive centralized logging with:

- ✅ ELK Stack integration
- ✅ Splunk integration
- ✅ Automatic log enrichment
- ✅ Pattern-based alerting
- ✅ Lifecycle management
- ✅ Performance optimization

**Status**: Production-ready log aggregation system!
