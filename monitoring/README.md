# Prometheus & Grafana Monitoring Setup

## Quick Start

### 1. Start Monitoring Stack

```bash
cd monitoring
docker-compose up -d
```

This starts:
- **Prometheus** on http://localhost:9090
- **Grafana** on http://localhost:3000 (admin/admin)
- **Alertmanager** on http://localhost:9093

### 2. Enable Metrics in Your Application

```python
from src.monitoring.prometheus_metrics import (
    record_agent_execution,
    record_agent_confidence,
    update_agent_fallback_rate,
    record_agent_stats
)

# In your agent code
import time

start_time = time.time()
result = agent.execute('analyze', data)
duration = time.time() - start_time

# Record metrics
record_agent_execution(
    agent_name="ReasoningAgent",
    duration=duration,
    status='success',
    used_fallback=False
)

# Record confidence
if hasattr(result, 'overall_confidence'):
    record_agent_confidence("ReasoningAgent", result.overall_confidence)

# Record stats periodically
stats = agent.get_stats()
record_agent_stats("ReasoningAgent", stats)
```

### 3. Access Dashboards

1. Open Grafana: http://localhost:3000
2. Login: admin/admin
3. Navigate to Dashboards → PCA Agent Monitoring - Phase 2

---

## Dashboard Panels

### 1. Agent Fallback Rate (Gauge)
- **Metric**: `agent_fallback_rate`
- **Thresholds**: 
  - Green: 0-10%
  - Yellow: 10-20%
  - Red: >20%
- **Alert**: Triggers at >20%

### 2. Agent Execution Rate (Time Series)
- **Metric**: `rate(agent_executions_total[5m])`
- **Shows**: Executions per second by agent and status

### 3. Average Confidence Score (Gauge)
- **Metric**: `agent_avg_confidence`
- **Thresholds**:
  - Red: <0.5
  - Yellow: 0.5-0.7
  - Green: >0.7

### 4. Executions by Agent (Pie Chart)
- **Metric**: `sum by (agent_name) (agent_executions_total)`
- **Shows**: Distribution of executions across agents

### 5. Agent Execution Duration (Time Series)
- **Metrics**: p50 and p95 latencies
- **Shows**: Performance trends over time

### 6. Circuit Breaker State (Stat)
- **Metric**: `circuit_breaker_state`
- **Values**: CLOSED (green), HALF_OPEN (yellow), OPEN (red)

### 7. Agent Health Status (Stat)
- **Metric**: `agent_health_status`
- **Values**: HEALTHY (green), DEGRADED (yellow), UNHEALTHY (red)

---

## Alerts

### Configured Alerts

1. **HighAgentFallbackRate** (Warning)
   - Condition: Fallback rate > 20% for 5 minutes
   - Action: Investigate primary agent failures

2. **CriticalAgentFallbackRate** (Critical)
   - Condition: Fallback rate > 50% for 2 minutes
   - Action: Immediate investigation required

3. **LowAgentConfidence** (Warning)
   - Condition: Average confidence < 0.5 for 10 minutes
   - Action: Review data quality and pattern detection

4. **AgentUnhealthy** (Critical)
   - Condition: Health status = UNHEALTHY for 5 minutes
   - Action: Check agent logs and restart if needed

5. **CircuitBreakerOpen** (Warning)
   - Condition: Circuit breaker OPEN for 5 minutes
   - Action: Check external service health

6. **HighAgentExecutionDuration** (Warning)
   - Condition: p95 latency > 5s for 10 minutes
   - Action: Optimize agent logic or add caching

7. **NoAgentExecutions** (Warning)
   - Condition: No executions for 15 minutes
   - Action: Check if agent is receiving requests

8. **HighAgentErrorRate** (Critical)
   - Condition: Error rate > 10% for 5 minutes
   - Action: Investigate errors immediately

---

## Metrics Reference

### Agent Execution Metrics
- `agent_executions_total{agent_name, status}` - Counter
- `agent_fallback_total{agent_name}` - Counter
- `agent_execution_duration_seconds{agent_name}` - Histogram

### Confidence Metrics
- `agent_confidence_score{agent_name}` - Histogram
- `agent_avg_confidence{agent_name}` - Gauge

### Fallback & Health
- `agent_fallback_rate{agent_name}` - Gauge (percentage)
- `agent_health_status{agent_name}` - Gauge (0/0.5/1)

### Output Metrics
- `agent_insights_count{agent_name}` - Gauge
- `agent_recommendations_count{agent_name}` - Gauge

### Circuit Breaker
- `circuit_breaker_state{service_name}` - Gauge (0/1/2)
- `circuit_breaker_failures_total{service_name}` - Counter

---

## Example Queries

### Fallback Rate Over Time
```promql
agent_fallback_rate
```

### Agent Success Rate
```promql
rate(agent_executions_total{status="success"}[5m]) 
/ 
rate(agent_executions_total[5m])
```

### Average Execution Time
```promql
rate(agent_execution_duration_seconds_sum[5m]) 
/ 
rate(agent_execution_duration_seconds_count[5m])
```

### Top 5 Slowest Agents
```promql
topk(5, 
  histogram_quantile(0.95, 
    sum(rate(agent_execution_duration_seconds_bucket[5m])) by (le, agent_name)
  )
)
```

---

## Troubleshooting

### Metrics Not Showing Up

1. Check if Prometheus is scraping:
   ```bash
   curl http://localhost:9090/api/v1/targets
   ```

2. Verify metrics endpoint:
   ```bash
   curl http://localhost:8000/metrics | grep agent_
   ```

3. Check Prometheus logs:
   ```bash
   docker logs pca-prometheus
   ```

### Dashboard Not Loading

1. Check Grafana datasource:
   - Go to Configuration → Data Sources
   - Verify Prometheus URL: http://prometheus:9090

2. Import dashboard manually:
   - Go to Dashboards → Import
   - Upload `monitoring/grafana/dashboards/agent_monitoring.json`

### Alerts Not Firing

1. Check alert rules:
   ```bash
   curl http://localhost:9090/api/v1/rules
   ```

2. Verify Alertmanager:
   ```bash
   curl http://localhost:9093/api/v1/status
   ```

---

## Production Recommendations

1. **Data Retention**: Configure Prometheus retention (default: 15 days)
   ```yaml
   command:
     - '--storage.tsdb.retention.time=30d'
   ```

2. **Backup**: Regularly backup Prometheus data
   ```bash
   docker exec pca-prometheus promtool tsdb snapshot /prometheus
   ```

3. **Security**: Change default Grafana password
   ```bash
   docker exec -it pca-grafana grafana-cli admin reset-admin-password <new-password>
   ```

4. **Scaling**: For high-volume metrics, consider:
   - Prometheus federation
   - Thanos for long-term storage
   - VictoriaMetrics as alternative

---

## Integration with Existing Code

Add to your agent wrapper:

```python
from src.monitoring.prometheus_metrics import (
    record_agent_execution,
    record_agent_confidence,
    record_agent_stats
)
import time

class MonitoredAgentFallback(AgentFallback):
    def execute(self, method_name: str, *args, **kwargs):
        start_time = time.time()
        status = 'success'
        used_fallback = False
        
        try:
            result = super().execute(method_name, *args, **kwargs)
            
            # Check if fallback was used
            stats = self.get_stats()
            used_fallback = stats['fallback_count'] > 0
            
            # Record confidence if available
            if hasattr(result, 'overall_confidence'):
                record_agent_confidence(self.name, result.overall_confidence)
            
            return result
            
        except Exception as e:
            status = 'error'
            raise
            
        finally:
            duration = time.time() - start_time
            record_agent_execution(
                agent_name=self.name,
                duration=duration,
                status=status,
                used_fallback=used_fallback
            )
            
            # Update stats
            stats = self.get_stats()
            record_agent_stats(self.name, stats)
```

---

## Next Steps

1. ✅ Start monitoring stack
2. ✅ Verify metrics are being collected
3. ✅ Set up alert notifications (email/Slack)
4. ✅ Create custom dashboards for specific use cases
5. ✅ Set up automated backups
