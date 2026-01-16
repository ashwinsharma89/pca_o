# Phase 2 Integration - Deployment Guide

## Quick Start

### 1. Import the Components

```python
# Validation schemas
from src.agents.schemas import (
    AgentOutput,
    AgentInsight,
    AgentRecommendation,
    filter_high_confidence_insights,
    filter_high_confidence_recommendations
)

# Resilience mechanisms
from src.agents.agent_resilience import (
    AgentFallback,
    retry_with_backoff,
    CircuitBreaker
)

# Validated agents
from src.agents.validated_reasoning_agent import ValidatedReasoningAgent
from src.agents.resilient_orchestrator import ResilientMultiAgentOrchestrator
```

### 2. Basic Usage

```python
# Option A: Simple fallback
from src.agents.agent_resilience import AgentFallback
from src.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
from src.agents.reasoning_agent import ReasoningAgent

primary = EnhancedReasoningAgent()
fallback = ReasoningAgent()

resilient_agent = AgentFallback(primary, fallback, name="ReasoningAgent")
result = resilient_agent.execute('analyze', campaign_data)

# Option B: Full orchestration
from src.agents.resilient_orchestrator import ResilientMultiAgentOrchestrator

orchestrator = ResilientMultiAgentOrchestrator(rag, benchmarks)
result = await orchestrator.run(query, data, platform)
```

### 3. Monitor Health

```python
# Get fallback statistics
stats = resilient_agent.get_stats()
print(f"Fallback rate: {stats['fallback_rate_percent']}%")

# Get health status
health = orchestrator.get_health_status()
if health['reasoning_agent']['status'] == 'degraded':
    print("⚠️ Agent performance degraded, consider investigation")
```

## Production Checklist

- [ ] Test with real campaign data
- [ ] Configure fallback agents
- [ ] Set confidence thresholds
- [ ] Enable monitoring
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

## Configuration

### Confidence Thresholds

```python
# Filter high-confidence outputs
HIGH_CONFIDENCE_THRESHOLD = 0.8
MEDIUM_CONFIDENCE_THRESHOLD = 0.6

high_conf_insights = filter_high_confidence_insights(
    output, 
    threshold=HIGH_CONFIDENCE_THRESHOLD
)
```

### Retry Configuration

```python
@retry_with_backoff(
    max_retries=3,           # Number of retry attempts
    initial_delay=1.0,       # Initial delay in seconds
    backoff_factor=2.0,      # Exponential backoff multiplier
    max_delay=60.0          # Maximum delay cap
)
async def analyze_campaign(data):
    return await agent.analyze(data)
```

### Circuit Breaker Configuration

```python
circuit_breaker = CircuitBreaker(
    failure_threshold=5,     # Open after 5 failures
    recovery_timeout=60.0,   # Wait 60s before retry
    expected_exception=ValueError
)
```

## Monitoring Metrics

Track these metrics in production:

1. **Fallback Rate**: `fallback_count / total_executions`
   - Target: < 5%
   - Alert: > 20%

2. **Overall Confidence**: Average of `overall_confidence`
   - Target: > 0.7
   - Alert: < 0.5

3. **Circuit Breaker State**: Monitor for `OPEN` state
   - Alert: Any circuit breaker open for > 5 minutes

4. **Execution Time**: `execution_time_ms`
   - Target: < 2000ms
   - Alert: > 5000ms

## Troubleshooting

### High Fallback Rate

**Symptom**: Fallback rate > 20%

**Causes**:
- Primary agent failing frequently
- External service (RAG, Benchmarks) unavailable
- Data quality issues

**Solutions**:
1. Check primary agent logs
2. Verify external service connectivity
3. Review input data quality
4. Consider adjusting fallback threshold

### Low Confidence Scores

**Symptom**: Overall confidence < 0.5

**Causes**:
- Insufficient data
- Conflicting patterns
- Weak statistical significance

**Solutions**:
1. Increase data collection period
2. Review pattern detection thresholds
3. Add more context (benchmarks, RAG)

### Circuit Breaker Stuck Open

**Symptom**: Circuit breaker in OPEN state for extended period

**Causes**:
- External service down
- Network issues
- Configuration error

**Solutions**:
1. Check external service health
2. Verify network connectivity
3. Manually reset: `orchestrator.reset_circuit_breakers()`

## Examples

See `examples/phase2_integration_examples.py` for complete working examples.

## Support

For issues or questions:
1. Check logs for detailed error messages
2. Review health status: `orchestrator.get_health_status()`
3. Check resilience stats: `agent.get_stats()`
