# Alertmanager Environment Variables

The Alertmanager configuration uses environment variables for sensitive credentials.
Set these before starting the alertmanager container.

## Required Environment Variables

```bash
# Slack Integration
export ALERTMANAGER_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Email Integration (Gmail example)
export ALERTMANAGER_EMAIL_FROM="alerts@yourdomain.com"
export ALERTMANAGER_EMAIL_USER="your-email@gmail.com"
export ALERTMANAGER_EMAIL_PASSWORD="your-app-password"
export ALERTMANAGER_EMAIL_TO="team@yourdomain.com"
```

## Docker Compose Configuration

Add to your `docker-compose.yml` for alertmanager:

```yaml
services:
  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager:/etc/alertmanager
    environment:
      - ALERTMANAGER_SLACK_WEBHOOK_URL=${ALERTMANAGER_SLACK_WEBHOOK_URL}
      - ALERTMANAGER_EMAIL_FROM=${ALERTMANAGER_EMAIL_FROM}
      - ALERTMANAGER_EMAIL_USER=${ALERTMANAGER_EMAIL_USER}
      - ALERTMANAGER_EMAIL_PASSWORD=${ALERTMANAGER_EMAIL_PASSWORD}
      - ALERTMANAGER_EMAIL_TO=${ALERTMANAGER_EMAIL_TO}
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
```

## Alert Routing Summary

| Severity | Receiver | Repeat Interval |
|----------|----------|-----------------|
| Critical | Slack #pca-alerts-critical + Email | 1 hour |
| Warning | Slack #pca-alerts | 4 hours |
| Info | Default (no notification) | 4 hours |

## Testing Alerts

To manually trigger a test alert:
```bash
curl -H "Content-Type: application/json" -d '[
  {
    "labels": {"alertname": "TestAlert", "severity": "warning"},
    "annotations": {"summary": "Test alert", "description": "This is a test"}
  }
]' http://localhost:9093/api/v1/alerts
```
