# Architecture Improvements - Deployment Guide

## Quick Start (Today!)

### 1. Install Dependencies
```bash
# Install new dependencies
pip install 'celery[redis]==5.3.4' flower==2.0.1 \
    opentelemetry-api==1.21.0 opentelemetry-sdk==1.21.0 \
    opentelemetry-instrumentation-fastapi==0.42b0 \
    opentelemetry-exporter-jaeger==1.21.0 httpx==0.25.2
```

### 2. Start Infrastructure with Docker Compose
```bash
# Start all services (Redis, Jaeger, API, Celery, Flower)
docker-compose up -d

# View logs
docker-compose logs -f

# Check status
docker-compose ps
```

### 3. Access Services

| Service | URL | Purpose |
|---------|-----|---------|
| **API** | http://localhost:8000 | Main API |
| **API Docs** | http://localhost:8000/api/docs | Swagger UI |
| **Jaeger UI** | http://localhost:16686 | Distributed tracing |
| **Flower** | http://localhost:5555 | Celery monitoring |
| **Redis** | localhost:6379 | Message broker |

---

## What's Implemented

### ✅ 1. API Gateway
**File**: `src/gateway/api_gateway.py`

**Features**:
- Rate limiting (100 requests/minute per IP)
- Authentication checking
- Request validation
- Gateway headers (processing time, version)
- Statistics endpoint

**Usage**:
```python
# Automatically integrated in src/api/main.py
# All requests go through gateway middleware
```

**Test**:
```bash
# Test rate limiting
for i in {1..150}; do curl http://localhost:8000/api/v1/campaigns; done
# Should see 429 after 100 requests

# Check gateway stats
curl http://localhost:8000/gateway/stats
```

---

### ✅ 2. Distributed Tracing (OpenTelemetry + Jaeger)
**File**: `src/utils/opentelemetry_config.py`

**Features**:
- Automatic request tracing
- FastAPI instrumentation
- HTTP client instrumentation
- Jaeger export

**View Traces**:
1. Make some API requests
2. Open http://localhost:16686
3. Select "pca-agent" service
4. Click "Find Traces"

**Add Custom Spans**:
```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

@app.get("/my-endpoint")
async def my_endpoint():
    with tracer.start_as_current_span("custom_operation"):
        # Your code here
        result = do_something()
    return result
```

---

### ✅ 3. Celery Async Tasks
**Files**:
- `src/tasks/celery_app.py` - Celery configuration
- `src/tasks/analytics.py` - Analytics tasks
- `src/tasks/maintenance.py` - Maintenance tasks
- `src/tasks/reports.py` - Report generation tasks

**Features**:
- Async report generation
- Bulk data processing
- Scheduled maintenance (cleanup, backups)
- Health checks every 5 minutes

**Available Tasks**:
1. `generate_campaign_report` - Generate analysis report
2. `process_bulk_upload` - Process bulk CSV/Excel uploads
3. `cleanup_old_reports` - Daily cleanup (2 AM)
4. `refresh_benchmarks` - Daily refresh (midnight)
5. `health_check` - Every 5 minutes
6. `generate_pdf_report` - PDF generation
7. `generate_excel_report` - Excel generation

**Usage**:
```python
# In your endpoint
from src.tasks.analytics import generate_campaign_report

@app.post("/analyze-async")
async def analyze_async(data: dict):
    task = generate_campaign_report.delay(data, user_id="123")
    return {
        "task_id": task.id,
        "status": "processing"
    }

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    from src.tasks.celery_app import celery_app
    task = celery_app.AsyncResult(task_id)
    return {
        "status": task.status,
        "result": task.result if task.ready() else None
    }
```

**Monitor Tasks**:
- Open http://localhost:5555 (Flower)
- See active tasks, workers, task history

---

### ✅ 4. Service Health Checks
**Integrated in Docker Compose**

**Features**:
- Redis health check
- Jaeger health check
- API health check
- Celery worker health check

**Check Health**:
```bash
# Check all services
docker-compose ps

# Check specific service
docker-compose exec api curl http://localhost:8000/health
```

---

## Docker Compose Services

### Services Overview
```
┌─────────────┐
│   API       │ :8000 - Main FastAPI app with Gateway
└──────┬──────┘
       │
       ├──────► Redis :6379 - Message broker
       ├──────► Jaeger :16686 - Tracing
       └──────► Celery Workers - Async tasks
                  └──► Flower :5555 - Monitoring
```

### Service Details

**redis**:
- Image: redis:7-alpine
- Port: 6379
- Volume: Persistent data storage
- Health check: Every 10s

**jaeger**:
- Image: jaegertracing/all-in-one
- Ports: 6831 (agent), 16686 (UI)
- Health check: Every 10s

**api**:
- Build: Current directory
- Port: 8000
- Auto-reload enabled
- Depends on: redis, jaeger

**celery-worker**:
- Concurrency: 2 workers
- Auto-restart on failure
- Depends on: redis

**celery-beat**:
- Scheduler for periodic tasks
- Runs cleanup, backups, health checks

**flower**:
- Celery monitoring dashboard
- Port: 5555

---

## Configuration

### Environment Variables
Create `.env` file:
```env
# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Jaeger
JAEGER_AGENT_HOST=localhost
JAEGER_AGENT_PORT=6831

# API
API_HOST=0.0.0.0
API_PORT=8000
```

### Docker Compose Override
Create `docker-compose.override.yml` for local customization:
```yaml
version: '3.8'

services:
  api:
    environment:
      - DEBUG=true
      - LOG_LEVEL=debug
```

---

## Testing

### 1. Test API Gateway
```bash
# Rate limiting
for i in {1..150}; do 
    curl -s http://localhost:8000/api/v1/campaigns | head -1
done

# Should see rate limit after 100 requests
```

### 2. Test Distributed Tracing
```bash
# Make requests
curl http://localhost:8000/api/v1/campaigns
curl http://localhost:8000/api/v1/campaigns/suggested-questions

# View in Jaeger
open http://localhost:16686
```

### 3. Test Celery Tasks
```bash
# Start Python shell
docker-compose exec api python

# In Python:
from src.tasks.analytics import generate_campaign_report
task = generate_campaign_report.delay({"test": "data"}, "user123")
print(f"Task ID: {task.id}")
print(f"Status: {task.status}")

# Check Flower
open http://localhost:5555
```

---

## Monitoring

### API Gateway Stats
```bash
curl http://localhost:8000/gateway/stats
```

### Celery Monitoring (Flower)
- Active tasks
- Task history
- Worker status
- Task rates
- Success/failure rates

### Distributed Tracing (Jaeger)
- Request traces
- Service dependencies
- Performance bottlenecks
- Error tracking

---

## Troubleshooting

### Redis Connection Issues
```bash
# Check Redis is running
docker-compose ps redis

# Test connection
docker-compose exec redis redis-cli ping
# Should return: PONG
```

### Celery Worker Not Starting
```bash
# Check logs
docker-compose logs celery-worker

# Restart worker
docker-compose restart celery-worker
```

### Jaeger Not Showing Traces
```bash
# Check Jaeger is running
docker-compose ps jaeger

# Check environment variables
docker-compose exec api env | grep JAEGER
```

### Rate Limiting Not Working
```bash
# Check gateway is initialized
docker-compose logs api | grep "API Gateway"
# Should see: "✅ API Gateway initialized"
```

---

## Production Deployment

### Security Enhancements
1. **Enable JWT Authentication**:
   - Uncomment JWT validation in `api_gateway.py`
   - Add JWT secret to environment

2. **HTTPS Only**:
   - Add SSL certificates
   - Configure reverse proxy (nginx)

3. **Rate Limiting**:
   - Adjust limits based on traffic
   - Use Redis for distributed rate limiting

### Scaling
1. **Horizontal Scaling**:
   ```bash
   # Scale workers
   docker-compose up -d --scale celery-worker=4
   ```

2. **Load Balancing**:
   - Add nginx reverse proxy
   - Configure multiple API instances

### Monitoring
1. **Add Prometheus**:
   - Metrics collection
   - Alerting

2. **Add Grafana**:
   - Dashboards
   - Visualization

---

## Cost Summary

| Component | Free Tier | Paid (if needed) |
|-----------|-----------|------------------|
| Redis | ✅ Docker | $10-50/month |
| Jaeger | ✅ Docker | $20-100/month |
| Celery | ✅ Free | - |
| Flower | ✅ Free | - |
| **Total** | **$0** | **$30-150/month** |

---

## Next Steps

1. ✅ **Today**: Start with `docker-compose up -d`
2. **Week 1**: Test all features, monitor performance
3. **Week 2**: Add custom async tasks for your use cases
4. **Week 3**: Configure production security (JWT, HTTPS)
5. **Week 4**: Set up monitoring and alerting

---

## Support

### Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api
docker-compose logs -f celery-worker
```

### Restart Services
```bash
# All
docker-compose restart

# Specific
docker-compose restart api
docker-compose restart celery-worker
```

### Stop Everything
```bash
docker-compose down

# With volumes (clean slate)
docker-compose down -v
```

---

**🎉 All architecture improvements are now deployed and ready to use!**
