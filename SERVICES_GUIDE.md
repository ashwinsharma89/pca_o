# Complete Services Deployment Guide

## 🎯 Services Overview

Your complete PCA Agent stack includes:

### Core Services
1. **pca-api** - Main FastAPI backend (port 8000)
2. **pca-postgres** - PostgreSQL database
3. **pca-redis** - Redis cache

### Background Processing
4. **celery-worker** - Async task processing
5. **celery-beat** - Scheduled task scheduler
6. **flower** - Celery monitoring UI (port 5555)

### Observability
7. **jaeger** - Distributed tracing UI (port 16686)

## 🚀 Quick Deploy All Services

```bash
cd ~/Desktop/pca_agent_copy

# Make script executable
chmod +x deploy-scripts/deploy-all-services.sh

# Deploy everything
./deploy-scripts/deploy-all-services.sh
```

This will:
- Upload the complete docker-compose configuration
- Start all 7 services
- Show status of each service

## 🎛️ Service Management

Use the management script for easy control:

```bash
chmod +x deploy-scripts/manage-services.sh

# Show status of all services
./deploy-scripts/manage-services.sh status

# Restart a specific service
./deploy-scripts/manage-services.sh restart pca-api

# View logs
./deploy-scripts/manage-services.sh logs celery-worker

# Stop all services
./deploy-scripts/manage-services.sh stop

# Start all services
./deploy-scripts/manage-services.sh start
```

## 🌐 Access Points

After deployment, you can access:

| Service | URL | Description |
|---------|-----|-------------|
| API | http://13.200.71.62:8000 | Main API endpoints |
| API Docs | http://13.200.71.62:8000/docs | Swagger UI |
| Health Check | http://13.200.71.62:8000/health | API health status |
| Flower | http://13.200.71.62:5555 | Celery task monitoring |
| Jaeger | http://13.200.71.62:16686 | Distributed tracing |

## 📊 Service Details

### pca-api (Main API)
- **Port**: 8000
- **Memory**: 4GB limit, 2GB reserved
- **Health Check**: Built-in at /health
- **Dependencies**: PostgreSQL, Redis

### celery-worker (Background Tasks)
- **Concurrency**: 2 workers
- **Memory**: 2GB limit, 1GB reserved
- **Purpose**: Process async tasks (data ingestion, reports, etc.)

### celery-beat (Scheduler)
- **Memory**: 512MB limit
- **Purpose**: Schedule periodic tasks

### flower (Monitoring)
- **Port**: 5555
- **Memory**: 512MB limit
- **Purpose**: Monitor Celery tasks, workers, and queues

### jaeger (Tracing)
- **Port**: 16686 (UI), 6831 (agent)
- **Memory**: 512MB limit
- **Purpose**: Distributed tracing and performance monitoring

## 🔍 Monitoring & Debugging

### Check All Services
```bash
ssh ubuntu@13.200.71.62 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml ps'
```

### View Logs
```bash
# All services
ssh ubuntu@13.200.71.62 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml logs --tail=50'

# Specific service
ssh ubuntu@13.200.71.62 'docker logs pca-api --tail 50'
ssh ubuntu@13.200.71.62 'docker logs pca-celery-worker --tail 50'
```

### Check Resource Usage
```bash
ssh ubuntu@13.200.71.62 'docker stats --no-stream'
```

### Test Celery Tasks
```bash
# Access Flower UI
open http://13.200.71.62:5555

# Or check from command line
ssh ubuntu@13.200.71.62 'docker exec pca-celery-worker celery -A src.workers.ingestion_worker:celery_app inspect active'
```

## 🔧 Troubleshooting

### Service Won't Start
```bash
# Check logs
./deploy-scripts/manage-services.sh logs [service-name]

# Check if port is in use
ssh ubuntu@13.200.71.62 'netstat -tlnp | grep -E "8000|5555|16686"'

# Restart service
./deploy-scripts/manage-services.sh restart [service-name]
```

### High Memory Usage
```bash
# Check resource usage
ssh ubuntu@13.200.71.62 'docker stats --no-stream'

# Restart heavy services
./deploy-scripts/manage-services.sh restart pca-api
./deploy-scripts/manage-services.sh restart celery-worker
```

### Celery Tasks Not Processing
```bash
# Check worker status
ssh ubuntu@13.200.71.62 'docker logs pca-celery-worker --tail 50'

# Check Redis connection
ssh ubuntu@13.200.71.62 'docker exec pca-redis redis-cli ping'

# Restart worker
./deploy-scripts/manage-services.sh restart celery-worker
```

## 📈 Scaling

### Add More Celery Workers
Edit `docker-compose.full.yml` and add:
```yaml
celery-worker-2:
  image: pca-agent:latest
  container_name: pca-celery-worker-2
  # ... same config as celery-worker
```

### Increase Worker Concurrency
Change in docker-compose.full.yml:
```yaml
command: celery -A src.workers.ingestion_worker:celery_app worker --loglevel=info --concurrency=4
```

## 🔐 Security Notes

- All services run on internal network (pca-network)
- Only API, Flower, and Jaeger ports are exposed
- Secrets are passed via environment variables
- Services run as non-root users where possible

## 📝 Configuration

The docker-compose file is located at:
- Local: `~/Desktop/pca_agent_copy/docker-compose.full.yml`
- EC2: `/home/ubuntu/pca_o/docker/docker-compose.full.yml`

To modify configuration:
1. Edit local `docker-compose.full.yml`
2. Run `./deploy-scripts/deploy-all-services.sh` to update

## ✅ Health Checks

All services have health checks:
- **API**: HTTP check on /health endpoint
- **PostgreSQL**: pg_isready command
- **Redis**: redis-cli ping
- **Celery**: Monitored via Flower

## 🎯 Next Steps

1. **Deploy all services**: Run `./deploy-scripts/deploy-all-services.sh`
2. **Verify deployment**: Check all access points
3. **Monitor**: Use Flower and Jaeger UIs
4. **Test**: Submit some tasks and watch them process

Enjoy your complete production stack! 🚀
