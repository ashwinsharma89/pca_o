# Docker Setup for PCA Agent

Complete Docker containerization with PostgreSQL, Redis, monitoring, and orchestration.

---

## 🐳 What's Included

### Core Services
- **PCA Agent API** - FastAPI backend (port 8000)
- **Streamlit UI** - Web interface (port 8501)
- **PostgreSQL** - Database (port 5432)
- **Redis** - Caching & rate limiting (port 6379)

### Monitoring Stack
- **Prometheus** - Metrics collection (port 9090)
- **Grafana** - Dashboards & visualization (port 3000)

---

## 📋 Prerequisites

1. **Docker Desktop** installed
   - Windows: https://docs.docker.com/desktop/install/windows-install/
   - Mac: https://docs.docker.com/desktop/install/mac-install/
   - Linux: https://docs.docker.com/engine/install/

2. **Docker Compose** (included with Docker Desktop)

3. **API Keys** (optional but recommended)
   - OpenAI API key
   - Anthropic API key
   - Google Gemini API key

---

## 🚀 Quick Start

### Step 1: Configure Environment

Copy the example environment file:
```bash
cp .env.docker .env
```

Edit `.env` and add your API keys:
```env
# API Keys
OPENAI_API_KEY=sk-your-openai-key-here
ANTHROPIC_API_KEY=sk-ant-your-anthropic-key-here
GEMINI_API_KEY=your-gemini-key-here

# Security (CHANGE THESE!)
DB_PASSWORD=your-secure-db-password
REDIS_PASSWORD=your-secure-redis-password
JWT_SECRET_KEY=your-super-secret-jwt-key-min-32-chars
```

### Step 2: Build and Start

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Check status
docker-compose ps
```

### Step 3: Initialize Database

```bash
# Run database initialization
docker-compose exec api python scripts/init_database.py

# Create first admin user
docker-compose exec api python scripts/init_users.py
```

### Step 4: Access Services

- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/api/docs
- **Streamlit UI**: http://localhost:8501
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

---

## 🔧 Docker Commands

### Service Management

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d api

# Stop all services
docker-compose down

# Stop and remove volumes (⚠️ deletes data)
docker-compose down -v

# Restart a service
docker-compose restart api

# View logs
docker-compose logs -f api
docker-compose logs -f postgres
```

### Build & Update

```bash
# Rebuild after code changes
docker-compose build

# Rebuild specific service
docker-compose build api

# Pull latest images
docker-compose pull

# Rebuild and restart
docker-compose up -d --build
```

### Database Operations

```bash
# Access PostgreSQL
docker-compose exec postgres psql -U pca_user -d pca_agent

# Backup database
docker-compose exec postgres pg_dump -U pca_user pca_agent > backup.sql

# Restore database
docker-compose exec -T postgres psql -U pca_user pca_agent < backup.sql

# View database logs
docker-compose logs -f postgres
```

### Redis Operations

```bash
# Access Redis CLI
docker-compose exec redis redis-cli -a redis_secure_password

# Monitor Redis
docker-compose exec redis redis-cli -a redis_secure_password MONITOR

# Check Redis info
docker-compose exec redis redis-cli -a redis_secure_password INFO
```

### Application Commands

```bash
# Run tests
docker-compose exec api pytest

# Access Python shell
docker-compose exec api python

# Run migrations
docker-compose exec api alembic upgrade head

# View application logs
docker-compose exec api tail -f logs/app.log
```

---

## 📊 Monitoring

### Prometheus

Access Prometheus at http://localhost:9090

**Key Metrics**:
- `api_requests_total` - Total API requests
- `api_errors_total` - Total errors
- `response_time_ms` - Response times
- `llm_tokens_total` - LLM token usage
- `llm_cost_usd` - LLM costs

### Grafana

Access Grafana at http://localhost:3000

**Default Credentials**: admin / admin (change on first login)

**Setup**:
1. Login to Grafana
2. Prometheus datasource is pre-configured
3. Import dashboards from `monitoring/grafana/dashboards/`

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Network                       │
│                                                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │ Streamlit│───▶│    API   │───▶│PostgreSQL│         │
│  │  :8501   │    │  :8000   │    │  :5432   │         │
│  └──────────┘    └──────────┘    └──────────┘         │
│                        │                                 │
│                        ▼                                 │
│                   ┌──────────┐                          │
│                   │  Redis   │                          │
│                   │  :6379   │                          │
│                   └──────────┘                          │
│                        │                                 │
│                        ▼                                 │
│  ┌──────────┐    ┌──────────┐                          │
│  │ Grafana  │◀───│Prometheus│                          │
│  │  :3000   │    │  :9090   │                          │
│  └──────────┘    └──────────┘                          │
└─────────────────────────────────────────────────────────┘
```

---

## 🔒 Security Best Practices

### 1. Change Default Passwords

```env
# .env
DB_PASSWORD=use-strong-password-here
REDIS_PASSWORD=use-strong-password-here
JWT_SECRET_KEY=use-random-32-char-string-here
GRAFANA_PASSWORD=change-from-admin
```

### 2. Use Secrets Management (Production)

```yaml
# docker-compose.yml
services:
  api:
    secrets:
      - db_password
      - jwt_secret

secrets:
  db_password:
    file: ./secrets/db_password.txt
  jwt_secret:
    file: ./secrets/jwt_secret.txt
```

### 3. Network Isolation

```yaml
# Expose only necessary ports
services:
  postgres:
    ports: []  # Don't expose to host
```

### 4. Read-Only Filesystem

```yaml
services:
  api:
    read_only: true
    tmpfs:
      - /tmp
      - /app/logs
```

---

## 🔄 Development vs Production

### Development Setup

```yaml
# docker-compose.dev.yml
services:
  api:
    volumes:
      - .:/app  # Mount source code
    command: uvicorn src.api.main_v3:app --reload --host 0.0.0.0
```

Run with:
```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

### Production Setup

```yaml
# docker-compose.prod.yml
services:
  api:
    restart: always
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '1'
          memory: 1G
```

Run with:
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

---

## 📦 Volumes

### Data Persistence

```bash
# List volumes
docker volume ls

# Inspect volume
docker volume inspect pca_agent_postgres_data

# Backup volume
docker run --rm -v pca_agent_postgres_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/postgres_backup.tar.gz /data

# Restore volume
docker run --rm -v pca_agent_postgres_data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/postgres_backup.tar.gz -C /
```

### Volume Locations

- `postgres_data` - PostgreSQL database
- `redis_data` - Redis persistence
- `prometheus_data` - Prometheus metrics
- `grafana_data` - Grafana dashboards

---

## 🐛 Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs api

# Check service health
docker-compose ps

# Restart service
docker-compose restart api
```

### Database Connection Issues

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Test connection
docker-compose exec postgres pg_isready -U pca_user

# Check logs
docker-compose logs postgres
```

### Redis Connection Issues

```bash
# Check Redis is running
docker-compose ps redis

# Test connection
docker-compose exec redis redis-cli -a redis_secure_password ping

# Check logs
docker-compose logs redis
```

### Port Already in Use

```bash
# Find process using port
netstat -ano | findstr :8000

# Kill process (Windows)
taskkill /PID <PID> /F

# Or change port in docker-compose.yml
ports:
  - "8001:8000"  # Use different host port
```

### Out of Disk Space

```bash
# Clean up Docker
docker system prune -a

# Remove unused volumes
docker volume prune

# Check disk usage
docker system df
```

---

## 🚀 Deployment

### Deploy to Cloud

#### AWS ECS
```bash
# Install ECS CLI
ecs-cli compose --file docker-compose.yml up

# Or use AWS Copilot
copilot init
copilot deploy
```

#### Google Cloud Run
```bash
# Build and push
docker build -t gcr.io/PROJECT_ID/pca-agent .
docker push gcr.io/PROJECT_ID/pca-agent

# Deploy
gcloud run deploy pca-agent --image gcr.io/PROJECT_ID/pca-agent
```

#### Azure Container Instances
```bash
# Create container group
az container create \
  --resource-group myResourceGroup \
  --file docker-compose.yml
```

### Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yml pca

# Scale service
docker service scale pca_api=3

# Update service
docker service update --image pca-agent:latest pca_api
```

### Kubernetes

```bash
# Convert to Kubernetes
kompose convert -f docker-compose.yml

# Apply to cluster
kubectl apply -f .
```

---

## 📈 Performance Tuning

### Resource Limits

```yaml
services:
  api:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G
```

### Connection Pooling

```yaml
services:
  api:
    environment:
      DB_POOL_SIZE: 20
      DB_MAX_OVERFLOW: 40
```

### Redis Optimization

```yaml
services:
  redis:
    command: >
      redis-server
      --maxmemory 512mb
      --maxmemory-policy allkeys-lru
      --appendonly yes
```

---

## 🧪 Testing

### Run Tests in Docker

```bash
# Run all tests
docker-compose exec api pytest

# Run with coverage
docker-compose exec api pytest --cov=src --cov-report=html

# Run specific test
docker-compose exec api pytest tests/unit/test_api_auth.py
```

### Integration Testing

```bash
# Start test environment
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
docker-compose exec api pytest tests/integration/

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

---

## 📝 Environment Variables

### Required

- `OPENAI_API_KEY` - OpenAI API key
- `DB_PASSWORD` - PostgreSQL password
- `JWT_SECRET_KEY` - JWT signing key

### Optional

- `ANTHROPIC_API_KEY` - Anthropic API key
- `GEMINI_API_KEY` - Google Gemini API key
- `REDIS_PASSWORD` - Redis password
- `LANGCHAIN_API_KEY` - LangSmith API key
- `GRAFANA_PASSWORD` - Grafana admin password

### Configuration

- `USE_SQLITE` - Use SQLite instead of PostgreSQL (default: false)
- `REDIS_ENABLED` - Enable Redis (default: true)
- `RATE_LIMIT_ENABLED` - Enable rate limiting (default: true)
- `LANGCHAIN_TRACING_V2` - Enable LangSmith tracing (default: false)

---

## 🎯 Next Steps

1. ✅ Configure environment variables
2. ✅ Start services with `docker-compose up -d`
3. ✅ Initialize database
4. ✅ Create admin user
5. ✅ Access API at http://localhost:8000
6. ✅ Set up Grafana dashboards
7. ✅ Configure backups
8. ✅ Deploy to production

---

## 📚 Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Docker Image](https://hub.docker.com/_/postgres)
- [Redis Docker Image](https://hub.docker.com/_/redis)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)

---

**Status**: ✅ **Docker setup complete and ready to use!**

Run `docker-compose up -d` to get started! 🚀
