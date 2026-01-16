# PCA Agent - Deployment Guide

Complete guide for deploying PCA Agent to production.

## 🎯 Deployment Options

### Option 1: Local Development (Current)
- **Use Case**: Testing, development
- **Setup Time**: 5 minutes
- **Scalability**: Single user
- **Cost**: Free (API costs only)

### Option 2: Cloud VM (Recommended for MVP)
- **Use Case**: Small team, MVP
- **Setup Time**: 30 minutes
- **Scalability**: 10-50 users
- **Cost**: $50-200/month

### Option 3: Container Orchestration (Production)
- **Use Case**: Enterprise, high scale
- **Setup Time**: 2-4 hours
- **Scalability**: 1000+ users
- **Cost**: $500-2000/month

---

## 🚀 Option 1: Local Development

### Prerequisites
- Python 3.11+
- 8GB RAM minimum
- OpenAI API key

### Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 3. Start API server
uvicorn src.api.main:app --reload --port 8000

# 4. Start Streamlit (separate terminal)
streamlit run streamlit_app.py
```

### Access
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Streamlit: http://localhost:8501

---

## ☁️ Option 2: Cloud VM Deployment

### Recommended Providers
- **AWS EC2**: t3.large (2 vCPU, 8GB RAM)
- **Google Cloud**: e2-standard-2
- **Azure**: Standard_B2s
- **DigitalOcean**: Basic Droplet ($24/month)

### Step-by-Step (Ubuntu 22.04)

#### 1. Provision VM

```bash
# SSH into your VM
ssh user@your-vm-ip

# Update system
sudo apt update && sudo apt upgrade -y
```

#### 2. Install Dependencies

```bash
# Install Python 3.11
sudo apt install python3.11 python3.11-venv python3-pip -y

# Install system dependencies
sudo apt install -y \
    build-essential \
    libpq-dev \
    redis-server \
    nginx \
    supervisor
```

#### 3. Setup Application

```bash
# Create app directory
sudo mkdir -p /opt/pca_agent
sudo chown $USER:$USER /opt/pca_agent
cd /opt/pca_agent

# Clone/upload your code
# (Upload via SCP, Git, or other method)

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt
pip install gunicorn
```

#### 4. Configure Environment

```bash
# Create .env file
nano .env

# Add your configuration:
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
DATABASE_URL=postgresql://user:pass@localhost:5432/pca_db
REDIS_URL=redis://localhost:6379/0
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=false
```

#### 5. Setup PostgreSQL (Optional but Recommended)

```bash
# Install PostgreSQL
sudo apt install postgresql postgresql-contrib -y

# Create database
sudo -u postgres psql
CREATE DATABASE pca_db;
CREATE USER pca_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE pca_db TO pca_user;
\q
```

#### 6. Configure Supervisor (Process Management)

```bash
# Create supervisor config
sudo nano /etc/supervisor/conf.d/pca_agent.conf
```

Add:
```ini
[program:pca_api]
directory=/opt/pca_agent
command=/opt/pca_agent/venv/bin/gunicorn src.api.main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
user=ubuntu
autostart=true
autorestart=true
stderr_logfile=/var/log/pca_agent/api.err.log
stdout_logfile=/var/log/pca_agent/api.out.log

[program:pca_streamlit]
directory=/opt/pca_agent
command=/opt/pca_agent/venv/bin/streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
user=ubuntu
autostart=true
autorestart=true
stderr_logfile=/var/log/pca_agent/streamlit.err.log
stdout_logfile=/var/log/pca_agent/streamlit.out.log
```

```bash
# Create log directory
sudo mkdir -p /var/log/pca_agent
sudo chown ubuntu:ubuntu /var/log/pca_agent

# Reload supervisor
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start all
```

#### 7. Configure Nginx (Reverse Proxy)

```bash
# Create Nginx config
sudo nano /etc/nginx/sites-available/pca_agent
```

Add:
```nginx
# API
server {
    listen 80;
    server_name api.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

# Streamlit
server {
    listen 80;
    server_name app.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
# Enable site
sudo ln -s /etc/nginx/sites-available/pca_agent /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

#### 8. Setup SSL (Let's Encrypt)

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx -y

# Get certificates
sudo certbot --nginx -d api.yourdomain.com -d app.yourdomain.com

# Auto-renewal is configured automatically
```

#### 9. Configure Firewall

```bash
# Allow HTTP, HTTPS, SSH
sudo ufw allow 22
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

### Access
- API: https://api.yourdomain.com
- Streamlit: https://app.yourdomain.com

---

## 🐳 Option 3: Docker Deployment

### Create Dockerfile

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose ports
EXPOSE 8000

# Run application
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Create docker-compose.yml

```yaml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - DATABASE_URL=postgresql://pca_user:pca_pass@db:5432/pca_db
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs

  streamlit:
    build: .
    command: streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
    ports:
      - "8501:8501"
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    depends_on:
      - api

  db:
    image: postgres:15
    environment:
      - POSTGRES_DB=pca_db
      - POSTGRES_USER=pca_user
      - POSTGRES_PASSWORD=pca_pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

### Deploy with Docker

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## 🔒 Security Checklist

### API Security
- [ ] Add JWT authentication
- [ ] Implement rate limiting
- [ ] Enable CORS properly
- [ ] Validate all inputs
- [ ] Sanitize file uploads
- [ ] Use HTTPS only

### Environment Security
- [ ] Store secrets in environment variables
- [ ] Use secrets management (AWS Secrets Manager, etc.)
- [ ] Rotate API keys regularly
- [ ] Enable firewall
- [ ] Keep dependencies updated

### Data Security
- [ ] Encrypt data at rest
- [ ] Encrypt data in transit
- [ ] Implement access controls
- [ ] Regular backups
- [ ] GDPR compliance (if applicable)

---

## 📊 Monitoring & Observability

### Application Monitoring

```python
# Add to requirements.txt
prometheus-client==0.19.0
sentry-sdk==1.40.0

# Add to src/api/main.py
from prometheus_client import Counter, Histogram, generate_latest
import sentry_sdk

# Initialize Sentry
sentry_sdk.init(dsn=settings.sentry_dsn)

# Add metrics endpoint
@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")
```

### Log Aggregation

**Option 1: ELK Stack**
- Elasticsearch
- Logstash
- Kibana

**Option 2: Cloud Services**
- AWS CloudWatch
- Google Cloud Logging
- Azure Monitor

### Uptime Monitoring
- UptimeRobot (free)
- Pingdom
- StatusCake

---

## 🚀 Performance Optimization

### 1. Caching

```python
# Add Redis caching
from redis import Redis
from functools import lru_cache

redis_client = Redis.from_url(settings.redis_url)

@lru_cache(maxsize=100)
def get_cached_analysis(campaign_id: str):
    # Cache analysis results
    pass
```

### 2. Async Processing

```python
# Use Celery for background tasks
from celery import Celery

celery_app = Celery('pca_agent', broker=settings.celery_broker_url)

@celery_app.task
def analyze_campaign_task(campaign_id: str):
    # Run analysis in background
    pass
```

### 3. Database Optimization
- Add indexes on frequently queried fields
- Use connection pooling
- Implement query caching

### 4. CDN for Static Assets
- CloudFlare
- AWS CloudFront
- Azure CDN

---

## 💰 Cost Estimation

### API Costs (OpenAI)
- GPT-4V: $0.01-0.03 per image
- GPT-4 Turbo: $0.01-0.03 per 1K tokens
- **Estimated**: $0.50-2.00 per campaign analysis

### Infrastructure Costs (Monthly)

**Small (10-50 users)**:
- VM: $50
- Database: $20
- Storage: $10
- **Total**: ~$80/month

**Medium (50-200 users)**:
- VM: $150
- Database: $50
- Storage: $30
- Load Balancer: $20
- **Total**: ~$250/month

**Large (200+ users)**:
- Kubernetes Cluster: $300
- Database: $100
- Storage: $100
- CDN: $50
- **Total**: ~$550/month

---

## 🔄 Backup & Disaster Recovery

### Database Backups

```bash
# Automated PostgreSQL backups
# Add to crontab
0 2 * * * pg_dump pca_db > /backups/pca_db_$(date +\%Y\%m\%d).sql

# Backup to S3
0 3 * * * aws s3 cp /backups/ s3://your-bucket/backups/ --recursive
```

### Application Backups
- Code: Git repository
- Data: S3/Azure Blob
- Configs: Version controlled

### Recovery Plan
1. Restore database from backup
2. Deploy latest code
3. Restore data files
4. Verify functionality

---

## 📈 Scaling Strategy

### Horizontal Scaling
1. Add more API instances
2. Use load balancer
3. Implement session management
4. Use distributed cache

### Vertical Scaling
1. Increase VM resources
2. Optimize database
3. Add read replicas
4. Use CDN

---

## ✅ Pre-Launch Checklist

- [ ] API keys configured
- [ ] Database setup complete
- [ ] SSL certificates installed
- [ ] Monitoring configured
- [ ] Backups automated
- [ ] Security hardening done
- [ ] Load testing completed
- [ ] Documentation updated
- [ ] Error tracking enabled
- [ ] Logging configured

---

## 🆘 Troubleshooting

### Common Issues

**API not starting**
```bash
# Check logs
sudo supervisorctl tail -f pca_api stderr

# Restart service
sudo supervisorctl restart pca_api
```

**Database connection failed**
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
psql -h localhost -U pca_user -d pca_db
```

**Out of memory**
```bash
# Check memory usage
free -h

# Increase swap
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

## 📞 Support

For deployment issues:
1. Check logs in `/var/log/pca_agent/`
2. Review Nginx logs: `/var/log/nginx/`
3. Check system logs: `journalctl -xe`

---

**Ready to deploy? Start with Option 2 (Cloud VM) for a production-ready MVP!**
