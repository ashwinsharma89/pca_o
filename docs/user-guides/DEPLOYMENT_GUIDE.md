# Production Deployment Guide

Complete guide for deploying PCA Agent to production environments.

---

## 📋 Pre-Deployment Checklist

### ✅ Security
- [ ] Change all default passwords
- [ ] Generate secure JWT secret (min 32 chars)
- [ ] Set up SSL/TLS certificates
- [ ] Configure firewall rules
- [ ] Enable database encryption
- [ ] Set up secrets management

### ✅ Infrastructure
- [ ] Provision servers/cloud resources
- [ ] Set up load balancer
- [ ] Configure DNS records
- [ ] Set up backup storage
- [ ] Configure monitoring alerts

### ✅ Configuration
- [ ] Review and update .env file
- [ ] Configure database connection pooling
- [ ] Set up Redis persistence
- [ ] Configure rate limits
- [ ] Set up log aggregation

### ✅ Testing
- [ ] Run all unit tests
- [ ] Run integration tests
- [ ] Perform load testing
- [ ] Test backup/restore procedures
- [ ] Verify monitoring dashboards

---

## 🚀 Deployment Options

### Option 1: Docker Compose (Single Server)

**Best for**: Small to medium deployments, development staging

```bash
# 1. Clone repository
git clone https://github.com/ashwinsharma89/pca_agent.git
cd pca_agent

# 2. Configure environment
cp .env.docker .env
nano .env  # Edit with production values

# 3. Deploy with production settings
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 4. Initialize database
docker-compose exec api python scripts/init_database.py
docker-compose exec api python scripts/init_users.py

# 5. Verify deployment
curl http://localhost:8000/health
```

**Pros**:
- Simple setup
- Easy to manage
- Good for single server

**Cons**:
- Single point of failure
- Limited scalability
- Manual scaling

---

### Option 2: Docker Swarm (Multi-Server)

**Best for**: Medium deployments, high availability

```bash
# 1. Initialize Swarm on manager node
docker swarm init --advertise-addr <MANAGER-IP>

# 2. Join worker nodes
# Run on each worker node:
docker swarm join --token <TOKEN> <MANAGER-IP>:2377

# 3. Create overlay network
docker network create --driver overlay pca-network

# 4. Deploy stack
docker stack deploy -c docker-compose.yml -c docker-compose.prod.yml pca

# 5. Scale services
docker service scale pca_api=5

# 6. Check status
docker stack services pca
```

**Pros**:
- Built-in orchestration
- Easy scaling
- Load balancing
- Rolling updates

**Cons**:
- More complex than Compose
- Limited compared to Kubernetes

---

### Option 3: Kubernetes (Enterprise)

**Best for**: Large deployments, enterprise scale

#### Step 1: Create Kubernetes Manifests

```bash
# Convert docker-compose to Kubernetes
kompose convert -f docker-compose.yml

# Or use provided manifests
kubectl apply -f k8s/
```

#### Step 2: Deploy to Cluster

```bash
# Create namespace
kubectl create namespace pca-agent

# Apply secrets
kubectl create secret generic pca-secrets \
  --from-env-file=.env \
  --namespace=pca-agent

# Deploy services
kubectl apply -f k8s/ --namespace=pca-agent

# Check status
kubectl get pods -n pca-agent
kubectl get services -n pca-agent
```

#### Step 3: Configure Ingress

```yaml
# k8s/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: pca-ingress
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  tls:
  - hosts:
    - api.yourdomain.com
    secretName: pca-tls
  rules:
  - host: api.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: pca-api
            port:
              number: 8000
```

**Pros**:
- Highly scalable
- Auto-healing
- Advanced orchestration
- Industry standard

**Cons**:
- Complex setup
- Steep learning curve
- Higher resource overhead

---

### Option 4: Cloud Platforms

#### AWS ECS (Elastic Container Service)

```bash
# 1. Install AWS CLI and ECS CLI
pip install awscli
ecs-cli configure --cluster pca-cluster --region us-east-1

# 2. Create cluster
ecs-cli up --cluster pca-cluster --size 3 --instance-type t3.medium

# 3. Deploy services
ecs-cli compose --file docker-compose.yml up

# 4. Configure load balancer
aws elbv2 create-load-balancer \
  --name pca-lb \
  --subnets subnet-xxx subnet-yyy
```

#### Google Cloud Run

```bash
# 1. Build and push image
gcloud builds submit --tag gcr.io/PROJECT_ID/pca-agent

# 2. Deploy to Cloud Run
gcloud run deploy pca-agent \
  --image gcr.io/PROJECT_ID/pca-agent \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="$(cat .env | xargs)"

# 3. Get service URL
gcloud run services describe pca-agent --format='value(status.url)'
```

#### Azure Container Instances

```bash
# 1. Create resource group
az group create --name pca-rg --location eastus

# 2. Deploy container
az container create \
  --resource-group pca-rg \
  --name pca-agent \
  --image pca-agent:latest \
  --dns-name-label pca-agent \
  --ports 8000 \
  --environment-variables $(cat .env)

# 3. Get IP address
az container show \
  --resource-group pca-rg \
  --name pca-agent \
  --query ipAddress.fqdn
```

---

## 🔒 Security Hardening

### 1. SSL/TLS Configuration

```nginx
# nginx.conf
server {
    listen 443 ssl http2;
    server_name api.yourdomain.com;

    ssl_certificate /etc/ssl/certs/cert.pem;
    ssl_certificate_key /etc/ssl/private/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### 2. Firewall Rules

```bash
# Allow only necessary ports
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw enable
```

### 3. Database Security

```sql
-- Create read-only user for reporting
CREATE USER pca_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE pca_agent TO pca_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pca_readonly;

-- Enable SSL
ALTER SYSTEM SET ssl = on;
```

### 4. Secrets Management

#### Using Docker Secrets

```bash
# Create secrets
echo "my_db_password" | docker secret create db_password -
echo "my_jwt_secret" | docker secret create jwt_secret -

# Use in docker-compose
services:
  api:
    secrets:
      - db_password
      - jwt_secret

secrets:
  db_password:
    external: true
  jwt_secret:
    external: true
```

#### Using HashiCorp Vault

```bash
# Store secrets
vault kv put secret/pca-agent \
  db_password="secure_password" \
  jwt_secret="secure_jwt_key"

# Retrieve in application
vault kv get -field=db_password secret/pca-agent
```

---

## 📊 Monitoring Setup

### 1. Prometheus Alerts

```yaml
# prometheus/alerts.yml
groups:
  - name: pca_alerts
    rules:
      - alert: HighErrorRate
        expr: rate(api_errors_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
          
      - alert: HighResponseTime
        expr: histogram_quantile(0.95, response_time_ms) > 1000
        for: 5m
        labels:
          severity: warning
```

### 2. Grafana Dashboards

Import pre-built dashboards:
- FastAPI Dashboard: 12856
- PostgreSQL Dashboard: 9628
- Redis Dashboard: 11835

### 3. Log Aggregation

#### Using ELK Stack

```yaml
# docker-compose.elk.yml
services:
  elasticsearch:
    image: elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
    ports:
      - "9200:9200"

  logstash:
    image: logstash:8.11.0
    volumes:
      - ./logstash.conf:/usr/share/logstash/pipeline/logstash.conf

  kibana:
    image: kibana:8.11.0
    ports:
      - "5601:5601"
```

---

## 🔄 CI/CD Pipeline

### GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy to Production

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Build Docker image
        run: docker build -t pca-agent:${{ github.sha }} .
      
      - name: Push to registry
        run: |
          docker tag pca-agent:${{ github.sha }} registry.com/pca-agent:latest
          docker push registry.com/pca-agent:latest
      
      - name: Deploy to production
        run: |
          ssh user@server "cd /app && docker-compose pull && docker-compose up -d"
```

---

## 💾 Backup Strategy

### Automated Backups

```bash
# backup.sh
#!/bin/bash

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups"

# Backup PostgreSQL
docker-compose exec -T postgres pg_dump -U pca_user pca_agent | \
  gzip > $BACKUP_DIR/db_$DATE.sql.gz

# Backup Redis
docker-compose exec redis redis-cli --rdb /data/dump.rdb
cp data/dump.rdb $BACKUP_DIR/redis_$DATE.rdb

# Backup volumes
tar czf $BACKUP_DIR/volumes_$DATE.tar.gz data/

# Upload to S3
aws s3 cp $BACKUP_DIR/ s3://pca-backups/ --recursive

# Cleanup old backups (keep 30 days)
find $BACKUP_DIR -mtime +30 -delete
```

### Restore Procedure

```bash
# Restore database
gunzip < backup.sql.gz | \
  docker-compose exec -T postgres psql -U pca_user pca_agent

# Restore Redis
docker-compose exec redis redis-cli --rdb /data/dump.rdb < backup.rdb

# Restart services
docker-compose restart
```

---

## 🧪 Health Checks

### Application Health

```bash
# Check API health
curl http://localhost:8000/health

# Check detailed health
curl http://localhost:8000/health/detailed

# Check database
docker-compose exec postgres pg_isready

# Check Redis
docker-compose exec redis redis-cli ping
```

### Automated Monitoring

```bash
# healthcheck.sh
#!/bin/bash

ENDPOINTS=(
  "http://localhost:8000/health"
  "http://localhost:8501"
  "http://localhost:9090/-/healthy"
  "http://localhost:3000/api/health"
)

for endpoint in "${ENDPOINTS[@]}"; do
  if ! curl -f -s $endpoint > /dev/null; then
    echo "❌ $endpoint is down"
    # Send alert
  else
    echo "✅ $endpoint is healthy"
  fi
done
```

---

## 📈 Scaling Guide

### Horizontal Scaling

```bash
# Scale API service
docker-compose up -d --scale api=5

# Or with Docker Swarm
docker service scale pca_api=5

# Or with Kubernetes
kubectl scale deployment pca-api --replicas=5
```

### Vertical Scaling

```yaml
# Increase resources
services:
  api:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
```

### Database Scaling

```bash
# Set up read replicas
docker-compose -f docker-compose.yml -f docker-compose.replicas.yml up -d

# Configure connection pooling
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=100
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Service Won't Start

```bash
# Check logs
docker-compose logs api

# Check resource usage
docker stats

# Restart service
docker-compose restart api
```

#### 2. Database Connection Issues

```bash
# Test connection
docker-compose exec postgres psql -U pca_user -d pca_agent

# Check connections
docker-compose exec postgres psql -U pca_user -c "SELECT count(*) FROM pg_stat_activity;"
```

#### 3. High Memory Usage

```bash
# Check memory
docker stats

# Restart services
docker-compose restart

# Increase limits
# Edit docker-compose.prod.yml
```

---

## 📞 Support

### Getting Help

- **Documentation**: See DOCKER_SETUP.md
- **Issues**: https://github.com/ashwinsharma89/pca_agent/issues
- **Monitoring**: Check Grafana dashboards
- **Logs**: `docker-compose logs -f`

---

## ✅ Post-Deployment Checklist

- [ ] All services running
- [ ] Health checks passing
- [ ] SSL/TLS configured
- [ ] Monitoring active
- [ ] Backups scheduled
- [ ] Alerts configured
- [ ] Documentation updated
- [ ] Team trained
- [ ] Disaster recovery tested

---

**Status**: ✅ **Ready for Production Deployment!**

Choose your deployment option and follow the guide above. 🚀
