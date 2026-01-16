# PCA Agent Deployment Guide

## Quick Start

### 1. OpenTelemetry / Jaeger Tracing

**Enable distributed tracing:**

```bash
# Set environment variables
export OPENTELEMETRY_ENABLED=true
export OTEL_SERVICE_NAME=pca-agent
export OTEL_EXPORTER_TYPE=jaeger
export JAEGER_HOST=localhost
export JAEGER_PORT=6831

# Start Jaeger (using Docker)
docker run -d --name jaeger \
  -p 6831:6831/udp \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# Access Jaeger UI
open http://localhost:16686
```

**Already integrated in:** `src/api/main_v3.py:54`

---

### 2. ELK Stack (Elasticsearch, Logstash, Kibana)

**Start ELK stack:**

```bash
# Start ELK services
docker-compose -f docker-compose.elk.yml up -d

# Check status
docker-compose -f docker-compose.elk.yml ps

# Access Kibana
open http://localhost:5601
```

**Configure log shipping:**
- Filebeat: Configured in `filebeat/`
- Logstash: Configured in `logstash/`
- Logs automatically shipped to Elasticsearch

---

### 3. Automated Backups

#### Option A: Systemd Service (Recommended for Linux)

```bash
# Copy systemd service file
sudo cp deployment/backup-scheduler.service /etc/systemd/system/

# Enable and start service
sudo systemctl enable backup-scheduler
sudo systemctl start backup-scheduler

# Check status
sudo systemctl status backup-scheduler

# View logs
sudo journalctl -u backup-scheduler -f
```

#### Option B: Cron Job (Alternative)

```bash
# Add to crontab
crontab -e

# Add this line (runs daily at 2 AM)
0 2 * * * cd /path/to/pca_agent && python scripts/backup_scheduler.py
```

#### Option C: Manual Backup

```bash
# Run backup manually
python scripts/backup_scheduler.py

# Or use backup manager directly
python -c "from src.backup.backup_manager import BackupManager; BackupManager().create_backup()"
```

---

### 4. Restore from Backup

**List available backups:**

```bash
python scripts/restore_backup.py --list
```

**Restore specific backup:**

```bash
python scripts/restore_backup.py backup_20231219_020000.sql.gz
```

---

### 5. S3 Off-Site Backups

**Setup AWS credentials:**

```bash
# Configure AWS CLI
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1
export S3_BACKUP_BUCKET=your-backup-bucket
```

**Upload backup to S3:**

```bash
# Install boto3
pip install boto3

# Upload latest backup
python scripts/s3_backup.py upload --file backups/backup_20231219_020000.sql.gz
```

**List S3 backups:**

```bash
python scripts/s3_backup.py list
```

**Download from S3:**

```bash
python scripts/s3_backup.py download \
  --key backups/backup_20231219_020000.sql.gz \
  --output ./restored_backup.sql.gz
```

**Cleanup old S3 backups:**

```bash
# Delete backups older than 90 days
python scripts/s3_backup.py cleanup --retention-days 90
```

---

## Environment Variables

### Required

```bash
# Security
export JWT_SECRET_KEY=$(python -c 'import secrets; print(secrets.token_urlsafe(32))')

# Database
export DATABASE_URL=postgresql://user:pass@localhost:5432/pca_agent
# OR
export USE_SQLITE=true
```

### Optional

```bash
# OpenTelemetry
export OPENTELEMETRY_ENABLED=true
export OTEL_EXPORTER_TYPE=jaeger
export JAEGER_HOST=localhost

# Backups
export BACKUP_DIR=./backups
export BACKUP_RETENTION_DAYS=30
export BACKUP_SCHEDULE_TIME=02:00

# S3
export S3_BACKUP_BUCKET=my-backup-bucket
export AWS_REGION=us-east-1

# CORS
export CORS_ORIGINS=https://yourdomain.com,https://app.yourdomain.com

# API
export API_HOST=127.0.0.1
export API_PORT=8000
```

---

## Production Deployment Checklist

### Security
- [ ] Set secure `JWT_SECRET_KEY`
- [ ] Configure `CORS_ORIGINS` (no wildcards)
- [ ] Use `API_HOST=127.0.0.1` with reverse proxy
- [ ] Enable HTTPS (nginx/caddy)
- [ ] Set strong database passwords

### Monitoring
- [ ] Enable OpenTelemetry tracing
- [ ] Start Jaeger for trace visualization
- [ ] Deploy ELK stack for log aggregation
- [ ] Configure alerting (optional)

### Backups
- [ ] Enable automated daily backups
- [ ] Test restore process
- [ ] Configure S3 off-site backups
- [ ] Set retention policies

### Infrastructure
- [ ] Use systemd for service management
- [ ] Configure log rotation
- [ ] Set up health checks
- [ ] Configure firewall rules

---

## Service Management

### Start Application

```bash
# Development
uvicorn src.api.main:app --host 127.0.0.1 --port 8000

# Production (with systemd)
sudo systemctl start pca-agent
```

### Stop Application

```bash
sudo systemctl stop pca-agent
```

### Restart Application

```bash
sudo systemctl restart pca-agent
```

### View Logs

```bash
# Application logs
sudo journalctl -u pca-agent -f

# Backup logs
sudo journalctl -u backup-scheduler -f
```

---

## Monitoring Endpoints

### Health Check

```bash
curl http://localhost:8000/health
```

### Detailed Health

```bash
curl http://localhost:8000/health/detailed
```

### Jaeger UI

```
http://localhost:16686
```

### Kibana (ELK)

```
http://localhost:5601
```

---

## Troubleshooting

### Backups Not Running

```bash
# Check scheduler status
sudo systemctl status backup-scheduler

# Check logs
sudo journalctl -u backup-scheduler -n 50

# Run manually to test
python scripts/backup_scheduler.py
```

### S3 Upload Failing

```bash
# Check AWS credentials
aws s3 ls s3://your-backup-bucket

# Test upload manually
python scripts/s3_backup.py upload --file backups/test.sql.gz
```

### Tracing Not Working

```bash
# Check Jaeger is running
docker ps | grep jaeger

# Check environment variables
echo $OPENTELEMETRY_ENABLED
echo $JAEGER_HOST

# Restart application
sudo systemctl restart pca-agent
```

---

## Disaster Recovery

### Full System Restore

1. **Restore Database:**
   ```bash
   # Download from S3
   python scripts/s3_backup.py download \
     --key backups/backup_20231219_020000.sql.gz \
     --output ./restore.sql.gz
   
   # Restore database
   python scripts/restore_backup.py restore.sql.gz
   ```

2. **Verify Application:**
   ```bash
   # Start application
   sudo systemctl start pca-agent
   
   # Check health
   curl http://localhost:8000/health/detailed
   ```

3. **Verify Data:**
   ```bash
   # Test API endpoints
   curl -H "Authorization: Bearer $TOKEN" \
     http://localhost:8000/api/v1/campaigns
   ```

---

## Scaling

### Horizontal Scaling

```bash
# Use docker-compose for multi-instance deployment
docker-compose -f docker-compose.scale.yml up -d --scale api=3
```

### Load Balancing

Configure nginx as reverse proxy:

```nginx
upstream pca_agent {
    server 127.0.0.1:8000;
    server 127.0.0.1:8001;
    server 127.0.0.1:8002;
}

server {
    listen 80;
    server_name yourdomain.com;
    
    location / {
        proxy_pass http://pca_agent;
    }
}
```

---

## Support

For issues or questions:
- Check logs: `sudo journalctl -u pca-agent -f`
- Review health endpoint: `/health/detailed`
- Check Jaeger traces for request flow
- Review ELK logs for errors
