#!/bin/bash

# Deployment Script for Test Environment (EC2)
# Usage: ./deploy.sh

echo "🚀 Starting Deployment to Test Environment..."

# 1. Stop existing services
echo "🛑 Stopping current services..."
docker-compose -f docker/docker-compose.deploy.yml down

# 2. Build updated images (Backend & Frontend)
# Note: This rebuilds images locally. In a CI/CD pipeline, you would pull from ECR.
echo "🏗️  Building Docker images..."
docker-compose -f docker/docker-compose.deploy.yml build

# 3. Start services in background
echo "▶️  Starting services..."
docker-compose -f docker/docker-compose.deploy.yml up -d

# 4. Check status
echo "✅ Deployment Complete! Checking status..."
docker-compose -f docker/docker-compose.deploy.yml ps

echo "
🌐 Access points:
- Frontend: http://localhost (or EC2 Public IP)
- Backend API: http://localhost/api/v1 (via Nginx)
- Jaeger UI: http://localhost:16686
- Prometheus: http://localhost:9090
"
