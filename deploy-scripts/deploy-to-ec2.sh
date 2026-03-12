#!/bin/bash
set -e

# Configuration
EC2_HOST="13.200.71.62"
EC2_USER="ubuntu"
IMAGE_NAME="pca-agent:latest"

echo "🚀 Deploying to EC2..."

# Save image to tar
echo "📦 Saving Docker image..."
docker save $IMAGE_NAME -o /tmp/pca-agent.tar

# Copy to EC2
echo "📤 Uploading to EC2..."
scp /tmp/pca-agent.tar ${EC2_USER}@${EC2_HOST}:/tmp/

# Deploy on EC2
echo "🔄 Loading and starting container on EC2..."
ssh ${EC2_USER}@${EC2_HOST} << 'ENDSSH'
set -e

# Load image
echo "Loading Docker image..."
docker load -i /tmp/pca-agent.tar
rm /tmp/pca-agent.tar

# Stop old container
echo "Stopping old container..."
docker stop pca-api 2>/dev/null || true
docker rm pca-api 2>/dev/null || true

# Get encryption key from .env
ENCRYPTION_KEY=$(grep ENCRYPTION_KEY /home/ubuntu/pca_o/.env | cut -d= -f2)

# Start new container
echo "Starting new container..."
docker run -d \
  --name pca-api \
  --network pca-network \
  --restart unless-stopped \
  -p 8000:8000 \
  -v /home/ubuntu/pca_o/docker/data:/app/data \
  -v /home/ubuntu/pca_o/docker/logs:/app/logs \
  -e POSTGRES_SERVER=db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pca_agent \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  -e DUCKDB_PATH=/app/data/analytics.duckdb \
  -e ENCRYPTION_KEY=${ENCRYPTION_KEY} \
  -e ENVIRONMENT=production \
  pca-agent:latest

# Wait and check
echo "Waiting for container to start..."
sleep 10

if docker ps | grep -q pca-api; then
  echo "✅ Container is running!"
  docker logs pca-api --tail 20
else
  echo "❌ Container failed to start!"
  docker logs pca-api --tail 50
  exit 1
fi
ENDSSH

# Cleanup local tar
rm /tmp/pca-agent.tar

echo ""
echo "✅ Deployment complete!"
echo "🔍 Check status: ssh ${EC2_USER}@${EC2_HOST} 'docker ps'"
echo "📋 View logs: ssh ${EC2_USER}@${EC2_HOST} 'docker logs pca-api'"
echo "🏥 Health check: curl http://${EC2_HOST}:8000/health"
