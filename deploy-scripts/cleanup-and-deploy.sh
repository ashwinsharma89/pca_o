#!/bin/bash
set -e

EC2_HOST="13.200.71.62"
EC2_USER="ubuntu"

echo "🧹 Cleaning up old containers and deploying all services"
echo "========================================================"
echo ""

# Copy docker-compose file
echo "📤 Uploading docker-compose configuration..."
scp docker-compose.full.yml ${EC2_USER}@${EC2_HOST}:/home/ubuntu/pca_o/docker/

# Deploy on EC2
echo "🔄 Cleaning up and starting services on EC2..."
ssh ${EC2_USER}@${EC2_HOST} << 'ENDSSH'
set -e

cd /home/ubuntu/pca_o/docker

# Load environment variables
export $(grep -v '^#' ../.env | xargs)

# Stop and remove ALL old containers
echo "Stopping all old containers..."
docker stop pca-api pca-celery-worker pca-celery-beat pca-flower pca-jaeger 2>/dev/null || true
docker rm pca-api pca-celery-worker pca-celery-beat pca-flower pca-jaeger 2>/dev/null || true

# Also stop any compose-managed containers
docker compose -f docker-compose.full.yml down 2>/dev/null || true
docker compose -f docker-compose.ec2.fixed.yml down 2>/dev/null || true

# Start all services with new compose file
echo "Starting all services..."
docker compose -f docker-compose.full.yml up -d

# Wait for services to start
echo "Waiting for services to initialize..."
sleep 20

# Check status
echo ""
echo "📊 Service Status:"
docker compose -f docker-compose.full.yml ps

echo ""
echo "🔍 Container Health:"
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check API health
echo ""
echo "🏥 API Health Check:"
sleep 5
curl -f http://localhost:8000/health && echo " ✅ API is healthy!" || echo " ⚠️  API health check pending..."

ENDSSH

echo ""
echo "✅ All services deployed!"
echo ""
echo "🌐 Access Points:"
echo "   API:          http://${EC2_HOST}:8000"
echo "   API Health:   http://${EC2_HOST}:8000/health"
echo "   API Docs:     http://${EC2_HOST}:8000/docs"
echo "   Flower:       http://${EC2_HOST}:5555"
echo "   Jaeger UI:    http://${EC2_HOST}:16686"
echo ""
echo "📋 Quick Commands:"
echo "   Status:       ./deploy-scripts/manage-services.sh status"
echo "   Logs:         ./deploy-scripts/manage-services.sh logs [service-name]"
echo "   Restart:      ./deploy-scripts/manage-services.sh restart [service-name]"
