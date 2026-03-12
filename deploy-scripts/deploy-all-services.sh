#!/bin/bash
set -e

EC2_HOST="13.200.71.62"
EC2_USER="ubuntu"

echo "🚀 Deploying All Services to EC2"
echo "================================="
echo ""

# Copy docker-compose file
echo "📤 Uploading docker-compose configuration..."
scp docker-compose.full.yml ${EC2_USER}@${EC2_HOST}:/home/ubuntu/pca_o/docker/

# Deploy on EC2
echo "🔄 Starting all services on EC2..."
ssh ${EC2_USER}@${EC2_HOST} << 'ENDSSH'
set -e

cd /home/ubuntu/pca_o/docker

# Load environment variables
export $(grep -v '^#' ../.env | xargs)

# Stop any existing containers
echo "Stopping existing containers..."
docker compose -f docker-compose.full.yml down 2>/dev/null || true

# Start all services
echo "Starting all services..."
docker compose -f docker-compose.full.yml up -d

# Wait for services to start
echo "Waiting for services to initialize..."
sleep 15

# Check status
echo ""
echo "📊 Service Status:"
docker compose -f docker-compose.full.yml ps

echo ""
echo "🔍 Health Checks:"
docker ps --format "table {{.Names}}\t{{.Status}}"

ENDSSH

echo ""
echo "✅ All services deployed!"
echo ""
echo "🌐 Access Points:"
echo "   API:          http://${EC2_HOST}:8000"
echo "   API Health:   http://${EC2_HOST}:8000/health"
echo "   Flower:       http://${EC2_HOST}:5555"
echo "   Jaeger UI:    http://${EC2_HOST}:16686"
echo ""
echo "📋 View logs:"
echo "   API:          ssh ${EC2_USER}@${EC2_HOST} 'docker logs pca-api'"
echo "   Celery:       ssh ${EC2_USER}@${EC2_HOST} 'docker logs pca-celery-worker'"
echo "   All services: ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml logs'"
