#!/bin/bash
echo "🔍 Verifying All Services Deployment"
echo "====================================="
echo ""

EC2_HOST="13.200.71.62"

echo "1️⃣  Checking Service Status:"
ssh ubuntu@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml ps'

echo ""
echo "2️⃣  API Health Check:"
curl -s http://${EC2_HOST}:8000/health | python3 -m json.tool 2>/dev/null || curl -s http://${EC2_HOST}:8000/health

echo ""
echo ""
echo "3️⃣  Service Endpoints:"
echo "   ✅ API:          http://${EC2_HOST}:8000"
echo "   ✅ API Docs:     http://${EC2_HOST}:8000/docs"
echo "   ✅ Flower:       http://${EC2_HOST}:5555"
echo "   ✅ Jaeger:       http://${EC2_HOST}:16686"

echo ""
echo "4️⃣  Quick Status Check:"
for service in pca-api pca-celery-worker pca-celery-beat pca-flower pca-jaeger pca-postgres pca-redis; do
    status=$(ssh ubuntu@${EC2_HOST} "docker inspect -f '{{.State.Status}}' $service 2>/dev/null" || echo "not found")
    if [ "$status" = "running" ]; then
        echo "   ✅ $service: running"
    else
        echo "   ❌ $service: $status"
    fi
done

echo ""
echo "5️⃣  Resource Usage:"
ssh ubuntu@${EC2_HOST} 'docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"'

echo ""
echo "✅ Verification complete!"
