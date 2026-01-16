#!/bin/bash

# Deploy Log Aggregation System
# This script sets up ELK Stack for centralized logging

set -e

echo "=================================="
echo "Log Aggregation Deployment"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker and Docker Compose are installed${NC}"
echo ""

# Create required directories
echo "Creating required directories..."
mkdir -p logs
mkdir -p logstash/pipeline
mkdir -p logstash/config
mkdir -p filebeat
echo -e "${GREEN}✅ Directories created${NC}"
echo ""

# Check if ELK stack is already running
if docker ps | grep -q "pca-elasticsearch"; then
    echo -e "${YELLOW}⚠️  ELK stack is already running${NC}"
    read -p "Do you want to restart it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing ELK stack..."
        docker-compose -f docker/docker-compose.elk.yml down
        echo -e "${GREEN}✅ Stopped${NC}"
    else
        echo "Keeping existing deployment"
        exit 0
    fi
fi

# Start ELK stack
echo ""
echo "Starting ELK stack..."
docker-compose -f docker/docker-compose.elk.yml up -d

# Wait for services to be ready
echo ""
echo "Waiting for services to be ready..."
echo "This may take a few minutes..."

# Wait for Elasticsearch
echo -n "Waiting for Elasticsearch..."
for i in {1..60}; do
    if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        echo -e " ${GREEN}✅${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

# Wait for Logstash
echo -n "Waiting for Logstash..."
for i in {1..60}; do
    if curl -s http://localhost:9600 > /dev/null 2>&1; then
        echo -e " ${GREEN}✅${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

# Wait for Kibana
echo -n "Waiting for Kibana..."
for i in {1..60}; do
    if curl -s http://localhost:5601/api/status > /dev/null 2>&1; then
        echo -e " ${GREEN}✅${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo -e "${GREEN}✅ All services are ready!${NC}"
echo ""

# Create index template
echo "Creating Elasticsearch index template..."
python3 << 'EOF'
import requests
import json

template = {
    "index_patterns": ["pca-agent-logs-*"],
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "level": {"type": "keyword"},
            "message": {"type": "text"},
            "service": {"type": "keyword"},
            "environment": {"type": "keyword"}
        }
    }
}

try:
    response = requests.put(
        "http://localhost:9200/_index_template/pca-agent-logs",
        json=template,
        headers={"Content-Type": "application/json"}
    )
    if response.status_code == 200:
        print("✅ Index template created")
    else:
        print(f"⚠️  Failed to create index template: {response.status_code}")
except Exception as e:
    print(f"⚠️  Error creating index template: {e}")
EOF

echo ""

# Display service URLs
echo "=================================="
echo "Service URLs"
echo "=================================="
echo ""
echo -e "${GREEN}Elasticsearch:${NC} http://localhost:9200"
echo -e "${GREEN}Kibana:${NC} http://localhost:5601"
echo -e "${GREEN}Logstash:${NC} http://localhost:9600"
echo ""

# Display next steps
echo "=================================="
echo "Next Steps"
echo "=================================="
echo ""
echo "1. Open Kibana: http://localhost:5601"
echo "2. Create index pattern: pca-agent-logs-*"
echo "3. Start your application with log aggregation enabled"
echo ""

# Display status
echo "=================================="
echo "Service Status"
echo "=================================="
echo ""
docker-compose -f docker/docker-compose.elk.yml ps

echo ""
echo -e "${GREEN}✅ Log aggregation deployment complete!${NC}"
echo ""

# Display logs command
echo "To view logs:"
echo "  docker-compose -f docker/docker-compose.elk.yml logs -f"
echo ""
echo "To stop services:"
echo "  docker-compose -f docker/docker-compose.elk.yml down"
echo ""
