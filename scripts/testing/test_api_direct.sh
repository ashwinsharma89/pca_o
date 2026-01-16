#!/bin/bash
# Test API endpoints to see what's happening

echo "=== Testing /campaigns/visualizations ==="
curl -s "http://localhost:8000/api/v1/campaigns/visualizations" | jq '.' || echo "Failed"

echo -e "\n=== Testing /campaigns/dashboard-stats ==="
curl -s "http://localhost:8000/api/v1/campaigns/dashboard-stats" | jq '.' || echo "Failed"

echo -e "\n=== Testing /campaigns/filters ==="
curl -s "http://localhost:8000/api/v1/campaigns/filters" | jq '.' || echo "Failed"
