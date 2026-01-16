#!/bin/bash
# Rollback Script
# Usage: ./scripts/rollback.sh <environment> <version>
# Example: ./scripts/rollback.sh production v2.0.1

set -e

ENVIRONMENT=${1:-staging}
VERSION=${2}

if [ -z "$VERSION" ]; then
    echo "❌ Error: Version required"
    echo "Usage: ./scripts/rollback.sh <environment> <version>"
    echo "Example: ./scripts/rollback.sh production v2.0.1"
    exit 1
fi

echo "🔄 Rolling back $ENVIRONMENT to version $VERSION"

# Get current version for backup
CURRENT_VERSION=$(curl -s "https://${ENVIRONMENT}.pca-agent.example.com/health" | jq -r '.version' 2>/dev/null || echo "unknown")
echo "📋 Current version: $CURRENT_VERSION"

# Pull the target version
echo "📦 Pulling version $VERSION..."
docker pull ghcr.io/pca-agent-api:$VERSION
docker pull ghcr.io/pca-agent-frontend:$VERSION

# Deploy based on environment
if [ "$ENVIRONMENT" == "production" ]; then
    echo "🚀 Deploying to production..."
    docker-compose -f docker/docker-compose.production.yml up -d
elif [ "$ENVIRONMENT" == "staging" ]; then
    echo "🚀 Deploying to staging..."
    docker-compose -f docker/docker-compose.staging.yml up -d
else
    echo "❌ Unknown environment: $ENVIRONMENT"
    exit 1
fi

# Wait for health check
echo "⏳ Waiting for services to be healthy..."
sleep 30

# Verify deployment
DEPLOYED_VERSION=$(curl -s "https://${ENVIRONMENT}.pca-agent.example.com/health" | jq -r '.version' 2>/dev/null)

if [ "$DEPLOYED_VERSION" == "$VERSION" ]; then
    echo "✅ Rollback successful!"
    echo "   Previous: $CURRENT_VERSION"
    echo "   Current:  $DEPLOYED_VERSION"
else
    echo "❌ Rollback verification failed!"
    echo "   Expected: $VERSION"
    echo "   Got:      $DEPLOYED_VERSION"
    exit 1
fi
