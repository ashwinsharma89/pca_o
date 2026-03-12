#!/bin/bash
set -e

echo "🔨 Building production Docker image locally..."

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Build the image
docker build \
  -f Dockerfile.production \
  -t pca-agent:latest \
  -t pca-agent:$(date +%Y%m%d-%H%M%S) \
  .

echo "✅ Build complete!"
echo ""
echo "Image tags:"
docker images | grep pca-agent | head -5
