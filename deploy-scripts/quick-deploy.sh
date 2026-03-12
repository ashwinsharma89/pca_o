#!/bin/bash
# Quick build and deploy script
set -e

echo "🚀 Quick Build & Deploy Pipeline"
echo "================================"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Build
echo "Step 1: Building image..."
bash "$SCRIPT_DIR/build-local.sh"

echo ""
echo "Step 2: Deploying to EC2..."
bash "$SCRIPT_DIR/deploy-to-ec2.sh"

echo ""
echo "✅ All done!"
