#!/bin/bash
echo "🛑 Stopping PCA Agent Services..."

# Kill backend
pkill -f "src.api.main"
# Kill frontend (Next.js/Node)
pkill -f "next-dev"
pkill -f "next-server"

echo "✅ Services stopped."
