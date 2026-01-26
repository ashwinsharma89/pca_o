#!/bin/bash

# Get the directory where this script is located
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "🚀 Starting PCA Agent Services..."

# 0. Kill existing processes on target ports (3000, 8001)
echo "Cleaning up existing processes on ports 3000 and 8001..."
lsof -ti :3000 | xargs kill -9 2>/dev/null || true
lsof -ti :8001 | xargs kill -9 2>/dev/null || true

# 1. Start Backend API in background
echo "Starting Backend API (Port 8001)..."
export API_PORT=8001
export PORT=3000 # For frontend
source .venv312/bin/activate
mkdir -p logs
python3 -m src.interface.api.main > logs/backend.log 2>&1 &
BACKEND_PID=$!
echo "Backend started (PID: $BACKEND_PID)"

# 2. Start Frontend in background
echo "Starting Frontend (Port 3000)..."
cd frontend
# Run in background and redirect output to frontend.log
# Force port 3000 for frontend
PORT=3000 npm run dev > ../logs/frontend.log 2>&1 &
FRONTEND_PID=$!
echo "Frontend started (PID: $FRONTEND_PID)"

echo "✅ All services are starting in the background."
echo "   - API: http://localhost:8000"
echo "   - Web: http://localhost:3000"
echo "   - Logs: logs/backend.log and logs/frontend.log"

# Keep script alive for a moment to ensure processes don't die immediately
sleep 2

# Check if processes are still running
if ps -p $BACKEND_PID > /dev/null && ps -p $FRONTEND_PID > /dev/null; then
    echo "Double check: Services confirmed running."
else
    echo "⚠️ Warning: One or more services failed to start. Check the logs."
fi
