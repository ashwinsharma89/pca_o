#!/bin/bash
# Local Development Run Script (No Docker Required)
# Usage: ./scripts/run_local.sh [api|streamlit|both|test]

set -e

MODE=${1:-both}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 not found. Please install Python 3.10+${NC}"
    exit 1
fi

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Install dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -r requirements.txt -q

# Load environment variables
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${GREEN}Loaded .env file${NC}"
fi

case $MODE in
    api)
        echo -e "\n${CYAN}Starting FastAPI server...${NC}"
        echo -e "${GREEN}API will be available at: http://localhost:8000${NC}"
        echo -e "${GREEN}Docs at: http://localhost:8000/docs${NC}"
        uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
        ;;
    streamlit)
        echo -e "\n${CYAN}Starting Streamlit app...${NC}"
        echo -e "${GREEN}App will be available at: http://localhost:8501${NC}"
        streamlit run streamlit_modular.py --server.port 8501
        ;;
    both)
        echo -e "\n${CYAN}Starting both API and Streamlit...${NC}"
        echo -e "${GREEN}API: http://localhost:8000${NC}"
        echo -e "${GREEN}Streamlit: http://localhost:8501${NC}"
        
        # Start API in background
        uvicorn src.api.main:app --reload --port 8000 &
        API_PID=$!
        
        # Start Streamlit
        streamlit run streamlit_modular.py --server.port 8501
        
        # Cleanup on exit
        trap "kill $API_PID 2>/dev/null" EXIT
        ;;
    test)
        echo -e "\n${CYAN}Running tests...${NC}"
        pytest tests/unit/ -v --cov=src --cov-report=term-missing
        ;;
    *)
        echo -e "${YELLOW}Usage: ./scripts/run_local.sh [api|streamlit|both|test]${NC}"
        echo "  api       - Start FastAPI server only"
        echo "  streamlit - Start Streamlit app only"
        echo "  both      - Start both (default)"
        echo "  test      - Run tests with coverage"
        ;;
esac
