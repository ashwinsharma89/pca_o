# Local Development Run Script (No Docker Required)
# Usage: .\scripts\run_local.ps1 [api|streamlit|both|test]

param(
    [string]$Mode = "both"
)

$ErrorActionPreference = "Stop"

# Check Python
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python not found. Please install Python 3.10+"
    exit 1
}

# Create virtual environment if not exists
if (-not (Test-Path "venv")) {
    Write-Host "Creating virtual environment..." -ForegroundColor Yellow
    python -m venv venv
}

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
& .\venv\Scripts\Activate.ps1

# Install dependencies
Write-Host "Installing dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt -q

# Load environment variables
if (Test-Path ".env") {
    Get-Content .env | ForEach-Object {
        if ($_ -match "^([^=]+)=(.*)$") {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
        }
    }
    Write-Host "Loaded .env file" -ForegroundColor Green
}

switch ($Mode.ToLower()) {
    "api" {
        Write-Host "`nStarting FastAPI server..." -ForegroundColor Cyan
        Write-Host "API will be available at: http://localhost:8000" -ForegroundColor Green
        Write-Host "Docs at: http://localhost:8000/docs" -ForegroundColor Green
        uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
    }
    "streamlit" {
        Write-Host "`nStarting Streamlit app..." -ForegroundColor Cyan
        Write-Host "App will be available at: http://localhost:8501" -ForegroundColor Green
        streamlit run streamlit_modular.py --server.port 8501
    }
    "both" {
        Write-Host "`nStarting both API and Streamlit..." -ForegroundColor Cyan
        Write-Host "API: http://localhost:8000" -ForegroundColor Green
        Write-Host "Streamlit: http://localhost:8501" -ForegroundColor Green
        
        # Start API in background
        Start-Process powershell -ArgumentList "-Command", "cd '$PWD'; .\venv\Scripts\Activate.ps1; uvicorn src.api.main:app --reload --port 8000"
        
        # Start Streamlit in foreground
        streamlit run streamlit_modular.py --server.port 8501
    }
    "test" {
        Write-Host "`nRunning tests..." -ForegroundColor Cyan
        pytest tests/unit/ -v --cov=src --cov-report=term-missing
    }
    default {
        Write-Host "Usage: .\scripts\run_local.ps1 [api|streamlit|both|test]" -ForegroundColor Yellow
        Write-Host "  api       - Start FastAPI server only"
        Write-Host "  streamlit - Start Streamlit app only"
        Write-Host "  both      - Start both (default)"
        Write-Host "  test      - Run tests with coverage"
    }
}
