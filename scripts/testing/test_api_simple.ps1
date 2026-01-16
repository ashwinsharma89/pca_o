# Simple API Test Script
Write-Host "PCA Agent API - Quick Test" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan

$baseUrl = "http://localhost:8000"

# Test 1: Root
Write-Host "`nTest 1: Root Endpoint" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/" -Method GET
    Write-Host "SUCCESS: API is running" -ForegroundColor Green
    Write-Host "Version: $($response.version)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 2: Health
Write-Host "`nTest 2: Health Check" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/health" -Method GET
    Write-Host "SUCCESS: Health check passed" -ForegroundColor Green
    Write-Host "Status: $($response.status)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 3: Detailed Health
Write-Host "`nTest 3: Detailed Health" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/health/detailed" -Method GET
    Write-Host "SUCCESS: Detailed health check passed" -ForegroundColor Green
    Write-Host "Database: $($response.components.database)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 4: Protected Endpoint (should fail without auth)
Write-Host "`nTest 4: Authentication Required" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/campaigns" -Method GET
    Write-Host "WARNING: Endpoint accessible without auth" -ForegroundColor Yellow
} catch {
    if ($_.Exception.Response.StatusCode -eq 401) {
        Write-Host "SUCCESS: Authentication is working" -ForegroundColor Green
    } else {
        Write-Host "FAILED: Unexpected error" -ForegroundColor Red
    }
}

Write-Host "`n======================================" -ForegroundColor Cyan
Write-Host "API Access Points:" -ForegroundColor Cyan
Write-Host "  API: $baseUrl" -ForegroundColor White
Write-Host "  Docs: $baseUrl/api/docs" -ForegroundColor White
Write-Host "  Health: $baseUrl/health" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Cyan
