# PCA Agent API Endpoint Testing Script
# Tests all major endpoints without Docker

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "PCA Agent API - Endpoint Testing" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$baseUrl = "http://localhost:8000"
$testsPassed = 0
$testsFailed = 0

function Test-Endpoint {
    param(
        [string]$Name,
        [string]$Url,
        [string]$Method = "GET",
        [hashtable]$Headers = @{},
        [string]$Body = $null
    )
    
    Write-Host "Testing: $Name" -ForegroundColor Yellow
    Write-Host "  URL: $Url" -ForegroundColor Gray
    
    try {
        $params = @{
            Uri = $Url
            Method = $Method
            Headers = $Headers
            ErrorAction = "Stop"
        }
        
        if ($Body) {
            $params.Body = $Body
            $params.ContentType = "application/json"
        }
        
        $response = Invoke-RestMethod @params
        Write-Host "  ✅ Status: SUCCESS" -ForegroundColor Green
        
        # Pretty print JSON response (first 200 chars)
        $jsonStr = ($response | ConvertTo-Json -Depth 3 -Compress)
        if ($jsonStr.Length -gt 200) {
            $jsonStr = $jsonStr.Substring(0, 200) + "..."
        }
        Write-Host "  Response: $jsonStr" -ForegroundColor Gray
        
        $script:testsPassed++
        return $response
    }
    catch {
        Write-Host "  ❌ Status: FAILED" -ForegroundColor Red
        Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
        $script:testsFailed++
        return $null
    }
    
    Write-Host ""
}

# Test 1: Root Endpoint
Write-Host "`n1. Root Endpoint" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
Test-Endpoint -Name "API Root" -Url "$baseUrl/"

# Test 2: Health Check
Write-Host "`n2. Health Check" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
Test-Endpoint -Name "Basic Health" -Url "$baseUrl/health"

# Test 3: Detailed Health Check
Write-Host "`n3. Detailed Health Check" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
Test-Endpoint -Name "Detailed Health" -Url "$baseUrl/health/detailed"

# Test 4: API Documentation
Write-Host "`n4. API Documentation" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
Write-Host "  📚 OpenAPI Docs: $baseUrl/api/docs" -ForegroundColor Green
Write-Host "  📚 ReDoc: $baseUrl/api/redoc" -ForegroundColor Green

# Test 5: Authentication Endpoints
Write-Host "`n5. Authentication Endpoints" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan

# Try to access protected endpoint without auth (should fail)
Write-Host "Testing: Protected Endpoint (No Auth)" -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "$baseUrl/api/v1/campaigns" -Method GET -ErrorAction Stop
    Write-Host "  ⚠️  Warning: Protected endpoint accessible without auth!" -ForegroundColor Yellow
}
catch {
    if ($_.Exception.Response.StatusCode -eq 401) {
        Write-Host "  ✅ Correctly requires authentication" -ForegroundColor Green
        $script:testsPassed++
    }
    else {
        Write-Host "  ❌ Unexpected error: $($_.Exception.Message)" -ForegroundColor Red
        $script:testsFailed++
    }
}

# Test 6: List Available Endpoints
Write-Host "`n6. Available API Endpoints" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
Write-Host "  Authentication:" -ForegroundColor White
Write-Host "    POST /api/v1/auth/login" -ForegroundColor Gray
Write-Host "    POST /api/v1/auth/register" -ForegroundColor Gray
Write-Host ""
Write-Host "  Campaigns:" -ForegroundColor White
Write-Host "    GET    /api/v1/campaigns" -ForegroundColor Gray
Write-Host "    POST   /api/v1/campaigns" -ForegroundColor Gray
Write-Host "    GET    /api/v1/campaigns/{id}" -ForegroundColor Gray
Write-Host "    PUT    /api/v1/campaigns/{id}" -ForegroundColor Gray
Write-Host "    DELETE /api/v1/campaigns/{id}" -ForegroundColor Gray
Write-Host ""
Write-Host "  Users:" -ForegroundColor White
Write-Host "    POST /api/v1/users/register" -ForegroundColor Gray
Write-Host "    GET  /api/v1/users/me" -ForegroundColor Gray
Write-Host ""
Write-Host "  Health & Monitoring:" -ForegroundColor White
Write-Host "    GET /health" -ForegroundColor Gray
Write-Host "    GET /health/detailed" -ForegroundColor Gray

# Test 7: Check Database
Write-Host "`n7. Database Status" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
if (Test-Path "pca_agent.db") {
    $dbSize = (Get-Item "pca_agent.db").Length / 1KB
    Write-Host "  ✅ SQLite Database: pca_agent.db" -ForegroundColor Green
    Write-Host "  Size: $([math]::Round($dbSize, 2)) KB" -ForegroundColor Gray
    $script:testsPassed++
}
else {
    Write-Host "  ⚠️  Database file not found" -ForegroundColor Yellow
}

# Test 8: Check Logs
Write-Host "`n8. Application Logs" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
if (Test-Path "logs") {
    $logFiles = Get-ChildItem "logs" -Filter "*.log" -ErrorAction SilentlyContinue
    if ($logFiles) {
        Write-Host "  ✅ Log directory exists" -ForegroundColor Green
        foreach ($log in $logFiles) {
            $logSize = $log.Length / 1KB
            Write-Host "    - $($log.Name): $([math]::Round($logSize, 2)) KB" -ForegroundColor Gray
        }
        $script:testsPassed++
    }
    else {
        Write-Host "  ⚠️  No log files found" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  ⚠️  Logs directory not found" -ForegroundColor Yellow
}

# Test 9: Check Configuration
Write-Host "`n9. Configuration" -ForegroundColor Cyan
Write-Host "-----------------------------------" -ForegroundColor Cyan
if (Test-Path ".env") {
    Write-Host "  ✅ Environment file: .env" -ForegroundColor Green
    $script:testsPassed++
}
else {
    Write-Host "  ⚠️  .env file not found" -ForegroundColor Yellow
}

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Test Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ✅ Tests Passed: $testsPassed" -ForegroundColor Green
Write-Host "  ❌ Tests Failed: $testsFailed" -ForegroundColor Red
Write-Host "  📊 Total Tests: $($testsPassed + $testsFailed)" -ForegroundColor White
Write-Host ""

# Access Information
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Access Information" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  🌐 API Base URL: $baseUrl" -ForegroundColor White
Write-Host "  📚 API Documentation: $baseUrl/api/docs" -ForegroundColor White
Write-Host "  📖 ReDoc: $baseUrl/api/redoc" -ForegroundColor White
Write-Host "  ❤️  Health Check: $baseUrl/health" -ForegroundColor White
Write-Host ""

# Next Steps
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Next Steps" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  1. Create admin user:" -ForegroundColor White
Write-Host "     python scripts/init_users.py" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Test authentication:" -ForegroundColor White
Write-Host "     Visit: $baseUrl/api/docs" -ForegroundColor Gray
Write-Host "     Click 'Authorize' and login" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Create a campaign:" -ForegroundColor White
Write-Host "     POST /api/v1/campaigns" -ForegroundColor Gray
Write-Host ""
Write-Host "  4. View logs:" -ForegroundColor White
Write-Host "     Get-Content logs/app.log -Tail 20" -ForegroundColor Gray
Write-Host ""

if ($testsFailed -eq 0) {
    Write-Host "All tests passed! API is working perfectly!" -ForegroundColor Green
}
else {
    Write-Host "Some tests failed. Check the errors above." -ForegroundColor Yellow
}

Write-Host ""
