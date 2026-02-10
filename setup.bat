@echo off
setlocal enabledelayedexpansion

echo 🚀 PCA Agent - Windows Setup
echo ============================

:: 1. Check for Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python not found. Please install Python 3.11+.
    pause
    exit /b 1
)

:: 2. Check for Node.js
npm --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Node.js/NPM not found. Please install Node.js.
    pause
    exit /b 1
)

:: 3. Run the Python management script
echo.
echo 📦 Running management setup...
python manage.py setup

if %errorlevel% neq 0 (
    echo ❌ Setup failed. Check the error messages above.
    pause
    exit /b 1
)

echo.
echo ✅ Setup successful!
echo To start the application, run: python manage.py start
echo.
pause
