#!/bin/bash
# Setup script for code quality tools
# Run: chmod +x scripts/setup-quality.sh && ./scripts/setup-quality.sh

set -e

echo "🔧 Setting up code quality tools..."

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✅ Python version: $python_version"

# Install dev dependencies
echo "📦 Installing dev dependencies..."
pip install --upgrade pip
pip install black isort flake8 mypy bandit safety pre-commit pytest pytest-cov pytest-asyncio

# Install pre-commit hooks
echo "🪝 Installing pre-commit hooks..."
pre-commit install
pre-commit install --hook-type commit-msg

# Run initial checks
echo "🔍 Running initial code quality checks..."

echo "  → Black (formatting)..."
black --check --line-length=100 src/ || echo "  ⚠️ Some files need formatting. Run: black src/"

echo "  → isort (imports)..."
isort --check-only --profile=black src/ || echo "  ⚠️ Some imports need sorting. Run: isort src/"

echo "  → flake8 (linting)..."
flake8 src/ --max-line-length=100 --ignore=E501,W503 --count || echo "  ⚠️ Some linting issues found."

echo "  → Bandit (security)..."
bandit -r src/ -ll -ii -x tests/ -q || echo "  ⚠️ Some security issues found."

echo ""
echo "✅ Code quality tools setup complete!"
echo ""
echo "📝 Available commands:"
echo "  • black src/           - Format code"
echo "  • isort src/           - Sort imports"
echo "  • flake8 src/          - Lint code"
echo "  • mypy src/            - Type check"
echo "  • bandit -r src/       - Security scan"
echo "  • safety check         - Dependency security"
echo "  • pre-commit run --all - Run all hooks"
echo "  • pytest tests/        - Run tests"
echo ""
