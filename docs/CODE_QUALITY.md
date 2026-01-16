# Code Quality Tools

## Quick Setup

```bash
# One-command setup
chmod +x scripts/setup-quality.sh && ./scripts/setup-quality.sh

# Or manually:
pip install black isort flake8 mypy bandit safety pre-commit
pre-commit install
```

## Tool Commands

| Tool | Command | Description |
|------|---------|-------------|
| **Black** | `black src/` | Code formatting |
| **isort** | `isort src/` | Import sorting |
| **flake8** | `flake8 src/` | Linting |
| **mypy** | `mypy src/` | Type checking |
| **Bandit** | `bandit -r src/` | Security scanning |
| **Safety** | `safety check` | Dependency vulnerabilities |
| **Pre-commit** | `pre-commit run --all` | Run all hooks |

## Pre-commit Hooks

Hooks run automatically on every commit:
- ✅ Black formatting
- ✅ isort imports
- ✅ flake8 linting
- ✅ mypy type checking
- ✅ Bandit security
- ✅ Trailing whitespace
- ✅ Large file check
- ✅ Private key detection

## CI/CD Pipeline

GitHub Actions automatically runs on push/PR:
- Backend quality checks (Black, isort, flake8, mypy)
- Security scanning (Bandit, Safety)
- Backend tests with coverage
- Frontend linting and build
- Docker image builds (on main)

## Configuration

All tools configured in `pyproject.toml`:
- Line length: 100
- Python version: 3.11
- Coverage threshold: 60%

## Troubleshooting

```bash
# Fix formatting issues
black src/ && isort src/

# Skip a hook temporarily
SKIP=mypy git commit -m "message"

# Update pre-commit hooks
pre-commit autoupdate
```
