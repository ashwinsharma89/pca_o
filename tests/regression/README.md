# Regression Tests

This directory contains regression tests that ensure core functionality continues to work after changes.

## What are Regression Tests?

Regression tests verify that previously working features still work after code changes. They act as a safety net to catch unintended side effects.

## Running Regression Tests

```bash
# Run all regression tests
pytest tests/regression/ -v

# Run specific test class
pytest tests/regression/test_core_functionality.py::TestAuthentication -v

# Run specific test
pytest tests/regression/test_core_functionality.py::test_login_with_valid_credentials -v

# Run with coverage
pytest tests/regression/ --cov=src --cov-report=html
```

## Test Categories

### Authentication Tests
- Login with valid credentials
- Login with invalid credentials
- Protected endpoints require auth

### Dashboard Data Tests
- Dashboard stats endpoint
- Visualizations endpoint
- Filter options endpoint

### Month Filtering Tests
- Monthly performance structure
- Platform performance filtering
- Data integrity

### Health Checks
- System health endpoint
- API documentation accessibility

## When to Run

**Before committing**: Pre-commit hooks will run automatically

**Before deploying**: 
```bash
pytest tests/regression/ -v
```

**After adding new features**: Add new regression tests for the feature

## Adding New Tests

When you add a new feature, add a regression test:

```python
class TestYourNewFeature:
    """Test your new feature doesn't break."""
    
    def test_feature_works(self):
        # Test the feature
        response = client.get("/your/endpoint")
        assert response.status_code == 200
```

## Test Fixtures

Common fixtures available:
- `auth_headers` - Authentication headers for protected endpoints
- `client` - FastAPI test client

## Expected Behavior

All tests should pass. If a test fails:
1. Check what changed recently
2. Determine if it's a real bug or test needs updating
3. Fix the bug or update the test
4. Re-run tests to confirm
