import os
import pytest

# Set environment variables before any other imports
os.environ["JWT_SECRET_KEY"] = "test_secret_for_unit_tests_only_32_chars_long_123"
os.environ["RATE_LIMIT_ENABLED"] = "true"
os.environ["DATABASE_URL"] = "sqlite:///./test_api.db"
