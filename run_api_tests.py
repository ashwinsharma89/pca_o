import os
import sys
import unittest

# 1. Establish Environment FIRST
os.environ["JWT_SECRET_KEY"] = "test_secret_for_unit_tests_only_32_chars_long_123"
os.environ["RATE_LIMIT_ENABLED"] = "true"
os.environ["DATABASE_URL"] = "sqlite:///./test_api.db"
os.environ["JWT_ALGORITHM"] = "HS256"

# 2. Setup discovery
sys.path.insert(0, os.path.abspath(os.getcwd()))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    # Discover all tests in interface/api
    suite = loader.discover('tests/unit/interface/api', pattern='test_*.py', top_level_dir='.')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    if not result.wasSuccessful():
        sys.exit(1)
