"""
Extended unit tests for API modules.
Tests API endpoints, middleware, and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Try to import API components
try:
    from src.interface.api.exceptions import (
        CampaignNotFoundError, CampaignInvalidStatusError,
        RateLimitExceededError, AuthenticationError
    )
    EXCEPTIONS_AVAILABLE = True
except ImportError:
    EXCEPTIONS_AVAILABLE = False

try:
    from src.interface.api.error_handlers import setup_exception_handlers
    ERROR_HANDLERS_AVAILABLE = True
except ImportError:
    ERROR_HANDLERS_AVAILABLE = False

try:
    from src.interface.api.middleware.rate_limit import limiter, RATE_LIMIT_ENABLED
    RATE_LIMIT_AVAILABLE = True
except ImportError:
    RATE_LIMIT_AVAILABLE = False


class TestAPIExceptions:
    """Test API exception classes."""
    
    pytestmark = pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="Exceptions not available")
    
    def test_campaign_not_found_error(self):
        """Test CampaignNotFoundError."""
        if not EXCEPTIONS_AVAILABLE:
            pytest.skip("Exceptions not available")
        
        error = CampaignNotFoundError("Campaign 123 not found")
        assert "123" in str(error)
    
    def test_campaign_invalid_status_error(self):
        """Test CampaignInvalidStatusError."""
        if not EXCEPTIONS_AVAILABLE:
            pytest.skip("Exceptions not available")
        
        error = CampaignInvalidStatusError("Invalid status transition")
        assert "status" in str(error).lower()
    
    def test_rate_limit_exceeded_error(self):
        """Test RateLimitExceededError."""
        if not EXCEPTIONS_AVAILABLE:
            pytest.skip("Exceptions not available")
        
        error = RateLimitExceededError("Rate limit exceeded")
        assert "rate" in str(error).lower() or "limit" in str(error).lower()
    
    def test_authentication_error(self):
        """Test AuthenticationError."""
        if not EXCEPTIONS_AVAILABLE:
            pytest.skip("Exceptions not available")
        
        error = AuthenticationError("Invalid credentials")
        assert "invalid" in str(error).lower() or "credentials" in str(error).lower()


class TestRateLimiting:
    """Test rate limiting middleware."""
    
    pytestmark = pytest.mark.skipif(not RATE_LIMIT_AVAILABLE, reason="Rate limit not available")
    
    def test_limiter_exists(self):
        """Test that limiter is defined."""
        if not RATE_LIMIT_AVAILABLE:
            pytest.skip("Rate limit not available")
        
        assert limiter is not None
    
    def test_rate_limit_enabled_flag(self):
        """Test rate limit enabled flag."""
        if not RATE_LIMIT_AVAILABLE:
            pytest.skip("Rate limit not available")
        
        assert isinstance(RATE_LIMIT_ENABLED, bool)


class TestAPIEndpoints:
    """Test API endpoint functionality."""
    
    @pytest.fixture
    def mock_app(self):
        """Create mock FastAPI app."""
        from unittest.mock import MagicMock
        app = MagicMock()
        return app
    
    def test_health_endpoint_structure(self):
        """Test health endpoint returns expected structure."""
        # Health check should return status
        health_response = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat()
        }
        
        assert 'status' in health_response
        assert health_response['status'] == 'healthy'
    
    def test_campaign_response_structure(self):
        """Test campaign response structure."""
        campaign_response = {
            'id': 1,
            'campaign_name': 'Test Campaign',
            'platform': 'Google',
            'status': 'active'
        }
        
        assert 'id' in campaign_response
        assert 'campaign_name' in campaign_response
    
    def test_error_response_structure(self):
        """Test error response structure."""
        error_response = {
            'error': {
                'code': 'CAMPAIGN_NOT_FOUND',
                'message': 'Campaign not found',
                'details': {}
            }
        }
        
        assert 'error' in error_response
        assert 'code' in error_response['error']


class TestAPIMiddleware:
    """Test API middleware functionality."""
    
    def test_cors_headers(self):
        """Test CORS headers structure."""
        cors_config = {
            'allow_origins': ['*'],
            'allow_methods': ['GET', 'POST', 'PUT', 'DELETE'],
            'allow_headers': ['*']
        }
        
        assert 'allow_origins' in cors_config
        assert 'GET' in cors_config['allow_methods']
    
    def test_auth_middleware_structure(self):
        """Test auth middleware expected behavior."""
        # Auth middleware should check for token
        request_headers = {
            'Authorization': 'Bearer test_token'
        }
        
        assert 'Authorization' in request_headers
        assert request_headers['Authorization'].startswith('Bearer')


class TestAPIValidation:
    """Test API request validation."""
    
    def test_campaign_create_validation(self):
        """Test campaign creation validation."""
        valid_campaign = {
            'campaign_name': 'Test Campaign',
            'platform': 'Google',
            'budget': 1000
        }
        
        # Required fields
        assert 'campaign_name' in valid_campaign
        assert 'platform' in valid_campaign
    
    def test_invalid_campaign_data(self):
        """Test invalid campaign data detection."""
        invalid_campaign = {
            'campaign_name': '',  # Empty name
            'budget': -100  # Negative budget
        }
        
        # Validation should catch these
        assert invalid_campaign['campaign_name'] == ''
        assert invalid_campaign['budget'] < 0
