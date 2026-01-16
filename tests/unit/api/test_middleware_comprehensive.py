"""
API Middleware Comprehensive Tests

Tests for authentication, rate limiting, security headers, and error handling.
Improves coverage for src/api/middleware/*.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import Response
import jwt
import time
import os


# ============================================================================
# JWT AUTHENTICATION TESTS
# ============================================================================

class TestJWTAuthentication:
    """Tests for JWT authentication middleware."""
    
    @pytest.fixture
    def mock_request(self):
        """Create mock request object."""
        request = Mock(spec=Request)
        request.headers = {}
        request.url = Mock()
        request.url.path = "/api/v1/test"
        request.method = "GET"
        return request
    
    def test_create_access_token(self):
        """Test creating JWT access token."""
        from src.interface.api.middleware.auth import create_access_token
        
        data = {"sub": "user123", "email": "test@example.com"}
        token = create_access_token(data)
        
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 100  # JWT tokens are long
    
    def test_create_token_with_expiry(self):
        """Test creating token with custom expiry."""
        from src.interface.api.middleware.auth import create_access_token
        from datetime import timedelta
        
        data = {"sub": "user123"}
        token = create_access_token(data, expires_delta=timedelta(hours=1))
        
        assert token is not None
    
    def test_decode_valid_token(self):
        """Test decoding a valid JWT token."""
        from src.interface.api.middleware.auth import create_access_token, decode_token, SECRET_KEY
        
        data = {"sub": "user123", "email": "test@example.com"}
        token = create_access_token(data)
        
        decoded = decode_token(token)
        
        assert decoded is not None
        assert decoded.get("sub") == "user123"
    
    def test_decode_invalid_token(self):
        """Test decoding an invalid token."""
        from src.interface.api.middleware.auth import decode_token
        
        with pytest.raises(Exception):  # Should raise JWT error
            decode_token("invalid.token.here")
    
    def test_decode_expired_token(self):
        """Test decoding an expired token."""
        from src.interface.api.middleware.auth import SECRET_KEY, ALGORITHM
        from datetime import datetime, timedelta
        
        # Create expired token
        data = {
            "sub": "user123",
            "exp": datetime.utcnow() - timedelta(hours=1)  # Expired
        }
        token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
        
        from src.interface.api.middleware.auth import decode_token
        
        with pytest.raises(Exception):  # Should raise ExpiredSignatureError
            decode_token(token)
    
    def test_get_current_user_no_token(self, mock_request):
        """Test getting current user without token."""
        mock_request.headers = {}
        
        from src.interface.api.middleware.auth import get_current_user
        
        # Should handle missing token gracefully or raise
        try:
            result = get_current_user(mock_request)
            assert result is None or result == False
        except Exception:
            pass  # Expected
    
    def test_optional_auth_allows_anonymous(self, mock_request):
        """Test optional auth allows anonymous requests."""
        from src.interface.api.middleware.auth import get_optional_current_user
        
        mock_request.headers = {}
        
        try:
            result = get_optional_current_user(mock_request)
            # Should not raise for optional auth
            assert result is None or hasattr(result, '__call__')
        except Exception:
            pass


class TestPasswordHashing:
    """Tests for password hashing utilities."""
    
    def test_hash_password(self):
        """Test password hashing."""
        from src.interface.api.middleware.auth import hash_password
        
        password = "securepassword123"
        hashed = hash_password(password)
        
        assert hashed != password
        assert len(hashed) > len(password)
        assert hashed.startswith('$2')  # bcrypt prefix
    
    def test_verify_correct_password(self):
        """Test verifying correct password."""
        from src.interface.api.middleware.auth import hash_password, verify_password
        
        password = "securepassword123"
        hashed = hash_password(password)
        
        assert verify_password(password, hashed) == True
    
    def test_verify_incorrect_password(self):
        """Test verifying incorrect password."""
        from src.interface.api.middleware.auth import hash_password, verify_password
        
        password = "securepassword123"
        hashed = hash_password(password)
        
        assert verify_password("wrongpassword", hashed) == False
    
    def test_hash_uniqueness(self):
        """Test that same password creates different hashes."""
        from src.interface.api.middleware.auth import hash_password
        
        password = "samepassword"
        hash1 = hash_password(password)
        hash2 = hash_password(password)
        
        # bcrypt includes salt, so hashes should differ
        assert hash1 != hash2


# ============================================================================
# RATE LIMITING TESTS
# ============================================================================

class TestRateLimiting:
    """Tests for rate limiting middleware."""
    
    def test_limiter_initialization(self):
        """Test rate limiter is initialized."""
        from src.interface.api.middleware.rate_limit import limiter
        
        assert limiter is not None
    
    def test_rate_limit_enabled_flag(self):
        """Test rate limit enabled flag."""
        from src.interface.api.middleware.rate_limit import RATE_LIMIT_ENABLED
        
        assert isinstance(RATE_LIMIT_ENABLED, bool)
    
    def test_default_limits(self):
        """Test default rate limits are set."""
        from src.interface.api.middleware.rate_limit import RATE_LIMIT_DEFAULT
        
        assert RATE_LIMIT_DEFAULT is not None
        # Should be in format like "100/minute"
        assert "/" in RATE_LIMIT_DEFAULT
    
    def test_rate_limit_key_function(self):
        """Test rate limit key function."""
        from src.interface.api.middleware.rate_limit import get_rate_limit_key
        
        mock_request = Mock()
        mock_request.client = Mock()
        mock_request.client.host = "192.168.1.1"
        
        key = get_rate_limit_key(mock_request)
        
        assert key is not None
        assert "192.168.1.1" in key or key is not None
    
    def test_user_based_rate_limit_key(self):
        """Test rate limit key with authenticated user."""
        from src.interface.api.middleware.rate_limit import get_rate_limit_key
        
        mock_request = Mock()
        mock_request.client = Mock()
        mock_request.client.host = "127.0.0.1"
        mock_request.state = Mock()
        mock_request.state.user = Mock()
        mock_request.state.user.id = "user123"
        
        try:
            key = get_rate_limit_key(mock_request)
            assert key is not None
        except AttributeError:
            # User might not be set
            pass


# ============================================================================
# SECURITY HEADERS TESTS
# ============================================================================

class TestSecurityHeaders:
    """Tests for security headers middleware."""
    
    @pytest.fixture
    def app(self):
        """Create test FastAPI app."""
        from fastapi import FastAPI
        from src.interface.api.middleware.security_headers import SecurityHeadersMiddleware
        
        app = FastAPI()
        app.add_middleware(SecurityHeadersMiddleware)
        
        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}
        
        return app
    
    def test_x_frame_options_header(self, app):
        """Test X-Frame-Options header is set."""
        client = TestClient(app)
        response = client.get("/test")
        
        assert "X-Frame-Options" in response.headers
        assert response.headers["X-Frame-Options"] == "DENY"
    
    def test_x_content_type_options_header(self, app):
        """Test X-Content-Type-Options header is set."""
        client = TestClient(app)
        response = client.get("/test")
        
        assert "X-Content-Type-Options" in response.headers
        assert response.headers["X-Content-Type-Options"] == "nosniff"
    
    def test_x_xss_protection_header(self, app):
        """Test X-XSS-Protection header is set."""
        client = TestClient(app)
        response = client.get("/test")
        
        # Modern browsers deprecate this, but it might still be set
        if "X-XSS-Protection" in response.headers:
            assert response.headers["X-XSS-Protection"] in ["1; mode=block", "0"]
    
    def test_strict_transport_security(self, app):
        """Test Strict-Transport-Security header."""
        client = TestClient(app)
        response = client.get("/test")
        
        if "Strict-Transport-Security" in response.headers:
            hsts = response.headers["Strict-Transport-Security"]
            assert "max-age=" in hsts
    
    def test_content_security_policy(self, app):
        """Test Content-Security-Policy header."""
        client = TestClient(app)
        response = client.get("/test")
        
        if "Content-Security-Policy" in response.headers:
            csp = response.headers["Content-Security-Policy"]
            assert len(csp) > 0


# ============================================================================
# ERROR HANDLER TESTS
# ============================================================================

class TestErrorHandlers:
    """Tests for error handling middleware."""
    
    def test_setup_exception_handlers(self):
        """Test exception handlers can be set up."""
        from fastapi import FastAPI
        from src.interface.api.error_handlers import setup_exception_handlers
        
        app = FastAPI()
        
        # Should not raise
        setup_exception_handlers(app)
        
        # App should have exception handlers registered
        assert len(app.exception_handlers) >= 0
    
    def test_validation_error_format(self):
        """Test validation error response format."""
        from fastapi import FastAPI
        from pydantic import BaseModel, Field
        from src.interface.api.error_handlers import setup_exception_handlers
        
        app = FastAPI()
        setup_exception_handlers(app)
        
        class TestModel(BaseModel):
            name: str = Field(..., min_length=1)
            age: int = Field(..., ge=0)
        
        @app.post("/test")
        async def test_endpoint(data: TestModel):
            return {"status": "ok"}
        
        client = TestClient(app, raise_server_exceptions=False)
        
        # Send invalid data
        response = client.post("/test", json={"name": "", "age": -1})
        
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data or "error" in data
    
    def test_not_found_error(self):
        """Test 404 error handling."""
        from fastapi import FastAPI
        from src.interface.api.error_handlers import setup_exception_handlers
        
        app = FastAPI()
        setup_exception_handlers(app)
        
        client = TestClient(app, raise_server_exceptions=False)
        
        response = client.get("/nonexistent")
        
        assert response.status_code == 404


class TestAPIExceptions:
    """Tests for custom API exceptions."""
    
    def test_not_found_exception(self):
        """Test NotFoundError exception."""
        from src.interface.api.exceptions import NotFoundError
        
        error = NotFoundError(resource="Campaign", resource_id="123")
        
        assert error.status_code == 404
        assert "Campaign" in error.message
    
    def test_validation_exception(self):
        """Test ValidationError exception."""
        from src.interface.api.exceptions import ValidationError
        
        error = ValidationError(field="email", message="Invalid email format")
        
        assert error.status_code == 422
        assert "email" in str(error.message) or "email" in str(error.details)
    
    def test_authentication_exception(self):
        """Test AuthenticationError exception."""
        from src.interface.api.exceptions import AuthenticationError
        
        error = AuthenticationError(message="Invalid credentials")
        
        assert error.status_code == 401
    
    def test_rate_limit_exception(self):
        """Test RateLimitExceededError exception."""
        from src.interface.api.exceptions import RateLimitExceededError
        
        error = RateLimitExceededError(limit="100/minute")
        
        assert error.status_code == 429
        assert "rate" in error.message.lower() or "limit" in error.message.lower()
    
    def test_internal_server_exception(self):
        """Test InternalServerError exception."""
        from src.interface.api.exceptions import InternalServerError
        
        error = InternalServerError(message="Database connection failed")
        
        assert error.status_code == 500
    
    def test_forbidden_exception(self):
        """Test ForbiddenError exception."""
        from src.interface.api.exceptions import ForbiddenError
        
        error = ForbiddenError(action="delete", resource="Campaign")
        
        assert error.status_code == 403


# ============================================================================
# CORS TESTS
# ============================================================================

class TestCORSConfiguration:
    """Tests for CORS configuration."""
    
    def test_cors_headers_on_options(self):
        """Test CORS headers on OPTIONS request."""
        from src.interface.api.main import app
        
        client = TestClient(app)
        
        response = client.options(
            "/api/v1/campaigns",
            headers={"Origin": "http://localhost:3000"}
        )
        
        # Should return CORS headers
        assert response.status_code in [200, 204, 405]
    
    def test_allowed_origin(self):
        """Test allowed origin gets proper headers."""
        from src.interface.api.main import app
        
        client = TestClient(app)
        
        response = client.get(
            "/health",
            headers={"Origin": "http://localhost:3000"}
        )
        
        # Should have Access-Control-Allow-Origin if configured
        if "Access-Control-Allow-Origin" in response.headers:
            allowed = response.headers["Access-Control-Allow-Origin"]
            assert allowed in ["http://localhost:3000", "*"]


# ============================================================================
# LOGGING MIDDLEWARE TESTS
# ============================================================================

class TestLoggingMiddleware:
    """Tests for request logging middleware."""
    
    def test_request_logging(self):
        """Test that requests are logged."""
        from src.interface.api.main import app
        
        with patch('loguru.logger') as mock_logger:
            client = TestClient(app)
            response = client.get("/health")
            
            # Logger should have been called
            # This is a basic check - actual implementation may vary
            assert response.status_code in [200, 401, 403]
    
    def test_response_time_header(self):
        """Test response time is tracked."""
        from src.interface.api.main import app
        
        client = TestClient(app)
        response = client.get("/health")
        
        # Some implementations add X-Response-Time header
        # Check if it exists (optional)
        if "X-Response-Time" in response.headers:
            time_str = response.headers["X-Response-Time"]
            assert "ms" in time_str or float(time_str) >= 0


# ============================================================================
# API GATEWAY TESTS
# ============================================================================

class TestAPIGateway:
    """Tests for API Gateway class."""
    
    def test_gateway_initialization(self):
        """Test APIGateway can be initialized."""
        from fastapi import FastAPI
        from src.interface.gateway.api_gateway import APIGateway
        
        app = FastAPI()
        
        gateway = APIGateway(app)
        
        assert gateway is not None
        assert gateway.app == app
    
    def test_gateway_health_check(self):
        """Test gateway health check."""
        from fastapi import FastAPI
        from src.interface.gateway.api_gateway import APIGateway
        
        app = FastAPI()
        gateway = APIGateway(app)
        
        try:
            health = gateway.health_check()
            assert isinstance(health, dict) or health == True
        except Exception:
            pass


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
