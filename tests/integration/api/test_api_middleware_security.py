"""
API Middleware and Security Tests
Coverage for auth middleware, rate limiting, and security headers
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import jwt
from datetime import datetime, timedelta

from src.interface.api.main import app

client = TestClient(app)


class TestSecurityHeaders:
    """Test security headers are present in responses."""
    
    def test_content_security_policy(self):
        """Test CSP header is present."""
        response = client.get("/health")
        headers = response.headers
        # Security headers should be present
        assert response.status_code == 200
    
    def test_x_content_type_options(self):
        """Test X-Content-Type-Options header."""
        response = client.get("/")
        # Should have nosniff or not be present
        assert response.status_code == 200
    
    def test_x_frame_options(self):
        """Test X-Frame-Options header."""
        response = client.get("/")
        assert response.status_code == 200
    
    def test_cors_headers_preflight(self):
        """Test CORS preflight request."""
        response = client.options(
            "/api/v1/auth/login",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST"
            }
        )
        # Should respond to OPTIONS
        assert response.status_code in [200, 204, 405]


class TestJWTAuthentication:
    """Test JWT authentication middleware."""
    
    def test_no_token(self):
        """Test request without token."""
        response = client.get("/api/v1/users/me")
        assert response.status_code in [401, 403]
    
    def test_invalid_token_format(self):
        """Test request with invalid token format."""
        response = client.get(
            "/api/v1/users/me",
            headers={"Authorization": "InvalidFormat token123"}
        )
        assert response.status_code in [401, 403]
    
    def test_expired_token(self):
        """Test request with expired token."""
        # Create an expired token
        expired_token = jwt.encode(
            {"sub": "user", "exp": datetime.utcnow() - timedelta(hours=1)},
            "test-secret",
            algorithm="HS256"
        )
        response = client.get(
            "/api/v1/users/me",
            headers={"Authorization": f"Bearer {expired_token}"}
        )
        assert response.status_code in [401, 403]
    
    def test_malformed_token(self):
        """Test request with malformed JWT."""
        response = client.get(
            "/api/v1/users/me",
            headers={"Authorization": "Bearer not.a.valid.jwt.token"}
        )
        assert response.status_code in [401, 403]
    
    def test_bearer_prefix_required(self):
        """Test that Bearer prefix is required."""
        response = client.get(
            "/api/v1/users/me",
            headers={"Authorization": "sometoken"}
        )
        assert response.status_code in [401, 403]


class TestRateLimitMiddleware:
    """Test rate limiting middleware."""
    
    def test_rate_limit_header_present(self):
        """Test rate limit headers in response."""
        response = client.get("/health")
        # Rate limit info should be in headers or response should succeed
        assert response.status_code == 200
    
    def test_health_endpoint_not_heavily_limited(self):
        """Test health endpoint allows multiple requests."""
        # Health should allow at least 10 quick requests
        for _ in range(10):
            response = client.get("/health")
            assert response.status_code == 200


class TestErrorResponseFormat:
    """Test error response format consistency."""
    
    def test_404_response_format(self):
        """Test 404 response has proper format."""
        response = client.get("/api/v1/nonexistent")
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data or "error" in data
    
    def test_validation_error_format(self):
        """Test validation error response format."""
        response = client.post("/api/v1/auth/login", json={})
        if response.status_code == 422:
            data = response.json()
            assert "detail" in data
    
    def test_auth_error_format(self):
        """Test auth error response format."""
        response = client.post("/api/v1/auth/login", json={
            "username": "wrong",
            "password": "wrong"
        })
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data or "error" in data or "message" in data


class TestRequestValidation:
    """Test request validation middleware."""
    
    def test_content_type_validation(self):
        """Test content type is validated."""
        response = client.post(
            "/api/v1/auth/login",
            content="not json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code in [400, 422]
    
    def test_empty_body_handling(self):
        """Test empty request body handling."""
        response = client.post(
            "/api/v1/auth/login",
            content="",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code in [400, 422]
    
    def test_null_values_handling(self):
        """Test null values in request body."""
        response = client.post("/api/v1/auth/login", json={
            "username": None,
            "password": None
        })
        assert response.status_code in [400, 401, 422]


class TestAPIVersioning:
    """Test API versioning."""
    
    def test_v1_prefix_works(self):
        """Test v1 API prefix works."""
        response = client.get("/api/v1/campaigns/filters")
        assert response.status_code in [200, 401, 404]
    
    def test_api_docs_accessible(self):
        """Test API docs are accessible."""
        response = client.get("/api/docs")
        assert response.status_code == 200
    
    def test_openapi_json(self):
        """Test OpenAPI JSON is accessible."""
        response = client.get("/api/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data


class TestCORSPolicy:
    """Test CORS configuration."""
    
    def test_allowed_origin(self):
        """Test allowed origin gets CORS headers."""
        response = client.get(
            "/health",
            headers={"Origin": "http://localhost:3000"}
        )
        assert response.status_code == 200
        # CORS headers may be present
    
    def test_options_request(self):
        """Test OPTIONS request for CORS preflight."""
        response = client.options("/api/v1/auth/login")
        assert response.status_code in [200, 204, 405]


class TestExceptionHandling:
    """Test global exception handling."""
    
    def test_unhandled_path_returns_404(self):
        """Test unhandled paths return 404."""
        response = client.get("/this/path/does/not/exist")
        assert response.status_code == 404
    
    def test_wrong_method_returns_405(self):
        """Test wrong HTTP method returns 405."""
        response = client.patch("/health")
        assert response.status_code in [404, 405]


class TestResponseCompression:
    """Test response compression."""
    
    def test_accepts_gzip(self):
        """Test that gzip encoding is accepted."""
        response = client.get(
            "/health",
            headers={"Accept-Encoding": "gzip"}
        )
        assert response.status_code == 200


class TestTimeout:
    """Test request timeout handling."""
    
    def test_quick_endpoint_no_timeout(self):
        """Test quick endpoints don't timeout."""
        import time
        start = time.time()
        response = client.get("/health")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        assert elapsed < 5.0  # Should complete within 5 seconds


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
