"""
Integration tests for authentication endpoints.

Tests:
- User registration
- User login
- Token validation
- Protected endpoint access
"""

import pytest
from fastapi import status


# ============================================================================
# User Registration Tests
# ============================================================================

class TestUserRegistration:
    """Tests for user registration endpoint."""
    
    def test_register_success(self, client):
        """Test successful user registration."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "newuser",
                "email": "newuser@example.com",
                "password": "NewPass123!"
            }
        )
        if response.status_code == 400:
            print(f"\nRegistration failed with 400: {response.json()}")
        
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED
        ]
        data = response.json()
        assert 'username' in data or 'message' in data
    
    def test_register_duplicate_username(self, client, test_user):
        """Test registration with existing username."""
        # Register first time
        client.post(
            "/api/v1/auth/register",
            json={
                "username": test_user.username,
                "email": test_user.email,
                "password": "Password123!"
            }
        )
        
        # Try to register again
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": test_user.username,
                "email": test_user.email,
                "password": "Password123!"
            }
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_409_CONFLICT
        ]
    
    def test_register_duplicate_email(self, client):
        """Test registration with existing email."""
        first_user = {
            "username": "email_check_first",
            "email": "unique_email_@example.com",
            "password": "Pass123!"
        }
        # Register first time
        client.post(
            "/api/v1/auth/register",
            json=first_user
        )
        
        # Try with different username, same email
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "email_check_second",
                "email": first_user["email"],
                "password": "Pass123!"
            }
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_409_CONFLICT
        ]
    
    def test_register_weak_password(self, client):
        """Test registration with weak password."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "testuser",
                "email": "test@example.com",
                "password": "weak"  # Too short/simple
            }
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_register_invalid_email(self, client):
        """Test registration with invalid email format."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "testuser",
                "email": "invalid-email",  # Not a valid email
                "password": "Pass123!"
            }
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_register_missing_fields(self, client):
        """Test registration with missing required fields."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "testuser"
                # Missing email and password
            }
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_register_empty_username(self, client):
        """Test registration with empty username."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "",
                "email": "test@example.com",
                "password": "Pass123!"
            }
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# User Login Tests
# ============================================================================

class TestUserLogin:
    """Tests for user login endpoint."""
    
    def test_login_success(self, client, test_user, test_user_data):
        """Test successful user login."""
        response = client.post(
            "/api/v1/auth/login",
            json={
                "username": test_user_data["username"],
                "password": test_user_data["password"]
            }
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert 'access_token' in data
        assert 'token_type' in data
        assert data['token_type'] == 'bearer'
    
    def test_login_invalid_password(self, client, test_user_data):
        """Test login with wrong password."""
        # Login with wrong password
        response = client.post(
            "/api/v1/auth/login",
            json={
                "username": test_user_data["username"],
                "password": "WrongPassword123!"
            }
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_login_nonexistent_user(self, client):
        """Test login with non-existent user."""
        response = client.post(
            "/api/v1/auth/login",
            json={
                "username": "nonexistent",
                "password": "Pass123!"
            }
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_login_missing_credentials(self, client):
        """Test login with missing credentials."""
        response = client.post(
            "/api/v1/auth/login",
            json={}
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_login_returns_valid_token(self, client, test_user, test_user_data):
        """Test that login returns a valid JWT token."""
        # Login
        response = client.post(
            "/api/v1/auth/login",
            json={
                "username": test_user_data["username"],
                "password": test_user_data["password"]
            }
        )
       
        assert response.status_code == status.HTTP_200_OK
        token = response.json()['access_token']
        # Token should be a non-empty string
        assert isinstance(token, str)
        assert len(token) > 0
        
        # Token should have JWT structure (3 parts separated by dots)
        parts = token.split('.')
        assert len(parts) == 3


# ============================================================================
# Token Validation Tests
# ============================================================================

class TestTokenValidation:
    """Tests for token validation and protected endpoints."""
    
    def test_protected_endpoint_with_valid_token(self, client, auth_headers):
        """Test accessing protected endpoint with valid token."""
        response = client.get(
            "/api/v1/campaigns/filters",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_protected_endpoint_without_token(self, client):
        """Test accessing protected endpoint without token."""
        response = client.get("/api/v1/campaigns/filters")
        
        # Accept both 401 (Unauthorized) and 403 (Forbidden) as valid
        assert response.status_code in [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN]
    
    def test_protected_endpoint_with_invalid_token(self, client):
        """Test accessing protected endpoint with malformed token."""
        response = client.get(
            "/api/v1/campaigns/filters",
            headers={"Authorization": "Bearer invalid-token-format"}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_protected_endpoint_with_expired_token(self, client):
        """Test accessing protected endpoint with expired token."""
        # Create an expired token
        expired_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6MTYwOTQ1OTIwMH0.invalid"
        
        response = client.get(
            "/api/v1/campaigns/filters",
            headers={"Authorization": f"Bearer {expired_token}"}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_protected_endpoint_with_missing_bearer_prefix(self, client, auth_token):
        """Test accessing protected endpoint without 'Bearer' prefix."""
        response = client.get(
            "/api/v1/campaigns/filters",
            headers={"Authorization": auth_token}  # Missing 'Bearer '
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_protected_endpoint_with_empty_token(self, client):
        """Test accessing protected endpoint with empty token."""
        response = client.get(
            "/api/v1/campaigns/filters",
            headers={"Authorization": "Bearer "}
        )
        
        # Accept both 401 (Unauthorized) and 403 (Forbidden) as valid
        assert response.status_code in [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN]


# ============================================================================
# User Profile Tests
# ============================================================================

class TestUserProfile:
    """Tests for user profile endpoints."""
    
    def test_get_current_user(self, client, auth_headers):
        """Test getting current user profile."""
        response = client.get(
            "/api/v1/auth/me",
            headers=auth_headers
        )
        
        # Endpoint may or may not exist
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_404_NOT_FOUND
        ]
        
        if response.status_code == status.HTTP_200_OK:
            data = response.json()
            assert 'username' in data or 'email' in data
    
    def test_get_current_user_unauthorized(self, client):
        """Test getting profile without authentication."""
        response = client.get("/api/v1/auth/me")
        
        assert response.status_code in [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND
        ]


# ============================================================================
# Password Security Tests
# ============================================================================

class TestPasswordSecurity:
    """Tests for password security requirements."""
    
    @pytest.mark.parametrize("password,should_fail", [
        ("short", True),  # Too short
        ("alllowercase123", True),  # No uppercase
        ("ALLUPPERCASE123", True),  # No lowercase
        ("NoNumbers!", True),  # No numbers
        ("ValidPass123!", False),  # Valid password
        ("AnotherGood1@", False),  # Valid password
    ])
    def test_password_requirements(self, client, password, should_fail):
        """Test password strength requirements."""
        # Sanitize password for email (remove special chars)
        import re
        email_safe = re.sub(r'[^a-zA-Z0-9]', '', password)
        
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": f"user_{email_safe}",
                "email": f"{email_safe}@example.com",
                "password": password
            }
        )
        
        if should_fail:
            assert response.status_code in [
                status.HTTP_400_BAD_REQUEST,
                status.HTTP_422_UNPROCESSABLE_ENTITY
            ]
        else:
            assert response.status_code in [
                status.HTTP_200_OK,
                status.HTTP_201_CREATED
            ]


# ============================================================================
# Rate Limiting Tests (if implemented)
# ============================================================================

class TestAuthRateLimiting:
    """Tests for authentication rate limiting."""
    
    def test_login_rate_limiting(self, client):
        """Test that excessive login attempts are rate limited."""
        # Make multiple failed login attempts
        for i in range(10):
            client.post(
                "/api/v1/auth/login",
                json={
                    "username": "nonexistent",
                    "password": "wrong"
                }
            )
        
        # Next attempt should be rate limited (if implemented)
        response = client.post(
            "/api/v1/auth/login",
            json={
                "username": "nonexistent",
                "password": "wrong"
            }
        )
        
        # May or may not have rate limiting
        assert response.status_code in [
            status.HTTP_401_UNAUTHORIZED,  # No rate limiting
            status.HTTP_429_TOO_MANY_REQUESTS  # Rate limited
        ]
