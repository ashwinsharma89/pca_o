"""
Unit tests for API authentication.

Tests JWT authentication with mocked dependencies.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone

from src.interface.api.middleware.auth import (
    create_access_token,
    verify_password,
    authenticate_user,
    get_user
)
from src.interface.api.exceptions import (
    InvalidCredentialsError,
    TokenExpiredError,
    TokenInvalidError
)


@pytest.mark.unit
class TestPasswordHashing:
    """Test password hashing and verification."""
    
    def test_verify_password_correct(self):
        """Test password verification with correct password."""
        import bcrypt
        
        password = "test123"
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        
        assert verify_password(password, hashed) is True
    
    def test_verify_password_incorrect(self):
        """Test password verification with incorrect password."""
        import bcrypt
        
        password = "test123"
        wrong_password = "wrong123"
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        
        assert verify_password(wrong_password, hashed) is False


@pytest.mark.unit
class TestJWTTokens:
    """Test JWT token creation and verification."""
    
    def test_create_access_token(self):
        """Test JWT token creation."""
        data = {"sub": "test_user", "role": "user"}
        token = create_access_token(data)
        
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_create_token_with_expiration(self):
        """Test token creation with custom expiration."""
        data = {"sub": "test_user"}
        expires_delta = timedelta(minutes=15)
        
        token = create_access_token(data, expires_delta=expires_delta)
        
        assert token is not None
        
        # Decode and verify expiration
        from jose import jwt
        import os
        
        secret_key = os.getenv("JWT_SECRET_KEY", "change-this-secret-key")
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        
        assert "exp" in payload
        assert "sub" in payload
        assert payload["sub"] == "test_user"
    
    @pytest.mark.asyncio
    async def test_verify_valid_token(self):
        """Test verification of valid token."""
        from fastapi.security import HTTPAuthorizationCredentials
        
        # Create token
        data = {"sub": "test_user", "role": "user"}
        token = create_access_token(data)
        
        # Create credentials
        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials=token
        )
        
        # Verify token
        from src.interface.api.middleware.auth import verify_token
        payload = await verify_token(credentials)
        
        assert payload is not None
        assert payload["sub"] == "test_user"
    
    @pytest.mark.asyncio
    async def test_verify_invalid_token(self):
        """Test verification of invalid token."""
        from fastapi.security import HTTPAuthorizationCredentials
        from fastapi import HTTPException
        
        # Create invalid token
        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials="invalid.token.here"
        )
        
        # Verify token should raise exception
        from src.interface.api.middleware.auth import verify_token
        
        with pytest.raises(HTTPException) as exc_info:
            await verify_token(credentials)
        
        assert exc_info.value.status_code == 401


@pytest.mark.unit
class TestUserAuthentication:
    """Test user authentication logic."""
    
    def test_get_existing_user(self):
        """Test getting existing user with mocked database."""
        import bcrypt
        mock_user = Mock()
        mock_user.username = "admin"
        mock_user.email = "admin@test.com"
        mock_user.hashed_password = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
        mock_user.role = "admin"
        mock_user.tier = "enterprise"
        
        mock_session = Mock()
        mock_service = Mock()
        mock_service.get_user_by_username.return_value = mock_user
        mock_service.get_user_by_email.return_value = None
        
        with patch('src.core.database.connection.get_db_manager') as mock_db:
            mock_db.return_value.get_session_direct.return_value = mock_session
            
            with patch('src.engine.services.user_service.UserService', return_value=mock_service):
                user = get_user("admin")
                
                assert user is not None
                assert user["username"] == "admin"
                assert "hashed_password" in user
    
    def test_get_nonexistent_user(self):
        """Test getting non-existent user."""
        mock_session = Mock()
        mock_service = Mock()
        mock_service.get_user_by_username.return_value = None
        mock_service.get_user_by_email.return_value = None
        
        with patch('src.core.database.connection.get_db_manager') as mock_db:
            mock_db.return_value.get_session_direct.return_value = mock_session
            
            with patch('src.engine.services.user_service.UserService', return_value=mock_service):
                user = get_user("nonexistent_user")
                
                assert user is None
    
    def test_authenticate_valid_credentials(self):
        """Test authentication with valid credentials."""
        import bcrypt
        hashed = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
        
        mock_user = Mock()
        mock_user.username = "admin"
        mock_user.email = "admin@test.com"
        mock_user.hashed_password = hashed
        mock_user.role = "admin"
        mock_user.tier = "enterprise"
        
        mock_session = Mock()
        mock_service = Mock()
        mock_service.get_user_by_username.return_value = mock_user
        mock_service.get_user_by_email.return_value = None
        
        with patch('src.core.database.connection.get_db_manager') as mock_db:
            mock_db.return_value.get_session_direct.return_value = mock_session
            
            with patch('src.engine.services.user_service.UserService', return_value=mock_service):
                user = authenticate_user("admin", "admin123")
                
                assert user is not None
                assert user["username"] == "admin"
    
    def test_authenticate_invalid_password(self):
        """Test authentication with invalid password."""
        import bcrypt
        hashed = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
        
        mock_user = Mock()
        mock_user.username = "admin"
        mock_user.email = "admin@test.com"
        mock_user.hashed_password = hashed
        mock_user.role = "admin"
        mock_user.tier = "enterprise"
        
        mock_session = Mock()
        mock_service = Mock()
        mock_service.get_user_by_username.return_value = mock_user
        mock_service.get_user_by_email.return_value = None
        
        with patch('src.core.database.connection.get_db_manager') as mock_db:
            mock_db.return_value.get_session_direct.return_value = mock_session
            
            with patch('src.engine.services.user_service.UserService', return_value=mock_service):
                user = authenticate_user("admin", "wrong_password")
                
                assert user is None
    
    def test_authenticate_nonexistent_user(self):
        """Test authentication with non-existent user."""
        mock_session = Mock()
        mock_service = Mock()
        mock_service.get_user_by_username.return_value = None
        mock_service.get_user_by_email.return_value = None
        
        with patch('src.core.database.connection.get_db_manager') as mock_db:
            mock_db.return_value.get_session_direct.return_value = mock_session
            
            with patch('src.engine.services.user_service.UserService', return_value=mock_service):
                user = authenticate_user("nonexistent", "password")
                
                assert user is None


@pytest.mark.unit
class TestAuthExceptions:
    """Test authentication exception handling."""
    
    def test_invalid_credentials_error(self):
        """Test InvalidCredentialsError."""
        error = InvalidCredentialsError()
        
        assert error.status_code == 401
        assert error.error_code == "AUTH_1001"
        assert "Invalid username or password" in error.message
    
    def test_token_expired_error(self):
        """Test TokenExpiredError."""
        error = TokenExpiredError()
        
        assert error.status_code == 401
        assert error.error_code == "AUTH_1002"
        assert "expired" in error.message.lower()
    
    def test_token_invalid_error(self):
        """Test TokenInvalidError."""
        error = TokenInvalidError()
        
        assert error.status_code == 401
        assert error.error_code == "AUTH_1003"
        assert "Invalid" in error.message
