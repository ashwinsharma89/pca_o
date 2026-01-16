"""
JWT Authentication Middleware.

Provides JWT token generation, verification, and user authentication.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import HTTPException, Security, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import bcrypt
from loguru import logger

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))

# Security validation on import
if not SECRET_KEY:
    logger.error("🔴 CRITICAL: JWT_SECRET_KEY environment variable not set!")
    logger.error("Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\"")
    sys.exit(1)

if SECRET_KEY in ["change-this-secret-key", "your-secure-random-secret-here-change-this"]:
    logger.error("🔴 CRITICAL: Using default/insecure JWT_SECRET_KEY!")
    logger.error("This is a MAJOR SECURITY VULNERABILITY. Generate a secure secret immediately.")
    logger.error("Refusing to start with insecure secret. Use 'python -c \"import secrets; print(secrets.token_urlsafe(32))\"' to generate one.")
    sys.exit(1)

# Security scheme
security = HTTPBearer()

# NOTE: User authentication now uses database
# See src/services/user_service.py for user management
# See src/api/v1/user_management.py for user endpoints
# Use scripts/init_users.py to create initial admin user


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify password against hash.
    
    Args:
        plain_password: Plain text password
        hashed_password: Bcrypt hashed password
        
    Returns:
        True if password matches
    """
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def get_user(username: str, db=None) -> Optional[Dict[str, Any]]:
    """
    Get user from database (PostgreSQL).
    
    Args:
        username: Username or Email
        db: Optional database session
        
    Returns:
        User dict or None
    """
    if not username:
        return None
        
    db_session = db
    should_close = False
    
    if db_session is None:
        try:
            from src.core.database.connection import get_db_manager
            db_manager = get_db_manager()
            # Log the database URL to verify it's the test DB
            db_url = os.getenv("DATABASE_URL")
            
            # Check file size if it's a local sqlite file
            file_info = ""
            if db_url and "sqlite:///" in db_url:
                path = db_url.replace("sqlite:///", "")
                if os.path.exists(path):
                    file_info = f" (Size: {os.path.getsize(path)} bytes)"
                else:
                    file_info = " (File NOT found!)"
                    
            logger.debug(f"get_user connecting to DB: {db_url}{file_info}")
            db_session = db_manager.get_session_direct()
            should_close = True
        except Exception as e:
            logger.error(f"Could not get DB session for user lookup: {e}")
            return None
            
    try:
        from src.engine.services.user_service import UserService
        logger.debug(f"UserService imported from: {UserService.__module__} at {getattr(UserService, '__file__', 'unknown')}")
        user_service = UserService(db_session)
        
        # Try username first
        user_model = user_service.get_user_by_username(username)
        
        # Then try email
        if not user_model:
            user_model = user_service.get_user_by_email(username)
            
        if user_model:
            return {
                "id": getattr(user_model, 'id', None),
                "username": getattr(user_model, 'username', None),
                "email": getattr(user_model, 'email', None),
                "hashed_password": getattr(user_model, 'hashed_password', None),
                "role": getattr(user_model, 'role', 'user'),
                "tier": getattr(user_model, 'tier', 'free') or "free"
            }
        return None
    except Exception as e:
        logger.error(f"Error in get_user: {e}")
        return None
    finally:
        if should_close and db_session:
            db_session.close()


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate user with username and password.
    
    Args:
        username: Username
        password: Password
        
    Returns:
        User dict if authenticated, None otherwise
    """
    logger.info(f"Authenticating user: {username}")
    
    user = get_user(username)
    if not user:
        logger.warning(f"User not found in DB: {username}")
        return None
    
    logger.info(f"User found: {username}, Role: {user.get('role')}")
    
    is_valid = verify_password(password, user["hashed_password"])
    if not is_valid:
        logger.warning(f"Password mismatch for user: {username}")
        return None
    
    logger.info(f"Password verified for user: {username}")
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token.
    
    Args:
        data: Data to encode in token
        expires_delta: Optional expiration time delta
        
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    # Ensure it's a string (python-jose can return bytes in some cases)
    if isinstance(encoded_jwt, bytes):
        encoded_jwt = encoded_jwt.decode('utf-8')
        
    logger.info(f"Created access token for user: {data.get('sub')}")
    
    return encoded_jwt


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode JWT token.
    
    Args:
        token: JWT token
        
    Returns:
        Token payload
        
    Raises:
        JWTError: If token is invalid
    """
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.
    
    Args:
        password: Plain password
        
    Returns:
        Hashed password
    """
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode()


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> Dict[str, Any]:
    """
    Verify JWT token and return payload.
    
    Args:
        credentials: HTTP Bearer credentials
        
    Returns:
        Token payload
        
    Raises:
        HTTPException: If token is invalid
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        
        return payload
        
    except JWTError as e:
        logger.error(f"JWT verification failed: {e}")
        raise credentials_exception


async def get_current_user(
    payload: Dict[str, Any] = Depends(verify_token)
) -> Dict[str, Any]:
    """
    Get current authenticated user.
    
    Args:
        payload: JWT token payload
        
    Returns:
        User dict
        
    Raises:
        HTTPException: If user not found
    """
    username = payload.get("sub")
    user = get_user(username)
    
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    return user


async def get_optional_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
) -> Optional[Dict[str, Any]]:
    """
    Get current authenticated user if token is present, otherwise None.
    
    Args:
        credentials: HTTP Bearer credentials (optional)
        
    Returns:
        User dict or None
    """
    if not credentials:
        return None
        
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        return get_user(username)
    except Exception:
        return None


async def get_current_active_admin(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current user and verify admin role.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        User dict
        
    Raises:
        HTTPException: If user is not admin
    """
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    return current_user


async def populate_user_state_middleware(request: Request, call_next):
    """Middleware to populate request.state.user from JWT token."""
    auth_header = request.headers.get("Authorization")
    request.state.user = None
    
    if auth_header and auth_header.startswith("Bearer "):
        try:
            token = auth_header.split(" ")[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username = payload.get("sub")
            if username:
                # Use a cached or lightweight lookup if possible, for now use get_user
                user = get_user(username)
                if user:
                    request.state.user = user
        except Exception as e:
            # Don't fail the request here, let auth dependencies handle errors
            logger.debug(f"Failed to populate user state: {e}")
            
    response = await call_next(request)
    return response


def create_user(username: str, email: str, password: str, role: str = "user", tier: str = "free", db=None) -> Dict[str, Any]:
    """
    Create a new user in database.
    
    Args:
        username: Username
        email: Email
        password: Plain password
        role: User role
        tier: User tier
        db: Database session (optional)
        
    Returns:
        Created user dict
        
    Note:
        This function is kept for backward compatibility.
        For new code, use UserService from src/services/user_service.py
        Or use scripts/init_users.py to create users
    """
    if db is None:
        try:
            from src.core.database.connection import get_db_manager
            db_manager = get_db_manager()
            db = db_manager.get_session_direct()
        except Exception as e:
            logger.error(f"Could not get database session: {e}")
            raise HTTPException(
                status_code=500,
                detail="Database connection failed"
            )
    
    try:
        from src.engine.services.user_service import UserService
        user_service = UserService(db)
        
        user_model = user_service.create_user(
            username=username,
            email=email,
            password=password,
            role=role,
            tier=tier
        )
        
        logger.info(f"Created user: {username}")
        
        return {
            "username": user_model.username,
            "email": user_model.email,
            "hashed_password": user_model.hashed_password,
            "role": user_model.role,
            "tier": user_model.tier or "free"
        }
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Failed to create user: {str(e)}"
        )
    finally:
        if db:
            try:
                db.close()
            except Exception as e:
                logger.debug(f"Error closing DB session: {e}")
