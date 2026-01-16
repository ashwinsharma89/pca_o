"""
Secure JWT Authentication Middleware with Database-Backed Users.

Replaces hardcoded credentials with database user management.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import HTTPException, Security, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from src.core.database.connection import get_db
from src.core.database.user_models import User
from loguru import logger

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY")

# Security validation - fail fast if secret is not properly configured
if not SECRET_KEY or SECRET_KEY in ["change-this-secret-key", "your-secure-random-secret-here-change-this"]:
    raise ValueError(
        "🔴 SECURITY ERROR: JWT_SECRET_KEY environment variable must be set to a secure value.\n"
        "Generate a secure key with: python -c 'import secrets; print(secrets.token_urlsafe(32))'\n"
        "Then set it in your environment: export JWT_SECRET_KEY='your-generated-key'"
    )

ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "480"))  # 8 hours

# Security scheme
security = HTTPBearer()


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
    
    logger.info(f"Created access token for user: {data.get('sub')}")
    
    return encoded_jwt


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
        detail={
            "error": {
                "code": "AUTH_1003",
                "message": "Could not validate credentials",
                "details": {}
            }
        },
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
    payload: Dict[str, Any] = Depends(verify_token),
    db: Session = Depends(get_db)
) -> User:
    """
    Get current authenticated user from database.
    
    Args:
        payload: JWT token payload
        db: Database session
        
    Returns:
        User object from database
        
    Raises:
        HTTPException: If user not found or inactive
    """
    username = payload.get("sub")
    
    # Get user from database
    user_service = UserService(db)
    user = user_service.get_user_by_username(username)
    
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": {
                    "code": "AUTH_1005",
                    "message": "User not found",
                    "details": {"username": username}
                }
            }
        )
    
    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": {
                    "code": "AUTH_1004",
                    "message": "User account is inactive",
                    "details": {}
                }
            }
        )
    
    # Check if user must change password
    if user.must_change_password:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": {
                    "code": "AUTH_1007",
                    "message": "Password change required",
                    "details": {
                        "action": "Please change your password before continuing"
                    }
                }
            }
        )
    
    return user


from enum import Enum

class Permission(str, Enum):
    """Fine-grained permissions."""
    CAMPAIGN_READ = "campaign:read"
    CAMPAIGN_WRITE = "campaign:write"
    CAMPAIGN_DELETE = "campaign:delete"
    ANALYTICS_READ = "analytics:read"
    REPORT_GENERATE = "report:generate"
    USER_MANAGE = "user:manage"
    ADMIN_ALL = "admin:all"

# Role to permission mapping
ROLE_PERMISSIONS = {
    "admin": [p for p in Permission],
    "user": [
        Permission.CAMPAIGN_READ,
        Permission.CAMPAIGN_WRITE,
        Permission.ANALYTICS_READ,
        Permission.REPORT_GENERATE
    ],
    "viewer": [
        Permission.CAMPAIGN_READ,
        Permission.ANALYTICS_READ
    ]
}

def has_permission(user_role: str, required_permission: Permission) -> bool:
    """Check if a role has a specific permission."""
    permissions = ROLE_PERMISSIONS.get(user_role, [])
    return required_permission in permissions or Permission.ADMIN_ALL in permissions

async def check_permissions(
    required_permission: Permission,
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Dependency to check if current user has the required permission.
    """
    if not has_permission(current_user.role, required_permission):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": {
                    "code": "AUTH_1004",
                    "message": "Insufficient permissions",
                    "details": {
                        "required_permission": required_permission,
                        "current_role": current_user.role
                    }
                }
            }
        )
    return current_user

async def get_current_active_admin(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Get current user and verify admin role.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        User object
        
    Raises:
        HTTPException: If user is not admin
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": {
                    "code": "AUTH_1004",
                    "message": "Insufficient permissions",
                    "details": {
                        "required_role": "admin",
                        "current_role": current_user.role
                    }
                }
            }
        )
    
    return current_user


def authenticate_user(username: str, password: str, db: Session) -> Optional[User]:
    """
    Authenticate user with username and password.
    
    Args:
        username: Username
        password: Password
        db: Database session
        
    Returns:
        User if authenticated, None otherwise
    """
    user_service = UserService(db)
    return user_service.authenticate_user(username, password)
