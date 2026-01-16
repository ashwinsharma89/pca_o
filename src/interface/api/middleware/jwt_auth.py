"""
JWT Authentication Middleware - Enterprise-grade authentication
Provides JWT token creation, validation, and role-based access control
"""
from fastapi import Request, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional, List
from pydantic import BaseModel
import os
import logging

logger = logging.getLogger(__name__)

# Security configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-CHANGE-IN-PRODUCTION-use-openssl-rand-hex-32")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

security = HTTPBearer()


class TokenData(BaseModel):
    """Token payload data"""
    user_id: str
    username: str
    email: Optional[str] = None
    role: str = "viewer"
    permissions: List[str] = []
    tier: str = "free"


class JWTAuthMiddleware:
    """JWT Authentication Middleware with RBAC"""
    
    # Role-based permissions
    ROLES = {
        "admin": ["*"],  # All permissions
        "analyst": [
            "read:campaigns",
            "read:analytics",
            "write:reports",
            "read:insights",
            "write:queries"
        ],
        "viewer": [
            "read:campaigns",
            "read:analytics",
            "read:insights"
        ],
        "api_user": [
            "read:campaigns",
            "write:campaigns",
            "read:analytics"
        ]
    }
    
    # Tier-based rate limits
    TIER_LIMITS = {
        "free": "10/minute",
        "pro": "100/minute",
        "enterprise": "1000/minute"
    }
    
    @staticmethod
    def create_access_token(
        data: dict,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create JWT access token
        
        Args:
            data: Token payload data
            expires_delta: Token expiration time
            
        Returns:
            JWT token string
        """
        to_encode = data.copy()
        expire = datetime.utcnow() + (
            expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })
        
        # Add permissions based on role
        role = data.get("role", "viewer")
        to_encode["permissions"] = JWTAuthMiddleware.ROLES.get(role, [])
        
        token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        logger.info(f"Created access token for user: {data.get('sub')}")
        return token
    
    @staticmethod
    def create_refresh_token(user_id: str) -> str:
        """
        Create JWT refresh token
        
        Args:
            user_id: User ID
            
        Returns:
            JWT refresh token
        """
        to_encode = {
            "sub": user_id,
            "exp": datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
            "iat": datetime.utcnow(),
            "type": "refresh"
        }
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    @staticmethod
    async def verify_token(
        credentials: HTTPAuthorizationCredentials = Depends(security)
    ) -> TokenData:
        """
        Verify JWT token and extract user data
        
        Args:
            credentials: HTTP Bearer credentials
            
        Returns:
            TokenData with user information
            
        Raises:
            HTTPException: If token is invalid or expired
        """
        try:
            token = credentials.credentials
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            
            # Validate token type
            if payload.get("type") != "access":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token type"
                )
            
            # Extract user data
            user_id: str = payload.get("sub")
            if user_id is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token payload"
                )
            
            token_data = TokenData(
                user_id=user_id,
                username=payload.get("username", user_id),
                email=payload.get("email"),
                role=payload.get("role", "viewer"),
                permissions=payload.get("permissions", []),
                tier=payload.get("tier", "free")
            )
            
            logger.debug(f"Token verified for user: {user_id}")
            return token_data
            
        except JWTError as e:
            logger.warning(f"JWT verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
    
    @staticmethod
    def check_permission(token_data: TokenData, required_permission: str) -> bool:
        """
        Check if user has required permission
        
        Args:
            token_data: User token data
            required_permission: Permission to check
            
        Returns:
            True if user has permission
        """
        # Admin has all permissions
        if "*" in token_data.permissions:
            return True
        
        # Check specific permission
        return required_permission in token_data.permissions
    
    @staticmethod
    async def require_permission(
        permission: str,
        token_data: TokenData = Depends(verify_token)
    ):
        """
        Dependency to require specific permission
        
        Args:
            permission: Required permission
            token_data: User token data
            
        Raises:
            HTTPException: If user doesn't have permission
        """
        if not JWTAuthMiddleware.check_permission(token_data, permission):
            logger.warning(
                f"Permission denied: {token_data.user_id} "
                f"attempted {permission}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {permission} required"
            )
        return token_data


# Convenience dependencies
async def get_current_user(
    token_data: TokenData = Depends(JWTAuthMiddleware.verify_token)
) -> TokenData:
    """Get current authenticated user"""
    return token_data


async def get_admin_user(
    token_data: TokenData = Depends(JWTAuthMiddleware.verify_token)
) -> TokenData:
    """Require admin role"""
    if token_data.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return token_data


# Permission-specific dependencies
async def require_read_campaigns(
    token_data: TokenData = Depends(
        lambda td=Depends(JWTAuthMiddleware.verify_token): 
        JWTAuthMiddleware.require_permission("read:campaigns", td)
    )
):
    """Require read:campaigns permission"""
    return token_data


async def require_write_campaigns(
    token_data: TokenData = Depends(
        lambda td=Depends(JWTAuthMiddleware.verify_token): 
        JWTAuthMiddleware.require_permission("write:campaigns", td)
    )
):
    """Require write:campaigns permission"""
    return token_data
