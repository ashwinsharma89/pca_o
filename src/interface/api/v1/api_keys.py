"""
API Key Management - Long-lived API keys for integrations
Provides API key generation, validation, and management
"""
from fastapi import APIRouter, HTTPException, Depends, Header, status
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import secrets
import hashlib
import logging

from ..middleware.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api-keys", tags=["api-keys"])


# In-memory storage (replace with database in production)
_api_keys_store = {}


class APIKeyCreate(BaseModel):
    """API key creation request"""
    name: str
    scopes: List[str] = ["read:campaigns"]
    expires_in_days: int = 365


class APIKeyResponse(BaseModel):
    """API key response (without the actual key)"""
    id: str
    name: str
    prefix: str
    scopes: List[str]
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    is_active: bool


class APIKeyCreateResponse(BaseModel):
    """Response when creating API key (includes the key ONCE)"""
    id: str
    name: str
    key: str  # Only returned once!
    prefix: str
    scopes: List[str]
    expires_at: Optional[datetime]
    message: str = "Store this key securely. It will not be shown again."


def generate_api_key() -> str:
    """Generate a new API key"""
    return f"pca_{secrets.token_urlsafe(32)}"


def hash_api_key(key: str) -> str:
    """Hash API key for storage"""
    return hashlib.sha256(key.encode()).hexdigest()


def get_key_prefix(key: str) -> str:
    """Get prefix for key identification"""
    return key[:12]


@router.post("", response_model=APIKeyCreateResponse)
async def create_api_key(
    request: APIKeyCreate,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new API key
    
    The key is only returned once. Store it securely.
    """
    # Generate key
    key = generate_api_key()
    key_id = secrets.token_urlsafe(8)
    
    # Calculate expiration
    expires_at = None
    if request.expires_in_days:
        expires_at = datetime.utcnow() + timedelta(days=request.expires_in_days)
    
    # Store key (hashed)
    _api_keys_store[key_id] = {
        "id": key_id,
        "user_id": current_user.get("id", "unknown"),
        "name": request.name,
        "key_hash": hash_api_key(key),
        "prefix": get_key_prefix(key),
        "scopes": request.scopes,
        "created_at": datetime.utcnow(),
        "expires_at": expires_at,
        "last_used_at": None,
        "is_active": True
    }
    
    logger.info(f"API key created: {key_id} for user {current_user.get('id')}")
    
    return APIKeyCreateResponse(
        id=key_id,
        name=request.name,
        key=key,  # Only time the key is returned!
        prefix=get_key_prefix(key),
        scopes=request.scopes,
        expires_at=expires_at
    )


@router.get("", response_model=List[APIKeyResponse])
async def list_api_keys(
    current_user: dict = Depends(get_current_user)
):
    """
    List user's API keys (without revealing the actual keys)
    """
    user_id = current_user.get("id", "unknown")
    
    keys = [
        APIKeyResponse(
            id=k["id"],
            name=k["name"],
            prefix=k["prefix"],
            scopes=k["scopes"],
            created_at=k["created_at"],
            expires_at=k["expires_at"],
            last_used_at=k["last_used_at"],
            is_active=k["is_active"]
        )
        for k in _api_keys_store.values()
        if k["user_id"] == user_id
    ]
    
    return keys


@router.delete("/{key_id}")
async def revoke_api_key(
    key_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Revoke an API key
    """
    if key_id not in _api_keys_store:
        raise HTTPException(status_code=404, detail="API key not found")
    
    key_data = _api_keys_store[key_id]
    if key_data["user_id"] != current_user.get("id", "unknown"):
        raise HTTPException(status_code=403, detail="Not authorized to revoke this key")
    
    # Mark as inactive (soft delete)
    _api_keys_store[key_id]["is_active"] = False
    
    logger.info(f"API key revoked: {key_id}")
    
    return {"message": "API key revoked successfully"}


async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """
    Verify API key from header
    
    Usage in endpoints:
        @router.get("/data", dependencies=[Depends(verify_api_key)])
        async def get_data():
            pass
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "X-API-Key"}
        )
    
    # Hash the provided key
    key_hash = hash_api_key(x_api_key)
    
    # Find matching key
    for key_id, key_data in _api_keys_store.items():
        if key_data["key_hash"] == key_hash:
            # Check if active
            if not key_data["is_active"]:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API key has been revoked"
                )
            
            # Check expiration
            if key_data["expires_at"] and key_data["expires_at"] < datetime.utcnow():
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API key has expired"
                )
            
            # Update last used
            _api_keys_store[key_id]["last_used_at"] = datetime.utcnow()
            
            logger.debug(f"API key validated: {key_id}")
            
            # Return user-like object for compatibility
            return {
                "id": key_data["user_id"],
                "type": "api_key",
                "key_id": key_id,
                "scopes": key_data["scopes"]
            }
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key"
    )


def check_api_key_scope(required_scope: str):
    """
    Dependency to check API key has required scope
    
    Usage:
        @router.get("/sensitive")
        async def sensitive_data(
            user = Depends(verify_api_key),
            _ = Depends(check_api_key_scope("admin:read"))
        ):
            pass
    """
    async def _check_scope(user: dict = Depends(verify_api_key)):
        if user.get("type") != "api_key":
            return user  # JWT auth, no scope check
        
        scopes = user.get("scopes", [])
        if "*" in scopes or required_scope in scopes:
            return user
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API key does not have required scope: {required_scope}"
        )
    
    return _check_scope
