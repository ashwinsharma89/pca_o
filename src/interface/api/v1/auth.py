"""
Authentication endpoints (v1).
"""

from fastapi import APIRouter, HTTPException, Depends, status, Request
from pydantic import BaseModel, EmailStr, Field
from typing import Dict, Any, Optional, List

from src.interface.api.middleware.auth import (
    authenticate_user,
    create_access_token,
    get_current_user,
    create_user,
    get_user as get_user_compat
)
from src.core.utils.encryption import DataEncrypter
from src.interface.api.middleware.rate_limit import limiter, get_user_rate_limit
from src.enterprise.mfa import get_mfa_manager
from sqlalchemy.orm import Session
from src.core.database.connection import get_db_manager, get_db
from src.engine.services.user_service import UserService
from datetime import timedelta

router = APIRouter(prefix="/auth", tags=["authentication"])


class LoginRequest(BaseModel):
    """Login request model."""
    username: str
    password: str


class LoginResponse(BaseModel):
    """Login response model."""
    access_token: Optional[str] = None
    token_type: str = "bearer"
    user: Optional[Dict[str, Any]] = None
    mfa_required: bool = False
    mfa_session: Optional[str] = None


class RegisterRequest(BaseModel):
    """Registration request model."""
    username: str = Field(..., min_length=1)
    email: EmailStr
    password: str = Field(..., min_length=6)


@router.post("/login", response_model=LoginResponse)
@limiter.limit("100/minute")
async def login(request: Request, login_req: LoginRequest):
    """
    Login endpoint.
    
    Authenticates user and returns JWT token.
    
    Args:
        request: Login credentials
        
    Returns:
        Access token and user info
        
    Raises:
        HTTPException: If credentials are invalid
    """
    user = authenticate_user(login_req.username, login_req.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check if MFA is enabled
    if user.get("mfa_enabled"):
        # Don't return token yet, return mfa_required
        # In a real app, we'd create a temporary session token here
        return {
            "mfa_required": True,
            "mfa_session": create_access_token(
                data={"sub": user["username"], "mfa_pending": True},
                expires_delta=timedelta(minutes=5)
            ),
            "user": {k: v for k, v in user.items() if k != "hashed_password"}
        }
    
    # Create access token
    access_token = create_access_token(
        data={"sub": user["username"], "role": user["role"], "tier": user["tier"]}
    )
    
    # Remove sensitive data
    user_safe = {k: v for k, v in user.items() if k != "hashed_password"}
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user_safe
    }


class MFASetupResponse(BaseModel):
    secret: str
    qr_code: str # base64


class MFAVerifyRequest(BaseModel):
    token: str
    mfa_session: Optional[str] = None


@router.post("/mfa/setup", response_model=MFASetupResponse)
async def setup_mfa(current_user: Dict = Depends(get_current_user)):
    """Initialize MFA setup for current user."""
    mfa = get_mfa_manager()
    secret = mfa.generate_secret()
    qr_code = mfa.generate_qr_code_base64(current_user["username"], secret)
    
    return {
        "secret": secret,
        "qr_code": qr_code
    }


@router.post("/mfa/verify")
async def verify_mfa(
    verify_req: MFAVerifyRequest,
    current_user: Optional[Dict] = None # Will fill this manually
):
    """Verify MFA token to enable it or finish login."""
    mfa = get_mfa_manager()
    db_manager = get_db_manager()
    
    with db_manager.get_session() as session:
        user_service = UserService(session)
        
        # Scenario 1: Finishing Login
        if verify_req.mfa_session:
            try:
                from ..middleware.auth import SECRET_KEY, ALGORITHM
                from jose import jwt
                payload = jwt.decode(verify_req.mfa_session, SECRET_KEY, algorithms=[ALGORITHM])
                if not payload.get("mfa_pending"):
                    raise HTTPException(status_code=400, detail="Invalid session")
                
                username = payload.get("sub")
                user_model = user_service.get_user_by_username(username)
                if not user_model or not user_model.mfa_enabled:
                    raise HTTPException(status_code=400, detail="MFA not enabled or user not found")
                
                encrypter = DataEncrypter()
                decrypted_secret = encrypter.decrypt(user_model.mfa_secret)
                
                if not mfa.verify_totp(decrypted_secret, verify_req.token):
                    raise HTTPException(status_code=401, detail="Invalid token")
                
                # Success - Create full token
                access_token = create_access_token(
                    data={"sub": user_model.username, "role": user_model.role, "tier": user_model.tier}
                )
                
                return {
                    "access_token": access_token,
                    "token_type": "bearer",
                    "user": user_model.to_dict()
                }
            except Exception as e:
                raise HTTPException(status_code=401, detail=f"MFA verification failed: {str(e)}")
        
        # Scenario 2: Enabling MFA (requires active session)
        # For simplicity, we assume this is called while logged in
        # We need a secret from the frontend which they got from /mfa/setup
        # This part requires a separate verify_req field or another endpoint
        # Let's add an endpoint /mfa/enable
        raise HTTPException(status_code=400, detail="MFA Session required for login verification")


@router.post("/mfa/enable")
async def enable_mfa(
    verify_req: MFAVerifyRequest,
    secret_to_verify: str, # Secret they just scanned
    current_user: Dict = Depends(get_current_user)
):
    """Enable MFA after verifying the first token."""
    mfa = get_mfa_manager()
    if not mfa.verify_totp(secret_to_verify, verify_req.token):
        raise HTTPException(status_code=401, detail="Invalid verification token")
    
    db_manager = get_db_manager()
    with db_manager.get_session() as session:
        user_service = UserService(session)
        user_model = user_service.get_user_by_username(current_user["username"])
        user_service.enable_mfa(user_model.id, secret_to_verify)
        
    return {"message": "MFA enabled successfully"}


@router.post("/register", response_model=Dict[str, Any])
@limiter.limit("20/minute")
async def register(request: Request, reg_req: RegisterRequest, db: Session = Depends(get_db)):
    """
    Register new user.
    
    Args:
        request: Registration data
        reg_req: User registration details
        db: Database session
        
    Returns:
        Created user info
    """
    # Check if user exists
    from ..middleware.auth import get_user
    
    existing_user = get_user(reg_req.username, db=db)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    # Create user
    user = create_user(
        username=reg_req.username,
        email=reg_req.email,
        password=reg_req.password,
        db=db
    )
    
    # Remove sensitive data
    user_safe = {k: v for k, v in user.items() if k != "hashed_password"}
    
    return {
        "message": "User created successfully",
        "user": user_safe
    }


@router.get("/me", response_model=Dict[str, Any])
async def get_me(request: Request, current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Get current user info.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        User info
    """
    # Remove sensitive data
    user_safe = {k: v for k, v in current_user.items() if k != "hashed_password"}
    
    return user_safe
