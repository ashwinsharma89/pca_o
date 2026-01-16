"""
User Management Endpoints (v1).

Secure user CRUD operations with password management.
"""

from fastapi import APIRouter, HTTPException, Depends, status, Request
from pydantic import BaseModel, EmailStr, Field, validator
from typing import Dict, Any, List
from sqlalchemy.orm import Session

from src.core.database.connection import get_db
from src.core.database.user_models import User
from src.interface.api.v1.models import sanitize_string
from src.engine.services.user_service import UserService, PasswordValidator
from ..middleware.secure_auth import (
    create_access_token,
    get_current_user,
    get_current_active_admin,
    authenticate_user
)
from ..middleware.rate_limit import limiter
from ..exceptions import (
    InvalidCredentialsError,
    UserAlreadyExistsError,
    UserNotFoundError
)

router = APIRouter(prefix="/users", tags=["user-management"])


# Request/Response Models

class UserCreate(BaseModel):
    """User creation request."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)
    role: str = Field(default="user", pattern="^(user|admin)$")
    tier: str = Field(default="free", pattern="^(free|pro|enterprise)$")

    @validator('username')
    def sanitize_username(cls, v):
        return sanitize_string(v)

    @validator('email')
    def sanitize_email(cls, v):
        return sanitize_string(v)


class UserResponse(BaseModel):
    """User response model."""
    id: int
    username: str
    email: str
    role: str
    tier: str
    is_active: bool
    is_verified: bool
    must_change_password: bool
    created_at: str
    last_login: str | None


class PasswordChange(BaseModel):
    """Password change request."""
    old_password: str
    new_password: str = Field(..., min_length=8)


class PasswordReset(BaseModel):
    """Password reset request."""
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Password reset confirmation."""
    token: str
    new_password: str = Field(..., min_length=8)


class UserUpdate(BaseModel):
    """User update request."""
    email: EmailStr | None = None
    role: str | None = Field(None, pattern="^(user|admin)$")
    tier: str | None = Field(None, pattern="^(free|pro|enterprise)$")
    is_active: bool | None = None

    @validator('email', pre=True)
    def sanitize_email(cls, v):
        if v:
            return sanitize_string(v)
        return v


# Endpoints

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit("5/minute")
async def register_user(
    request: Request,
    user_data: UserCreate,
    db: Session = Depends(get_db)
):
    """
    Register a new user.
    
    Public endpoint for user registration.
    """
    try:
        user_service = UserService(db)
        
        user = user_service.create_user(
            username=user_data.username,
            email=user_data.email,
            password=user_data.password,
            role=user_data.role,
            tier=user_data.tier
        )
        
        return user.to_dict()
        
    except ValueError as e:
        if "already exists" in str(e):
            raise UserAlreadyExistsError(username=user_data.username)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/me", response_model=UserResponse)
@limiter.limit("60/minute")
async def get_current_user_info(
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """
    Get current user information.
    
    Requires authentication.
    """
    return current_user.to_dict()


@router.post("/change-password")
@limiter.limit("5/minute")
async def change_password(
    request: Request,
    password_data: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Change current user's password.
    
    Requires authentication and current password.
    """
    try:
        user_service = UserService(db)
        
        user_service.change_password(
            user_id=current_user.id,
            old_password=password_data.old_password,
            new_password=password_data.new_password
        )
        
        return {
            "message": "Password changed successfully",
            "must_change_password": False
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/request-password-reset")
@limiter.limit("3/minute")
async def request_password_reset(
    request: Request,
    reset_data: PasswordReset,
    db: Session = Depends(get_db)
):
    """
    Request password reset token.
    
    Public endpoint. Sends reset token to user's email.
    """
    user_service = UserService(db)
    token = user_service.create_password_reset_token(reset_data.email)
    
    # Always return success (don't reveal if email exists)
    return {
        "message": "If the email exists, a password reset link has been sent"
    }


@router.post("/reset-password")
async def reset_password(
    reset_data: PasswordResetConfirm,
    db: Session = Depends(get_db)
):
    """
    Reset password using token.
    
    Public endpoint for password reset.
    """
    try:
        user_service = UserService(db)
        
        user_service.reset_password_with_token(
            token=reset_data.token,
            new_password=reset_data.new_password
        )
        
        return {
            "message": "Password reset successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# Admin-only endpoints

@router.get("", response_model=List[UserResponse])
@limiter.limit("30/minute")
async def list_users(
    request: Request,
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db)
):
    """
    List all users.
    
    Admin only.
    """
    user_service = UserService(db)
    users = user_service.list_users(skip=skip, limit=limit)
    
    return [user.to_dict() for user in users]


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db)
):
    """
    Get user by ID.
    
    Admin only.
    """
    user_service = UserService(db)
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        raise UserNotFoundError()
    
    return user.to_dict()


@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db)
):
    """
    Update user.
    
    Admin only.
    """
    user_service = UserService(db)
    
    update_data = user_data.dict(exclude_unset=True)
    user = user_service.update_user(user_id, **update_data)
    
    if not user:
        raise UserNotFoundError()
    
    return user.to_dict()


@router.post("/{user_id}/force-password-change")
async def force_password_change(
    user_id: int,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db)
):
    """
    Force user to change password on next login.
    
    Admin only.
    """
    user_service = UserService(db)
    
    success = user_service.force_password_change(user_id)
    
    if not success:
        raise UserNotFoundError()
    
    return {
        "message": "User will be required to change password on next login"
    }


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db)
):
    """
    Delete user.
    
    Admin only.
    """
    # Prevent self-deletion
    if user_id == current_user.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete your own account"
        )
    
    user_service = UserService(db)
    success = user_service.delete_user(user_id)
    
    if not success:
        raise UserNotFoundError()
    
    return {
        "message": "User deleted successfully"
    }
