"""
User service for secure user management.

Handles user CRUD operations, password management, and security.
"""

import bcrypt
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from src.core.database.user_models import User, PasswordResetToken
from src.core.utils.encryption import DataEncrypter
from loguru import logger


class PasswordValidator:
    """Validate password complexity requirements."""
    
    MIN_LENGTH = 8
    REQUIRE_UPPERCASE = True
    REQUIRE_LOWERCASE = True
    REQUIRE_DIGIT = True
    REQUIRE_SPECIAL = True
    
    @classmethod
    def validate(cls, password: str) -> tuple[bool, str]:
        """
        Validate password meets complexity requirements.
        
        Returns:
            (is_valid, error_message)
        """
        if len(password) < cls.MIN_LENGTH:
            return False, f"Password must be at least {cls.MIN_LENGTH} characters"
        
        if cls.REQUIRE_UPPERCASE and not any(c.isupper() for c in password):
            return False, "Password must contain at least one uppercase letter"
        
        if cls.REQUIRE_LOWERCASE and not any(c.islower() for c in password):
            return False, "Password must contain at least one lowercase letter"
        
        if cls.REQUIRE_DIGIT and not any(c.isdigit() for c in password):
            return False, "Password must contain at least one digit"
        
        if cls.REQUIRE_SPECIAL and not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            return False, "Password must contain at least one special character"
        
        return True, ""


class UserService:
    """Service for user management operations."""
    
    MAX_FAILED_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 30
    
    def __init__(self, db: Session):
        self.db = db
        self.encrypter = DataEncrypter()
    
    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt with explicit rounds (OWASP recommendation)."""
        # Use 12 rounds (OWASP recommendation for bcrypt)
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify password against hash."""
        return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())
    
    def create_user(
        self,
        username: str,
        email: str,
        password: str,
        role: str = "user",
        tier: str = "free",
        must_change_password: bool = False
    ) -> User:
        """
        Create a new user with password validation.
        
        Args:
            username: Username
            email: Email address
            password: Plain password
            role: User role (user, admin)
            tier: User tier (free, pro, enterprise)
            must_change_password: Force password change on first login
            
        Returns:
            Created user
            
        Raises:
            ValueError: If validation fails
        """
        # Validate password
        is_valid, error = PasswordValidator.validate(password)
        if not is_valid:
            raise ValueError(error)
        
        # Check if user exists
        existing = self.db.query(User).filter(
            (User.username == username) | (User.email == email)
        ).first()
        
        if existing:
            if existing.username == username:
                raise ValueError("Username already exists")
            else:
                raise ValueError("Email already exists")
        
        # Create user
        user = User(
            username=username,
            email=email,
            hashed_password=self.hash_password(password),
            role=role,
            tier=tier,
            password_changed_at=datetime.utcnow()
        )
        
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        
        logger.info(f"User created: {username} ({email})")
        
        return user
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        logger.debug(f"UserService.get_user_by_username searching for {username}")
        user = self.db.query(User).filter(User.username == username).first()
        if user:
            logger.debug(f"UserService found user: {user.username} (ID: {user.id})")
        return user
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        return self.db.query(User).filter(User.email == email).first()
    
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """
        Authenticate user with username and password.
        
        Handles failed login attempts and account lockout.
        
        Returns:
            User if authenticated, None otherwise
        """
        user = self.get_user_by_username(username)
        
        if not user:
            return None
        
        # Check if account is locked
        if user.is_locked():
            logger.warning(f"Login attempt for locked account: {username}")
            return None
        
        # Check if account is active
        if not user.is_active:
            logger.warning(f"Login attempt for inactive account: {username}")
            return None
        
        # Verify password
        if not self.verify_password(password, user.hashed_password):
            # Increment failed attempts
            user.failed_login_attempts += 1
            
            # Lock account if too many failures
            if user.failed_login_attempts >= self.MAX_FAILED_ATTEMPTS:
                user.locked_until = datetime.utcnow() + timedelta(
                    minutes=self.LOCKOUT_DURATION_MINUTES
                )
                logger.warning(
                    f"Account locked due to failed attempts: {username}"
                )
            
            self.db.commit()
            return None
        
        # Successful login - reset failed attempts
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        self.db.commit()
        
        logger.info(f"User authenticated: {username}")
        
        return user
    
    def change_password(
        self,
        user_id: int,
        old_password: str,
        new_password: str
    ) -> bool:
        """
        Change user password.
        
        Args:
            user_id: User ID
            old_password: Current password
            new_password: New password
            
        Returns:
            True if successful
            
        Raises:
            ValueError: If validation fails
        """
        user = self.db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise ValueError("User not found")
        
        # Verify old password
        if not self.verify_password(old_password, user.hashed_password):
            raise ValueError("Current password is incorrect")
        
        # Validate new password
        is_valid, error = PasswordValidator.validate(new_password)
        if not is_valid:
            raise ValueError(error)
        
        # Check if new password is same as old
        if self.verify_password(new_password, user.hashed_password):
            raise ValueError("New password must be different from current password")
        
        # Update password
        user.hashed_password = self.hash_password(new_password)
        user.must_change_password = False
        user.password_changed_at = datetime.utcnow()
        
        self.db.commit()
        
        logger.info(f"Password changed for user: {user.username}")
        
        return True
    
    def force_password_change(self, user_id: int) -> bool:
        """Force user to change password on next login."""
        user = self.db.query(User).filter(User.id == user_id).first()
        
        if not user:
            return False
        
        user.must_change_password = True
        self.db.commit()
        
        logger.info(f"Password change forced for user: {user.username}")
        
        return True
    
    def create_password_reset_token(self, email: str) -> Optional[str]:
        """
        Create password reset token for user.
        
        Returns:
            Reset token if user exists, None otherwise
        """
        user = self.get_user_by_email(email)
        
        if not user:
            # Don't reveal if email exists
            return None
        
        # Generate secure token
        token = secrets.token_urlsafe(32)
        
        # Create reset token
        reset_token = PasswordResetToken(
            user_id=user.id,
            token=token,
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        
        self.db.add(reset_token)
        self.db.commit()
        
        logger.info(f"Password reset token created for user: {user.username}")
        
        return token
    
    def reset_password_with_token(
        self,
        token: str,
        new_password: str
    ) -> bool:
        """
        Reset password using reset token.
        
        Returns:
            True if successful
            
        Raises:
            ValueError: If validation fails
        """
        # Find token
        reset_token = self.db.query(PasswordResetToken).filter(
            PasswordResetToken.token == token
        ).first()
        
        if not reset_token or not reset_token.is_valid():
            raise ValueError("Invalid or expired reset token")
        
        # Validate new password
        is_valid, error = PasswordValidator.validate(new_password)
        if not is_valid:
            raise ValueError(error)
        
        # Get user
        user = self.db.query(User).filter(User.id == reset_token.user_id).first()
        
        if not user:
            raise ValueError("User not found")
        
        # Update password
        user.hashed_password = self.hash_password(new_password)
        user.must_change_password = False
        user.password_changed_at = datetime.utcnow()
        
        # Mark token as used
        reset_token.used = True
        
        self.db.commit()
        
        logger.info(f"Password reset for user: {user.username}")
        
        return True
    
    def list_users(self, skip: int = 0, limit: int = 100) -> list[User]:
        """List all users."""
        return self.db.query(User).offset(skip).limit(limit).all()
    
    def update_user(
        self,
        user_id: int,
        **kwargs
    ) -> Optional[User]:
        """Update user fields."""
        user = self.db.query(User).filter(User.id == user_id).first()
        
        if not user:
            return None
        
        # Update allowed fields
        allowed_fields = ['email', 'role', 'tier', 'is_active', 'is_verified', 'mfa_enabled']
        for field, value in kwargs.items():
            if field in allowed_fields:
                setattr(user, field, value)
        
        user.updated_at = datetime.utcnow()
        self.db.commit()
        self.db.refresh(user)
        
        logger.info(f"User updated: {user.username}")
        
        return user

    def enable_mfa(self, user_id: int, secret: str) -> bool:
        """Enable MFA for user."""
        user = self.db.query(User).filter(User.id == user_id).first()
        if not user:
            return False
        
        user.mfa_enabled = True
        user.mfa_secret = self.encrypter.encrypt(secret)
        self.db.commit()
        
        logger.info(f"MFA enabled for user: {user.username}")
        return True

    def disable_mfa(self, user_id: int) -> bool:
        """Disable MFA for user."""
        user = self.db.query(User).filter(User.id == user_id).first()
        if not user:
            return False
        
        user.mfa_enabled = False
        user.mfa_secret = None
        self.db.commit()
        
        logger.info(f"MFA disabled for user: {user.username}")
        return True
    
    def delete_user(self, user_id: int) -> bool:
        """Delete user."""
        user = self.db.query(User).filter(User.id == user_id).first()
        
        if not user:
            return False
        
        self.db.delete(user)
        self.db.commit()
        
        logger.info(f"User deleted: {user.username}")
        
        return True
