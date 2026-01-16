"""
Enterprise Authentication & Authorization System
"""
import jwt
import bcrypt
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from enum import Enum
from loguru import logger
import json
from pathlib import Path


class UserRole(Enum):
    """User roles with different permission levels."""
    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"
    API_USER = "api_user"


class Permission(Enum):
    """Granular permissions."""
    VIEW_ANALYSIS = "view_analysis"
    CREATE_ANALYSIS = "create_analysis"
    DELETE_ANALYSIS = "delete_analysis"
    EXPORT_DATA = "export_data"
    MANAGE_USERS = "manage_users"
    VIEW_FEEDBACK = "view_feedback"
    MANAGE_SETTINGS = "manage_settings"
    API_ACCESS = "api_access"


ROLE_PERMISSIONS = {
    UserRole.ADMIN: [p for p in Permission],
    UserRole.ANALYST: [
        Permission.VIEW_ANALYSIS,
        Permission.CREATE_ANALYSIS,
        Permission.EXPORT_DATA,
        Permission.VIEW_FEEDBACK
    ],
    UserRole.VIEWER: [
        Permission.VIEW_ANALYSIS,
        Permission.EXPORT_DATA
    ],
    UserRole.API_USER: [
        Permission.VIEW_ANALYSIS,
        Permission.CREATE_ANALYSIS,
        Permission.API_ACCESS
    ]
}


class AuthenticationManager:
    """Manages user authentication and authorization."""
    
    def __init__(self, secret_key: str, users_file: str = "./data/enterprise/users.json"):
        """Initialize authentication manager."""
        self.secret_key = secret_key
        self.users_file = Path(users_file)
        self.users_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize with default admin if no users exist
        if not self.users_file.exists():
            self._create_default_admin()
    
    def _create_default_admin(self):
        """Create default admin user with secure random password."""
        # Generate cryptographically secure random password
        admin_password = secrets.token_urlsafe(16)
        
        users = {
            "admin": {
                "username": "admin",
                "password_hash": self._hash_password(admin_password),
                "email": "admin@company.com",
                "role": UserRole.ADMIN.value,
                "created_at": datetime.now().isoformat(),
                "active": True,
                "organization": "default",
                "metadata": {}
            }
        }
        
        with open(self.users_file, 'w') as f:
            json.dump(users, f, indent=2)
        
        # Log the generated password securely - in production, use a secure channel
        logger.warning(
            f"Created default admin user. Username: admin, Password: {admin_password} "
            "- Store this securely and change immediately!"
        )
    
    def _hash_password(self, password: str) -> str:
        """Hash password using bcrypt with explicit rounds (OWASP recommendation)."""
        # Use 12 rounds (OWASP recommendation for bcrypt)
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')
    
    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    
    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate user.
        
        Args:
            username: Username
            password: Password
            
        Returns:
            User data if authenticated, None otherwise
        """
        users = self._load_users()
        
        if username not in users:
            logger.warning(f"Authentication failed: User {username} not found")
            return None
        
        user = users[username]
        
        if not user.get('active', False):
            logger.warning(f"Authentication failed: User {username} is inactive")
            return None
        
        if not self._verify_password(password, user['password_hash']):
            logger.warning(f"Authentication failed: Invalid password for {username}")
            return None
        
        logger.info(f"User {username} authenticated successfully")
        return {
            "username": user['username'],
            "email": user['email'],
            "role": user['role'],
            "organization": user.get('organization', 'default'),
            "metadata": user.get('metadata', {})
        }
    
    def generate_token(self, user_data: Dict[str, Any], expires_in_hours: int = 24) -> str:
        """
        Generate JWT token.
        
        Args:
            user_data: User information
            expires_in_hours: Token expiration time
            
        Returns:
            JWT token
        """
        payload = {
            "username": user_data['username'],
            "email": user_data['email'],
            "role": user_data['role'],
            "organization": user_data.get('organization', 'default'),
            "exp": datetime.utcnow() + timedelta(hours=expires_in_hours),
            "iat": datetime.utcnow()
        }
        
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        logger.info(f"Generated token for user {user_data['username']}")
        
        return token
    
    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Verify JWT token.
        
        Args:
            token: JWT token
            
        Returns:
            Decoded token data if valid, None otherwise
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token verification failed: Token expired")
            return None
        except jwt.InvalidTokenError:
            logger.warning("Token verification failed: Invalid token")
            return None
    
    def has_permission(self, user_role: str, permission: Permission) -> bool:
        """
        Check if user role has specific permission.
        
        Args:
            user_role: User's role
            permission: Permission to check
            
        Returns:
            True if user has permission
        """
        try:
            role = UserRole(user_role)
            return permission in ROLE_PERMISSIONS.get(role, [])
        except ValueError:
            return False
    
    def create_user(
        self,
        username: str,
        password: str,
        email: str,
        role: UserRole,
        organization: str = "default",
        metadata: Optional[Dict] = None
    ) -> bool:
        """
        Create new user.
        
        Args:
            username: Username
            password: Password
            email: Email
            role: User role
            organization: Organization name
            metadata: Additional metadata
            
        Returns:
            True if user created successfully
        """
        users = self._load_users()
        
        if username in users:
            logger.error(f"User creation failed: Username {username} already exists")
            return False
        
        users[username] = {
            "username": username,
            "password_hash": self._hash_password(password),
            "email": email,
            "role": role.value,
            "created_at": datetime.now().isoformat(),
            "active": True,
            "organization": organization,
            "metadata": metadata or {}
        }
        
        self._save_users(users)
        logger.info(f"Created user {username} with role {role.value}")
        
        return True
    
    def update_user(self, username: str, updates: Dict[str, Any]) -> bool:
        """Update user information."""
        users = self._load_users()
        
        if username not in users:
            return False
        
        # Don't allow direct password_hash updates
        if 'password' in updates:
            updates['password_hash'] = self._hash_password(updates.pop('password'))
        
        users[username].update(updates)
        users[username]['updated_at'] = datetime.now().isoformat()
        
        self._save_users(users)
        logger.info(f"Updated user {username}")
        
        return True
    
    def deactivate_user(self, username: str) -> bool:
        """Deactivate user."""
        return self.update_user(username, {"active": False})
    
    def _load_users(self) -> Dict:
        """Load users from file."""
        if not self.users_file.exists():
            return {}
        
        with open(self.users_file, 'r') as f:
            return json.load(f)
    
    def _save_users(self, users: Dict):
        """Save users to file."""
        with open(self.users_file, 'w') as f:
            json.dump(users, f, indent=2)


class SessionManager:
    """Manages user sessions."""
    
    def __init__(self, session_file: str = "./data/enterprise/sessions.json"):
        """Initialize session manager."""
        self.session_file = Path(session_file)
        self.session_file.parent.mkdir(parents=True, exist_ok=True)
    
    def create_session(self, username: str, token: str, metadata: Optional[Dict] = None) -> str:
        """Create new session."""
        session_id = secrets.token_urlsafe(32)
        
        sessions = self._load_sessions()
        sessions[session_id] = {
            "session_id": session_id,
            "username": username,
            "token": token,
            "created_at": datetime.now().isoformat(),
            "last_activity": datetime.now().isoformat(),
            "metadata": metadata or {},
            "active": True
        }
        
        self._save_sessions(sessions)
        logger.info(f"Created session {session_id} for user {username}")
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session data."""
        sessions = self._load_sessions()
        session = sessions.get(session_id)
        
        if session and session.get('active'):
            # Update last activity
            session['last_activity'] = datetime.now().isoformat()
            self._save_sessions(sessions)
            return session
        
        return None
    
    def invalidate_session(self, session_id: str):
        """Invalidate session."""
        sessions = self._load_sessions()
        
        if session_id in sessions:
            sessions[session_id]['active'] = False
            sessions[session_id]['invalidated_at'] = datetime.now().isoformat()
            self._save_sessions(sessions)
            logger.info(f"Invalidated session {session_id}")
    
    def cleanup_expired_sessions(self, max_age_hours: int = 24):
        """Remove expired sessions."""
        sessions = self._load_sessions()
        current_time = datetime.now()
        
        expired = []
        for session_id, session in sessions.items():
            last_activity = datetime.fromisoformat(session['last_activity'])
            age = (current_time - last_activity).total_seconds() / 3600
            
            if age > max_age_hours:
                expired.append(session_id)
        
        for session_id in expired:
            del sessions[session_id]
        
        if expired:
            self._save_sessions(sessions)
            logger.info(f"Cleaned up {len(expired)} expired sessions")
    
    def _load_sessions(self) -> Dict:
        """Load sessions from file."""
        if not self.session_file.exists():
            return {}
        
        with open(self.session_file, 'r') as f:
            return json.load(f)
    
    def _save_sessions(self, sessions: Dict):
        """Save sessions to file."""
        with open(self.session_file, 'w') as f:
            json.dump(sessions, f, indent=2)


class OrganizationManager:
    """Manages multi-tenant organizations."""
    
    def __init__(self, orgs_file: str = "./data/enterprise/organizations.json"):
        """Initialize organization manager."""
        self.orgs_file = Path(orgs_file)
        self.orgs_file.parent.mkdir(parents=True, exist_ok=True)
        
        if not self.orgs_file.exists():
            self._create_default_org()
    
    def _create_default_org(self):
        """Create default organization."""
        orgs = {
            "default": {
                "org_id": "default",
                "name": "Default Organization",
                "created_at": datetime.now().isoformat(),
                "active": True,
                "settings": {
                    "max_users": 100,
                    "max_analyses_per_month": 1000,
                    "data_retention_days": 90,
                    "features": ["basic_analytics", "csv_upload", "screenshot_upload"]
                },
                "metadata": {}
            }
        }
        
        with open(self.orgs_file, 'w') as f:
            json.dump(orgs, f, indent=2)
    
    def get_organization(self, org_id: str) -> Optional[Dict]:
        """Get organization data."""
        orgs = self._load_orgs()
        return orgs.get(org_id)
    
    def create_organization(
        self,
        org_id: str,
        name: str,
        settings: Optional[Dict] = None,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Create new organization."""
        orgs = self._load_orgs()
        
        if org_id in orgs:
            return False
        
        orgs[org_id] = {
            "org_id": org_id,
            "name": name,
            "created_at": datetime.now().isoformat(),
            "active": True,
            "settings": settings or {},
            "metadata": metadata or {}
        }
        
        self._save_orgs(orgs)
        logger.info(f"Created organization {org_id}")
        
        return True
    
    def _load_orgs(self) -> Dict:
        """Load organizations from file."""
        if not self.orgs_file.exists():
            return {}
        
        with open(self.orgs_file, 'r') as f:
            return json.load(f)
    
    def _save_orgs(self, orgs: Dict):
        """Save organizations to file."""
        with open(self.orgs_file, 'w') as f:
            json.dump(orgs, f, indent=2)
