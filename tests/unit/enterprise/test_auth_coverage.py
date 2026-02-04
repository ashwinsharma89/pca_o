import unittest
import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import jwt
from src.enterprise.auth import AuthenticationManager, UserRole, Permission, SessionManager, OrganizationManager

class TestAuthenticationManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/auth")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.users_file = self.test_dir / "users.json"
        self.secret_key = "test_secret_key"
        self.auth = AuthenticationManager(self.secret_key, str(self.users_file))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_default_admin_creation(self):
        self.assertTrue(self.users_file.exists())
        with open(self.users_file, 'r') as f:
            users = json.load(f)
        self.assertIn("admin", users)
        self.assertEqual(users["admin"]["role"], UserRole.ADMIN.value)

    def test_hash_and_verify_password(self):
        password = "secure_password"
        hashed = self.auth._hash_password(password)
        self.assertNotEqual(password, hashed)
        self.assertTrue(self.auth._verify_password(password, hashed))
        self.assertFalse(self.auth._verify_password("wrong_password", hashed))

    def test_create_and_authenticate_user(self):
        username = "testuser"
        password = "testpassword"
        email = "test@example.com"
        role = UserRole.ANALYST
        
        success = self.auth.create_user(username, password, email, role)
        self.assertTrue(success)
        
        # Test authentication
        user_data = self.auth.authenticate(username, password)
        self.assertIsNotNone(user_data)
        self.assertEqual(user_data["username"], username)
        self.assertEqual(user_data["role"], role.value)
        
        # Test failed authentication
        self.assertIsNone(self.auth.authenticate(username, "wrong"))
        self.assertIsNone(self.auth.authenticate("nonexistent", password))

    def test_duplicate_user_creation(self):
        username = "admin" # admin already exists by default
        success = self.auth.create_user(username, "pass", "email@test.com", UserRole.VIEWER)
        self.assertFalse(success)

    def test_jwt_token_lifecycle(self):
        user_data = {
            "username": "testuser",
            "email": "test@example.com",
            "role": "analyst",
            "organization": "default"
        }
        token = self.auth.generate_token(user_data)
        self.assertIsInstance(token, str)
        
        decoded = self.auth.verify_token(token)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["username"], "testuser")
        
        # Test expired token
        expired_token = self.auth.generate_token(user_data, expires_in_hours=-1)
        self.assertIsNone(self.auth.verify_token(expired_token))
        
        # Test invalid token
        self.assertIsNone(self.auth.verify_token("invalid.token.here"))

    def test_has_permission(self):
        self.assertTrue(self.auth.has_permission(UserRole.ADMIN.value, Permission.MANAGE_USERS))
        self.assertTrue(self.auth.has_permission(UserRole.ANALYST.value, Permission.VIEW_ANALYSIS))
        self.assertFalse(self.auth.has_permission(UserRole.VIEWER.value, Permission.MANAGE_USERS))
        self.assertFalse(self.auth.has_permission("invalid_role", Permission.VIEW_ANALYSIS))

    def test_update_and_deactivate_user(self):
        username = "update_user"
        self.auth.create_user(username, "pass", "email@test.com", UserRole.VIEWER)
        
        # Update email
        self.auth.update_user(username, {"email": "new@test.com"})
        user_auth = self.auth.authenticate(username, "pass")
        self.assertEqual(user_auth["email"], "new@test.com")
        
        # Update password
        self.auth.update_user(username, {"password": "new_password"})
        self.assertIsNotNone(self.auth.authenticate(username, "new_password"))
        self.assertIsNone(self.auth.authenticate(username, "pass"))
        
        # Deactivate
        self.auth.deactivate_user(username)
        self.assertIsNone(self.auth.authenticate(username, "new_password"))

class TestSessionManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/sessions")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.session_file = self.test_dir / "sessions.json"
        self.sessions = SessionManager(str(self.session_file))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_session_lifecycle(self):
        username = "testuser"
        token = "test_token"
        session_id = self.sessions.create_session(username, token)
        self.assertIsNotNone(session_id)
        
        session = self.sessions.get_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session["username"], username)
        
        self.sessions.invalidate_session(session_id)
        self.assertIsNone(self.sessions.get_session(session_id))

    def test_cleanup_expired_sessions(self):
        session_id = self.sessions.create_session("old_user", "token")
        
        # Manually modify last_activity to be old
        sessions_data = self.sessions._load_sessions()
        old_time = datetime.now() - timedelta(hours=48)
        sessions_data[session_id]["last_activity"] = old_time.isoformat()
        self.sessions._save_sessions(sessions_data)
        
        self.sessions.cleanup_expired_sessions(max_age_hours=24)
        self.assertIsNone(self.sessions.get_session(session_id))

class TestOrganizationManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/orgs")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.orgs_file = self.test_dir / "organizations.json"
        self.orgs = OrganizationManager(str(self.orgs_file))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_org_management(self):
        org_id = "test_org"
        name = "Test Organization"
        success = self.orgs.create_organization(org_id, name)
        self.assertTrue(success)
        
        org = self.orgs.get_organization(org_id)
        self.assertIsNotNone(org)
        self.assertEqual(org["name"], name)
        
        # Duplicate org
        self.assertFalse(self.orgs.create_organization(org_id, name))

if __name__ == "__main__":
    unittest.main()
