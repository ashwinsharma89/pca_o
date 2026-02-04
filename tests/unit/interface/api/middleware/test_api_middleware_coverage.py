import unittest
import os
from datetime import timedelta
from fastapi import Request, HTTPException
from jose import jwt
from src.interface.api.middleware.auth import (
    create_access_token, decode_token, verify_password, hash_password
)
from src.interface.api.middleware.rate_limit import (
    get_rate_limit_key, get_user_rate_limit, RATE_LIMITS
)

class MockRequest:
    def __init__(self, username=None, tier=None, client_host="127.0.0.1"):
        self.state = type('obj', (object,), {'user': None})
        if username:
            self.state.user = {"username": username, "tier": tier or "free"}
        self.client = type('obj', (object,), {'host': client_host})

class TestAPISecurityMiddleware(unittest.TestCase):
    def setUp(self):
        # Ensure secret key for testing
        os.environ["JWT_SECRET_KEY"] = "test_secret_for_unit_tests_only"

    def test_password_hashing(self):
        password = "secret_password"
        hashed = hash_password(password)
        self.assertNotEqual(password, hashed)
        self.assertTrue(verify_password(password, hashed))
        self.assertFalse(verify_password("wrong_password", hashed))

    def test_jwt_lifecycle(self):
        data = {"sub": "testuser", "role": "admin"}
        token = create_access_token(data, expires_delta=timedelta(minutes=15))
        self.assertIsInstance(token, str)
        
        payload = decode_token(token)
        self.assertEqual(payload["sub"], "testuser")
        self.assertEqual(payload["role"], "admin")
        self.assertIn("exp", payload)

    def test_rate_limit_keys(self):
        # User key
        req_user = MockRequest(username="ashwin", tier="pro")
        self.assertEqual(get_rate_limit_key(req_user), "user:ashwin")
        
        # IP key (no user)
        req_ip = MockRequest(client_host="192.168.1.1")
        # In actual execution, slowapi.util.get_remote_address is used
        # which depends on real request object, but we can verify our logic
        key = get_rate_limit_key(req_ip)
        self.assertIn(key, ["192.168.1.1", "127.0.0.1"]) # Depends on environment

    def test_tier_based_limits(self):
        # Free tier
        req_free = MockRequest(username="u1", tier="free")
        self.assertEqual(get_user_rate_limit(req_free), RATE_LIMITS["free"])
        
        # Pro tier
        req_pro = MockRequest(username="u2", tier="pro")
        self.assertEqual(get_user_rate_limit(req_pro), RATE_LIMIT_LIMITS := RATE_LIMITS["pro"])
        
        # Enterprise tier
        req_ent = MockRequest(username="u3", tier="enterprise")
        self.assertEqual(get_user_rate_limit(req_ent), RATE_LIMITS["enterprise"])
        
        # Default (Unauthenticated)
        req_anon = MockRequest()
        self.assertEqual(get_user_rate_limit(req_anon), "10/minute")

if __name__ == "__main__":
    unittest.main()
