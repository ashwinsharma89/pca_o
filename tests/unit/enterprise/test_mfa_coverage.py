import unittest
import pyotp
from src.enterprise.mfa import MFAManager

class TestMFAManager(unittest.TestCase):
    def setUp(self):
        self.mfa = MFAManager(issuer_name="TestIssuer")

    def test_generate_secret(self):
        secret = self.mfa.generate_secret()
        self.assertIsNotNone(secret)
        self.assertEqual(len(secret), 32) # pyotp default base32 length

    def test_provisioning_uri(self):
        secret = self.mfa.generate_secret()
        username = "test@example.com"
        # The username will be URL encoded in the URI
        encoded_username = "test%40example.com"
        uri = self.mfa.get_provisioning_uri(username, secret)
        self.assertIn("TestIssuer", uri)
        self.assertIn(encoded_username, uri)
        self.assertIn(secret, uri)

    def test_verify_totp(self):
        secret = pyotp.random_base32()
        totp = pyotp.TOTP(secret)
        token = totp.now()
        
        # Test valid token
        self.assertTrue(self.mfa.verify_totp(secret, token))
        
        # Test invalid token
        self.assertFalse(self.mfa.verify_totp(secret, "000000"))
        
        # Test empty inputs
        self.assertFalse(self.mfa.verify_totp("", token))
        self.assertFalse(self.mfa.verify_totp(secret, ""))

    def test_qr_code_generation(self):
        secret = self.mfa.generate_secret()
        qr_base64 = self.mfa.generate_qr_code_base64("user", secret)
        self.assertIsInstance(qr_base64, str)
        self.assertTrue(len(qr_base64) > 100) # Basic check that it's not empty

if __name__ == "__main__":
    unittest.main()
