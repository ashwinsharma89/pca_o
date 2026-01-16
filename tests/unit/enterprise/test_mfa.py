import pytest
import os
from src.enterprise.mfa import MFAManager

class TestMFAManager:
    @pytest.fixture
    def mfa(self):
        return MFAManager(issuer_name="PCATest")

    def test_generate_secret(self, mfa):
        secret = mfa.generate_secret()
        assert len(secret) == 32
        # Should be base32 (standard for TOTP)
        import base64
        base64.b32decode(secret) # Should not raise

    def test_totp_verification(self, mfa):
        secret = mfa.generate_secret()
        import pyotp
        totp = pyotp.TOTP(secret)
        token = totp.now()
        
        assert mfa.verify_totp(secret, token) == True
        assert mfa.verify_totp(secret, "000000") == False

    def test_provisioning_uri(self, mfa):
        secret = mfa.generate_secret()
        uri = mfa.get_provisioning_uri("test@example.com", secret)
        assert "otpauth://totp/" in uri
        assert "PCATest" in uri
        assert "test" in uri
        assert "example.com" in uri
