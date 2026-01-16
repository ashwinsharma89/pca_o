"""
Encryption at Rest Utility.
Uses cryptography.fernet for symmetric encryption of sensitive fields.
"""

import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from loguru import logger

class DataEncrypter:
    """
    Utility for encrypting and decrypting sensitive data.
    """
    
    def __init__(self, master_key: str = None):
        """
        Initialize the encrypter with a master key.
        If no key is provided, it attempts to load from ENCRYPTION_KEY env var.
        """
        key = master_key or os.getenv("ENCRYPTION_KEY")
        
        if not key:
            logger.warning("⚠️ ENCRYPTION_KEY not set. Generating a temporary key for this session.")
            # In production, this should fail or load from a secure vault
            self.key = Fernet.generate_key()
        else:
            # Ensure key is in Fernet format (base64)
            try:
                # If it's a raw string, we might need to derive a key
                if len(key) < 32:
                    kdf = PBKDF2HMAC(
                        algorithm=hashes.SHA256(),
                        length=32,
                        salt=b'pca-agent-salt',
                        iterations=100000,
                    )
                    key = base64.urlsafe_b64encode(kdf.derive(key.encode()))
                self.key = key if isinstance(key, bytes) else key.encode()
                Fernet(self.key) # Test if valid
            except Exception as e:
                logger.error(f"Invalid encryption key provided: {e}")
                raise ValueError("Encryption key must be a 32-byte base64 encoded string")

        self.fernet = Fernet(self.key)

    def encrypt(self, data: str) -> str:
        """Encrypt string data."""
        if not data:
            return ""
        try:
            return self.fernet.encrypt(data.encode()).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def decrypt(self, token: str) -> str:
        """Decrypt token back to string."""
        if not token:
            return ""
        try:
            return self.fernet.decrypt(token.encode()).decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise
