"""
MFA (Multi-Factor Authentication) implementation using TOTP.
"""
import pyotp
import qrcode
import io
import base64
from typing import Optional, Tuple
from loguru import logger

class MFAManager:
    """Manages TOTP-based Multi-Factor Authentication."""
    
    def __init__(self, issuer_name: str = "PCA Agent"):
        self.issuer_name = issuer_name

    def generate_secret(self) -> str:
        """Generate a new random TOTP secret."""
        return pyotp.random_base32()

    def get_provisioning_uri(self, username: str, secret: str) -> str:
        """Get the provisioning URI for QR code generation."""
        return pyotp.totp.TOTP(secret).provisioning_uri(
            name=username, 
            issuer_name=self.issuer_name
        )

    def generate_qr_code_base64(self, username: str, secret: str) -> str:
        """Generate a QR code as a base64 string."""
        uri = self.get_provisioning_uri(username, secret)
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(uri)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode()

    def verify_totp(self, secret: str, token: str) -> bool:
        """Verify a TOTP token."""
        if not secret or not token:
            return False
            
        totp = pyotp.TOTP(secret)
        return totp.verify(token)

# Global Instance
_mfa_manager = MFAManager()

def get_mfa_manager() -> MFAManager:
    return _mfa_manager
