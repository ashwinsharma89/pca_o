"""
Secrets Management
Integration with HashiCorp Vault and environment-based fallback
"""
import os
import json
import logging
from typing import Optional, Dict, Any
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Try to import hvac (Vault client)
try:
    import hvac
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False
    logger.info("hvac not installed, Vault integration disabled")


class SecretsManager:
    """
    Centralized secrets management
    
    Priority order:
    1. HashiCorp Vault (if configured)
    2. Environment variables
    3. .env file
    4. Default values
    
    Usage:
        secrets = SecretsManager()
        api_key = secrets.get("OPENAI_API_KEY")
        db_password = secrets.get("DB_PASSWORD", required=True)
    """
    
    def __init__(
        self,
        vault_url: str = None,
        vault_token: str = None,
        vault_path: str = "secret/data/pca-agent"
    ):
        self.vault_url = vault_url or os.getenv("VAULT_ADDR")
        self.vault_token = vault_token or os.getenv("VAULT_TOKEN")
        self.vault_path = vault_path
        self._vault_client = self._init_vault()
        self._cache: Dict[str, str] = {}
        self._load_env_file()
    
    def _init_vault(self):
        """Initialize Vault client if available"""
        if not VAULT_AVAILABLE or not self.vault_url or not self.vault_token:
            return None
        
        try:
            client = hvac.Client(url=self.vault_url, token=self.vault_token)
            if client.is_authenticated():
                logger.info(f"Vault connected: {self.vault_url}")
                return client
            else:
                logger.warning("Vault authentication failed")
        except Exception as e:
            logger.error(f"Vault connection failed: {e}")
        
        return None
    
    def _load_env_file(self):
        """Load secrets from .env file"""
        env_files = [".env", ".env.local", ".env.production"]
        
        for env_file in env_files:
            path = Path(env_file)
            if path.exists():
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, value = line.split("=", 1)
                            self._cache[key.strip()] = value.strip().strip('"').strip("'")
    
    def get(
        self,
        key: str,
        default: str = None,
        required: bool = False
    ) -> Optional[str]:
        """
        Get a secret value
        
        Args:
            key: Secret key name
            default: Default value if not found
            required: Raise error if not found
        """
        # Check cache first
        if key in self._cache:
            return self._cache[key]
        
        value = None
        
        # 1. Try Vault
        if self._vault_client:
            try:
                secret = self._vault_client.secrets.kv.v2.read_secret_version(
                    path=self.vault_path.replace("secret/data/", "")
                )
                value = secret.get("data", {}).get("data", {}).get(key)
                if value:
                    self._cache[key] = value
                    return value
            except Exception as e:
                logger.debug(f"Vault lookup failed for {key}: {e}")
        
        # 2. Try environment variable
        value = os.getenv(key)
        if value:
            self._cache[key] = value
            return value
        
        # 3. Check cached .env values
        if key in self._cache:
            return self._cache[key]
        
        # 4. Use default
        if default is not None:
            return default
        
        if required:
            raise SecretNotFoundError(f"Required secret not found: {key}")
        
        return None
    
    def get_many(self, keys: list[str]) -> Dict[str, Optional[str]]:
        """Get multiple secrets at once"""
        return {key: self.get(key) for key in keys}
    
    def set(self, key: str, value: str, persist: bool = False):
        """
        Set a secret (primarily for testing)
        
        Args:
            key: Secret key
            value: Secret value
            persist: Write to Vault if available
        """
        self._cache[key] = value
        
        if persist and self._vault_client:
            try:
                # Read existing secrets
                existing = {}
                try:
                    secret = self._vault_client.secrets.kv.v2.read_secret_version(
                        path=self.vault_path.replace("secret/data/", "")
                    )
                    existing = secret.get("data", {}).get("data", {})
                except:
                    pass
                
                # Update with new value
                existing[key] = value
                
                self._vault_client.secrets.kv.v2.create_or_update_secret(
                    path=self.vault_path.replace("secret/data/", ""),
                    secret=existing
                )
                logger.info(f"Secret {key} persisted to Vault")
            except Exception as e:
                logger.error(f"Failed to persist secret to Vault: {e}")
    
    def rotate(self, key: str, new_value: str):
        """Rotate a secret (update and persist)"""
        self.set(key, new_value, persist=True)
        logger.info(f"Secret {key} rotated")
    
    def list_keys(self) -> list[str]:
        """List available secret keys"""
        keys = set(self._cache.keys())
        
        if self._vault_client:
            try:
                secret = self._vault_client.secrets.kv.v2.read_secret_version(
                    path=self.vault_path.replace("secret/data/", "")
                )
                vault_keys = secret.get("data", {}).get("data", {}).keys()
                keys.update(vault_keys)
            except:
                pass
        
        return sorted(keys)
    
    def health_check(self) -> Dict[str, Any]:
        """Check secrets manager health"""
        return {
            "vault_available": VAULT_AVAILABLE,
            "vault_connected": self._vault_client is not None,
            "vault_url": self.vault_url,
            "cached_secrets": len(self._cache),
            "keys": self.list_keys()
        }


class SecretNotFoundError(Exception):
    """Raised when a required secret is not found"""
    pass


# ============================================================================
# Global Instance
# ============================================================================

_secrets_manager: Optional[SecretsManager] = None

def get_secrets_manager() -> SecretsManager:
    """Get the global secrets manager instance"""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager

def get_secret(key: str, default: str = None, required: bool = False) -> Optional[str]:
    """Convenience function to get a secret"""
    return get_secrets_manager().get(key, default, required)


# ============================================================================
# Common Secret Keys
# ============================================================================

# Pre-defined secret keys for the application
class SecretKeys:
    OPENAI_API_KEY = "OPENAI_API_KEY"  # nosec B105
    GROQ_API_KEY = "GROQ_API_KEY"  # nosec B105
    JWT_SECRET_KEY = "JWT_SECRET_KEY"  # nosec B105
    DATABASE_URL = "DATABASE_URL"  # nosec B105
    REDIS_URL = "REDIS_URL"  # nosec B105
    SLACK_WEBHOOK_URL = "SLACK_WEBHOOK_URL"  # nosec B105
    SENTRY_DSN = "SENTRY_DSN"  # nosec B105
