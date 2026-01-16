"""
Secrets Management Module

Enterprise-grade secrets management with support for:
- Environment variables (development)
- HashiCorp Vault (production)
- AWS Secrets Manager (cloud)
- Azure Key Vault (cloud)

Usage:
    from src.core.config.settings import settings
    
    # Get a secret
    db_password = secrets.get("DATABASE_PASSWORD")
    
    # Get with default
    api_key = secrets.get("OPTIONAL_API_KEY", default="")
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod
from functools import lru_cache
import json
from loguru import logger


class SecretsBackend(Enum):
    """Supported secrets backends."""
    ENV = "env"              # Environment variables
    VAULT = "vault"          # HashiCorp Vault
    AWS = "aws"              # AWS Secrets Manager
    AZURE = "azure"          # Azure Key Vault
    GCP = "gcp"              # Google Secret Manager


class BaseSecretsProvider(ABC):
    """Abstract base class for secrets providers."""
    
    @abstractmethod
    def get_secret(self, key: str) -> Optional[str]:
        """Get a secret by key."""
        pass
    
    @abstractmethod
    def set_secret(self, key: str, value: str) -> bool:
        """Set a secret (if supported)."""
        pass
    
    @abstractmethod
    def list_secrets(self) -> list:
        """List available secret keys."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if the provider is healthy."""
        pass


class EnvSecretsProvider(BaseSecretsProvider):
    """
    Environment variables secrets provider.
    Used for development and simple deployments.
    """
    
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        logger.info(f"EnvSecretsProvider initialized with prefix: '{prefix}'")
    
    def get_secret(self, key: str) -> Optional[str]:
        full_key = f"{self.prefix}{key}" if self.prefix else key
        return os.getenv(full_key)
    
    def set_secret(self, key: str, value: str) -> bool:
        full_key = f"{self.prefix}{key}" if self.prefix else key
        os.environ[full_key] = value
        return True
    
    def list_secrets(self) -> list:
        if self.prefix:
            return [k for k in os.environ.keys() if k.startswith(self.prefix)]
        return list(os.environ.keys())
    
    def health_check(self) -> bool:
        return True


class VaultSecretsProvider(BaseSecretsProvider):
    """
    HashiCorp Vault secrets provider.
    Used for production with centralized secrets management.
    
    Required environment variables:
    - VAULT_ADDR: Vault server address
    - VAULT_TOKEN: Authentication token
    - VAULT_PATH: Secret path prefix (optional)
    """
    
    def __init__(
        self,
        addr: str = None,
        token: str = None,
        path: str = "secret/data/pca-agent"
    ):
        self.addr = addr or os.getenv("VAULT_ADDR", "http://localhost:8200")
        self.token = token or os.getenv("VAULT_TOKEN")
        self.path = path or os.getenv("VAULT_PATH", "secret/data/pca-agent")
        self._client = None
        self._cache: Dict[str, str] = {}
        
        logger.info(f"VaultSecretsProvider initialized: {self.addr}")
    
    def _get_client(self):
        """Lazy initialization of Vault client."""
        if self._client is None:
            try:
                import hvac
                self._client = hvac.Client(url=self.addr, token=self.token)
            except ImportError:
                logger.warning("hvac package not installed. Install with: pip install hvac")
                return None
        return self._client
    
    def get_secret(self, key: str) -> Optional[str]:
        # Check cache first
        if key in self._cache:
            return self._cache[key]
        
        client = self._get_client()
        if not client:
            # Fallback to environment
            return os.getenv(key)
        
        try:
            secret = client.secrets.kv.v2.read_secret_version(
                path=self.path.replace("secret/data/", "")
            )
            data = secret.get("data", {}).get("data", {})
            value = data.get(key)
            
            if value:
                self._cache[key] = value
            
            return value
        except Exception as e:
            logger.warning(f"Failed to read secret '{key}' from Vault: {e}")
            return os.getenv(key)  # Fallback
    
    def set_secret(self, key: str, value: str) -> bool:
        client = self._get_client()
        if not client:
            return False
        
        try:
            # Read existing secrets
            current = {}
            try:
                secret = client.secrets.kv.v2.read_secret_version(
                    path=self.path.replace("secret/data/", "")
                )
                current = secret.get("data", {}).get("data", {})
            except Exception:
                pass
            
            # Update with new secret
            current[key] = value
            
            client.secrets.kv.v2.create_or_update_secret(
                path=self.path.replace("secret/data/", ""),
                secret=current
            )
            
            self._cache[key] = value
            return True
        except Exception as e:
            logger.error(f"Failed to set secret '{key}' in Vault: {e}")
            return False
    
    def list_secrets(self) -> list:
        client = self._get_client()
        if not client:
            return []
        
        try:
            secret = client.secrets.kv.v2.read_secret_version(
                path=self.path.replace("secret/data/", "")
            )
            return list(secret.get("data", {}).get("data", {}).keys())
        except Exception:
            return []
    
    def health_check(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        
        try:
            return client.is_authenticated()
        except Exception:
            return False


class AWSSecretsProvider(BaseSecretsProvider):
    """
    AWS Secrets Manager provider.
    
    Required environment variables:
    - AWS_REGION
    - AWS_ACCESS_KEY_ID (or use IAM role)
    - AWS_SECRET_ACCESS_KEY (or use IAM role)
    """
    
    def __init__(self, secret_name: str = "pca-agent/secrets"):
        self.secret_name = secret_name
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self._client = None
        self._cache: Dict[str, str] = {}
        
        logger.info(f"AWSSecretsProvider initialized: {secret_name}")
    
    def _get_client(self):
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    service_name='secretsmanager',
                    region_name=self.region
                )
            except ImportError:
                logger.warning("boto3 package not installed. Install with: pip install boto3")
                return None
        return self._client
    
    def get_secret(self, key: str) -> Optional[str]:
        if key in self._cache:
            return self._cache[key]
        
        client = self._get_client()
        if not client:
            return os.getenv(key)
        
        try:
            response = client.get_secret_value(SecretId=self.secret_name)
            secrets = json.loads(response['SecretString'])
            
            # Cache all secrets
            self._cache.update(secrets)
            
            return secrets.get(key)
        except Exception as e:
            logger.warning(f"Failed to read secret '{key}' from AWS: {e}")
            return os.getenv(key)
    
    def set_secret(self, key: str, value: str) -> bool:
        client = self._get_client()
        if not client:
            return False
        
        try:
            # Get current secrets
            try:
                response = client.get_secret_value(SecretId=self.secret_name)
                secrets = json.loads(response['SecretString'])
            except Exception:
                secrets = {}
            
            # Update
            secrets[key] = value
            
            client.put_secret_value(
                SecretId=self.secret_name,
                SecretString=json.dumps(secrets)
            )
            
            self._cache[key] = value
            return True
        except Exception as e:
            logger.error(f"Failed to set secret in AWS: {e}")
            return False
    
    def list_secrets(self) -> list:
        if self._cache:
            return list(self._cache.keys())
        
        # Force load
        self.get_secret("__probe__")
        return list(self._cache.keys())
    
    def health_check(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        
        try:
            client.describe_secret(SecretId=self.secret_name)
            return True
        except Exception:
            return False


@dataclass
class SecretsConfig:
    """Configuration for secrets management."""
    backend: SecretsBackend = SecretsBackend.ENV
    vault_addr: str = ""
    vault_token: str = ""
    vault_path: str = "secret/data/pca-agent"
    aws_secret_name: str = "pca-agent/secrets"
    aws_region: str = "us-east-1"
    env_prefix: str = ""


class SecretsManager:
    """
    Unified secrets manager with multiple backend support.
    
    Usage:
        secrets = SecretsManager()
        
        # Get a secret
        password = secrets.get("DATABASE_PASSWORD")
        
        # Get with default
        key = secrets.get("API_KEY", default="dev-key")
        
        # Check health
        if not secrets.health_check():
            logger.warning("Secrets backend unhealthy!")
    """
    
    def __init__(self, config: SecretsConfig = None):
        self.config = config or self._load_config()
        self._provider = self._create_provider()
        
        logger.info(f"SecretsManager initialized with backend: {self.config.backend.value}")
    
    def _load_config(self) -> SecretsConfig:
        """Load configuration from environment."""
        backend_str = os.getenv("SECRETS_BACKEND", "env").lower()
        
        try:
            backend = SecretsBackend(backend_str)
        except ValueError:
            logger.warning(f"Unknown secrets backend: {backend_str}, using 'env'")
            backend = SecretsBackend.ENV
        
        return SecretsConfig(
            backend=backend,
            vault_addr=os.getenv("VAULT_ADDR", "http://localhost:8200"),
            vault_token=os.getenv("VAULT_TOKEN", ""),
            vault_path=os.getenv("VAULT_PATH", "secret/data/pca-agent"),
            aws_secret_name=os.getenv("AWS_SECRET_NAME", "pca-agent/secrets"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            env_prefix=os.getenv("SECRETS_ENV_PREFIX", "")
        )
    
    def _create_provider(self) -> BaseSecretsProvider:
        """Create the appropriate secrets provider."""
        if self.config.backend == SecretsBackend.VAULT:
            return VaultSecretsProvider(
                addr=self.config.vault_addr,
                token=self.config.vault_token,
                path=self.config.vault_path
            )
        elif self.config.backend == SecretsBackend.AWS:
            return AWSSecretsProvider(
                secret_name=self.config.aws_secret_name
            )
        else:
            return EnvSecretsProvider(prefix=self.config.env_prefix)
    
    def get(self, key: str, default: str = None) -> Optional[str]:
        """
        Get a secret by key.
        
        Args:
            key: Secret key name
            default: Default value if not found
            
        Returns:
            Secret value or default
        """
        value = self._provider.get_secret(key)
        
        if value is None and default is not None:
            return default
        
        return value
    
    def get_required(self, key: str) -> str:
        """
        Get a required secret. Raises if not found.
        
        Args:
            key: Secret key name
            
        Returns:
            Secret value
            
        Raises:
            ValueError: If secret not found
        """
        value = self.get(key)
        
        if value is None:
            raise ValueError(f"Required secret '{key}' not found")
        
        return value
    
    def set(self, key: str, value: str) -> bool:
        """
        Set a secret (if backend supports it).
        
        Args:
            key: Secret key name
            value: Secret value
            
        Returns:
            True if successful
        """
        return self._provider.set_secret(key, value)
    
    def list_keys(self) -> list:
        """List available secret keys."""
        return self._provider.list_secrets()
    
    def health_check(self) -> bool:
        """Check if secrets backend is healthy."""
        return self._provider.health_check()
    
    def get_backend_info(self) -> Dict[str, Any]:
        """Get information about the current backend."""
        return {
            "backend": self.config.backend.value,
            "healthy": self.health_check(),
            "secrets_count": len(self.list_keys())
        }


# Global instance for easy import
@lru_cache(maxsize=1)
def get_secrets_manager() -> SecretsManager:
    """Get the global secrets manager instance."""
    return SecretsManager()


# Convenience accessor
secrets = get_secrets_manager()


# Required secrets for the application
REQUIRED_SECRETS = [
    "JWT_SECRET_KEY",
    "DATABASE_URL",
]

OPTIONAL_SECRETS = [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GROQ_API_KEY",
    "REDIS_URL",
    "SENTRY_DSN",
]


def validate_required_secrets() -> Dict[str, bool]:
    """
    Validate that all required secrets are present.
    
    Returns:
        Dict of secret names to availability status
    """
    manager = get_secrets_manager()
    results = {}
    
    for secret in REQUIRED_SECRETS:
        value = manager.get(secret)
        results[secret] = value is not None and len(value) > 0
        
        if not results[secret]:
            logger.error(f"🔴 CRITICAL: Required secret '{secret}' not found!")
    
    return results


def get_secrets_health_report() -> Dict[str, Any]:
    """
    Get a comprehensive health report for secrets.
    
    Returns:
        Health report dictionary
    """
    manager = get_secrets_manager()
    required_status = validate_required_secrets()
    
    return {
        "backend": manager.config.backend.value,
        "backend_healthy": manager.health_check(),
        "required_secrets": required_status,
        "all_required_present": all(required_status.values()),
        "optional_secrets": {
            secret: manager.get(secret) is not None
            for secret in OPTIONAL_SECRETS
        }
    }
