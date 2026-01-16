"""
Configuration settings for PCA Agent system.
"""
from typing import Optional, List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # API Keys
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI API key (optional for local testing)",
    )
    anthropic_api_key: Optional[str] = Field(None, description="Anthropic API key")
    
    # LangSmith (Optional)
    langchain_tracing_v2: bool = Field(False, description="Enable LangSmith tracing")
    langchain_api_key: Optional[str] = Field(None, description="LangSmith API key")
    langchain_project: str = Field("pca-agent", description="LangSmith project name")
    
    # Database
    database_url: str = Field(
        "postgresql://user:password@localhost:5432/pca_db",
        description="PostgreSQL connection URL"
    )
    redis_url: str = Field(
        "redis://localhost:6379/0",
        description="Redis connection URL"
    )
    
    # Storage Paths
    upload_dir: Path = Field(Path("./data/uploads"), description="Upload directory")
    report_dir: Path = Field(Path("./data/reports"), description="Report output directory")
    snapshot_dir: Path = Field(Path("./data/snapshots"), description="Snapshot storage directory")
    
    # API Configuration
    api_host: str = Field("127.0.0.1", description="API host (use 127.0.0.1 for localhost, 0.0.0.0 for all interfaces)")
    api_port: int = Field(8000, description="API port")
    api_workers: int = Field(4, description="Number of API workers")
    debug: bool = Field(False, description="Debug mode")
    
    # Celery
    celery_broker_url: str = Field(
        "redis://localhost:6379/1",
        description="Celery broker URL"
    )
    celery_result_backend: str = Field(
        "redis://localhost:6379/2",
        description="Celery result backend URL"
    )
    
    # Model Configuration
    default_vlm_model: str = Field(
        "gpt-4-vision-preview",
        description="Default Vision Language Model"
    )
    default_llm_model: str = Field(
        "gpt-4-turbo-preview",
        description="Default Large Language Model"
    )
    temperature: float = Field(0.1, description="Model temperature")
    max_tokens: int = Field(4096, description="Max tokens per response")
    
    # Report Configuration
    default_template: str = Field("corporate", description="Default report template")
    brand_color: str = Field("#0066CC", description="Brand primary color")
    company_name: str = Field("Your Company", description="Company name")
    company_logo_path: Optional[Path] = Field(None, description="Company logo path")
    
    # Processing Limits
    max_upload_size_mb: int = Field(50, description="Max upload size in MB")
    max_snapshots_per_campaign: int = Field(20, description="Max snapshots per campaign")
    concurrent_vision_calls: int = Field(3, description="Concurrent vision API calls")
    
    # Monitoring
    sentry_dsn: Optional[str] = Field(None, description="Sentry DSN for error tracking")
    prometheus_port: int = Field(9090, description="Prometheus metrics port")
    
    # Feature Flags
    enable_ocr: bool = Field(True, description="Enable OCR for text extraction")
    enable_chart_recreation: bool = Field(True, description="Enable chart recreation")
    enable_achievement_detection: bool = Field(True, description="Enable achievement detection")
    enable_cross_channel_reasoning: bool = Field(True, description="Enable cross-channel reasoning")
    
    # Supported Platforms
    supported_platforms: List[str] = Field(
        default=[
            "google_ads",
            "cm360",
            "dv360",
            "meta_ads",
            "snapchat_ads",
            "linkedin_ads"
        ],
        description="Supported advertising platforms"
    )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create directories if they don't exist
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def max_upload_size_bytes(self) -> int:
        """Convert MB to bytes."""
        return self.max_upload_size_mb * 1024 * 1024


# Global settings instance
settings = Settings()

def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings

if settings.openai_api_key in (None, ""):
    import warnings

    warnings.warn(
        "OpenAI API key not set. Features requiring OpenAI will be disabled until the key is provided.",
        RuntimeWarning,
    )
