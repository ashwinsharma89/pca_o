"""
Tests for Settings coverage (Phase 5.2).
"""

import pytest
from pathlib import Path
from src.core.config.settings import Settings, get_settings

class TestSettings:
    """Tests for application settings."""
    
    def test_settings_initialization(self, tmp_path):
        """Test that settings create directories on init."""
        upload_dir = tmp_path / "uploads"
        report_dir = tmp_path / "reports"
        snapshot_dir = tmp_path / "snapshots"
        
        settings = Settings(
            upload_dir=upload_dir,
            report_dir=report_dir,
            snapshot_dir=snapshot_dir
        )
        
        assert upload_dir.exists()
        assert report_dir.exists()
        assert snapshot_dir.exists()

    def test_max_upload_size_bytes(self):
        """Test MB to bytes conversion."""
        settings = Settings(max_upload_size_mb=10)
        assert settings.max_upload_size_bytes == 10 * 1024 * 1024

    def test_get_settings_singleton(self):
        """Test global settings getter."""
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2
