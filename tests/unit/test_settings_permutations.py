"""
Tests for Settings Configuration Permutations (Phase A.2).
Verifies that Settings handles environment variations safely.
"""

import pytest
import os
from pathlib import Path
from src.core.config.settings import Settings

class TestSettingsPermutations:
    """Tests for various setting combinations and environment overrides."""

    def test_settings_overrides(self, monkeypatch):
        """Verify that settings can be explicitly overridden."""
        settings = Settings(api_port=9999)
        assert settings.api_port == 9999

    def test_env_override_numeric(self, monkeypatch):
        """Test numeric environment overrides."""
        monkeypatch.setenv("API_PORT", "9000")
        monkeypatch.setenv("MAX_UPLOAD_SIZE_MB", "100")
        settings = Settings()
        assert settings.api_port == 9000
        assert settings.max_upload_size_mb == 100
        assert settings.max_upload_size_bytes == 100 * 1024 * 1024

    def test_env_override_boolean(self, monkeypatch):
        """Test boolean environment overrides."""
        monkeypatch.setenv("DEBUG", "True")
        monkeypatch.setenv("ENABLE_OCR", "false")
        settings = Settings()
        assert settings.debug is True
        assert settings.enable_ocr is False

    def test_env_override_path(self, monkeypatch, tmp_path):
        """Test path-based overrides."""
        custom_dir = tmp_path / "custom_uploads"
        monkeypatch.setenv("UPLOAD_DIR", str(custom_dir))
        settings = Settings()
        assert settings.upload_dir == custom_dir
        assert custom_dir.exists()

    def test_env_override_list(self, monkeypatch):
        """Test list/comma-separated overrides if supported by pydantic-settings."""
        # Pydantic Settings handles JSON lists or comma separated if configured, 
        # but the current settings.py uses a default list. 
        # Let's see if we can override it with a JSON string.
        monkeypatch.setenv("SUPPORTED_PLATFORMS", '["custom_plat"]')
        settings = Settings()
        assert settings.supported_platforms == ["custom_plat"]

    def test_invalid_numeric_falls_back(self, monkeypatch):
        """Verify that invalid numeric types raise pydantic validation errors (as expected)."""
        monkeypatch.setenv("API_PORT", "not-a-number")
        with pytest.raises(Exception): # Pydantic ValidationError
            Settings()

    def test_extra_env_ignored(self, monkeypatch):
        """Verify extra environment variables don't crash the loader."""
        monkeypatch.setenv("UNDEFINED_SETTING_XYZ", "ignore-me")
        settings = Settings()
        assert not hasattr(settings, "undefined_setting_xyz")
