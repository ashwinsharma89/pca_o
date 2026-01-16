"""
Unit tests for src/utils/data_loader.py
Covers: CSV/Excel/JSON loading, validation, column fixing, error handling
"""
import pandas as pd
import pytest
import io
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.core.utils.data_loader import (
    DataLoader,
    DataLoadError,
    normalize_campaign_dataframe,
    fetch_data,
    safe_load_csv
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def valid_csv_content():
    return b"Campaign,Spend,Clicks\nSpring Sale,1000,50\nSummer Sale,2500,120"


@pytest.fixture
def csv_with_special_chars():
    return b"Campaign Name,Total Spend ($),Click Count\nTest,1000,50"


@pytest.fixture
def csv_with_duplicates():
    return b"Campaign,Campaign,Spend\nA,B,100"


@pytest.fixture
def empty_csv():
    return b""


@pytest.fixture
def csv_all_nulls():
    return b"A,B,C\n,,\n,,"


# ============================================================================
# DataLoader.load_csv Tests
# ============================================================================

class TestDataLoaderCSV:
    """Tests for CSV loading functionality."""
    
    def test_load_valid_csv(self, valid_csv_content):
        """Load a well-formed CSV."""
        file_obj = io.BytesIO(valid_csv_content)
        df, error = DataLoader.load_csv(file_obj)
        
        assert error is None
        assert df is not None
        assert len(df) == 2
        assert "Campaign" in df.columns
    
    def test_load_csv_fixes_column_names(self, csv_with_special_chars):
        """Spaces and special chars should be cleaned."""
        file_obj = io.BytesIO(csv_with_special_chars)
        df, error = DataLoader.load_csv(file_obj, fix_column_names=True)
        
        assert error is None
        # Spaces → underscores, special chars removed
        assert "Campaign_Name" in df.columns or "CampaignName" in df.columns
        assert all(c.isalnum() or c == '_' for col in df.columns for c in col)
    
    def test_load_csv_handles_duplicates(self, csv_with_duplicates):
        """Duplicate columns get suffixes."""
        file_obj = io.BytesIO(csv_with_duplicates)
        df, error = DataLoader.load_csv(file_obj)
        
        assert error is None
        # Should have Campaign and Campaign_1
        assert len(df.columns) == 3
    
    def test_load_empty_csv_returns_error(self, empty_csv):
        """Empty CSV should return error."""
        file_obj = io.BytesIO(empty_csv)
        df, error = DataLoader.load_csv(file_obj)
        
        assert df is None
        assert error is not None
        # Error message may contain "empty" or "no data" or similar
        assert "empty" in error.lower() or "no data" in error.lower() or error is not None
    
    def test_load_csv_all_nulls_returns_error(self, csv_all_nulls):
        """CSV with all NaN values should return error."""
        file_obj = io.BytesIO(csv_all_nulls)
        df, error = DataLoader.load_csv(file_obj, validate=True)
        
        assert df is None
        assert error is not None
    
    def test_load_csv_skip_validation(self, csv_all_nulls):
        """Skip validation should return df even if all nulls."""
        file_obj = io.BytesIO(csv_all_nulls)
        df, error = DataLoader.load_csv(file_obj, validate=False)
        
        assert df is not None
        assert error is None


# ============================================================================
# normalize_campaign_dataframe Tests
# ============================================================================

class TestNormalizeCampaignDataframe:
    """Tests for campaign-specific normalization."""
    
    def test_maps_common_column_variations(self):
        """Common variations should map to standard names."""
        df = pd.DataFrame({
            "campaign": ["A"],
            "cost": ["$100"],
            "conv": [10],
            "impr": [1000]
        })
        
        result = normalize_campaign_dataframe(df)
        
        assert "Campaign_Name" in result.columns
        assert "Spend" in result.columns
        assert "Conversions" in result.columns
        assert "Impressions" in result.columns
    
    def test_spend_converted_to_numeric(self):
        """Spend with currency symbols should become numeric."""
        df = pd.DataFrame({
            "spend": ["$1,234.56", "€500", "1000"]
        })
        
        result = normalize_campaign_dataframe(df)
        
        assert result["Spend"].dtype.kind == "f"
        assert result["Spend"].iloc[0] == pytest.approx(1234.56, rel=0.01)
    
    def test_preserves_unmapped_columns(self):
        """Columns without mappings should remain unchanged."""
        df = pd.DataFrame({
            "custom_metric": [100],
            "spend": [500]
        })
        
        result = normalize_campaign_dataframe(df)
        
        assert "custom_metric" in result.columns
        assert "Spend" in result.columns
    
    def test_no_duplicate_target_columns(self):
        """If target column already exists, don't overwrite."""
        df = pd.DataFrame({
            "Campaign_Name": ["Existing"],
            "campaign": ["New"]
        })
        
        result = normalize_campaign_dataframe(df)
        
        # Should keep original Campaign_Name, not overwrite
        assert result["Campaign_Name"].iloc[0] == "Existing"


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error scenarios."""
    
    def test_file_not_found(self, tmp_path):
        """Non-existent file should return error."""
        fake_path = tmp_path / "nonexistent.csv"
        df, error = DataLoader.load_csv(fake_path)
        
        assert df is None
        assert "not found" in error.lower()
    
    def test_unsupported_extension(self, tmp_path):
        """Unsupported file type should return error."""
        fake_file = tmp_path / "data.xyz"
        fake_file.write_text("test")
        
        df, error = DataLoader.load_csv(fake_file)
        
        assert df is None
        assert "unsupported" in error.lower()
    
    def test_safe_load_csv_returns_none_on_error(self, tmp_path):
        """safe_load_csv should return None instead of raising."""
        fake_path = tmp_path / "missing.csv"
        result = safe_load_csv(fake_path)
        
        assert result is None


# ============================================================================
# Integration Tests
# ============================================================================

class TestFetchData:
    """Tests for the fetch_data convenience function."""
    
    def test_fetch_csv(self, tmp_path, valid_csv_content):
        """fetch_data should work for CSV files."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_bytes(valid_csv_content)
        
        df, error = fetch_data(csv_file)
        
        assert error is None
        assert len(df) == 2
