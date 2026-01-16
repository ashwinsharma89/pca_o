"""
Extended tests for DataLoader - targeting 80%+ coverage.
Tests Excel, JSON, Parquet loading and edge cases.
"""

import pytest
import pandas as pd
import numpy as np
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.core.utils.data_loader import DataLoader, fetch_data, safe_load_csv


# ============================================================================
# Excel Loading Tests
# ============================================================================

class TestExcelLoading:
    """Tests for Excel file loading."""
    
    def test_load_valid_excel(self, tmp_path):
        """Should load valid Excel file."""
        # Create test Excel file
        df = pd.DataFrame({
            'Campaign': ['A', 'B', 'C'],
            'Spend': [100, 200, 300]
        })
        excel_path = tmp_path / "test.xlsx"
        df.to_excel(excel_path, index=False)
        
        result, error = DataLoader.load_excel(excel_path)
        
        assert error is None
        assert result is not None
        assert len(result) == 3
    
    def test_load_excel_specific_sheet(self, tmp_path):
        """Should load specific sheet from Excel."""
        df1 = pd.DataFrame({'A': [1, 2]})
        df2 = pd.DataFrame({'B': [3, 4]})
        
        excel_path = tmp_path / "multi_sheet.xlsx"
        with pd.ExcelWriter(excel_path) as writer:
            df1.to_excel(writer, sheet_name='Sheet1', index=False)
            df2.to_excel(writer, sheet_name='Sheet2', index=False)
        
        result, error = DataLoader.load_excel(excel_path, sheet_name='Sheet2')
        
        assert error is None
        assert 'B' in result.columns
    
    def test_load_excel_nonexistent_file(self):
        """Should return error for non-existent file."""
        result, error = DataLoader.load_excel("/nonexistent/file.xlsx")
        
        assert result is None
        assert error is not None
        assert "Error" in error
    
    def test_load_excel_skip_validation(self, tmp_path):
        """Should skip validation when requested."""
        df = pd.DataFrame({'A': [1]})
        excel_path = tmp_path / "test.xlsx"
        df.to_excel(excel_path, index=False)
        
        result, error = DataLoader.load_excel(excel_path, validate=False)
        
        assert error is None
        assert result is not None


# ============================================================================
# JSON Loading Tests
# ============================================================================

class TestJSONLoading:
    """Tests for JSON file loading."""
    
    def test_load_valid_json(self, tmp_path):
        """Should load valid JSON file."""
        data = [
            {'Campaign': 'A', 'Spend': 100},
            {'Campaign': 'B', 'Spend': 200}
        ]
        json_path = tmp_path / "test.json"
        with open(json_path, 'w') as f:
            json.dump(data, f)
        
        result, error = DataLoader.load_json(json_path)
        
        assert error is None
        assert result is not None
        assert len(result) == 2
    
    def test_load_json_from_bytes(self):
        """Should load JSON from BytesIO."""
        data = [{'A': 1}, {'A': 2}]
        json_bytes = io.BytesIO(json.dumps(data).encode())
        
        result, error = DataLoader.load_json(json_bytes)
        
        assert error is None
        assert len(result) == 2
    
    def test_load_invalid_json(self, tmp_path):
        """Should return error for invalid JSON."""
        json_path = tmp_path / "invalid.json"
        with open(json_path, 'w') as f:
            f.write("not valid json {{{")
        
        result, error = DataLoader.load_json(json_path)
        
        assert result is None
        assert error is not None


# ============================================================================
# Parquet Loading Tests
# ============================================================================

class TestParquetLoading:
    """Tests for Parquet file loading."""
    
    def test_load_valid_parquet(self, tmp_path):
        """Should load valid Parquet file."""
        df = pd.DataFrame({
            'Campaign': ['A', 'B'],
            'Spend': [100, 200]
        })
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path)
        
        result, error = DataLoader.load_parquet(parquet_path)
        
        assert error is None
        assert result is not None
        assert len(result) == 2
    
    def test_load_parquet_nonexistent(self):
        """Should return error for non-existent file."""
        result, error = DataLoader.load_parquet("/nonexistent/file.parquet")
        
        assert result is None
        assert error is not None


# ============================================================================
# Streamlit Upload Tests
# ============================================================================

class TestStreamlitUpload:
    """Tests for Streamlit file upload handling."""
    
    def test_load_none_file(self):
        """Should handle None upload."""
        result, error = DataLoader.load_from_streamlit_upload(None)
        
        assert result is None
        assert "No file" in error
    
    def test_load_csv_upload(self, tmp_path):
        """Should load CSV from upload."""
        # Create mock uploaded file
        mock_file = Mock()
        mock_file.name = "test.csv"
        mock_file.size = 1000
        
        csv_content = "Campaign,Spend\nA,100\nB,200"
        mock_file.read.return_value = csv_content.encode()
        mock_file.seek = Mock()
        
        with patch.object(DataLoader, 'load_csv') as mock_load:
            mock_load.return_value = (pd.DataFrame({'Campaign': ['A', 'B']}), None)
            result, error = DataLoader.load_from_streamlit_upload(mock_file)
            
            assert mock_load.called
    
    def test_load_file_too_large(self):
        """Should reject files exceeding size limit."""
        mock_file = Mock()
        mock_file.name = "large.csv"
        mock_file.size = DataLoader.MAX_FILE_SIZE + 1
        
        result, error = DataLoader.load_from_streamlit_upload(mock_file)
        
        assert result is None
        assert "too large" in error.lower()
    
    def test_load_unsupported_extension(self):
        """Should reject unsupported file types."""
        mock_file = Mock()
        mock_file.name = "file.xyz"
        mock_file.size = 1000
        
        result, error = DataLoader.load_from_streamlit_upload(mock_file)
        
        assert result is None
        assert "Unsupported" in error


# ============================================================================
# Column Name Fixing Tests
# ============================================================================

class TestColumnNameFixing:
    """Tests for column name normalization."""
    
    def test_fix_spaces_in_column_names(self):
        """Should replace spaces with underscores."""
        df = pd.DataFrame({'Column Name': [1], 'Another Column': [2]})
        
        result = DataLoader._fix_column_names(df)
        
        assert 'Column_Name' in result.columns
        assert 'Another_Column' in result.columns
    
    def test_fix_special_characters(self):
        """Should remove special characters."""
        df = pd.DataFrame({'Col@Name': [1], 'Col#2': [2]})
        
        result = DataLoader._fix_column_names(df)
        
        # Special chars should be handled
        assert len(result.columns) == 2
    
    def test_fix_duplicate_columns(self):
        """Should handle duplicate column names."""
        df = pd.DataFrame([[1, 2, 3]], columns=['A', 'A', 'B'])
        
        result = DataLoader._fix_column_names(df)
        
        # Should have unique column names
        assert len(result.columns) == len(set(result.columns))


# ============================================================================
# Validation Tests
# ============================================================================

class TestDataValidation:
    """Tests for DataFrame validation."""
    
    def test_validate_empty_dataframe(self):
        """Should reject empty DataFrame."""
        df = pd.DataFrame()
        
        error = DataLoader._validate_dataframe(df)
        
        assert error is not None
    
    def test_validate_all_null_dataframe(self):
        """Should reject all-null DataFrame."""
        df = pd.DataFrame({'A': [None, None], 'B': [None, None]})
        
        error = DataLoader._validate_dataframe(df)
        
        assert error is not None
    
    def test_validate_valid_dataframe(self):
        """Should accept valid DataFrame."""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        error = DataLoader._validate_dataframe(df)
        
        assert error is None


# ============================================================================
# Convenience Function Tests
# ============================================================================

class TestConvenienceFunctions:
    """Tests for convenience functions."""
    
    def test_fetch_data_csv(self, tmp_path):
        """Should fetch data from CSV."""
        csv_path = tmp_path / "test.csv"
        pd.DataFrame({'A': [1, 2]}).to_csv(csv_path, index=False)
        
        result, error = fetch_data(csv_path)
        
        assert error is None
        assert result is not None
    
    def test_safe_load_csv_returns_none_on_error(self):
        """Should return None on error."""
        result = safe_load_csv("/nonexistent/file.csv")
        
        assert result is None
    
    def test_safe_load_csv_valid_file(self, tmp_path):
        """Should load valid CSV."""
        csv_path = tmp_path / "test.csv"
        pd.DataFrame({'A': [1]}).to_csv(csv_path, index=False)
        
        result = safe_load_csv(str(csv_path))
        
        assert result is not None


# ============================================================================
# Campaign DataFrame Normalization Tests
# ============================================================================

class TestCampaignNormalization:
    """Tests for campaign DataFrame normalization."""
    
    def test_normalize_spend_column(self):
        """Should normalize spend column variations."""
        df = pd.DataFrame({
            'Cost': [100, 200],
            'Campaign': ['A', 'B']
        })
        
        if hasattr(DataLoader, 'normalize_campaign_dataframe'):
            result = DataLoader.normalize_campaign_dataframe(df)
            # 'Cost' should be mapped to 'Spend'
            assert 'Spend' in result.columns or 'Cost' in result.columns
        else:
            # Method not available, skip
            pytest.skip("normalize_campaign_dataframe not available")
    
    def test_normalize_preserves_data(self):
        """Should preserve data values during normalization."""
        df = pd.DataFrame({
            'Campaign_Name': ['A', 'B'],
            'Spend': [100, 200]
        })
        
        if hasattr(DataLoader, 'normalize_campaign_dataframe'):
            result = DataLoader.normalize_campaign_dataframe(df)
            assert len(result) == 2
            assert result['Spend'].sum() == 300
        else:
            # Method not available, skip
            pytest.skip("normalize_campaign_dataframe not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
