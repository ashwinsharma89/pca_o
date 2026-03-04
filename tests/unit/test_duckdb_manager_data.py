"""
Tests for DuckDBManager data operations (Phase 2.2).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
import tempfile
import os

from src.core.database.duckdb_manager import DuckDBManager

class TestDuckDBManagerData:
    """Tests for DuckDBManager data operations like saving, appending, and retrieving."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for DuckDB data."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'campaign': ['C1', 'C2'],
            'spend': [100.0, 200.0],
            'impressions': [1000, 2000],
            'clicks': [10, 20],
            'conversions': [1, 2]
        })

    def test_save_campaigns_success(self, temp_data_dir, sample_df):
        """Test saving campaigns with partitioning."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            with patch('src.core.database.duckdb_manager.CAMPAIGNS_DIR', temp_data_dir / "campaigns"):
                with patch('src.core.database.duckdb_manager.CAMPAIGNS_PATTERN', str(temp_data_dir / "campaigns" / "**" / "*.parquet")):
                    manager = DuckDBManager()
                    # Mock connection to avoid real DuckDB file if possible, 
                    # but here we might want to test the full flow including ensure_indexes
                    mock_conn = MagicMock()
                    manager._conn = mock_conn
                    
                    rows = manager.save_campaigns(sample_df)
                    
                    assert rows == 2
                    # check for partition columns
                    assert 'year' in sample_df.columns
                    assert 'month' in sample_df.columns
                    
                    # check if directory exists
                    assert (temp_data_dir / "campaigns").exists()
                    # check if partitioned files exist (Hive style)
                    assert (temp_data_dir / "campaigns" / "year=2024" / "month=1").exists()
                    assert (temp_data_dir / "campaigns" / "year=2024" / "month=2").exists()

    def test_save_campaigns_invalid_dates(self, temp_data_dir):
        """Test handling of invalid dates during save."""
        DuckDBManager._instance = None
        df = pd.DataFrame({
            'date': ['invalid-date'],
            'campaign': ['CX']
        })
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            manager = DuckDBManager()
            with pytest.raises(ValueError, match="All rows have invalid or missing dates"):
                manager.save_campaigns(df)

    @patch('src.core.database.duckdb_manager.DuckDBManager.save_campaigns')
    def test_append_campaigns(self, mock_save, temp_data_dir, sample_df):
        """Test append_campaigns aliases save_campaigns."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            manager = DuckDBManager()
            manager.append_campaigns(sample_df)
            mock_save.assert_called_once_with(sample_df)

    def test_get_campaigns_empty(self, temp_data_dir):
        """Test get_campaigns when no data."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            with patch('src.core.database.duckdb_manager.CAMPAIGNS_DIR', temp_data_dir / "campaigns"):
                manager = DuckDBManager()
                df = manager.get_campaigns()
                assert df.empty

    def test_get_campaigns_with_normalization(self, temp_data_dir, sample_df):
        """Test getting campaigns and normalizing columns."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            with patch('src.core.database.duckdb_manager.CAMPAIGNS_DIR', temp_data_dir / "campaigns"):
                with patch('src.core.database.duckdb_manager.CAMPAIGNS_PATTERN', str(temp_data_dir / "campaigns" / "**" / "*.parquet")):
                    manager = DuckDBManager()
                    
                    # Create some variation in column names
                    df_var = pd.DataFrame({
                        'date': pd.to_datetime(['2024-01-01']),
                        'Campaign_Name': ['Special'],
                        'Spend_USD': [500.0]
                    })
                    
                    # We need a real DuckDB connection to test get_campaigns properly since it runs SQL
                    # Or we mock the connection.execute().df()
                    mock_conn = MagicMock()
                    mock_result_df = pd.DataFrame({
                        'date': ['2024-01-01'],
                        'Campaign_Name': ['Special'],
                        'Spend_USD': [500.0]
                    })
                    mock_conn.execute.return_value.df.return_value = mock_result_df
                    manager._conn = mock_conn
                    
                    # Mock has_data to True
                    with patch('src.core.database.duckdb_manager.DuckDBManager.has_data', return_value=True):
                        result = manager.get_campaigns()
                        
                        # Verify normalization
                        assert 'Campaign' in result.columns
                        assert 'Spend' in result.columns
                        assert result['Campaign'].iloc[0] == 'Special'
