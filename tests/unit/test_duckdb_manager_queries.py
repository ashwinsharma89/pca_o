"""
Tests for DuckDBManager query operations (Phase 2.3).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
import tempfile
import os

from src.core.database.duckdb_manager import DuckDBManager

class TestDuckDBManagerQueries:
    """Tests for complex query operations in DuckDBManager."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for DuckDB data."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def manager(self, temp_data_dir):
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            manager = DuckDBManager()
            mock_conn = MagicMock()
            manager._conn = mock_conn
            return manager

    def test_get_total_count(self, manager):
        """Test simple record count."""
        manager._conn.execute.return_value.fetchone.return_value = [100]
        with patch.object(manager, 'has_data', return_value=True):
            assert manager.get_total_count() == 100

    def test_get_filtered_count(self, manager):
        """Test count with filters."""
        manager._conn.execute.return_value.fetchone.return_value = [50]
        with patch.object(manager, 'has_data', return_value=True):
            count = manager.get_filtered_count(filters={'platform': 'Google'})
            assert count == 50
            # Verify IN clause generation for list filter
            manager.get_filtered_count(filters={'platform': ['Google', 'Meta']})
            last_query = manager._conn.execute.call_args[0][0]
            assert "IN (?, ?)" in last_query

    def test_get_job_summary(self, manager):
        """Test summary metrics for a job_id."""
        # Mocking fetchall result
        # Order: row_count, total_spend, total_impressions, total_clicks, total_conversions
        manager._conn.execute.return_value.fetchall.return_value = [[10, 100.5, 1000, 50, 5]]
        
        # Mock columns DF
        mock_cols_df = MagicMock()
        mock_cols_df.columns = ['spend', 'impressions', 'clicks', 'conversions', 'job_id']
        manager._conn.execute.return_value.df.return_value = mock_cols_df
        
        summary = manager.get_job_summary("job-123")
        assert summary['row_count'] == 10
        assert summary['total_spend'] == 100.5
        assert summary['total_conversions'] == 5

    def test_get_aggregated_data(self, manager):
        """Test grouping and aggregation."""
        mock_df = pd.DataFrame({
            'name': ['Google', 'Meta'],
            'spend': [100.0, 200.0],
            'impressions': [1000, 2000],
            'clicks': [100, 200],
            'conversions': [10, 20]
        })
        manager._conn.execute.return_value.df.return_value = mock_df
        with patch.object(manager, 'has_data', return_value=True):
            result = manager.get_aggregated_data(group_by="Platform")
            assert not result.empty
            assert 'ctr' in result.columns
            assert result['ctr'].iloc[0] == 10.0 # 100/1000 * 100

    def test_get_trend_data(self, manager):
        """Test time-series aggregation."""
        mock_df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'spend': [50.0, 60.0],
            'impressions': [500, 600],
            'clicks': [5, 6],
            'conversions': [1, 2]
        })
        manager._conn.execute.return_value.df.return_value = mock_df
        with patch.object(manager, 'has_data', return_value=True):
            result = manager.get_trend_data()
            assert len(result) == 2
            assert 'cpm' in result.columns

    def test_get_filter_options(self, manager):
        """Test dynamic filter discovery."""
        # Mock DESCRIBE output
        desc_df = pd.DataFrame({
            'column_name': ['Platform', 'Campaign', 'Spend', 'Date']
        })
        # Mocking the two calls in get_filter_options: 1. DESCRIBE, 2. SELECT DISTINCT for each col
        def mock_execute(query, *args, **kwargs):
            m = MagicMock()
            if "DESCRIBE" in query:
                m.df.return_value = desc_df
            elif "DISTINCT" in query:
                col = query.split('"')[1]
                m.df.return_value = pd.DataFrame({'val': [f'{col}_V1', f'{col}_V2']})
            return m
            
        manager._conn.execute.side_effect = mock_execute
        
        with patch.object(manager, 'has_data', return_value=True):
            options = manager.get_filter_options()
            # Numeric 'Spend' and Date 'Date' should be excluded
            assert 'platform' in options
            assert 'campaign' in options
            assert 'spend' not in options
            assert 'date' not in options

    def test_get_total_metrics(self, manager):
        """Test overall aggregation metrics."""
        summary_df = pd.DataFrame({
            'total_spend': [1000.0],
            'total_impressions': [10000],
            'total_clicks': [500],
            'total_conversions': [50],
            'campaign_count': [5]
        })
        manager._conn.execute.return_value.df.return_value = summary_df
        with patch.object(manager, 'has_data', return_value=True):
            metrics = manager.get_total_metrics()
            assert metrics['total_spend'] == 1000.0
            assert metrics['avg_ctr'] == 5.0
            assert metrics['campaign_count'] == 5

    def test_get_campaigns_polars(self, manager):
        """Test polars retrieval method."""
        import polars as pl
        mock_pl = pl.DataFrame({
            'Campaign_ID': [1],
            'Spend_USD': [100.0]
        })
        manager._conn.execute.return_value.pl.return_value = mock_pl
        with patch.object(manager, 'has_data', return_value=True):
            result = manager.get_campaigns_polars()
            assert isinstance(result, pl.DataFrame)
            # check normalization
            assert 'Spend' in result.columns
            # Campaign_ID is not in the default mapping, so it should still be there
            assert 'Campaign_ID' in result.columns
