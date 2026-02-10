"""
Tests for DuckDBRepository coverage (Phase 3.1).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from pathlib import Path

from src.core.database.duckdb_repository import DuckDBRepository, get_duckdb_repository

class TestDuckDBRepository:
    """Tests for DuckDBRepository methods."""
    
    @pytest.fixture
    def mock_manager(self):
        mock = MagicMock()
        return mock

    @pytest.fixture
    def repository(self, mock_manager):
        repo = DuckDBRepository()
        repo._manager = mock_manager
        return repo

    def test_manager_property(self):
        """Test lazy loading of manager."""
        repo = DuckDBRepository()
        with patch('src.core.database.duckdb_repository.get_duckdb_manager') as mock_get_mgr:
            _ = repo.manager
            mock_get_mgr.assert_called_once()

    def test_get_campaigns_df(self, repository, mock_manager):
        """Test variations of get_campaigns_df."""
        mock_conn = MagicMock()
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        # Test with multiple filters
        repository.get_campaigns_df(
            start_date="2024-01-01", 
            end_date="2024-01-31", 
            platforms=["Google", "Meta"],
            limit=100
        )
        
        args = mock_conn.execute.call_args[0]
        query = args[0]
        params = args[1]
        
        assert "Date\" >= ?" in query
        assert "Date\" <= ?" in query
        assert "Platform\" IN (?, ?)" in query
        assert "LIMIT 100" in query
        assert "2024-01-01" in params
        assert "Google" in params

    def test_get_aggregated_metrics(self, repository, mock_manager):
        """Test aggregated metrics retrieval."""
        mock_conn = MagicMock()
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        repository.get_aggregated_metrics(group_by="Channel", platforms=["LinkedIn"])
        
        query = mock_conn.execute.call_args[0][0]
        assert "GROUP BY \"Channel\"" in query
        assert "Platform\" IN (?)" in query
        assert "SUM(COALESCE(\"Spend\", 0))" in query

    def test_get_total_metrics(self, repository, mock_manager):
        """Test total metrics fetching."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = [1000.5, 10000, 500, 50, 2000.0, 10]
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        result = repository.get_total_metrics()
        
        assert result['spend'] == 1000.5
        assert result['row_count'] == 10

    def test_get_time_series(self, repository, mock_manager):
        """Test time series variations."""
        mock_conn = MagicMock()
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        # Weekly granularity
        repository.get_time_series(metric="clicks", granularity="weekly")
        query = mock_conn.execute.call_args[0][0]
        assert "DATE_TRUNC('week'" in query
        assert "SUM(COALESCE(\"Clicks\", 0))" in query

        # Monthly granularity
        repository.get_time_series(metric="spend", granularity="monthly")
        query = mock_conn.execute.call_args[0][0]
        assert "DATE_TRUNC('month'" in query

    @patch('src.core.database.duckdb_repository.find_column')
    def test_get_schema_info(self, mock_find, repository, mock_manager):
        """Test schema info detection."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.df.side_effect = [
            pd.DataFrame({'column_name': ['Platform', 'Spend']}), # DESCRIBE
            pd.DataFrame({'Platform': ['G'], 'Spend': [1.0]})     # SELECT * LIMIT 1
        ]
        mock_conn.execute.return_value.fetchone.return_value = [1000]
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        # mock find_column to return something for some keys
        mock_find.side_effect = lambda df, key: "Platform" if key in ['platform', 'spend'] else None
        
        info = repository.get_schema_info()
        assert info['row_count'] == 1000
        assert info['metrics']['spend'] is True
        assert info['dimensions']['platform'] is True
        assert info['dimensions']['channel'] is False

    @patch('src.core.database.duckdb_repository.find_column', return_value="Platform")
    def test_get_filter_options(self, mock_find, repository, mock_manager):
        """Test filter options retrieval."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({'a':[1]}) # sample
        mock_conn.execute.return_value.fetchall.return_value = [['Google'], ['Meta']]
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        options = repository.get_filter_options()
        assert 'platform' in options
        assert 'Google' in options['platform']

    def test_get_date_range(self, repository, mock_manager):
        """Test date range retrieval."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ["2024-01-01", "2024-01-05"]
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        rng = repository.get_date_range()
        assert rng == ("2024-01-01", "2024-01-05")

    def test_execute_raw_query(self, repository, mock_manager):
        """Test raw query execution."""
        mock_conn = MagicMock()
        mock_manager.connection.return_value.__enter__.return_value = mock_conn
        
        repository.execute_raw_query("SELECT 1", params=[1])
        mock_conn.execute.assert_called_with("SELECT 1", [1])

def test_get_duckdb_repository():
    """Test repository singleton."""
    from src.core.database.duckdb_repository import _repository
    import src.core.database.duckdb_repository as ddr
    ddr._repository = None
    
    r1 = get_duckdb_repository()
    r2 = get_duckdb_repository()
    assert r1 is r2
    assert ddr._repository is not None
