"""
Tests for DuckDBManager basic operations (Phase 2.1).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import os

from src.core.database.duckdb_manager import DuckDBManager, get_duckdb_manager

class TestDuckDBManagerBasic:
    """Tests for basic DuckDBManager operations."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for DuckDB data."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    def test_init_singleton(self, temp_data_dir):
        """Test singleton pattern for DuckDBManager."""
        # Reset singleton instance
        DuckDBManager._instance = None
        
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            mgr1 = DuckDBManager()
            mgr2 = DuckDBManager()
            assert mgr1 is mgr2
            assert mgr1.data_dir == temp_data_dir

    @patch('duckdb.connect')
    def test_get_connection_persistent(self, mock_duckdb_connect, temp_data_dir):
        """Test connection management."""
        DuckDBManager._instance = None
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            with patch('src.core.database.duckdb_manager.DUCKDB_FILE', temp_data_dir / "test.duckdb"):
                manager = DuckDBManager()
                # Test the real get_connection method
                conn = manager.get_connection()
                
                assert conn == mock_conn
                # Should be called with the file path
                args, _ = mock_duckdb_connect.call_args
                assert str(temp_data_dir / "test.duckdb") in args[0]
                
                # Connection should be cached
                conn2 = manager.get_connection()
                assert conn2 == mock_conn
                assert mock_duckdb_connect.call_count == 1

    def test_has_data_logic(self, temp_data_dir):
        """Test has_data logic with real file system."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            # Update CAMPAIGNS_DIR to match temp_data_dir
            with patch('src.core.database.duckdb_manager.CAMPAIGNS_DIR', temp_data_dir / "campaigns"):
                manager = DuckDBManager()
                assert manager.has_data() is False
                
                # Create a parquet file
                campaigns_dir = temp_data_dir / "campaigns"
                campaigns_dir.mkdir(parents=True, exist_ok=True)
                (campaigns_dir / "test.parquet").touch()
                
                assert manager.has_data() is True

    @patch('src.core.database.duckdb_manager.DuckDBManager.get_connection')
    def test_initialize_tables(self, mock_get_conn, temp_data_dir):
        """Test explicit table initialization."""
        DuckDBManager._instance = None
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            manager = DuckDBManager()
            manager._tables_initialized = False # Force it
            manager.initialize()
            
            # Should execute CREATE TABLE statements
            assert mock_conn.execute.called
            assert "recommendation_history" in mock_conn.execute.call_args_list[0][0][0]
            assert manager._tables_initialized is True

    def test_clear_data_extended(self, temp_data_dir):
        """Test clearing data files and tables."""
        DuckDBManager._instance = None
        with patch('src.core.database.duckdb_manager.DATA_DIR', temp_data_dir):
            with patch('src.core.database.duckdb_manager.CAMPAIGNS_DIR', temp_data_dir / "campaigns"):
                manager = DuckDBManager()
                campaigns_dir = temp_data_dir / "campaigns"
                campaigns_dir.mkdir(parents=True, exist_ok=True)
                (campaigns_dir / "test.parquet").touch()
                
                mock_conn = MagicMock()
                manager._conn = mock_conn
                manager._indexed = True
                
                manager.clear_data()
                
                assert not campaigns_dir.exists()
                assert manager._indexed is False
                assert mock_conn.execute.called
                # Check for DROP TABLE
                found_drop = any("DROP TABLE" in call[0][0] for call in mock_conn.execute.call_args_list)
                assert found_drop
