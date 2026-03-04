"""
Comprehensive tests for ingestion worker module.

Tests cover:
- TaskProgress class
- process_upload task
- process_parquet_streaming task  
- health_check task
- Error handling and cleanup
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
from datetime import datetime
import tempfile
import os
import polars as pl

# Import the module components directly
from src.workers.ingestion_worker import TaskProgress, process_upload, process_parquet_streaming, health_check


class TestTaskProgress:
    """Tests for TaskProgress helper class."""
    
    def test_init(self):
        """Test TaskProgress initialization."""
        mock_task = Mock()
        progress = TaskProgress(mock_task)
        
        assert progress.task == mock_task
        assert progress.current == 0
        assert progress.total == 100
    
    def test_update_default_total(self):
        """Test update with default total."""
        mock_task = Mock()
        progress = TaskProgress(mock_task)
        
        progress.update(50, message="Processing...")
        
        assert progress.current == 50
        assert progress.total == 100
        mock_task.update_state.assert_called_once_with(
            state="PROGRESS",
            meta={
                "current": 50,
                "total": 100,
                "percent": 50.0,
                "message": "Processing..."
            }
        )
    
    def test_update_custom_total(self):
        """Test update with custom total."""
        mock_task = Mock()
        progress = TaskProgress(mock_task)
        
        progress.update(75, 150, "Almost done")
        
        assert progress.current == 75
        assert progress.total == 150
        mock_task.update_state.assert_called_once_with(
            state="PROGRESS",
            meta={
                "current": 75,
                "total": 150,
                "percent": 50.0,
                "message": "Almost done"
            }
        )


class TestProcessUpload:
    """Tests for process_upload Celery task."""
    
    @patch('src.workers.ingestion_worker.logger')
    @patch('src.workers.ingestion_worker.get_duckdb_manager')
    @patch('src.workers.ingestion_worker.pd.read_csv')
    def test_process_csv_success(self, mock_read_csv, mock_get_duckdb, mock_logger):
        """Test successful CSV file processing."""
        # Setup
        mock_task = Mock()
        mock_task.update_state = Mock()
        
        # Create a real temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
            f.write("col1,col2\n1,2\n")
        
        try:
            # Mock DataFrame
            df = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-02'],
                'campaign': ['C1', 'C2'],
                'spend': [100, 200]
            })
            mock_read_csv.return_value = df
            
            # Mock DuckDB manager
            mock_duckdb_mgr = Mock()
            mock_duckdb_mgr.save_campaigns.return_value = 2
            mock_get_duckdb.return_value = mock_duckdb_mgr
            
            # Execute - use __wrapped__ to access the unwrapped function if it's a celery task
            # If it's not a celery task directly (imported), call it normally
            func = getattr(process_upload, '__wrapped__', process_upload)
            result = func(
                mock_task,
                temp_path,
                "abc123",
                original_filename="test.csv"
            )
            
            # Verify
            assert result["status"] == "completed"
            assert result["row_count"] == 2
            assert result["file_hash"] == "abc123"
            assert "Successfully imported 2 rows" in result["message"]
        finally:
            # Cleanup
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @patch('src.workers.ingestion_worker.logger')
    def test_process_upload_file_not_found(self, mock_logger):
        """Test handling of missing file."""
        mock_task = Mock()
        
        func = getattr(process_upload, '__wrapped__', process_upload)
        result = func(
            mock_task,
            "/tmp/non_existent_file_12345.csv",
            "xyz789",
            original_filename="missing.csv"
        )
        
        assert result["status"] == "failed"
        assert "Temp file not found" in result["message"]
    
    @patch('src.workers.ingestion_worker.logger')
    def test_process_upload_unsupported_format(self, mock_logger):
        """Test handling of unsupported file format."""
        mock_task = Mock()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            temp_path = f.name
            f.write("some text")
        
        try:
            func = getattr(process_upload, '__wrapped__', process_upload)
            result = func(
                mock_task,
                temp_path,
                "bad123",
                original_filename="test.txt"
            )
            
            assert result["status"] == "failed"
            assert "Unsupported file format" in result["message"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestProcessParquetStreaming:
    """Tests for process_parquet_streaming Celery task."""
    
    @patch('src.workers.ingestion_worker.logger')
    @patch('src.workers.ingestion_worker.pl')
    def test_parquet_conversion_success(self, mock_polars, mock_logger):
        """Test successful Parquet conversion."""
        mock_task = Mock()
        
        # Mock Polars lazy frame
        mock_lf = Mock()
        mock_lf.sink_parquet = Mock()
        mock_polars.scan_csv.return_value = mock_lf
        
        # Mock result frame
        mock_result_lf = Mock()
        mock_count_result = Mock()
        mock_count_result.item.return_value = 1000
        mock_result_lf.select.return_value.collect.return_value = mock_count_result
        mock_polars.scan_parquet.return_value = mock_result_lf
        mock_polars.count.return_value = "count"
        
        func = getattr(process_parquet_streaming, '__wrapped__', process_parquet_streaming)
        result = func(
            mock_task,
            "/tmp/input.csv",
            "/tmp/output.parquet"
        )
        
        assert result["status"] == "completed"
        assert result["row_count"] == 1000
        assert result["output_path"] == "/tmp/output.parquet"
    
    @patch('src.workers.ingestion_worker.logger')
    @patch('src.workers.ingestion_worker.pl')
    def test_parquet_conversion_error(self, mock_polars, mock_logger):
        """Test Parquet conversion error handling."""
        mock_task = Mock()
        mock_polars.scan_csv.side_effect = Exception("CSV read error")
        
        func = getattr(process_parquet_streaming, '__wrapped__', process_parquet_streaming)
        result = func(
            mock_task,
            "/tmp/bad.csv",
            "/tmp/output.parquet"
        )
        
        assert result["status"] == "failed"
        assert "CSV read error" in result["error"]


class TestHealthCheck:
    """Tests for health_check task."""
    
    def test_health_check_returns_status(self):
        """Test health check returns proper status."""
        func = getattr(health_check, '__wrapped__', health_check)
        result = func()
        
        assert result["status"] == "healthy"
        assert result["worker"] == "ingestion_worker"
        assert "timestamp" in result
