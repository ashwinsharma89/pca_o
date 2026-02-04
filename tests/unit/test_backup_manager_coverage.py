"""
Tests for BackupManager coverage.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import os
from datetime import datetime, timedelta
import tempfile
import shutil

from src.core.backup.backup_manager import BackupManager, get_backup_manager

class TestBackupManager:
    """Tests for BackupManager class."""
    
    @pytest.fixture
    def temp_backup_dir(self):
        """Create a temporary directory for backups."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_init(self, mock_db_config, temp_backup_dir):
        """Test BackupManager initialization."""
        manager = BackupManager(backup_dir=str(temp_backup_dir), retention_days=10, compress=False)
        
        assert manager.backup_dir == temp_backup_dir
        assert manager.retention_days == 10
        assert manager.compress is False
        assert manager.backup_dir.exists()
        mock_db_config.assert_called_once()

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_validate_pg_dump_params_success(self, mock_db_config_class, temp_backup_dir):
        """Test validation of safe pg_dump parameters."""
        mock_config = Mock()
        mock_config.host = "localhost"
        mock_config.user = "postgres"
        mock_config.database = "test_db"
        mock_config.port = 5432
        mock_db_config_class.return_value = mock_config
        
        manager = BackupManager(backup_dir=str(temp_backup_dir))
        manager._validate_pg_dump_params() # Should not raise

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    @pytest.mark.parametrize("host,user,database,port", [
        ("local;host", "user", "db", 5432),
        ("localhost", "user;--", "db", 5432),
        ("localhost", "user", "db;DROP", 5432),
        ("localhost", "user", "db", 70000),
        ("localhost", "user", "db", 0),
    ])
    def test_validate_pg_dump_params_failure(self, mock_db_config_class, host, user, database, port, temp_backup_dir):
        """Test validation of unsafe pg_dump parameters."""
        mock_config = Mock()
        mock_config.host = host
        mock_config.user = user
        mock_config.database = database
        mock_config.port = port
        mock_db_config_class.return_value = mock_config
        
        manager = BackupManager(backup_dir=str(temp_backup_dir))
        with pytest.raises(ValueError):
            manager._validate_pg_dump_params()

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_list_backups_empty(self, mock_db_config, temp_backup_dir):
        """Test listing backups when none exist."""
        manager = BackupManager(backup_dir=str(temp_backup_dir))
        backups = manager.list_backups()
        assert backups == []

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_list_backups_with_files(self, mock_db_config, temp_backup_dir):
        """Test listing existing backup files."""
        # Create some dummy files
        (temp_backup_dir / "backup_20240101.db").touch()
        (temp_backup_dir / "backup_20240102.db.gz").touch()
        (temp_backup_dir / "other_file.txt").touch()
        
        manager = BackupManager(backup_dir=str(temp_backup_dir))
        backups = manager.list_backups()
        
        assert len(backups) == 2
        # Verify sorting (reverse chronological)
        assert backups[0]['file'] == "backup_20240102.db.gz"
        assert backups[1]['file'] == "backup_20240101.db"
        assert backups[0]['compressed'] is True
        assert backups[1]['compressed'] is False

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_get_backup_stats(self, mock_db_config, temp_backup_dir):
        """Test backup statistics calculation."""
        manager = BackupManager(backup_dir=str(temp_backup_dir))
        
        # Stats when empty
        stats = manager.get_backup_stats()
        assert stats['total_backups'] == 0
        
        # Stats with files
        (temp_backup_dir / "backup_20240101.db").write_text("dummy content")
        stats = manager.get_backup_stats()
        assert stats['total_backups'] == 1
        assert stats['total_size_mb'] > 0
        assert stats['newest_backup'] is not None

    @patch('src.core.backup.backup_manager.DatabaseConfig')
    def test_cleanup_old_backups(self, mock_db_config, temp_backup_dir):
        """Test removal of old backups based on retention policy."""
        manager = BackupManager(backup_dir=str(temp_backup_dir), retention_days=1)
        
        # Create an old file and a new file
        old_file = temp_backup_dir / "backup_old.db"
        old_file.touch()
        # Set mtime to 2 days ago
        old_mtime = (datetime.now() - timedelta(days=2)).timestamp()
        os.utime(str(old_file), (old_mtime, old_mtime))
        
        new_file = temp_backup_dir / "backup_new.db"
        new_file.touch()
        
        manager._cleanup_old_backups()
        
        assert not old_file.exists()
        assert new_file.exists()

    @patch('src.core.backup.backup_manager.BackupManager')
    def test_get_backup_manager_singleton(self, mock_manager_class):
        """Test singleton pattern for BackupManager."""
        import src.core.backup.backup_manager as bm
        
        # Reset global state for test
        bm._backup_manager = None
        
        mgr1 = bm.get_backup_manager()
        mgr2 = bm.get_backup_manager()
        
        assert mgr1 is mgr2
        assert bm._backup_manager is not None
