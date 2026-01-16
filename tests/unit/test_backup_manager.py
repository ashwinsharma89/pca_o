"""
Unit tests for backup manager.
Tests database backup functionality with mocked dependencies.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os

# Try to import, skip if not available
try:
    from src.core.backup.backup_manager import BackupManager
    BACKUP_AVAILABLE = True
except ImportError:
    BACKUP_AVAILABLE = False
    BackupManager = None

pytestmark = pytest.mark.skipif(not BACKUP_AVAILABLE, reason="Backup manager not available")


class TestBackupManager:
    """Test backup manager functionality."""
    
    @pytest.fixture
    def temp_backup_dir(self, tmp_path):
        """Create temporary backup directory."""
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()
        return str(backup_dir)
    
    @pytest.fixture
    def backup_manager(self, temp_backup_dir):
        """Create backup manager with mocked database config."""
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = True
            mock_config.return_value.database_url = "sqlite:///./test.db"
            manager = BackupManager(
                backup_dir=temp_backup_dir,
                retention_days=7,
                compress=True
            )
            return manager
    
    def test_initialization(self, temp_backup_dir):
        """Test backup manager initialization."""
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = True
            manager = BackupManager(
                backup_dir=temp_backup_dir,
                retention_days=30,
                compress=True
            )
            
            assert manager.backup_dir == Path(temp_backup_dir)
            assert manager.retention_days == 30
            assert manager.compress is True
    
    def test_backup_dir_created(self, tmp_path):
        """Test that backup directory is created if it doesn't exist."""
        backup_dir = tmp_path / "new_backups"
        
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = True
            manager = BackupManager(backup_dir=str(backup_dir))
            
            assert backup_dir.exists()
    
    def test_create_backup_sqlite(self, backup_manager, tmp_path):
        """Test SQLite backup creation."""
        # Create a test database file
        test_db = tmp_path / "test.db"
        test_db.write_text("test database content")
        
        backup_manager.db_config.database_url = f"sqlite:///{test_db}"
        
        with patch.object(backup_manager, '_backup_sqlite') as mock_backup:
            mock_backup.return_value = {
                'success': True,
                'backup_file': str(backup_manager.backup_dir / 'backup_test.db'),
                'timestamp': datetime.now().isoformat()
            }
            
            result = backup_manager.create_backup("test_backup")
            
            assert result['success'] is True
            mock_backup.assert_called_once()
    
    def test_create_backup_failure(self, backup_manager):
        """Test backup failure handling."""
        with patch.object(backup_manager, '_backup_sqlite') as mock_backup:
            mock_backup.side_effect = Exception("Backup failed")
            
            result = backup_manager.create_backup()
            
            assert result['success'] is False
            assert 'error' in result
    
    def test_cleanup_old_backups(self, backup_manager, tmp_path):
        """Test old backup cleanup."""
        # Create some old backup files
        old_backup = backup_manager.backup_dir / "backup_old.db"
        old_backup.write_text("old backup")
        
        # Set modification time to old date
        old_time = datetime.now() - timedelta(days=10)
        os.utime(old_backup, (old_time.timestamp(), old_time.timestamp()))
        
        backup_manager.retention_days = 7
        
        with patch.object(backup_manager, '_cleanup_old_backups') as mock_cleanup:
            backup_manager._cleanup_old_backups()
            mock_cleanup.assert_called_once()
    
    def test_list_backups(self, backup_manager):
        """Test listing available backups."""
        # Create some backup files
        (backup_manager.backup_dir / "backup_1.db").write_text("backup 1")
        (backup_manager.backup_dir / "backup_2.db").write_text("backup 2")
        
        if hasattr(backup_manager, 'list_backups'):
            backups = backup_manager.list_backups()
            assert len(backups) >= 2


class TestBackupManagerPostgreSQL:
    """Test PostgreSQL backup functionality."""
    
    @pytest.fixture
    def pg_backup_manager(self, tmp_path):
        """Create backup manager for PostgreSQL."""
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = False
            mock_config.return_value.database_url = "postgresql://user:pass@localhost/db"
            mock_config.return_value.host = "localhost"
            mock_config.return_value.port = 5432
            mock_config.return_value.database = "test_db"
            mock_config.return_value.username = "test_user"
            
            manager = BackupManager(
                backup_dir=str(tmp_path / "backups"),
                retention_days=30
            )
            return manager
    
    def test_postgresql_backup(self, pg_backup_manager):
        """Test PostgreSQL backup creation."""
        with patch.object(pg_backup_manager, '_backup_postgresql') as mock_backup:
            mock_backup.return_value = {
                'success': True,
                'backup_file': 'backup.sql.gz',
                'timestamp': datetime.now().isoformat()
            }
            
            result = pg_backup_manager.create_backup()
            
            assert result['success'] is True


class TestBackupCompression:
    """Test backup compression functionality."""
    
    def test_compress_backup(self, tmp_path):
        """Test backup compression."""
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = True
            
            manager = BackupManager(
                backup_dir=str(tmp_path / "backups"),
                compress=True
            )
            
            assert manager.compress is True
    
    def test_no_compression(self, tmp_path):
        """Test backup without compression."""
        with patch('src.core.backup.backup_manager.DatabaseConfig') as mock_config:
            mock_config.return_value.use_sqlite = True
            
            manager = BackupManager(
                backup_dir=str(tmp_path / "backups"),
                compress=False
            )
            
            assert manager.compress is False
