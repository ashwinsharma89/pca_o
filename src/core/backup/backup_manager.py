"""
Automated database backup manager.
Supports PostgreSQL and SQLite backups with rotation and compression.
"""

import os
import subprocess
import logging
import shutil
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
import gzip
import json
import re

from src.core.database.connection import get_db_manager, DatabaseConfig

logger = logging.getLogger(__name__)


class BackupManager:
    """Manages automated database backups."""
    
    def __init__(
        self,
        backup_dir: str = None,
        retention_days: int = 30,
        compress: bool = True
    ):
        """
        Initialize backup manager.
        
        Args:
            backup_dir: Directory to store backups
            retention_days: Number of days to keep backups
            compress: Whether to compress backups
        """
        self.backup_dir = Path(backup_dir or os.getenv('BACKUP_DIR', './backups'))
        self.retention_days = retention_days
        self.compress = compress
        
        # Create backup directory
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Get database config
        self.db_config = DatabaseConfig()
    
    def create_backup(self, backup_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a database backup.
        
        Args:
            backup_name: Optional custom backup name
            
        Returns:
            Dictionary with backup information
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = backup_name or f"backup_{timestamp}"
        
        try:
            if self.db_config.use_sqlite:
                result = self._backup_sqlite(backup_name)
            else:
                result = self._backup_postgresql(backup_name)
            
            logger.info(f"✅ Backup created: {result['backup_file']}")
            
            # Clean old backups
            self._cleanup_old_backups()
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _validate_pg_dump_params(self):
        """
        Validate pg_dump parameters to prevent command injection.
        
        Raises:
            ValueError: If any parameter contains invalid characters
        """
        # Whitelist pattern for database parameters (alphanumeric, underscore, hyphen, dot)
        safe_pattern = re.compile(r'^[a-zA-Z0-9_\-\.]+$')
        
        # Validate host
        if not safe_pattern.match(self.db_config.host):
            raise ValueError(
                f"Invalid database host: '{self.db_config.host}'. "
                "Only alphanumeric characters, underscores, hyphens, and dots are allowed."
            )
        
        # Validate user
        if not safe_pattern.match(self.db_config.user):
            raise ValueError(
                f"Invalid database user: '{self.db_config.user}'. "
                "Only alphanumeric characters, underscores, and hyphens are allowed."
            )
        
        # Validate database name
        if not safe_pattern.match(self.db_config.database):
            raise ValueError(
                f"Invalid database name: '{self.db_config.database}'. "
                "Only alphanumeric characters, underscores, and hyphens are allowed."
            )
        
        # Validate port
        if not (1 <= self.db_config.port <= 65535):
            raise ValueError(
                f"Invalid database port: {self.db_config.port}. "
                "Port must be between 1 and 65535."
            )
        
        logger.info("✅ pg_dump parameters validated")
    
    def _backup_sqlite(self, backup_name: str) -> Dict[str, Any]:
        """Backup SQLite database."""
        source_db = Path('./data/pca_agent.db')
        
        if not source_db.exists():
            raise FileNotFoundError(f"SQLite database not found: {source_db}")
        
        # Create backup file
        backup_file = self.backup_dir / f"{backup_name}.db"
        
        # Copy database file
        shutil.copy2(source_db, backup_file)
        
        # Compress if enabled
        if self.compress:
            compressed_file = self._compress_file(backup_file)
            backup_file.unlink()  # Remove uncompressed file
            backup_file = compressed_file
        
        # Get file size
        file_size = backup_file.stat().st_size
        
        return {
            'success': True,
            'backup_file': str(backup_file),
            'backup_name': backup_name,
            'database_type': 'sqlite',
            'file_size': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'compressed': self.compress,
            'timestamp': datetime.now().isoformat()
        }
    
    def _backup_postgresql(self, backup_name: str) -> Dict[str, Any]:
        """Backup PostgreSQL database using pg_dump."""
        backup_file = self.backup_dir / f"{backup_name}.sql"
        
        # Security: Validate all parameters before subprocess call
        self._validate_pg_dump_params()
        
        # Set password in environment
        env = os.environ.copy()
        if self.db_config.password:
            env['PGPASSWORD'] = self.db_config.password
        
        cmd = [
            'pg_dump',
            '-h', self.db_config.host,
            '-p', str(self.db_config.port),
            '-U', self.db_config.user,
            '-d', self.db_config.database,
            '-F', 'p',  # Plain text format
            '-f', str(backup_file)
        ]
        
        # Execute pg_dump
        try:
            subprocess.run(  # nosec B603
                cmd,
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"pg_dump failed: {e.stderr}")
        
        # Compress if enabled
        if self.compress:
            compressed_file = self._compress_file(backup_file)
            backup_file.unlink()  # Remove uncompressed file
            backup_file = compressed_file
        
        # Get file size
        file_size = backup_file.stat().st_size
        
        return {
            'success': True,
            'backup_file': str(backup_file),
            'backup_name': backup_name,
            'database_type': 'postgresql',
            'file_size': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'compressed': self.compress,
            'timestamp': datetime.now().isoformat()
        }
    
    def _compress_file(self, file_path: Path) -> Path:
        """Compress file using gzip."""
        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
        
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        return compressed_path
    
    def _cleanup_old_backups(self):
        """Remove backups older than retention period."""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        removed_count = 0
        
        for backup_file in self.backup_dir.glob('backup_*'):
            # Get file modification time
            mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
            
            if mtime < cutoff_date:
                backup_file.unlink()
                removed_count += 1
                logger.info(f"Removed old backup: {backup_file.name}")
        
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old backups")
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups."""
        backups = []
        
        for backup_file in sorted(self.backup_dir.glob('backup_*'), reverse=True):
            stat = backup_file.stat()
            
            backups.append({
                'name': backup_file.stem,
                'file': backup_file.name,
                'path': str(backup_file),
                'size': stat.st_size,
                'size_mb': stat.st_size / (1024 * 1024),
                'created': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'compressed': backup_file.suffix == '.gz'
            })
        
        return backups
    
    def restore_backup(self, backup_name: str) -> Dict[str, Any]:
        """
        Restore database from backup.
        
        Args:
            backup_name: Name of backup to restore
            
        Returns:
            Dictionary with restore information
        """
        try:
            # Find backup file
            backup_files = list(self.backup_dir.glob(f"{backup_name}*"))
            
            if not backup_files:
                raise FileNotFoundError(f"Backup not found: {backup_name}")
            
            backup_file = backup_files[0]
            
            if self.db_config.use_sqlite:
                result = self._restore_sqlite(backup_file)
            else:
                result = self._restore_postgresql(backup_file)
            
            logger.info(f"✅ Backup restored: {backup_name}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Restore failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _restore_sqlite(self, backup_file: Path) -> Dict[str, Any]:
        """Restore SQLite database."""
        target_db = Path('./data/pca_agent.db')
        
        # Decompress if needed
        if backup_file.suffix == '.gz':
            temp_file = backup_file.with_suffix('')
            with gzip.open(backup_file, 'rb') as f_in:
                with open(temp_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            backup_file = temp_file
        
        # Backup current database
        if target_db.exists():
            backup_current = target_db.with_suffix('.db.backup')
            shutil.copy2(target_db, backup_current)
        
        # Restore backup
        shutil.copy2(backup_file, target_db)
        
        # Clean up temp file
        if backup_file.suffix != '.gz':
            backup_file.unlink()
        
        return {
            'success': True,
            'database_type': 'sqlite',
            'restored_from': str(backup_file),
            'timestamp': datetime.now().isoformat()
        }
    
    def _restore_postgresql(self, backup_file: Path) -> Dict[str, Any]:
        """Restore PostgreSQL database using psql."""
        # Decompress if needed
        if backup_file.suffix == '.gz':
            temp_file = backup_file.with_suffix('')
            with gzip.open(backup_file, 'rb') as f_in:
                with open(temp_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            backup_file = temp_file
        
        # Build psql command
        env = os.environ.copy()
        if self.db_config.password:
            env['PGPASSWORD'] = self.db_config.password
        
        cmd = [
            'psql',
            '-h', self.db_config.host,
            '-p', str(self.db_config.port),
            '-U', self.db_config.user,
            '-d', self.db_config.database,
            '-f', str(backup_file)
        ]
        
        # Execute psql
        try:
            subprocess.run(  # nosec B603
                cmd,
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"psql restore failed: {e.stderr}")
        finally:
            # Clean up temp file
            if backup_file.suffix != '.gz':
                backup_file.unlink()
        
        return {
            'success': True,
            'database_type': 'postgresql',
            'restored_from': str(backup_file),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_backup_stats(self) -> Dict[str, Any]:
        """Get backup statistics."""
        backups = self.list_backups()
        
        if not backups:
            return {
                'total_backups': 0,
                'total_size_mb': 0.0,
                'oldest_backup': None,
                'newest_backup': None
            }
        
        total_size = sum(b['size'] for b in backups)
        
        return {
            'total_backups': len(backups),
            'total_size_mb': total_size / (1024 * 1024),
            'oldest_backup': backups[-1]['created'],
            'newest_backup': backups[0]['created'],
            'retention_days': self.retention_days,
            'backup_dir': str(self.backup_dir)
        }


# Global backup manager instance
_backup_manager: Optional[BackupManager] = None


def get_backup_manager() -> BackupManager:
    """Get or create global backup manager."""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = BackupManager()
    return _backup_manager
