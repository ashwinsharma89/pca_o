"""
Database Restore Script
Restores database from backup files
"""
import os
import sys
import subprocess
import gzip
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConfig
from loguru import logger


class BackupRestorer:
    """Restores database from backup files."""
    
    def __init__(self):
        """Initialize backup restorer."""
        self.db_config = DatabaseConfig()
        self.backup_dir = Path(os.getenv('BACKUP_DIR', './backups'))
    
    def list_backups(self):
        """List available backup files."""
        backups = []
        
        for file in sorted(self.backup_dir.glob('*.sql*'), reverse=True):
            backups.append({
                'file': file.name,
                'path': str(file),
                'size_mb': file.stat().st_size / (1024 * 1024),
                'modified': file.stat().st_mtime
            })
        
        return backups
    
    def restore_postgresql(self, backup_file: Path) -> bool:
        """
        Restore PostgreSQL database from backup.
        
        Args:
            backup_file: Path to backup file
            
        Returns:
            True if successful
        """
        logger.info(f"Restoring PostgreSQL from: {backup_file}")
        
        # Decompress if needed
        if backup_file.suffix == '.gz':
            logger.info("Decompressing backup...")
            decompressed = backup_file.with_suffix('')
            
            with gzip.open(backup_file, 'rb') as f_in:
                with open(decompressed, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            backup_file = decompressed
        
        # Set password in environment
        env = os.environ.copy()
        if self.db_config.password:
            env['PGPASSWORD'] = self.db_config.password
        
        # Restore using psql
        cmd = [
            'psql',
            '-h', self.db_config.host,
            '-p', str(self.db_config.port),
            '-U', self.db_config.user,
            '-d', self.db_config.database,
            '-f', str(backup_file)
        ]
        
        try:
            result = subprocess.run(
                cmd,
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("✅ PostgreSQL restore completed")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ PostgreSQL restore failed: {e.stderr}")
            return False
    
    def restore_sqlite(self, backup_file: Path) -> bool:
        """
        Restore SQLite database from backup.
        
        Args:
            backup_file: Path to backup file
            
        Returns:
            True if successful
        """
        logger.info(f"Restoring SQLite from: {backup_file}")
        
        # Decompress if needed
        if backup_file.suffix == '.gz':
            logger.info("Decompressing backup...")
            decompressed = backup_file.with_suffix('')
            
            with gzip.open(backup_file, 'rb') as f_in:
                with open(decompressed, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            backup_file = decompressed
        
        # Copy backup to database location
        target_db = Path('./data/pca_agent.db')
        
        # Backup current database
        if target_db.exists():
            backup_current = target_db.with_suffix('.db.backup')
            logger.info(f"Backing up current database to: {backup_current}")
            target_db.rename(backup_current)
        
        # Restore from backup
        import shutil
        shutil.copy2(backup_file, target_db)
        
        logger.info("✅ SQLite restore completed")
        return True
    
    def restore(self, backup_file: str) -> bool:
        """
        Restore database from backup file.
        
        Args:
            backup_file: Name or path of backup file
            
        Returns:
            True if successful
        """
        # Find backup file
        backup_path = Path(backup_file)
        if not backup_path.exists():
            backup_path = self.backup_dir / backup_file
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        logger.info("=" * 60)
        logger.info("Database Restore")
        logger.info("=" * 60)
        logger.info(f"Backup file: {backup_path}")
        logger.info(f"Size: {backup_path.stat().st_size / (1024 * 1024):.2f} MB")
        logger.info("=" * 60)
        
        # Restore based on database type
        if self.db_config.use_sqlite:
            success = self.restore_sqlite(backup_path)
        else:
            success = self.restore_postgresql(backup_path)
        
        logger.info("=" * 60)
        return success


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Restore database from backup')
    parser.add_argument('backup_file', nargs='?', help='Backup file to restore')
    parser.add_argument('--list', action='store_true', help='List available backups')
    
    args = parser.parse_args()
    
    restorer = BackupRestorer()
    
    if args.list:
        # List available backups
        backups = restorer.list_backups()
        
        if not backups:
            print("No backups found")
            return
        
        print("\nAvailable backups:")
        print("=" * 80)
        for i, backup in enumerate(backups, 1):
            from datetime import datetime
            modified = datetime.fromtimestamp(backup['modified'])
            print(f"{i}. {backup['file']}")
            print(f"   Size: {backup['size_mb']:.2f} MB")
            print(f"   Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}")
            print()
        
        return
    
    if not args.backup_file:
        parser.print_help()
        return
    
    # Confirm restore
    print(f"\n⚠️  WARNING: This will restore the database from: {args.backup_file}")
    print("Current data will be backed up but may be overwritten.")
    response = input("\nContinue? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Restore cancelled")
        return
    
    # Perform restore
    success = restorer.restore(args.backup_file)
    
    if success:
        print("\n✅ Restore completed successfully")
    else:
        print("\n❌ Restore failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
