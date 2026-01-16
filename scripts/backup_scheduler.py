"""
Automated Backup Scheduler
Runs database backups on a schedule and manages retention
"""
import os
import sys
import time
import schedule
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.backup.backup_manager import BackupManager
from loguru import logger


class BackupScheduler:
    """Schedules and runs automated backups."""
    
    def __init__(
        self,
        backup_dir: str = None,
        retention_days: int = 30,
        schedule_time: str = "02:00"  # 2 AM daily
    ):
        """
        Initialize backup scheduler.
        
        Args:
            backup_dir: Directory to store backups
            retention_days: Number of days to keep backups
            schedule_time: Time to run daily backup (HH:MM format)
        """
        self.backup_manager = BackupManager(
            backup_dir=backup_dir,
            retention_days=retention_days,
            compress=True
        )
        self.schedule_time = schedule_time
        self.last_backup_time = None
        self.backup_count = 0
    
    def run_backup(self):
        """Run a backup and log results."""
        try:
            logger.info("=" * 60)
            logger.info("Starting scheduled backup")
            logger.info("=" * 60)
            
            result = self.backup_manager.create_backup()
            
            if result.get('success', False):
                self.last_backup_time = datetime.now()
                self.backup_count += 1
                
                logger.info(f"✅ Backup #{self.backup_count} completed successfully")
                logger.info(f"Backup file: {result.get('backup_file')}")
                logger.info(f"Size: {result.get('size_mb', 0):.2f} MB")
                logger.info(f"Duration: {result.get('duration_seconds', 0):.2f}s")
            else:
                logger.error(f"❌ Backup failed: {result.get('error')}")
            
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Backup scheduler error: {e}")
    
    def start(self):
        """Start the backup scheduler."""
        logger.info("=" * 60)
        logger.info("Backup Scheduler Starting")
        logger.info("=" * 60)
        logger.info(f"Schedule: Daily at {self.schedule_time}")
        logger.info(f"Retention: {self.backup_manager.retention_days} days")
        logger.info(f"Backup directory: {self.backup_manager.backup_dir}")
        logger.info("=" * 60)
        
        # Schedule daily backup
        schedule.every().day.at(self.schedule_time).do(self.run_backup)
        
        # Run initial backup
        logger.info("Running initial backup...")
        self.run_backup()
        
        # Run scheduler loop
        logger.info("Scheduler running. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user")
            self.print_stats()
    
    def print_stats(self):
        """Print backup statistics."""
        logger.info("=" * 60)
        logger.info("Backup Statistics")
        logger.info("=" * 60)
        logger.info(f"Total backups: {self.backup_count}")
        if self.last_backup_time:
            logger.info(f"Last backup: {self.last_backup_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    # Configuration from environment
    backup_dir = os.getenv('BACKUP_DIR', './backups')
    retention_days = int(os.getenv('BACKUP_RETENTION_DAYS', '30'))
    schedule_time = os.getenv('BACKUP_SCHEDULE_TIME', '02:00')
    
    # Create and start scheduler
    scheduler = BackupScheduler(
        backup_dir=backup_dir,
        retention_days=retention_days,
        schedule_time=schedule_time
    )
    
    scheduler.start()


if __name__ == '__main__':
    main()
