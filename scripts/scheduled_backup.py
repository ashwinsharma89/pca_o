"""
Scheduled backup script.
Run this script with a task scheduler (Windows Task Scheduler, cron, etc.)
"""

import sys
import os
import logging
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.backup import get_backup_manager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backups/backup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Run scheduled backup."""
    logger.info("=" * 60)
    logger.info("Starting scheduled backup")
    logger.info("=" * 60)
    
    try:
        # Get backup manager
        backup_manager = get_backup_manager()
        
        # Create backup
        result = backup_manager.create_backup()
        
        if result['success']:
            logger.info(f"✅ Backup successful!")
            logger.info(f"   File: {result['backup_file']}")
            logger.info(f"   Size: {result['file_size_mb']:.2f} MB")
            logger.info(f"   Type: {result['database_type']}")
            logger.info(f"   Compressed: {result['compressed']}")
            
            # Get backup stats
            stats = backup_manager.get_backup_stats()
            logger.info(f"\nBackup Statistics:")
            logger.info(f"   Total backups: {stats['total_backups']}")
            logger.info(f"   Total size: {stats['total_size_mb']:.2f} MB")
            logger.info(f"   Retention: {stats['retention_days']} days")
            
            return 0
        else:
            logger.error(f"❌ Backup failed: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Backup script failed: {e}", exc_info=True)
        return 1
    finally:
        logger.info("=" * 60)


if __name__ == "__main__":
    sys.exit(main())
