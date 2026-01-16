"""
Initialize database schema.
Run this script to create all tables.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from src.database.connection import DatabaseManager, DatabaseConfig
from src.database.models import Base
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize database and create all tables."""
    try:
        logger.info("Initializing database...")
        
        # Create database manager
        config = DatabaseConfig()
        db_manager = DatabaseManager(config)
        
        # Initialize (creates tables)
        db_manager.initialize()
        
        # Health check
        if db_manager.health_check():
            logger.info("✅ Database initialized successfully!")
            logger.info(f"Database URL: {config.get_database_url().split('@')[-1] if '@' in config.get_database_url() else config.get_database_url()}")
            logger.info("\nTables created:")
            for table in Base.metadata.sorted_tables:
                logger.info(f"  - {table.name}")
        else:
            logger.error("❌ Database health check failed")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        return False


if __name__ == "__main__":
    success = init_database()
    sys.exit(0 if success else 1)
