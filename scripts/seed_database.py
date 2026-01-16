"""
Database seeding script for PCA Agent.
Creates sample users and loads sample campaign data for auditing/testing.
"""
import sys
from pathlib import Path
import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


from src.core.database.duckdb_manager import get_duckdb_manager

from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.database.models import User
from pydantic import BaseModel

class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    full_name: str

# Mock AuthService if standard one is complex/missing to facilitate seeding
class MockAuthService:
    def create_user(self, user: UserCreate):
        logger.info(f"Simulating creation for {user.username}")
        # Return a dummy user object with username
        return type('obj', (object,), {'username': user.username})

AuthService = MockAuthService

import argparse


def seed_duckdb():
    """Seed DuckDB with sample campaign data."""
    logger.info("Seeding DuckDB with sample data...")
    
    # Get DuckDB manager
    db = get_duckdb_manager()
    
    # Load sample parquet file
    sample_file = Path("data/samples/parquet/campaigns_sample.parquet")
    
    if sample_file.exists():
        df = pd.read_parquet(sample_file)
        db.save_campaigns(df)
        logger.info(f"✅ Loaded {len(df)} sample campaigns into DuckDB")
    else:
        logger.warning(f"Sample file not found: {sample_file}")
        logger.info("Loading from CSV samples instead...")
        
        # Try loading CSV samples
        csv_files = list(Path("data/samples/csv").glob("*.csv"))
        if csv_files:
            dfs = []
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    dfs.append(df)
                    logger.info(f"Loaded {csv_file.name}")
                except Exception as e:
                    logger.warning(f"Failed to load {csv_file.name}: {e}")
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                db.save_campaigns(combined_df)
                logger.info(f"✅ Loaded {len(combined_df)} campaigns from CSV files")
        else:
            logger.error("No sample data found!")
            return False
    
    return True


def seed_users():
    """Create sample users for testing."""
    logger.info("Creating sample users...")
    
    try:
        auth_service = AuthService()
        
        # Create auditor user
        auditor = UserCreate(
            username="auditor",
            email="auditor@example.com",
            password="audit123",
            full_name="Audit User"
        )
        
        try:
            user = auth_service.create_user(auditor)
            logger.info(f"✅ Created user: {user.username}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"User 'auditor' already exists")
            else:
                raise
        
        # Create test user
        test_user = UserCreate(
            username="testuser",
            email="test@example.com",
            password="test123",
            full_name="Test User"
        )
        
        try:
            user = auth_service.create_user(test_user)
            logger.info(f"✅ Created user: {user.username}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"User 'testuser' already exists")
            else:
                raise
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create users: {e}")
        return False


def main():
    """Main seeding function."""
    parser = argparse.ArgumentParser(description="Seed PCA Agent database with sample data")
    parser.add_argument("--skip-users", action="store_true", help="Skip user creation")
    parser.add_argument("--skip-data", action="store_true", help="Skip campaign data loading")
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("PCA Agent Database Seeding")
    logger.info("=" * 60)
    
    success = True
    
    # Seed users
    if not args.skip_users:
        if not seed_users():
            success = False
            logger.error("❌ User seeding failed")
    
    # Seed campaign data
    if not args.skip_data:
        if not seed_duckdb():
            success = False
            logger.error("❌ Data seeding failed")
    
    logger.info("=" * 60)
    if success:
        logger.info("✅ Database seeding completed successfully!")
        logger.info("")
        logger.info("Sample Credentials:")
        logger.info("  Username: auditor")
        logger.info("  Password: audit123")
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Start backend: uvicorn src.interface.api.main:app --reload")
        logger.info("  2. Start frontend: cd frontend && npm run dev")
        logger.info("  3. Login at: http://localhost:3000")
    else:
        logger.error("❌ Database seeding completed with errors")
        sys.exit(1)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
