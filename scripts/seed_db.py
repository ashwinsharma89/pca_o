
import os
import sys
import logging
from datetime import datetime
import bcrypt

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db_manager
from src.database.user_models import User
from src.database.models import Campaign, Base

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_password_hash(password):
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')



def seed_data():
    logger.info("Starting database seed...")
    
    db_manager = get_db_manager()
    
    # HARD RESET: Drop and recreate tables to ensure schema matches models
    logger.warning("Resetting database schema...")
    Base.metadata.drop_all(bind=db_manager.engine)
    Base.metadata.create_all(bind=db_manager.engine)
    logger.info("Database schema reset.")
    
    with db_manager.get_session() as session:
        # 1. Create Test User
        test_user_email = "test@example.com"
        logger.info("Creating test user...")
        test_user = User(
            username=test_user_email,
            email=test_user_email,
            hashed_password=get_password_hash("testpassword123"),
            role="admin",
            is_active=True,
            is_verified=True
        )
        session.add(test_user)

        # 2. Check for generic 'testuser' used in some tests
        generic_user_name = "testuser"
        logger.info(f"Creating generic user '{generic_user_name}'...")
        generic_user = User(
            username=generic_user_name,
            email="testuser@example.com",
            hashed_password=get_password_hash("testpassword123"),
            role="user",
            is_active=True,
            is_verified=True
        )
        session.add(generic_user)

        # 3. Create Sample Campaigns
        logger.info("Creating sample campaigns...")
        campaigns = [
            Campaign(
                campaign_id="cmp_001",
                campaign_name="Summer Sale 2025",
                platform="Meta",
                channel="Facebook",
                spend=1200.50,
                impressions=50000,
                clicks=1200,
                conversions=45,
                ctr=2.4,
                cpc=1.00,
                cpa=26.67,
                roas=3.5,
                date=datetime.now(),
                objective="Conversions"
            ),
            Campaign(
                campaign_id="cmp_002",
                campaign_name="Tech Gadgets Promo",
                platform="Google",
                channel="Search",
                spend=2500.00,
                impressions=15000,
                clicks=800,
                conversions=80,
                ctr=5.3,
                cpc=3.12,
                cpa=31.25,
                roas=4.2,
                date=datetime.now(),
                objective="Sales"
            ),
                Campaign(
                campaign_id="cmp_003",
                campaign_name="LinkedIn B2B Outreach",
                platform="LinkedIn",
                channel="Feed",
                spend=3000.00,
                impressions=8000,
                clicks=150,
                conversions=15,
                ctr=1.8,
                cpc=20.00,
                cpa=200.00,
                roas=1.5,
                date=datetime.now(),
                objective="Lead Generation"
            )
        ]
        session.add_all(campaigns)
        
        session.commit()
        logger.info("Database seeding completed successfully!")


if __name__ == "__main__":
    seed_data()
