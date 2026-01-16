
import sys
import os
import logging
import bcrypt

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db_manager
from src.database.user_models import User
from src.interface.api.middleware.auth import verify_password

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_login():
    logger.info("Starting login debug...")
    
    db_manager = get_db_manager()
    
    with db_manager.get_session() as session:
        username = "test@example.com"
        logger.info(f"Looking for user: {username}")
        
        user = session.query(User).filter(User.username == username).first()
        
        if not user:
            logger.error("❌ User NOT FOUND in database!")
            # List all users
            users = session.query(User).all()
            logger.info(f"Found {len(users)} users in DB:")
            for u in users:
                logger.info(f" - {u.username} (Email: {u.email})")
            return
            
        logger.info(f"✅ User found: {user.username}")
        logger.info(f"Stored Hash: {user.hashed_password}")
        
        test_pass = "testpassword123"
        logger.info(f"Testing password: {test_pass}")
        
        # Test 1: Direct bcrypt check
        try:
            is_valid = bcrypt.checkpw(test_pass.encode('utf-8'), user.hashed_password.encode('utf-8'))
            logger.info(f"Direct bcrypt check: {'✅ MATCH' if is_valid else '❌ MISMATCH'}")
        except Exception as e:
            logger.error(f"Direct bcrypt check failed with error: {e}")

        # Test 2: auth.py verify_password
        try:
            is_valid_auth = verify_password(test_pass, user.hashed_password)
            logger.info(f"auth.py verify_password: {'✅ MATCH' if is_valid_auth else '❌ MISMATCH'}")
        except Exception as e:
            logger.error(f"auth.py verify failed with error: {e}")

if __name__ == "__main__":
    debug_login()
