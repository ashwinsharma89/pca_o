#!/usr/bin/env python3
"""
Reset password for testuser account.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.engine.services.user_service import UserService
from src.database.user_models import Base
import os

def main():
    """Reset testuser password."""
    print("Resetting password for testuser...")
    
    # Create engine (using SQLite)
    use_sqlite = os.getenv("USE_SQLITE", "true").lower() == "true"
    
    if use_sqlite:
        db_url = "sqlite:///./data/pca_agent.db"
    else:
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "pca_agent")
        db_user = os.getenv("DB_USER", "postgres")
        db_pass = os.getenv("DB_PASSWORD", "postgres")
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    
    # Get session
    session = SessionLocal()
    user_service = UserService(session)
    
    try:
        # Get testuser
        user = user_service.get_user_by_username("testuser")
        
        if not user:
            print("Error: testuser not found in database")
            return 1
        
        # Reset password directly (bypass old password check)
        new_password = "SecurePass123!"
        user.hashed_password = user_service.hash_password(new_password)
        user.must_change_password = False
        user.failed_login_attempts = 0
        user.locked_until = None
        
        session.commit()
        
        print(f"✅ Password reset successful!")
        print(f"Username: testuser")
        print(f"Password: {new_password}")
        print(f"\nYou can now login at http://localhost:3000/login")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
        return 1
    finally:
        session.close()

if __name__ == "__main__":
    sys.exit(main())
