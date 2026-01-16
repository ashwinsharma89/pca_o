#!/usr/bin/env python3
"""Create user ashwin with specified credentials."""

import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_db_manager
from src.engine.services.user_service import UserService

def main():
    """Create user 'ashwin'."""
    print("Creating user 'ashwin'...")
    
    # Ensure we use the correct database as configured in environment
    db_manager = get_db_manager()
    session = db_manager.get_session_direct()
    user_service = UserService(session)
    
    username = "ashwin"
    password = "Pca12345!"
    email = "ashwin@example.com"
    
    try:
        # Check if user exists
        existing = user_service.get_user_by_username(username)
        if existing:
            print(f"⚠️ User '{username}' already exists. Updating password...")
            # For simplicity in this script, we'll delete and recreate if existing to ensure freshness
            # Alternatively, could add an update_password method to user_service
            session.delete(existing)
            session.commit()
            print(f"   Existing user deleted.")
        
        user = user_service.create_user(
            username=username,
            email=email,
            password=password,
            role="admin",
            tier="enterprise"
        )
        print(f"✅ User '{username}' created successfully!")
        print(f"   Username: {user.username}")
        print(f"   Password: {password}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
