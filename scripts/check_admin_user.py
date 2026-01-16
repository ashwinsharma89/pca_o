
import sys
import os
sys.path.append(os.getcwd())
from src.database.connection import get_db_manager
from src.engine.services.user_service import UserService

def check_user():
    print("Checking for admin user...")
    try:
        manager = get_db_manager()
        db = manager.get_session_direct()
        service = UserService(db)
        user = service.get_user_by_username("admin")
        if user:
            print(f"✅ User 'admin' found. Role: {user.role}, Tier: {user.tier}")
            # Verify password hash (roughly, just that it exists)
            print(f"   Password Hash: {user.hashed_password[:10]}...")
        else:
            print("❌ User 'admin' NOT found.")
            # Create it
            print("Creating 'admin' user...")
            service.create_user("admin", "admin@example.com", "password", "admin", "enterprise")
            print("✅ User 'admin' created with password 'password'.")
    except Exception as e:
        print(f"❌ Error checking user: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_user()
