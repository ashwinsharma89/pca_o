import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_db_manager
from src.engine.services.user_service import UserService

def debug():
    db_manager = get_db_manager()
    session = db_manager.get_session_direct()
    user_service = UserService(session)
    
    username = "ashwin"
    password = "Pca12345!"
    
    print(f"Debugging auth for {username}...")
    user = user_service.get_user_by_username(username)
    if not user:
        print("❌ User not found in DB")
        return
    
    print(f"✅ User found: {user.username}")
    print(f"   Hashed password in DB: {user.hashed_password}")
    
    # Try direct hash
    manual_hash = user_service.hash_password(password)
    print(f"   Manual hash of '{password}': {manual_hash}")
    
    print(f"   Verifying '{password}' against DB hash...")
    if user_service.verify_password(password, user.hashed_password):
        print("   ✅ Password verification SUCCESS (Service)")
    else:
        print("   ❌ Password verification FAILED (Service)")
        
    session.close()

if __name__ == "__main__":
    debug()
