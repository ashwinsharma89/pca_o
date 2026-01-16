import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.interface.api.middleware.auth import authenticate_user, SECRET_KEY, get_user

def debug():
    username = "ashwin"
    password = "Pca12345!"
    
    print(f"SECRET_KEY in internal code: {SECRET_KEY}")
    print(f"DATABASE_URL env: {os.getenv('DATABASE_URL')}")
    
    print(f"Debugging internal auth for {username}...")
    user = authenticate_user(username, password)
    if user:
        print(f"✅ Auth SUCCESS: {user['username']}")
    else:
        print(f"❌ Auth FAILED")
        u = get_user(username)
        if u:
            print(f"   User found, but password check failed.")
        else:
            print(f"   User NOT found.")

if __name__ == "__main__":
    debug()
