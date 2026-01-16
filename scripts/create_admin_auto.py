import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from src.database.user_models import Base
from src.engine.services.user_service import UserService

load_dotenv()

def main():
    print("Initializing user database...")
    
    # Force use of Postgres (USE_SQLITE is absent or false in env usually, but let's be safe)
    # The script reads env vars. I will rely on the env vars being correct or defaults.
    # But wait, my run_command won't have the env vars unless I pass them or they are in .env
    # The original script does: use_sqlite = os.getenv("USE_SQLITE", "true").lower() == "true"
    # I need to make sure I run this with USE_SQLITE=false because the backend is running on Postgres.
    
    use_sqlite = os.getenv("USE_SQLITE", "false").lower() == "true"
    
    if use_sqlite:
        db_url = "sqlite:///./data/pca_agent.db"
    else:
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "pca_agent")
        db_user = os.getenv("DB_USER", "postgres")
        # db_pass = os.getenv("DB_PASSWORD", "postgres") # Original script default
        # But wait, I didn't set password when creating user? 
        # `createuser -s postgres` creates a superuser. 
        # By default on local brew postgres, it might be trust auth or no password.
        # Let's try to assume empty password or "postgres".
        db_pass = os.getenv("DB_PASSWORD", "")
        
        # Try both usually if one fails? No, let's stick to simple.
        # If password is empty, it might be empty string.
        if db_pass:
            db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        else:
             db_url = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"

    print(f"Connecting to: {db_url}")
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    session = SessionLocal()
    user_service = UserService(session)
    
    try:
        user = user_service.create_user(
            username="admin",
            email="admin@example.com",
            password="Admin1234!",
            role="admin",
            tier="enterprise"
        )
        print(f"Admin user created successfully: {user.username}")
    except Exception as e:
        if "already exists" in str(e):
             print("Admin user already exists.")
        else:
             print(f"Error: {e}")
    
    session.close()

if __name__ == "__main__":
    main()
