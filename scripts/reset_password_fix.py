import sys
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseConfig
from src.engine.services.user_service import UserService

def reset_password():
    config = DatabaseConfig()
    db_url = "sqlite:///./data/pca_agent.db"
    
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        user_service = UserService(session)
        user = user_service.get_user_by_username("testuser")
        
        if not user:
            print("User testuser not found")
            return
            
        password = "SecurePass123!"
        user.hashed_password = user_service.hash_password(password)
        session.commit()
        print(f"Successfully reset password for {user.username}")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    reset_password()
