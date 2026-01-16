#!/usr/bin/env python3
"""Create demo user automatically."""

import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

os.environ["USE_SQLITE"] = "true"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database.user_models import Base
from src.engine.services.user_service import UserService

def main():
    """Create demo user."""
    print("Creating demo user...")
    
    # Use SQLite
    db_url = "sqlite:///./data/pca_agent.db"
    os.makedirs("./data", exist_ok=True)
    
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Create demo user
    session = SessionLocal()
    user_service = UserService(session)
    
    try:
        # Check if user exists
        existing = user_service.get_user_by_username("demo")
        if existing:
            print("✅ Demo user already exists")
            print("   Username: demo")
            print("   Password: Demo123!")
        else:
            user = user_service.create_user(
                username="demo",
                email="demo@example.com",
                password="Demo123!",
                role="admin",
                tier="enterprise"
            )
            print("✅ Demo user created successfully!")
            print(f"   Username: {user.username}")
            print("   Password: Demo123!")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
