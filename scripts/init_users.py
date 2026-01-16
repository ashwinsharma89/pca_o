"""
Initialize user database and create first admin user.
"""

import sys
import os
from pathlib import Path
import getpass

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from src.database.user_models import Base
from src.engine.services.user_service import UserService, PasswordValidator

load_dotenv()


def main():
    """Initialize user database."""
    print("Initializing user database...")
    
    # Create engine
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
    
    # Create tables
    print("Creating tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created")
    
    # Create admin user
    session = SessionLocal()
    user_service = UserService(session)
    
    print("\nCreate first admin user:")
    username = input("Username: ")
    email = input("Email: ")
    password = getpass.getpass("Password: ")
    
    try:
        user = user_service.create_user(
            username=username,
            email=email,
            password=password,
            role="admin",
            tier="enterprise"
        )
        print(f"\nAdmin user created: {user.username}")
    except Exception as e:
        print(f"Error: {e}")
    
    session.close()


if __name__ == "__main__":
    main()
