#!/usr/bin/env python
"""Create admin user in Supabase PostgreSQL."""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import bcrypt

db_url = "postgresql://postgres.opftawkvjbkpwqigqizd:Q21175bptp!@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
os.environ["DATABASE_URL"] = db_url

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database.user_models import Base, User

engine = create_engine(db_url)

# Create tables if not exist
Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()

# Check if admin exists
admin = session.query(User).filter(User.username == "admin").first()
if admin:
    # Reset password
    new_hash = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
    admin.hashed_password = new_hash
    session.commit()
    print("✅ Password reset for existing admin user")
else:
    # Create admin user
    new_hash = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
    admin = User(
        username="admin",
        email="admin@pca.local",
        hashed_password=new_hash,
        role="admin",
        tier="enterprise"
    )
    session.add(admin)
    session.commit()
    print("✅ Admin user created in Supabase")

print("Credentials: admin / admin123")
session.close()
