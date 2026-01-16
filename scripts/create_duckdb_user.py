#!/usr/bin/env python3
"""Create demo user in DuckDB."""

import sys
from pathlib import Path
import duckdb
import bcrypt

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Create demo user in DuckDB."""
    print("Creating demo user in DuckDB...")
    
    db_path = "./data/campaigns.duckdb"
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Create users table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username VARCHAR UNIQUE NOT NULL,
            email VARCHAR UNIQUE NOT NULL,
            hashed_password VARCHAR NOT NULL,
            role VARCHAR DEFAULT 'user',
            tier VARCHAR DEFAULT 'free',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Check if demo user exists
    result = conn.execute("SELECT username FROM users WHERE username = 'demo'").fetchone()
    
    if result:
        print("✅ Demo user already exists in DuckDB")
        print("   Username: demo")
        print("   Password: Demo123!")
    else:
        # Get next ID
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
        next_id = max_id + 1
        
        # Hash password
        password = "Demo123!"
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        
        # Insert demo user with explicit ID
        conn.execute("""
            INSERT INTO users (id, username, email, hashed_password, role, tier)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [next_id, 'demo', 'demo@example.com', hashed, 'admin', 'enterprise'])
        
        print("✅ Demo user created successfully in DuckDB!")
        print("   Username: demo")
        print("   Password: Demo123!")
    
    conn.close()

if __name__ == "__main__":
    main()
