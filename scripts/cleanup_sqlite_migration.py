#!/usr/bin/env python3
"""
Database Migration Cleanup Script

Removes legacy SQLite files and validates PostgreSQL + DuckDB setup.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def find_sqlite_files() -> List[Path]:
    """Find all SQLite database files."""
    root = Path.cwd()
    sqlite_files = []
    
    # Patterns to search for
    patterns = ['*.db', '*.sqlite', '*.sqlite3']
    
    # Directories to exclude
    exclude_dirs = {
        'node_modules', '.git', '__pycache__', 'venv', 'env',
        '.pytest_cache', '.mypy_cache', 'dist', 'build'
    }
    
    for pattern in patterns:
        for file_path in root.rglob(pattern):
            # Skip if in excluded directory
            if any(excluded in file_path.parts for excluded in exclude_dirs):
                continue
            
            # Skip test databases (they're temporary)
            if 'test' in file_path.name.lower() and file_path.stat().st_size < 1_000_000:
                continue
            
            sqlite_files.append(file_path)
    
    return sqlite_files


def check_postgresql_connection() -> Tuple[bool, str]:
    """Check if PostgreSQL connection is working."""
    try:
        from src.database.connection import get_db_manager
        
        db_manager = get_db_manager()
        
        if db_manager.health_check():
            return True, "PostgreSQL connection successful"
        else:
            return False, "PostgreSQL health check failed"
            
    except Exception as e:
        return False, f"PostgreSQL connection error: {e}"


def check_duckdb_setup() -> Tuple[bool, str]:
    """Check if DuckDB is properly set up."""
    try:
        from src.database.duckdb_manager import get_duckdb_manager
        
        duckdb = get_duckdb_manager()
        
        if duckdb.has_data():
            count = duckdb.get_total_count()
            return True, f"DuckDB working - {count:,} campaign records found"
        else:
            return True, "DuckDB working - No campaign data yet (upload CSV to populate)"
            
    except Exception as e:
        return False, f"DuckDB error: {e}"


def remove_sqlite_files(files: List[Path], dry_run: bool = True) -> None:
    """Remove SQLite files."""
    if not files:
        print("✅ No legacy SQLite files found")
        return
    
    print(f"\n{'DRY RUN - ' if dry_run else ''}Found {len(files)} SQLite files:")
    
    total_size = 0
    for file_path in files:
        size = file_path.stat().st_size
        total_size += size
        size_mb = size / (1024 * 1024)
        
        print(f"  - {file_path} ({size_mb:.2f} MB)")
        
        if not dry_run:
            try:
                file_path.unlink()
                print(f"    ✅ Deleted")
            except Exception as e:
                print(f"    ❌ Error: {e}")
    
    total_mb = total_size / (1024 * 1024)
    print(f"\nTotal size: {total_mb:.2f} MB")
    
    if dry_run:
        print("\n⚠️  DRY RUN - No files were deleted")
        print("Run with --delete flag to actually remove files")


def main():
    """Main migration cleanup."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database migration cleanup')
    parser.add_argument('--delete', action='store_true', help='Actually delete files (default is dry run)')
    parser.add_argument('--skip-checks', action='store_true', help='Skip database connection checks')
    args = parser.parse_args()
    
    print("="*60)
    print("DATABASE MIGRATION CLEANUP")
    print("="*60)
    
    # Check PostgreSQL
    if not args.skip_checks:
        print("\n1. Checking PostgreSQL connection...")
        pg_ok, pg_msg = check_postgresql_connection()
        if pg_ok:
            print(f"   ✅ {pg_msg}")
        else:
            print(f"   ❌ {pg_msg}")
            print("\n⚠️  PostgreSQL not working - migration may not be complete")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                sys.exit(1)
        
        # Check DuckDB
        print("\n2. Checking DuckDB setup...")
        duck_ok, duck_msg = check_duckdb_setup()
        if duck_ok:
            print(f"   ✅ {duck_msg}")
        else:
            print(f"   ❌ {duck_msg}")
            print("\n⚠️  DuckDB not working - migration may not be complete")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                sys.exit(1)
    
    # Find SQLite files
    print("\n3. Scanning for legacy SQLite files...")
    sqlite_files = find_sqlite_files()
    
    # Remove files
    print("\n4. Removing legacy SQLite files...")
    remove_sqlite_files(sqlite_files, dry_run=not args.delete)
    
    print("\n" + "="*60)
    if args.delete:
        print("✅ MIGRATION CLEANUP COMPLETE")
    else:
        print("ℹ️  DRY RUN COMPLETE - Run with --delete to remove files")
    print("="*60)


if __name__ == '__main__':
    main()
