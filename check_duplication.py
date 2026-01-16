
import sys
import os
sys.path.append(os.getcwd())
from src.core.database.duckdb_manager import get_duckdb_manager

mgr = get_duckdb_manager()
total = mgr.get_total_count()
print(f"Total rows in DB: {total}")

# Get sample breakdown by upload timestamp (if possible) or just verify count
# Since we don't store upload timestamp in the parquet explicitly (only in partitioning), 
# we can just look at the high total. 
# Typical file is ~15k rows. If multiple, we see multiples of that.
