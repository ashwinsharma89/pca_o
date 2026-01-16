
import sys
import os
sys.path.append(os.getcwd())
from src.core.database.duckdb_manager import get_duckdb_manager

mgr = get_duckdb_manager()
totals = mgr.get_total_metrics()
print(f"Total Spend in DB: ${totals.get('total_spend', 0):,.2f}")
print(f"Total Campaigns: {totals.get('campaign_count', 0)}")
