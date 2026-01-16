
import pytest
import time
from fastapi.testclient import TestClient
import pandas as pd
import io
import os
from src.interface.api.main import app
from src.core.schema.columns import Columns

client = TestClient(app)

def test_single_load_debug():
    """
    Debug Test: Single upload to verify job_id persistence.
    """
    print("\n\n=== STARTING DEBUG SINGLE LOAD TEST ===")
    
    run_id = int(time.time())
    
    # 1. Create File
    df = pd.DataFrame([
        {'date': '2025-02-01', 'spend': 100, 'platform': 'DebugPlat', 'impressions': 100, 'run_id': run_id},
    ])
    csv_content = df.to_csv(index=False)
    file_obj = io.BytesIO(csv_content.encode('utf-8'))
    file_obj.name = f"debug_data_{run_id}.csv"
    
    # 2. Upload
    print(f"Starting Upload...")
    try:
        res = client.post(
            "/api/v1/upload/stream",
            files={"file": (file_obj.name, file_obj, "text/csv")}
        )
        print(f"Response: {res.json()}")
        
        if res.status_code == 200:
            print("Upload Success")
        else:
            print(f"Upload Failed: {res.status_code}")
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_single_load_debug()
