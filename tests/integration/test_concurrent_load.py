
import pytest
import concurrent.futures
from fastapi.testclient import TestClient
import pandas as pd
import io
import time
import random
from src.interface.api.main import app

# Setup Test Client
# Note: TestClient is synchronous, but we can use threading to simulate concurrent requests 
# hitting the app (which will run in threadpool if async). 
# Real concurrent load needs full HTTP server, but this verifies the Manager's locking logic.
client = TestClient(app)

def single_upload_task(run_id, thread_id):
    """Refactored upload task for concurrency"""
    time.sleep(random.random()) # Random jitter
    
    # 1. Create File
    df = pd.DataFrame([
        {'date': '2025-02-01', 'spend': 100, 'platform': f'Thread_{thread_id}', 'impressions': 100, 'run_id': run_id, 'thread_id': thread_id},
        {'date': '2025-02-01', 'spend': 200, 'platform': f'Thread_{thread_id}', 'impressions': 200, 'run_id': run_id, 'thread_id': thread_id},
    ])
    csv_content = df.to_csv(index=False)
    file_obj = io.BytesIO(csv_content.encode('utf-8'))
    file_obj.name = f"concurrent_data_{run_id}_{thread_id}.csv"
    
    # 2. Upload
    print(f"[Thread {thread_id}] Starting Upload...")
    try:
        res = client.post(
            "/api/v1/upload/stream",
            files={"file": (file_obj.name, file_obj, "text/csv")}
        )
        job_id = res.json().get('job_id')
        status = res.json().get('status')
        print(f"[Thread {thread_id}] Upload Req Complete: {status} Job: {job_id}")
        return status
    except Exception as e:
        print(f"[Thread {thread_id}] ERROR: {e}")
        return "error"

def test_concurrent_load():
    """
    Concurrent Giga-Test:
    Spawns multiple threads to upload files simultaneously.
    Verifies that DuckDBManager (Singleton) handles the locking queues correctly without crashing.
    """
    print("\n\n=== STARTING CONCURRENT GIGA-TEST ===")
    
    run_id = int(time.time())
    num_threads = 5
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(single_upload_task, run_id, i) 
            for i in range(num_threads)
        ]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
    print(f"Results: {results}")
    
    # Assertions
    # We define success as: NO Internal Server Errors (500)
    # The Singleton should serialize writes, so response times might be higher, but no crash.
    assert "error" not in results
    print("=== CONCURRENT LOAD TEST PASSED ===")

if __name__ == "__main__":
    test_concurrent_load()
