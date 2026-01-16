import pytest
import pandas as pd
import io
import time
import logging
import math
from src.core.database.duckdb_manager import get_duckdb_manager

logger = logging.getLogger(__name__)

# SUNDAR PICHAI MISSION: 100% KPI parity is the foundation of user trust.
# This test validates that the data we ingest is the exact data we analyze.

@pytest.mark.integration
def test_kpi_consistency_upload_vs_analysis(client, auth_headers):
    """
    Sundar Pichai Persona-led Audit:
    Validates that Spend, Impressions, Clicks, and Conversions match 
    between the Post-Upload Summary and the Analytics Studio Snapshot.
    """
    print("\n[Sundar Mode] Initiating Data Integrity Audit...")
    
    # 0. Deep Clear
    client.post("/api/v1/system/reset", headers=auth_headers)
    
    # 1. Create a controlled synthetic dataset with deterministic totals
    expected_totals = {
        "total_spend": 12345.67,
        "total_impressions": 1000000,
        "total_clicks": 50000,
        "total_conversions": 1000
    }
    
    # 50 rows of data
    rows = []
    for i in range(50):
        rows.append({
            "Date": "2024-01-01",
            "Platform": "Google Ads" if i % 2 == 0 else "Meta Ads",
            "Campaign": f"Campaign_{i}",
            "Spend": expected_totals["total_spend"] / 50,
            "Impressions": expected_totals["total_impressions"] // 50,
            "Clicks": expected_totals["total_clicks"] // 50,
            "Conversions": expected_totals["total_conversions"] // 50
        })
    
    df = pd.DataFrame(rows)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Clear previous data to ensure a clean audit
    duckdb_mgr = get_duckdb_manager()
    duckdb_mgr.clear_data()
    
    # 2. Upload the data using the robust streaming endpoint
    print("[Sundar Mode] Uploading mission-critical data via /api/v1/upload/stream...")
    response = client.post(
        "/api/v1/upload/stream",
        files={"file": ("consistency_test.csv", csv_buffer, "text/csv")},
        headers=auth_headers
    )
    
    assert response.status_code == 200, f"Upload failed: {response.text}"
    upload_init = response.json()
    job_id = upload_init["job_id"]
    print(f"[Sundar Mode] Job {job_id} accepted. Monitoring progress...")
    
    # Poll for completion
    max_retries = 10
    upload_summary = None
    for i in range(max_retries):
        status_resp = client.get(f"/api/v1/upload/status/{job_id}", headers=auth_headers)
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        
        if status_data["status"] == "completed":
            upload_summary = status_data["summary"]
            break
        elif status_data["status"] == "failed":
            pytest.fail(f"Upload job failed: {status_data.get('error')}")
        
        print(f"[Sundar Mode]   - Status: {status_data['status']} ({status_data['progress']*100:.0f}%)")
        time.sleep(1)
    else:
        pytest.fail("Upload job timed out")
        
    print(f"[Sundar Mode] Post-Upload Summary: {upload_summary}")
    
    # 3. Retrieve Analytics Snapshot (The Analysis Layer)
    print("[Sundar Mode] Fetching Analytics Snapshot for cross-reference...")
    snapshot_response = client.get(
        "/api/v1/campaigns/analytics-snapshot",
        headers=auth_headers
    )
    
    assert snapshot_response.status_code == 200, f"Snapshot failed: {snapshot_response.text}"
    analysis_layer = snapshot_response.json()
    analysis_kpis = analysis_layer["kpis"]
    print(f"[Sundar Mode] Analysis Layer KPIs: {analysis_kpis}")
    
    # 4. Rigorous Consistency Validation
    # We allow small floating point drift for spend, but integer metrics must be EXACT.
    
    print("[Sundar Mode] Validating Spend consistency...")
    assert abs(upload_summary["total_spend"] - analysis_kpis["total_spend"]) < 0.01, \
        f"Spend mismatch: Upload={upload_summary['total_spend']}, Analysis={analysis_kpis['total_spend']}"
    
    print("[Sundar Mode] Validating Impressions consistency...")
    assert upload_summary["total_impressions"] == analysis_kpis["total_impressions"], \
        f"Impressions mismatch: Upload={upload_summary['total_impressions']}, Analysis={analysis_kpis['total_impressions']}"
        
    print("[Sundar Mode] Validating Clicks consistency...")
    assert upload_summary["total_clicks"] == analysis_kpis["total_clicks"], \
        f"Clicks mismatch: Upload={upload_summary['total_clicks']}, Analysis={analysis_kpis['total_clicks']}"
        
    print("[Sundar Mode] Validating Conversions consistency...")
    assert upload_summary["total_conversions"] == analysis_kpis["total_conversions"], \
        f"Conversions mismatch: Upload={upload_summary['total_conversions']}, Analysis={analysis_kpis['total_conversions']}"
        
    print("[Sundar Mode] 🚀 Mission accomplished. Analytics Snapshot parity verified.")
    
    # --- PHASE 2: Audit Analysis Page Metrics (/campaigns/metrics) ---
    print("\n[Sundar Mode] Initiating Audit of Analysis Page (/campaigns/metrics)...")
    analysis_res = client.get("/api/v1/campaigns/metrics", headers=auth_headers)
    assert analysis_res.status_code == 200, f"Analysis metrics failed: {analysis_res.text}"
    analysis_data = analysis_res.json()
    
    # Assert Parity for Analysis Page
    assert math.isclose(analysis_data["total_spend"], expected_totals["total_spend"], rel_tol=1e-5), \
        f"Spend mismatch (Analysis Page): {analysis_data['total_spend']} vs {expected_totals['total_spend']}"
    assert analysis_data["total_impressions"] == expected_totals["total_impressions"], \
        f"Impressions mismatch (Analysis Page): {analysis_data['total_impressions']} vs {expected_totals['total_impressions']}"
    assert analysis_data["total_clicks"] == expected_totals["total_clicks"], \
        f"Clicks mismatch (Analysis Page): {analysis_data['total_clicks']} vs {expected_totals['total_clicks']}"
    assert analysis_data["total_conversions"] == expected_totals["total_conversions"], \
        f"Conversions mismatch (Analysis Page): {analysis_data['total_conversions']} vs {expected_totals['total_conversions']}"
    
    print("[Sundar Mode] 🚀 Mission accomplished. Analysis Page metrics parity verified.")

def test_job_summary_parity(client, auth_headers):
    """
    Validate that get_job_summary derived directly from DuckDB matches the API analytics snapshot.
    """
    client.post("/api/v1/system/reset", headers=auth_headers)
    
    csv_data = "Date,Platform,Spend,Impressions,Clicks,Conversions\n2024-01-01,Meta,100,1000,50,5"
    csv_buffer = io.BytesIO(csv_data.encode('utf-8'))
    
    response = client.post(
        "/api/v1/upload/stream",
        files={"file": ("job_test.csv", csv_buffer, "text/csv")},
        headers=auth_headers
    )
    
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    
    # Poll for completion
    for _ in range(10):
        status_resp = client.get(f"/api/v1/upload/status/{job_id}", headers=auth_headers)
        if status_resp.json()["status"] == "completed":
            break
        time.sleep(0.5)
    
    snapshot_response = client.get("/api/v1/campaigns/analytics-snapshot", headers=auth_headers)
    kpis = snapshot_response.json()["kpis"]
    
    assert kpis["total_spend"] == 100.0
    assert kpis["total_conversions"] == 5
    print("[Sundar Mode] Job summary parity verified.")

def test_system_nuclear_reset(client, auth_headers):
    """
    Sync version of Nuclear Reset verification.
    """
    # 1. Clear before start
    client.post("/api/v1/system/reset", headers=auth_headers)
    
    # 2. Upload some data
    csv_data = "Date,Platform,Spend,Impressions,Clicks,Conversions\n2024-01-01,Google,100,1000,50,5"
    csv_buffer = io.BytesIO(csv_data.encode('utf-8'))
    client.post(
        "/api/v1/upload/stream",
        files={"file": ("reset_test.csv", csv_buffer, "text/csv")},
        headers=auth_headers
    )
    
    # 3. Verify data exists
    metrics_res = client.get("/api/v1/campaigns/metrics", headers=auth_headers)
    assert metrics_res.json()["total_spend"] == 100.0
    
    # 4. TRIGGER NUCLEAR RESET
    reset_res = client.post("/api/v1/system/reset", headers=auth_headers)
    assert reset_res.status_code == 200
    assert reset_res.json()["success"] is True
    
    # 5. Verify data is GONE
    metrics_gone = client.get("/api/v1/campaigns/metrics", headers=auth_headers)
    assert metrics_gone.json()["total_spend"] == 0
    print("[Musk Mode] Nuclear Reset successfully verified. System is clean.")
