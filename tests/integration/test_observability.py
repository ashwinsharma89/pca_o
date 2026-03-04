
import pytest
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.interface.api.main_v3 import app
from src.core.utils.observability import MetricsCollector
from src.core.utils.opentelemetry_config import setup_opentelemetry

client = TestClient(app)

def test_observability_integration():
    """Verify that regression pipeline triggers metrics and spans."""
    # 1. Setup OPENTELEMETRY_ENABLED=true for this test
    os.environ["OPENTELEMETRY_ENABLED"] = "true"
    
    # 2. Get auth token
    response = client.post("/api/v1/auth/login", json={
        "username": "test@example.com",
        "password": "Password123!"
    })
    # If login fails, register first
    if response.status_code != 200:
        client.post("/api/v1/auth/register", json={
            "username": "test@example.com",
            "email": "test@example.com",
            "password": "Password123!"
        })
        response = client.post("/api/v1/auth/login", json={
            "username": "test@example.com",
            "password": "Password123!"
        })
    
    token = response.json().get("access_token")
    if not token:
        # Check if mfa required
        if response.json().get("mfa_required"):
             token = response.json().get("mfa_session")
             
    headers = {
        "Authorization": f"Bearer {token}",
        "X-CSRF-Token": "test-token"
    }
    
    # 3. Upload synthetic data
    df = pd.DataFrame({
        'Date': pd.date_range(start='2023-01-01', periods=100),
        'Platform': ['Meta', 'Google'] * 50,
        'Spend': np.random.uniform(100, 1000, 100),
        'Impressions': np.random.uniform(1000, 10000, 100),
        'Clicks': np.random.uniform(10, 100, 100),
        'Conversions': np.random.uniform(1, 10, 100)
    })
    df.to_csv("test_obs_data.csv", index=False)
    
    with open("test_obs_data.csv", "rb") as f:
        client.post("/api/v1/upload/stream", files={"file": f}, headers=headers)
    
    # 4. Trigger Regression V2
    response = client.get(
        "/api/v1/campaigns/regression/v2",
        params={
            "target": "Conversions",
            "features": "Spend,Impressions,Clicks"
        },
        headers=headers
    )
    
    assert response.status_code == 200
    
    # 5. Verify Metrics were recorded
    # We check the metrics collector internal state (if accessible) or just verify the call didn't crash
    # In a real Prometheus setup, we'd check /metrics
    metrics_response = client.get("/metrics")
    assert metrics_response.status_code == 200
    content = metrics_response.text
    
    assert "regression_pipeline_duration_ms" in content
    assert "regression_best_model_r2" in content
    assert "regression_runs_total" in content
    assert "api_regression_v2_latency_ms" in content
    assert "marketing_recommendation_total" in content
    
    print("Observability verification successful!")

if __name__ == "__main__":
    test_observability_integration()
