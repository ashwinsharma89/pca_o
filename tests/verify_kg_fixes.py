import os
# Set mock env vars before imports
os.environ["JWT_SECRET_KEY"] = "mock_secret_key_for_testing_12345"
os.environ["DATABASE_TYPE"] = "sqlite"

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

# Create a sample dataframe with recent dates
today = datetime.now()
data = {
    "date": pd.date_range(end=today, periods=100),
    "platform": (["Meta", "Google"] * 50), # Interleave
    "channel": (["Social", "Search"] * 50),
    "campaign_name": [f"Campaign {i}" for i in range(100)],
    "spend": [10.0] * 100,
    "impressions": [1000] * 100,
    "clicks": [50] * 100,
    "conversions": [5] * 100,
    "revenue": [50.0] * 100,
    "device": ["Mobile"] * 100
}
df_sample = pd.DataFrame(data)
# Ensure columns exist
df_sample["platform"] = df_sample["platform"]
df_sample["campaign_name"] = df_sample["campaign_name"]

def mock_get_current_user():
    return {"username": "testuser"}

@pytest.fixture
def client():
    from src.interface.api.main import app
    from src.interface.api.middleware.auth import get_current_user
    
    app.dependency_overrides[get_current_user] = mock_get_current_user
    yield TestClient(app)
    app.dependency_overrides.clear()

def test_kg_query_meta_last_2_months(client):
    with patch("src.interface.api.v1.routers.kg_summary.get_duckdb_manager") as mock_mgr:
        mock_instance = MagicMock()
        mock_instance.has_data.return_value = True
        mock_instance.get_campaigns.return_value = df_sample
        mock_mgr.return_value = mock_instance
        
        response = client.post(
            "/api/v1/kg/query",
            json={"query": "Show me Meta ads performance for the last 2 months"},
            headers={"Authorization": "Bearer mock_token"}
        )
        
        assert response.status_code == 200
        res_data = response.json()
        assert res_data["success"] is True
        assert res_data["metadata"]["intent"] == "platform"
        # Since it's Meta, and there are roughly 30 Meta rows in the last 60 days (interleaved)
        # Expected: ~30 * 10 = 300.0, but definitely > 0 and < 500
        assert res_data["summary"]["total_spend"] > 0
        assert res_data["summary"]["total_spend"] < 500.0

def test_kg_query_top_5_ranking(client):
    with patch("src.interface.api.v1.routers.kg_summary.get_duckdb_manager") as mock_mgr:
        mock_instance = MagicMock()
        mock_instance.has_data.return_value = True
        mock_instance.get_campaigns.return_value = df_sample
        mock_mgr.return_value = mock_instance
        
        response = client.post(
            "/api/v1/kg/query",
            json={"query": "Top 5 campaigns by spend", "limit": 5},
            headers={"Authorization": "Bearer mock_token"}
        )
        
        assert response.status_code == 200
        res_data = response.json()
        assert res_data["success"] is True
        assert res_data["metadata"]["intent"] == "ranking"
        assert len(res_data["data"]) == 5
        # CRITICAL FIX: Summary should be for ALL 100 campaigns (100 * 10 = 1000), not just Top 5 (5 * 10 = 50)
        assert res_data["summary"]["total_spend"] == 1000.0

if __name__ == "__main__":
    # Manual run if needed
    import sys
    pytest.main([__file__])
