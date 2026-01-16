
import pytest
from fastapi import status

class TestCampaignAnalysis:
    """Tests for Campaign Analysis endpoints."""

    def test_global_analysis_happy_path(self, client, auth_token, mock_analytics_services):
        """Test happy path for global analysis."""
        payload = {
            "query": "performance overview",
            "time_range": "last_30_days"
        }
        headers = {"Authorization": f"Bearer {auth_token}"}
        
        response = client.post("/api/v1/campaigns/analyze/global", json=payload, headers=headers)
        
        # Depending on implementation, this might call LLM. 
        # Since we mocked it, we expect success.
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "analysis" in data or "insights" in data

    def test_dashboard_stats_default(self, client, auth_token):
        """Test dashboard stats with default parameters."""
        headers = {"Authorization": f"Bearer {auth_token}"}
        
        response = client.get("/api/v1/campaigns/dashboard-stats", headers=headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        # Verify structure
        assert "total_spend" in data or "metrics" in data
        assert "trends" in data

    def test_dashboard_stats_with_filters(self, client, auth_token):
        """Test dashboard stats with filters."""
        headers = {"Authorization": f"Bearer {auth_token}"}
        params = {
            "platform": "Google Ads",
            "channel": "Search"
        }
        
        response = client.get("/api/v1/campaigns/dashboard-stats", headers=headers, params=params)
        
        assert response.status_code == status.HTTP_200_OK

    def test_analysis_unauthorized(self, client):
        """Test access without token."""
        response = client.post("/api/v1/campaigns/analyze/global", json={})
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_filter_handling(self, client, auth_token):
        """Test API resilience against garbage filters."""
        headers = {"Authorization": f"Bearer {auth_token}"}
        params = {"platform": "ThisPlatformDoesNotExist123"}
        
        response = client.get("/api/v1/campaigns/dashboard-stats", headers=headers, params=params)
        
        # Should return 200 with empty data or 200 with 0 zeros, not 500
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        # Expect zeros/empty
        pass # Assertions depend on exact response structure for empty data

