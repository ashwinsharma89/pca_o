"""
Regression Test Suite

Tests critical functionality to ensure new changes don't break existing features.
Run these tests before deploying any changes.

Usage:
    pytest tests/regression/ -v
    pytest tests/regression/test_core_functionality.py::test_login_works -v
"""

import pytest
from fastapi.testclient import TestClient
from src.interface.api.main import app
from tests.routes import APIV1

client = TestClient(app)


class TestAuthentication:
    """Test that authentication still works after changes."""

    def test_login_with_valid_credentials(self):
        """Ensure login endpoint works with valid credentials."""
        # Ensure user exists first
        client.post(
            APIV1.REGISTER,
            json={"username": "demo", "email": "demo@example.com", "password": "Demo123!"}
        )
        
        response = client.post(
            APIV1.LOGIN,
            json={"username": "demo", "password": "Demo123!"}
        )
        assert response.status_code == 200, f"Login failed: {response.json()}"
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["username"] == "demo"

    def test_login_with_invalid_credentials(self):
        """Ensure login fails with invalid credentials."""
        response = client.post(
            APIV1.LOGIN,
            json={"username": "demo", "password": "wrongpassword"}
        )
        assert response.status_code == 401

    def test_protected_endpoint_without_auth(self):
        """Ensure protected endpoints require authentication."""
        response = client.get(APIV1.CAMPAIGNS)
        assert response.status_code == 401


class TestDashboardData:
    """Test that dashboard data endpoints work correctly."""

    @pytest.fixture
    def auth_headers(self):
        """Get authentication headers for protected endpoints."""
        response = client.post(
            APIV1.LOGIN,
            json={"username": "demo", "password": "Demo123!"}
        )
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def test_dashboard_stats_endpoint(self, auth_headers):
        """Ensure dashboard stats endpoint returns data."""
        response = client.get(
            APIV1.DASHBOARD_STATS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "summary_groups" in data or "monthly_performance" in data

    def test_visualizations_endpoint(self, auth_headers):
        """Ensure visualizations endpoint returns data."""
        response = client.get(
            APIV1.VISUALIZATIONS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        # Should have at least one of these keys
        assert any(key in data for key in ["trend", "platform", "channel", "device"])

    def test_filter_options_endpoint(self, auth_headers):
        """Ensure filter options endpoint works."""
        response = client.get(
            APIV1.FILTERS,
            headers=auth_headers
        )
        assert response.status_code == 200


class TestMonthFiltering:
    """Test the new month filtering feature."""

    @pytest.fixture
    def auth_headers(self):
        """Get authentication headers."""
        response = client.post(
            APIV1.LOGIN,
            json={"username": "demo", "password": "Demo123!"}
        )
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def test_monthly_performance_has_month_field(self, auth_headers):
        """Ensure monthly performance data includes month field."""
        response = client.get(
            APIV1.DASHBOARD_STATS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        
        if data.get("monthly_performance"):
            for month_data in data["monthly_performance"]:
                assert "month" in month_data, "Monthly performance missing 'month' field"

    def test_platform_performance_has_month_field(self, auth_headers):
        """Ensure platform performance can be filtered by month."""
        response = client.get(
            APIV1.DASHBOARD_STATS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        
        if data.get("platform_performance"):
            # Platform performance should have month field for filtering
            for platform_data in data["platform_performance"]:
                assert "month" in platform_data, "Platform performance missing 'month' field"


class TestDataIntegrity:
    """Test that data integrity is maintained."""

    @pytest.fixture
    def auth_headers(self):
        """Get authentication headers."""
        response = client.post(
            APIV1.LOGIN,
            json={"username": "demo", "password": "Demo123!"}
        )
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def test_dashboard_stats_structure(self, auth_headers):
        """Ensure dashboard stats maintains expected structure."""
        response = client.get(
            APIV1.DASHBOARD_STATS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        
        # Check expected keys exist
        expected_keys = ["monthly_performance", "platform_performance"]
        for key in expected_keys:
            assert key in data, f"Missing expected key: {key}"
        
        # Check data types
        assert isinstance(data["monthly_performance"], list)
        assert isinstance(data["platform_performance"], list)

    def test_visualizations_data_types(self, auth_headers):
        """Ensure visualizations data has correct types."""
        response = client.get(
            APIV1.VISUALIZATIONS,
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        
        # All values should be lists
        for key, value in data.items():
            assert isinstance(value, list), f"{key} should be a list, got {type(value)}"


class TestHealthChecks:
    """Test system health endpoints."""

    def test_health_endpoint(self):
        """Ensure health check endpoint works."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_api_docs_accessible(self):
        """Ensure API documentation is accessible."""
        response = client.get("/api/docs")
        assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
