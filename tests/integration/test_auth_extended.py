import pytest
from src.interface.api.middleware.auth import create_access_token
from datetime import timedelta

class TestAuthApiExtended:
    def test_login_success(self, client, test_user):
        response = client.post(
            "/api/v1/auth/login",
            json={"username": test_user.username, "password": "Password123!"}
        )
        assert response.status_code == 200
        assert "access_token" in response.json()
        assert response.json()["user"]["tier"] == "free"

    def test_login_invalid_credentials(self, client, test_user):
        response = client.post(
            "/api/v1/auth/login",
            json={"username": test_user.username, "password": "WrongPassword"}
        )
        assert response.status_code == 401

    @pytest.mark.parametrize("tier", ["free", "pro", "enterprise"])
    def test_register_tiers(self, client, tier):
        username = f"user_{tier}_{uuid.uuid4().hex[:6]}"
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": username,
                "email": f"{username}@example.com",
                "password": "Password123!"
            }
        )
        assert response.status_code == 200
        # In a real system, the tier might be set via a payment flow, 
        # but here we test if the service handles it.
        # Note: current register endpoint defaults to "free".
        assert response.json()["user"]["tier"] == "free" 

    def test_get_me_flow(self, client, auth_headers, test_user):
        response = client.get("/api/v1/auth/me", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["username"] == test_user.username

    def test_mfa_setup_requires_auth(self, client):
        response = client.post("/api/v1/auth/mfa/setup")
        assert response.status_code == 401

    def test_mfa_setup_success(self, client, auth_headers):
        response = client.post("/api/v1/auth/mfa/setup", headers=auth_headers)
        assert response.status_code == 200
        assert "secret" in response.json()
        assert "qr_code" in response.json()

import uuid
