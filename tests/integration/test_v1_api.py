import pytest
from fastapi.testclient import TestClient
from src.interface.api.main_v3 import app
from src.interface.api.middleware.auth import create_access_token
from datetime import timedelta

class TestV1ApiIntegration:
    @pytest.fixture(autouse=True)
    def setup_db(self):
        from src.core.database.connection import get_db_manager
        from src.core.database.models import Base
        from src.core.database.user_models import User
        
        db_manager = get_db_manager()
        # For testing, we can drop and recreate to ensure schema is fresh
        Base.metadata.drop_all(bind=db_manager.engine)
        Base.metadata.create_all(bind=db_manager.engine)
        
        # Create a test user for auth logic
        from src.engine.services.user_service import UserService
        with db_manager.get_session() as session:
            service = UserService(session)
            service.create_user("testuser", "test@example.com", "Password123!")
        
        yield

    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def auth_headers(self):
        token = create_access_token(
            data={"sub": "testuser", "role": "admin", "tier": "enterprise"},
            expires_delta=timedelta(hours=1)
        )
        return {"Authorization": f"Bearer {token}"}

    def test_root(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert "PCA Agent API v3.0" in response.json()["message"]

    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_protected_endpoint_unauthorized(self, client):
        # Campaigns list requires auth
        response = client.get("/api/v1/campaigns")
        assert response.status_code == 401

    def test_cors_headers(self, client):
        response = client.options("/health", headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET"
        })
        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers

    def test_security_headers(self, client):
        response = client.get("/health")
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["X-Frame-Options"] == "DENY"
        assert "Content-Security-Policy" in response.headers

    def test_mfa_setup_flow(self, client, auth_headers):
        # Requires auth
        response = client.post("/api/v1/auth/mfa/setup", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert "secret" in data
        assert "qr_code" in data
