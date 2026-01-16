import pytest
from fastapi import Request
from src.interface.api.middleware import rate_limit
from src.interface.api.middleware.rate_limit import get_user_rate_limit

class TestRateLimitTiers:
    @pytest.fixture(autouse=True)
    def enable_rate_limiting(self, monkeypatch):
        monkeypatch.setattr(rate_limit, "RATE_LIMIT_ENABLED", True)
    def test_get_user_rate_limit_free(self, client):
        """Verify free user gets 10/minute."""
        # Create a mock request with the user state
        mock_request = type('obj', (object,), {
            'state': type('obj', (object,), {'user': {'tier': 'free'}})
        })
        # We need a real Request object or something that behaves like it
        # Actually, we can just test the function directly
        from fastapi import Request
        # The function signature is (request: Request)
        # We'll just pass an object that matches
        class MockRequest:
            def __init__(self, user):
                self.state = type('obj', (object,), {'user': user})
        
        limit = get_user_rate_limit(MockRequest({'tier': 'free'}))
        assert limit == "10/minute"

    def test_get_user_rate_limit_pro(self, client):
        """Verify pro user gets 100/minute."""
        class MockRequest:
            def __init__(self, user):
                self.state = type('obj', (object,), {'user': user})
        
        limit = get_user_rate_limit(MockRequest({'tier': 'pro'}))
        assert limit == "100/minute"

    def test_get_user_rate_limit_enterprise(self, client):
        """Verify enterprise user gets 1000/minute."""
        class MockRequest:
            def __init__(self, user):
                self.state = type('obj', (object,), {'user': user})
        
        limit = get_user_rate_limit(MockRequest({'tier': 'enterprise'}))
        assert limit == "1000/minute"

    def test_get_user_rate_limit_anonymous(self, client):
        """Verify anonymous user gets default (10/minute)."""
        class MockRequest:
            def __init__(self, user):
                self.state = type('obj', (object,), {'user': user})
        
        limit = get_user_rate_limit(MockRequest(None))
        assert limit == "10/minute"

    def test_rate_limit_middleware_populates_state(self, client, pro_auth_headers):
        """Verify that populate_user_state_middleware extracts the tier correctly."""
        # We can't easily check request.state from outside, 
        # but we can call an endpoint and verify no 401.
        response = client.get("/api/v1/auth/me", headers=pro_auth_headers)
        assert response.status_code == 200
        assert response.json()["tier"] == "pro"
