import unittest
import os
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.interface.api.main import app
import pandas as pd

class TestV1RoutesCoverage(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        # Mocking the JWT secret for the app
        os.environ["JWT_SECRET_KEY"] = "test_secret_for_unit_tests_only_32_chars_long_123"

    @patch("src.interface.api.v1.health_check.get_campaign_data")
    @patch("src.engine.agents.agent_chain.campaign_health_check")
    def test_run_health_check(self, mock_workflow, mock_get_data):
        # Mock data
        mock_get_data.return_value = pd.DataFrame([{"Campaign": "Test", "Spend": 100}])
        mock_workflow.return_value = {
            "success": True,
            "workflow": "campaign_health_check",
            "started_at": "2024-01-01T00:00:00",
            "steps": [{"step": "Init", "status": "completed"}]
        }
        
        response = self.client.post("/api/v1/analyze/health-check", json={"question": "test?"})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["workflow"], "campaign_health_check")

    def test_workflow_status(self):
        with patch("src.engine.agents.agent_chain.get_workflow_status") as mock_status:
            mock_status.return_value = {
                "context": {}, "insights": [], "recommendations": [], "anomalies": [], "recent_queries": []
            }
            response = self.client.get("/api/v1/analyze/workflow-status")
            self.assertEqual(response.status_code, 200)

    @patch("src.engine.services.user_service.UserService.create_user")
    def test_user_registration(self, mock_create):
        mock_user = MagicMock()
        mock_user.to_dict.return_value = {
            "id": 1, "username": "testuser", "email": "test@test.com", 
            "role": "user", "tier": "free", "is_active": True, 
            "is_verified": False, "must_change_password": False,
            "created_at": "2024-01-01", "last_login": None
        }
        mock_create.return_value = mock_user
        
        user_payload = {
            "username": "testuser",
            "email": "test@test.com",
            "password": "securepassword123",
            "role": "user",
            "tier": "free"
        }
        response = self.client.post("/api/v1/users/register", json=user_payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["username"], "testuser")

if __name__ == "__main__":
    import os
    unittest.main()
