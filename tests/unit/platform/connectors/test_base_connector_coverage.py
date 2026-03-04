import unittest
from datetime import datetime, timedelta
from src.platform.connectors.base_connector import BaseAdConnector, Campaign, PerformanceMetrics, ConnectorStatus

class MockConnector(BaseAdConnector):
    PLATFORM_NAME = "test_platform"
    
    def _validate_credentials(self) -> bool:
        return True
    
    def _connect_real(self):
        return None
    
    def _get_campaigns_real(self, start_date=None, end_date=None):
        return []
    
    def _get_performance_real(self, start_date, end_date, campaign_ids=None):
        return None
    
    def _get_mock_campaigns(self):
        return [Campaign(
            id="c1", name="Test", status="ACTIVE", platform="test",
            account_id="a1", budget=1000, spend=500, impressions=10000,
            clicks=200, conversions=5
        )]
    
    def _get_mock_performance(self, start_date, end_date):
        return PerformanceMetrics(
            platform="test", account_id="a1", 
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=500, impressions=10000, clicks=200, conversions=5, revenue=1200
        )

class TestBaseConnectorCoverage(unittest.TestCase):
    def setUp(self):
        self.connector = MockConnector(use_mock=True)

    def test_campaign_calculations(self):
        campaign = Campaign(
            id="c1", name="Test", status="ACTIVE", platform="meta",
            account_id="a1", budget=1000, spend=500, impressions=10000,
            clicks=200, conversions=5
        )
        self.assertEqual(campaign.ctr, 2.0)
        self.assertEqual(campaign.cpc, 2.5)
        self.assertEqual(campaign.cpa, 100.0)
        
        dict_rep = campaign.to_dict()
        self.assertEqual(dict_rep["ctr"], 2.0)

    def test_performance_metrics_calculations(self):
        metrics = PerformanceMetrics(
            platform="meta", account_id="a1", 
            date_range={"start": "2023-01-01", "end": "2023-01-31"},
            spend=500, impressions=10000, clicks=200, conversions=5, revenue=1500
        )
        self.assertEqual(metrics.roas, 3.0)
        self.assertEqual(metrics.ctr, 2.0)
        
        dict_rep = metrics.to_dict()
        self.assertEqual(dict_rep["roas"], 3.0)

    def test_mock_mode_behavior(self):
        conn_res = self.connector.test_connection()
        self.assertTrue(conn_res.success)
        self.assertEqual(conn_res.status, ConnectorStatus.MOCK)
        
        campaigns = self.connector.get_campaigns()
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0].id, "c1")
        
        metrics = self.connector.get_performance()
        self.assertEqual(metrics.spend, 500)
        self.assertEqual(metrics.roas, 2.4) # 1200 / 500

    def test_connection_state_management(self):
        self.connector.disconnect()
        self.assertFalse(self.connector._connected)
        # Even if disconnected, if use_mock is True, is_connected returns True
        self.assertTrue(self.connector.is_connected)

if __name__ == "__main__":
    unittest.main()
