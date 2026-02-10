import unittest
from unittest.mock import MagicMock, patch
import sys
from datetime import datetime

# Pre-mocking programmatic SDKs
sys.modules["google.ads.googleads.client"] = MagicMock()
sys.modules["google.auth"] = MagicMock()

from src.platform.connectors.google_ads_connector import GoogleAdsConnector
from src.platform.connectors.dv360_connector import DV360Connector
from src.platform.connectors.amazon_dsp_connector import AmazonDSPConnector
from src.platform.connectors.base_connector import ConnectorStatus

class TestProgrammaticConnectorsCoverage(unittest.TestCase):
    def test_google_ads_mock_mode(self):
        connector = GoogleAdsConnector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.platform, "google_ads")
        
        campaigns = connector.get_campaigns()
        self.assertTrue(len(campaigns) > 0)
        self.assertEqual(campaigns[0].platform, "google_ads")

    def test_dv360_mock_mode(self):
        connector = DV360Connector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.platform, "dv360")
        
        campaigns = connector.get_campaigns()
        self.assertTrue(len(campaigns) > 0)
        self.assertEqual(campaigns[0].platform, "dv360")

    def test_amazon_dsp_mock_mode(self):
        connector = AmazonDSPConnector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.platform, "amazon_dsp")
        
        campaigns = connector.get_campaigns()
        self.assertTrue(len(campaigns) > 0)
        self.assertEqual(campaigns[0].platform, "amazon_dsp")

if __name__ == "__main__":
    unittest.main()
