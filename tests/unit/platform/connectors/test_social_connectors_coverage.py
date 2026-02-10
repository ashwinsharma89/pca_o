import unittest
from unittest.mock import MagicMock, patch
import sys
from datetime import datetime

# Mock the SDKs before importing connectors that might use them at top-level
mock_fb = MagicMock()
sys.modules["facebook_business"] = mock_fb
sys.modules["facebook_business.api"] = MagicMock()
sys.modules["facebook_business.adobjects"] = MagicMock()
sys.modules["facebook_business.adobjects.adaccount"] = MagicMock()
sys.modules["facebook_business.adobjects.campaign"] = MagicMock()

from src.platform.connectors.meta_ads_connector import MetaAdsConnector
from src.platform.connectors.linkedin_ads_connector import LinkedInAdsConnector
from src.platform.connectors.tiktok_ads_connector import TikTokAdsConnector
from src.platform.connectors.base_connector import ConnectorStatus

class TestSocialConnectorsCoverage(unittest.TestCase):
    def test_meta_ads_mock_mode(self):
        connector = MetaAdsConnector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.status, ConnectorStatus.MOCK)
        
        campaigns = connector.get_campaigns()
        self.assertTrue(len(campaigns) > 0)
        self.assertEqual(campaigns[0].platform, "meta_ads")

    @patch("src.platform.connectors.meta_ads_connector.os.getenv")
    def test_meta_ads_real_connection_logic(self, mock_getenv):
        # Setup mocks for the internal Meta SDK calls
        with patch("facebook_business.api.FacebookAdsApi.init") as mock_init, \
             patch("facebook_business.adobjects.adaccount.AdAccount") as mock_account_cls:
            
            mock_account = MagicMock()
            mock_account_cls.return_value = mock_account
            mock_account.api_get.return_value = {
                "name": "Real Account", "account_id": "12345", "currency": "USD"
            }
            
            connector = MetaAdsConnector(use_mock=False, access_token="test", ad_account_id="act_123")
            res = connector.test_connection()
            
            self.assertTrue(res.success)
            self.assertEqual(res.account_name, "Real Account")
            mock_init.assert_called_once()

    def test_linkedin_ads_mock_mode(self):
        connector = LinkedInAdsConnector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.platform, "linkedin_ads")

    def test_tiktok_ads_mock_mode(self):
        connector = TikTokAdsConnector(use_mock=True)
        res = connector.test_connection()
        self.assertEqual(res.platform, "tiktok_ads")

if __name__ == "__main__":
    unittest.main()
