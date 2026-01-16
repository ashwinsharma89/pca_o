"""
Twitter/X Ads API Connector.

Supports Twitter Ads API with OAuth 1.0a authentication.
Includes mock mode for testing without real accounts.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
import os

from loguru import logger

from src.platform.connectors.base_connector import (
    BaseAdConnector,
    Campaign,
    ConnectionResult,
    ConnectorStatus,
    PerformanceMetrics,
)


TWITTER_CAMPAIGNS = [
    {
        "id": "tw_camp_001",
        "name": "Promoted Tweets - Brand Awareness",
        "status": "ACTIVE",
        "budget": 20000.00,
        "spend": 17450.75,
        "impressions": 4500000,
        "clicks": 135000,
        "conversions": 4500.0,
        "objective": "AWARENESS",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "tw_camp_002",
        "name": "Video Views - Product Launch",
        "status": "ACTIVE",
        "budget": 15000.00,
        "spend": 12875.50,
        "impressions": 3200000,
        "clicks": 96000,
        "conversions": 2800.0,
        "objective": "VIDEO_VIEWS",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "tw_camp_003",
        "name": "Website Clicks - Lead Gen",
        "status": "ACTIVE",
        "budget": 25000.00,
        "spend": 21234.25,
        "impressions": 5800000,
        "clicks": 232000,
        "conversions": 8500.0,
        "objective": "WEBSITE_CLICKS",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "tw_camp_004",
        "name": "Followers - Community Growth",
        "status": "PAUSED",
        "budget": 8000.00,
        "spend": 7650.00,
        "impressions": 2100000,
        "clicks": 42000,
        "conversions": 15000.0,
        "objective": "FOLLOWERS",
        "start_date": "2024-08-01",
        "end_date": "2024-10-31",
    },
]

TWITTER_ACCOUNT = {
    "id": "tw_acc_456789",
    "name": "Demo Twitter Ads Account",
    "currency": "USD",
    "timezone": "America/Chicago",
    "status": "ACTIVE",
}


class TwitterAdsConnector(BaseAdConnector):
    """
    Twitter/X Ads API connector.
    
    Credentials needed:
    - TWITTER_CONSUMER_KEY: API Key
    - TWITTER_CONSUMER_SECRET: API Secret
    - TWITTER_ACCESS_TOKEN: Access Token
    - TWITTER_ACCESS_SECRET: Access Token Secret
    - TWITTER_AD_ACCOUNT_ID: Ad Account ID
    """
    
    PLATFORM_NAME = "twitter_ads"
    API_VERSION = "12"
    
    def __init__(self, use_mock: bool = None, **credentials):
        creds = {
            "consumer_key": credentials.get("consumer_key") or os.getenv("TWITTER_CONSUMER_KEY"),
            "consumer_secret": credentials.get("consumer_secret") or os.getenv("TWITTER_CONSUMER_SECRET"),
            "access_token": credentials.get("access_token") or os.getenv("TWITTER_ACCESS_TOKEN"),
            "access_secret": credentials.get("access_secret") or os.getenv("TWITTER_ACCESS_SECRET"),
            "ad_account_id": credentials.get("ad_account_id") or os.getenv("TWITTER_AD_ACCOUNT_ID"),
        }
        super().__init__(use_mock=use_mock, **creds)
    
    def _validate_credentials(self) -> bool:
        required = ["consumer_key", "consumer_secret", "access_token", "access_secret"]
        return all(self.credentials.get(k) for k in required)
    
    def _connect_real(self) -> ConnectionResult:
        try:
            # Would use twitter-ads library
            return ConnectionResult(
                success=False, status=ConnectorStatus.ERROR,
                message="Real API not implemented", platform=self.PLATFORM_NAME,
            )
        except Exception as e:
            return ConnectionResult(
                success=False, status=ConnectorStatus.ERROR,
                message="Connection failed", platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(self, start_date=None, end_date=None) -> List[Campaign]:
        return []
    
    def _get_performance_real(self, start_date, end_date, campaign_ids=None) -> PerformanceMetrics:
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id="", date_range={},
            spend=0, impressions=0, clicks=0, conversions=0, revenue=0
        )
    
    def _get_mock_campaigns(self) -> List[Campaign]:
        return [
            Campaign(
                id=c["id"], name=c["name"], status=c["status"],
                platform=self.PLATFORM_NAME, account_id=TWITTER_ACCOUNT["id"],
                budget=c["budget"], spend=c["spend"], impressions=c["impressions"],
                clicks=c["clicks"], conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
            )
            for c in TWITTER_CAMPAIGNS
        ]
    
    def _get_mock_performance(self, start_date, end_date) -> PerformanceMetrics:
        total = {k: sum(c[k] for c in TWITTER_CAMPAIGNS) for k in ["spend", "impressions", "clicks", "conversions"]}
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id=TWITTER_ACCOUNT["id"],
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=total["spend"], impressions=total["impressions"],
            clicks=total["clicks"], conversions=total["conversions"],
            revenue=total["conversions"] * 25.0,
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        return [TWITTER_ACCOUNT] if self.use_mock else []
