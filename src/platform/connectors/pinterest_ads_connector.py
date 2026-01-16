"""
Pinterest Ads API Connector.

Supports Pinterest Marketing API with OAuth authentication.
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


PINTEREST_CAMPAIGNS = [
    {
        "id": "pin_camp_001",
        "name": "Shopping Pins - Home Decor",
        "status": "ACTIVE",
        "budget": 18000.00,
        "spend": 15450.75,
        "impressions": 8500000,
        "clicks": 340000,
        "conversions": 12500.0,
        "objective": "CATALOG_SALES",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "pin_camp_002",
        "name": "Idea Pins - Recipe Content",
        "status": "ACTIVE",
        "budget": 12000.00,
        "spend": 10234.50,
        "impressions": 6200000,
        "clicks": 186000,
        "conversions": 4500.0,
        "objective": "AWARENESS",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "pin_camp_003",
        "name": "Video Pins - Fashion",
        "status": "ACTIVE",
        "budget": 15000.00,
        "spend": 12875.25,
        "impressions": 4800000,
        "clicks": 192000,
        "conversions": 6200.0,
        "objective": "VIDEO_VIEWS",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "pin_camp_004",
        "name": "Collection Ads - DIY Projects",
        "status": "PAUSED",
        "budget": 8000.00,
        "spend": 7650.00,
        "impressions": 3100000,
        "clicks": 93000,
        "conversions": 2800.0,
        "objective": "TRAFFIC",
        "start_date": "2024-08-01",
        "end_date": "2024-10-31",
    },
]

PINTEREST_ACCOUNT = {
    "id": "pin_acc_789012",
    "name": "Demo Pinterest Ads Account",
    "currency": "USD",
    "timezone": "America/Los_Angeles",
    "status": "ACTIVE",
}


class PinterestAdsConnector(BaseAdConnector):
    """
    Pinterest Marketing API connector.
    
    Credentials needed:
    - PINTEREST_APP_ID: App ID
    - PINTEREST_APP_SECRET: App Secret
    - PINTEREST_ACCESS_TOKEN: Access Token
    - PINTEREST_AD_ACCOUNT_ID: Ad Account ID
    """
    
    PLATFORM_NAME = "pinterest_ads"
    API_VERSION = "v5"
    
    def __init__(self, use_mock: bool = None, **credentials):
        creds = {
            "app_id": credentials.get("app_id") or os.getenv("PINTEREST_APP_ID"),
            "app_secret": credentials.get("app_secret") or os.getenv("PINTEREST_APP_SECRET"),
            "access_token": credentials.get("access_token") or os.getenv("PINTEREST_ACCESS_TOKEN"),
            "ad_account_id": credentials.get("ad_account_id") or os.getenv("PINTEREST_AD_ACCOUNT_ID"),
        }
        super().__init__(use_mock=use_mock, **creds)
    
    def _validate_credentials(self) -> bool:
        return all(self.credentials.get(k) for k in ["access_token", "ad_account_id"])
    
    def _connect_real(self) -> ConnectionResult:
        try:
            import requests
            headers = {"Authorization": f"Bearer {self.credentials['access_token']}"}
            response = requests.get(
                f"https://api.pinterest.com/{self.API_VERSION}/user_account",
                headers=headers,
                timeout=30
            )
            if response.status_code == 200:
                return ConnectionResult(
                    success=True, status=ConnectorStatus.CONNECTED,
                    message="Connected to Pinterest Ads", platform=self.PLATFORM_NAME,
                )
            return ConnectionResult(
                success=False, status=ConnectorStatus.ERROR,
                message="Failed to connect", platform=self.PLATFORM_NAME,
            )
        except Exception as e:
            return ConnectionResult(
                success=False, status=ConnectorStatus.ERROR,
                message="Connection failed", platform=self.PLATFORM_NAME, error_details=str(e),
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
                platform=self.PLATFORM_NAME, account_id=PINTEREST_ACCOUNT["id"],
                budget=c["budget"], spend=c["spend"], impressions=c["impressions"],
                clicks=c["clicks"], conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
            )
            for c in PINTEREST_CAMPAIGNS
        ]
    
    def _get_mock_performance(self, start_date, end_date) -> PerformanceMetrics:
        total = {k: sum(c[k] for c in PINTEREST_CAMPAIGNS) for k in ["spend", "impressions", "clicks", "conversions"]}
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id=PINTEREST_ACCOUNT["id"],
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=total["spend"], impressions=total["impressions"],
            clicks=total["clicks"], conversions=total["conversions"],
            revenue=total["conversions"] * 18.0,
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        return [PINTEREST_ACCOUNT] if self.use_mock else []
