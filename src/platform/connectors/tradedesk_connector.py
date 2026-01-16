"""
The Trade Desk API Connector.

Supports The Trade Desk API with Partner/Advertiser authentication.
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


TRADEDESK_CAMPAIGNS = [
    {
        "id": "ttd_camp_001",
        "name": "Programmatic Display - Prospecting",
        "status": "Active",
        "budget": 75000.00,
        "spend": 65450.75,
        "impressions": 45000000,
        "clicks": 450000,
        "conversions": 8500.0,
        "objective": "PERFORMANCE",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "ttd_camp_002",
        "name": "CTV - Household Targeting",
        "status": "Active",
        "budget": 120000.00,
        "spend": 98750.50,
        "impressions": 25000000,
        "clicks": 125000,
        "conversions": 4500.0,
        "objective": "AWARENESS",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "ttd_camp_003",
        "name": "Audio - Podcast & Streaming",
        "status": "Active",
        "budget": 35000.00,
        "spend": 29875.25,
        "impressions": 18000000,
        "clicks": 180000,
        "conversions": 2800.0,
        "objective": "REACH",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "ttd_camp_004",
        "name": "Native - Content Distribution",
        "status": "Active",
        "budget": 25000.00,
        "spend": 21234.00,
        "impressions": 12000000,
        "clicks": 360000,
        "conversions": 5600.0,
        "objective": "ENGAGEMENT",
        "start_date": "2024-10-15",
        "end_date": None,
    },
    {
        "id": "ttd_camp_005",
        "name": "Retargeting - Cross-Device",
        "status": "Paused",
        "budget": 45000.00,
        "spend": 43500.00,
        "impressions": 32000000,
        "clicks": 640000,
        "conversions": 12500.0,
        "objective": "CONVERSIONS",
        "start_date": "2024-07-01",
        "end_date": "2024-10-31",
    },
]

TRADEDESK_ACCOUNT = {
    "id": "ttd_partner_123",
    "name": "Demo Trade Desk Account",
    "currency": "USD",
    "timezone": "America/New_York",
    "status": "Active",
}


class TradeDeskConnector(BaseAdConnector):
    """
    The Trade Desk API connector.
    
    Credentials needed:
    - TRADEDESK_PARTNER_ID: Partner ID
    - TRADEDESK_API_TOKEN: API Token
    - TRADEDESK_ADVERTISER_ID: Advertiser ID
    """
    
    PLATFORM_NAME = "tradedesk"
    API_VERSION = "v3"
    
    def __init__(self, use_mock: bool = None, **credentials):
        creds = {
            "partner_id": credentials.get("partner_id") or os.getenv("TRADEDESK_PARTNER_ID"),
            "api_token": credentials.get("api_token") or os.getenv("TRADEDESK_API_TOKEN"),
            "advertiser_id": credentials.get("advertiser_id") or os.getenv("TRADEDESK_ADVERTISER_ID"),
        }
        super().__init__(use_mock=use_mock, **creds)
    
    def _validate_credentials(self) -> bool:
        return all(self.credentials.get(k) for k in ["partner_id", "api_token"])
    
    def _connect_real(self) -> ConnectionResult:
        try:
            import requests
            headers = {"TTD-Auth": self.credentials["api_token"]}
            response = requests.get(
                f"https://api.thetradedesk.com/{self.API_VERSION}/partner/query",
                headers=headers,
                timeout=30
            )
            if response.status_code == 200:
                return ConnectionResult(
                    success=True, status=ConnectorStatus.CONNECTED,
                    message="Connected to The Trade Desk", platform=self.PLATFORM_NAME,
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
                platform=self.PLATFORM_NAME, account_id=TRADEDESK_ACCOUNT["id"],
                budget=c["budget"], spend=c["spend"], impressions=c["impressions"],
                clicks=c["clicks"], conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
            )
            for c in TRADEDESK_CAMPAIGNS
        ]
    
    def _get_mock_performance(self, start_date, end_date) -> PerformanceMetrics:
        total = {k: sum(c[k] for c in TRADEDESK_CAMPAIGNS) for k in ["spend", "impressions", "clicks", "conversions"]}
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id=TRADEDESK_ACCOUNT["id"],
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=total["spend"], impressions=total["impressions"],
            clicks=total["clicks"], conversions=total["conversions"],
            revenue=total["conversions"] * 45.0,
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        return [TRADEDESK_ACCOUNT] if self.use_mock else []
