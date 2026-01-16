"""
Snapchat Ads API Connector.

Supports Snapchat Marketing API with OAuth authentication.
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


# Mock data for Snapchat Ads
SNAPCHAT_CAMPAIGNS = [
    {
        "id": "snap_camp_001",
        "name": "AR Lens - Brand Experience",
        "status": "ACTIVE",
        "budget": 25000.00,
        "spend": 21450.75,
        "impressions": 8500000,
        "clicks": 425000,
        "conversions": 12500.0,
        "objective": "AWARENESS",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "snap_camp_002",
        "name": "Story Ads - App Install",
        "status": "ACTIVE",
        "budget": 18000.00,
        "spend": 15234.50,
        "impressions": 6200000,
        "clicks": 248000,
        "conversions": 45000.0,
        "objective": "APP_INSTALL",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "snap_camp_003",
        "name": "Collection Ads - E-commerce",
        "status": "ACTIVE",
        "budget": 12000.00,
        "spend": 9875.25,
        "impressions": 4100000,
        "clicks": 164000,
        "conversions": 3200.0,
        "objective": "CATALOG_SALES",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "snap_camp_004",
        "name": "Commercials - Video Views",
        "status": "PAUSED",
        "budget": 30000.00,
        "spend": 28500.00,
        "impressions": 12000000,
        "clicks": 360000,
        "conversions": 8500.0,
        "objective": "VIDEO_VIEWS",
        "start_date": "2024-08-01",
        "end_date": "2024-10-31",
    },
]

# Ad-level mock data for Snapchat
SNAPCHAT_ADS = [
    # Ads for Campaign 001 - AR Lens
    {"id": "snap_ad_001", "campaign_id": "snap_camp_001", "name": "Face Lens - Brand Filter", "status": "ACTIVE", "format": "AR_LENS", "spend": 8500.25, "impressions": 3500000, "clicks": 175000, "conversions": 5000.0},
    {"id": "snap_ad_002", "campaign_id": "snap_camp_001", "name": "World Lens - Product Try-On", "status": "ACTIVE", "format": "AR_LENS", "spend": 7250.50, "impressions": 2800000, "clicks": 140000, "conversions": 4200.0},
    {"id": "snap_ad_003", "campaign_id": "snap_camp_001", "name": "Lens Carousel - Multiple Effects", "status": "ACTIVE", "format": "AR_LENS", "spend": 5700.00, "impressions": 2200000, "clicks": 110000, "conversions": 3300.0},
    # Ads for Campaign 002 - Story Ads
    {"id": "snap_ad_004", "campaign_id": "snap_camp_002", "name": "Story Ad - App Preview Video", "status": "ACTIVE", "format": "STORY", "spend": 6500.00, "impressions": 2500000, "clicks": 100000, "conversions": 18000.0},
    {"id": "snap_ad_005", "campaign_id": "snap_camp_002", "name": "Story Ad - Swipe Up Install", "status": "ACTIVE", "format": "STORY", "spend": 8734.50, "impressions": 3700000, "clicks": 148000, "conversions": 27000.0},
    # Ads for Campaign 003 - Collection
    {"id": "snap_ad_006", "campaign_id": "snap_camp_003", "name": "Collection - Hero Product", "status": "ACTIVE", "format": "COLLECTION", "spend": 4500.00, "impressions": 1800000, "clicks": 72000, "conversions": 1400.0},
    {"id": "snap_ad_007", "campaign_id": "snap_camp_003", "name": "Collection - Product Grid", "status": "ACTIVE", "format": "COLLECTION", "spend": 5375.25, "impressions": 2300000, "clicks": 92000, "conversions": 1800.0},
    # Ads for Campaign 004 - Commercials
    {"id": "snap_ad_008", "campaign_id": "snap_camp_004", "name": "6s Non-Skippable", "status": "PAUSED", "format": "COMMERCIAL", "spend": 12000.00, "impressions": 5000000, "clicks": 150000, "conversions": 3500.0},
    {"id": "snap_ad_009", "campaign_id": "snap_camp_004", "name": "Extended Play Video", "status": "PAUSED", "format": "COMMERCIAL", "spend": 16500.00, "impressions": 7000000, "clicks": 210000, "conversions": 5000.0},
]

SNAPCHAT_ACCOUNT = {
    "id": "snap_org_123456",
    "name": "Demo Snapchat Ads Account",
    "currency": "USD",
    "timezone": "America/Los_Angeles",
    "status": "ACTIVE",
}


class SnapchatAdsConnector(BaseAdConnector):
    """
    Snapchat Marketing API connector.
    
    Supports:
    - OAuth 2.0 authentication
    - Campaign, Ad Squad, and Ad management
    - Insights and performance metrics
    
    Credentials needed:
    - SNAPCHAT_CLIENT_ID: OAuth Client ID
    - SNAPCHAT_CLIENT_SECRET: OAuth Client Secret
    - SNAPCHAT_ACCESS_TOKEN: Access Token
    - SNAPCHAT_AD_ACCOUNT_ID: Ad Account ID
    """
    
    PLATFORM_NAME = "snapchat_ads"
    API_VERSION = "v1"
    
    def __init__(self, use_mock: bool = None, **credentials):
        creds = {
            "client_id": credentials.get("client_id") or os.getenv("SNAPCHAT_CLIENT_ID"),
            "client_secret": credentials.get("client_secret") or os.getenv("SNAPCHAT_CLIENT_SECRET"),
            "access_token": credentials.get("access_token") or os.getenv("SNAPCHAT_ACCESS_TOKEN"),
            "ad_account_id": credentials.get("ad_account_id") or os.getenv("SNAPCHAT_AD_ACCOUNT_ID"),
        }
        super().__init__(use_mock=use_mock, **creds)
    
    def _validate_credentials(self) -> bool:
        required = ["access_token", "ad_account_id"]
        missing = [k for k in required if not self.credentials.get(k)]
        if missing:
            logger.warning(f"Missing Snapchat credentials: {missing}")
            return False
        return True
    
    def _connect_real(self) -> ConnectionResult:
        try:
            import requests
            
            headers = {"Authorization": f"Bearer {self.credentials['access_token']}"}
            response = requests.get(
                f"https://adsapi.snapchat.com/{self.API_VERSION}/me/organizations",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                org = data.get("organizations", [{}])[0].get("organization", {})
                self._account_id = org.get("id")
                self._account_name = org.get("name")
                
                return ConnectionResult(
                    success=True,
                    status=ConnectorStatus.CONNECTED,
                    message="Successfully connected to Snapchat Ads",
                    platform=self.PLATFORM_NAME,
                    account_id=self._account_id,
                    account_name=self._account_name,
                    permissions=["ads_read", "ads_management"],
                )
            else:
                return ConnectionResult(
                    success=False,
                    status=ConnectorStatus.ERROR,
                    message="Failed to connect to Snapchat Ads",
                    platform=self.PLATFORM_NAME,
                    error_details=response.text,
                )
        except ImportError:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="requests library not installed",
                platform=self.PLATFORM_NAME,
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="Connection failed",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(self, start_date=None, end_date=None) -> List[Campaign]:
        return []  # Implement with real API
    
    def _get_performance_real(self, start_date, end_date, campaign_ids=None) -> PerformanceMetrics:
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id="", date_range={},
            spend=0, impressions=0, clicks=0, conversions=0, revenue=0
        )
    
    def _get_mock_campaigns(self) -> List[Campaign]:
        return [
            Campaign(
                id=c["id"], name=c["name"], status=c["status"],
                platform=self.PLATFORM_NAME, account_id=SNAPCHAT_ACCOUNT["id"],
                budget=c["budget"], spend=c["spend"], impressions=c["impressions"],
                clicks=c["clicks"], conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
            )
            for c in SNAPCHAT_CAMPAIGNS
        ]
    
    def _get_mock_performance(self, start_date, end_date) -> PerformanceMetrics:
        total = {k: sum(c[k] for c in SNAPCHAT_CAMPAIGNS) for k in ["spend", "impressions", "clicks", "conversions"]}
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id=SNAPCHAT_ACCOUNT["id"],
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=total["spend"], impressions=total["impressions"],
            clicks=total["clicks"], conversions=total["conversions"],
            revenue=total["conversions"] * 12.5,
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        if self.use_mock:
            return [SNAPCHAT_ACCOUNT]
        return []
    
    def get_ads(self, campaign_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ads, optionally filtered by campaign_id."""
        if self.use_mock:
            ads = [a.copy() for a in SNAPCHAT_ADS]
            if campaign_id:
                ads = [a for a in ads if a["campaign_id"] == campaign_id]
            # Add calculated metrics
            for ad in ads:
                ad["ctr"] = round((ad["clicks"] / ad["impressions"] * 100) if ad["impressions"] > 0 else 0, 2)
                ad["cpc"] = round(ad["spend"] / ad["clicks"] if ad["clicks"] > 0 else 0, 2)
                ad["cpa"] = round(ad["spend"] / ad["conversions"] if ad["conversions"] > 0 else 0, 2)
            return ads
        return []

