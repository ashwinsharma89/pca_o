"""
Amazon DSP API Connector.

Supports Amazon Advertising API with LWA OAuth authentication.
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


AMAZON_DSP_CAMPAIGNS = [
    {
        "id": "amz_camp_001",
        "name": "Sponsored Display - Product Targeting",
        "status": "DELIVERING",
        "budget": 50000.00,
        "spend": 43250.75,
        "impressions": 28000000,
        "clicks": 560000,
        "conversions": 18500.0,
        "objective": "PRODUCT_CONSIDERATION",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "amz_camp_002",
        "name": "Video - OTT & Streaming TV",
        "status": "DELIVERING",
        "budget": 85000.00,
        "spend": 72450.50,
        "impressions": 15000000,
        "clicks": 75000,
        "conversions": 2800.0,
        "objective": "BRAND_AWARENESS",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "amz_camp_003",
        "name": "Audio - Amazon Music & Alexa",
        "status": "DELIVERING",
        "budget": 25000.00,
        "spend": 21875.25,
        "impressions": 12000000,
        "clicks": 120000,
        "conversions": 1500.0,
        "objective": "REACH",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "amz_camp_004",
        "name": "Retargeting - Shopping Intent",
        "status": "DELIVERING",
        "budget": 35000.00,
        "spend": 29750.00,
        "impressions": 22000000,
        "clicks": 880000,
        "conversions": 32000.0,
        "objective": "CONVERSIONS",
        "start_date": "2024-10-15",
        "end_date": None,
    },
    {
        "id": "amz_camp_005",
        "name": "Twitch - Gaming Audience",
        "status": "PAUSED",
        "budget": 40000.00,
        "spend": 38500.00,
        "impressions": 18000000,
        "clicks": 900000,
        "conversions": 45000.0,
        "objective": "ENGAGEMENT",
        "start_date": "2024-07-01",
        "end_date": "2024-10-31",
    },
]

# Ad-level mock data for Amazon DSP
AMAZON_DSP_ADS = [
    # Ads for Campaign 001 - Sponsored Display
    {"id": "amz_ad_001", "campaign_id": "amz_camp_001", "name": "Echo Dot - Product Page", "status": "DELIVERING", "format": "PRODUCT_DISPLAY", "spend": 12500.25, "impressions": 8000000, "clicks": 160000, "conversions": 5200.0},
    {"id": "amz_ad_002", "campaign_id": "amz_camp_001", "name": "Fire TV Stick - Carousel", "status": "DELIVERING", "format": "CAROUSEL", "spend": 15750.50, "impressions": 10000000, "clicks": 200000, "conversions": 6800.0},
    {"id": "amz_ad_003", "campaign_id": "amz_camp_001", "name": "Ring Doorbell - Video", "status": "DELIVERING", "format": "VIDEO", "spend": 15000.00, "impressions": 10000000, "clicks": 200000, "conversions": 6500.0},
    # Ads for Campaign 002 - OTT Video
    {"id": "amz_ad_004", "campaign_id": "amz_camp_002", "name": "Prime Video - 15s Pre-roll", "status": "DELIVERING", "format": "OTT_VIDEO", "spend": 35000.00, "impressions": 7500000, "clicks": 37500, "conversions": 1400.0},
    {"id": "amz_ad_005", "campaign_id": "amz_camp_002", "name": "Freevee - 30s Mid-roll", "status": "DELIVERING", "format": "OTT_VIDEO", "spend": 37450.50, "impressions": 7500000, "clicks": 37500, "conversions": 1400.0},
    # Ads for Campaign 003 - Audio
    {"id": "amz_ad_006", "campaign_id": "amz_camp_003", "name": "Amazon Music - Audio Spot", "status": "DELIVERING", "format": "AUDIO", "spend": 12000.00, "impressions": 7000000, "clicks": 70000, "conversions": 850.0},
    {"id": "amz_ad_007", "campaign_id": "amz_camp_003", "name": "Alexa Flash Briefing", "status": "DELIVERING", "format": "AUDIO", "spend": 9875.25, "impressions": 5000000, "clicks": 50000, "conversions": 650.0},
    # Ads for Campaign 004 - Retargeting
    {"id": "amz_ad_008", "campaign_id": "amz_camp_004", "name": "Cart Abandoners - Display", "status": "DELIVERING", "format": "DISPLAY", "spend": 14875.00, "impressions": 11000000, "clicks": 440000, "conversions": 16000.0},
    {"id": "amz_ad_009", "campaign_id": "amz_camp_004", "name": "Product Viewers - Native", "status": "DELIVERING", "format": "NATIVE", "spend": 14875.00, "impressions": 11000000, "clicks": 440000, "conversions": 16000.0},
    # Ads for Campaign 005 - Twitch
    {"id": "amz_ad_010", "campaign_id": "amz_camp_005", "name": "Twitch Display Banner", "status": "PAUSED", "format": "DISPLAY", "spend": 18000.00, "impressions": 8000000, "clicks": 400000, "conversions": 20000.0},
    {"id": "amz_ad_011", "campaign_id": "amz_camp_005", "name": "Twitch Premium Video", "status": "PAUSED", "format": "VIDEO", "spend": 20500.00, "impressions": 10000000, "clicks": 500000, "conversions": 25000.0},
]

AMAZON_DSP_ACCOUNT = {
    "id": "amz_dsp_456789",
    "name": "Demo Amazon DSP Account",
    "currency": "USD",
    "timezone": "America/Los_Angeles",
    "status": "ACTIVE",
}


class AmazonDSPConnector(BaseAdConnector):
    """
    Amazon DSP API connector.
    
    Credentials needed:
    - AMAZON_ADS_CLIENT_ID: LWA Client ID
    - AMAZON_ADS_CLIENT_SECRET: LWA Client Secret
    - AMAZON_ADS_REFRESH_TOKEN: Refresh Token
    - AMAZON_ADS_PROFILE_ID: Advertising Profile ID
    """
    
    PLATFORM_NAME = "amazon_dsp"
    API_VERSION = "v2"
    
    def __init__(self, use_mock: bool = None, **credentials):
        creds = {
            "client_id": credentials.get("client_id") or os.getenv("AMAZON_ADS_CLIENT_ID"),
            "client_secret": credentials.get("client_secret") or os.getenv("AMAZON_ADS_CLIENT_SECRET"),
            "refresh_token": credentials.get("refresh_token") or os.getenv("AMAZON_ADS_REFRESH_TOKEN"),
            "profile_id": credentials.get("profile_id") or os.getenv("AMAZON_ADS_PROFILE_ID"),
        }
        super().__init__(use_mock=use_mock, **creds)
    
    def _validate_credentials(self) -> bool:
        return all(self.credentials.get(k) for k in ["client_id", "client_secret", "refresh_token"])
    
    def _connect_real(self) -> ConnectionResult:
        try:
            import requests
            # Get access token first
            token_response = requests.post(
                "https://api.amazon.com/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.credentials["client_id"],
                    "client_secret": self.credentials["client_secret"],
                    "refresh_token": self.credentials["refresh_token"],
                },
                timeout=30
            )
            if token_response.status_code == 200:
                return ConnectionResult(
                    success=True, status=ConnectorStatus.CONNECTED,
                    message="Connected to Amazon DSP", platform=self.PLATFORM_NAME,
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
                platform=self.PLATFORM_NAME, account_id=AMAZON_DSP_ACCOUNT["id"],
                budget=c["budget"], spend=c["spend"], impressions=c["impressions"],
                clicks=c["clicks"], conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
            )
            for c in AMAZON_DSP_CAMPAIGNS
        ]
    
    def _get_mock_performance(self, start_date, end_date) -> PerformanceMetrics:
        total = {k: sum(c[k] for c in AMAZON_DSP_CAMPAIGNS) for k in ["spend", "impressions", "clicks", "conversions"]}
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME, account_id=AMAZON_DSP_ACCOUNT["id"],
            date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
            spend=total["spend"], impressions=total["impressions"],
            clicks=total["clicks"], conversions=total["conversions"],
            revenue=total["conversions"] * 22.0,
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        return [AMAZON_DSP_ACCOUNT] if self.use_mock else []
    
    def get_ads(self, campaign_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ads, optionally filtered by campaign_id."""
        if self.use_mock:
            ads = AMAZON_DSP_ADS
            if campaign_id:
                ads = [a for a in ads if a["campaign_id"] == campaign_id]
            # Add calculated metrics
            for ad in ads:
                ad["ctr"] = round((ad["clicks"] / ad["impressions"] * 100) if ad["impressions"] > 0 else 0, 2)
                ad["cpc"] = round(ad["spend"] / ad["clicks"] if ad["clicks"] > 0 else 0, 2)
                ad["cpa"] = round(ad["spend"] / ad["conversions"] if ad["conversions"] > 0 else 0, 2)
            return ads
        return []
