"""
Meta Ads (Facebook/Instagram) API Connector.

Supports Meta Marketing API v18 with Access Token authentication.
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
from src.platform.connectors.mock_responses import (
    META_ADS_CAMPAIGNS,
    META_ADS_ACCOUNT,
    get_mock_performance_data,
)


class MetaAdsConnector(BaseAdConnector):
    """
    Meta (Facebook/Instagram) Marketing API connector.
    
    Supports:
    - Access Token authentication
    - Campaigns, Ad Sets, and Ads
    - Insights and performance metrics
    
    Credentials needed:
    - META_ADS_APP_ID: App ID from Meta Developer Portal
    - META_ADS_APP_SECRET: App Secret
    - META_ADS_ACCESS_TOKEN: User or System User access token
    - META_ADS_AD_ACCOUNT_ID: Ad Account ID (format: act_123456)
    """
    
    PLATFORM_NAME = "meta_ads"
    API_VERSION = "v18.0"
    
    def __init__(self, use_mock: bool = None, **credentials):
        """
        Initialize Meta Ads connector.
        
        Args:
            use_mock: Use mock mode for testing.
            **credentials: Override credentials (otherwise uses env vars).
        """
        creds = {
            "app_id": credentials.get("app_id") or os.getenv("META_ADS_APP_ID"),
            "app_secret": credentials.get("app_secret") or os.getenv("META_ADS_APP_SECRET"),
            "access_token": credentials.get("access_token") or os.getenv("META_ADS_ACCESS_TOKEN"),
            "ad_account_id": credentials.get("ad_account_id") or os.getenv("META_ADS_AD_ACCOUNT_ID"),
        }
        
        super().__init__(use_mock=use_mock, **creds)
        self._api = None
        self._ad_account = None
    
    def _validate_credentials(self) -> bool:
        """Check that all required credentials are present."""
        required = ["access_token", "ad_account_id"]
        missing = [k for k in required if not self.credentials.get(k)]
        
        if missing:
            logger.warning(f"Missing Meta Ads credentials: {missing}")
            return False
        return True
    
    def _connect_real(self) -> ConnectionResult:
        """Establish real connection to Meta Marketing API."""
        try:
            from facebook_business.api import FacebookAdsApi
            from facebook_business.adobjects.adaccount import AdAccount
            
            # Initialize the API
            FacebookAdsApi.init(
                app_id=self.credentials.get("app_id"),
                app_secret=self.credentials.get("app_secret"),
                access_token=self.credentials["access_token"],
            )
            
            ad_account_id = self.credentials["ad_account_id"]
            if not ad_account_id.startswith("act_"):
                ad_account_id = f"act_{ad_account_id}"
            
            # Test connection by fetching account info
            self._ad_account = AdAccount(ad_account_id)
            account_info = self._ad_account.api_get(fields=[
                "name", "account_id", "currency", "timezone_name", "account_status"
            ])
            
            self._account_id = account_info.get("account_id")
            self._account_name = account_info.get("name")
            
            return ConnectionResult(
                success=True,
                status=ConnectorStatus.CONNECTED,
                message="Successfully connected to Meta Ads",
                platform=self.PLATFORM_NAME,
                account_id=self._account_id,
                account_name=self._account_name,
                permissions=["ads_read", "ads_management", "read_insights"],
            )
            
        except ImportError:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="facebook-business library not installed",
                platform=self.PLATFORM_NAME,
                error_details="Install with: pip install facebook-business",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="Failed to connect to Meta Ads",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """Fetch campaigns from Meta Marketing API."""
        if not self._ad_account:
            raise RuntimeError("Account not initialized. Call test_connection() first.")
        
        from facebook_business.adobjects.campaign import Campaign as MetaCampaign
        
        fields = [
            "id", "name", "status", "objective", "buying_type",
            "daily_budget", "lifetime_budget", "start_time", "stop_time",
        ]
        
        # Fetch campaigns with insights
        campaigns = self._ad_account.get_campaigns(fields=fields)
        
        result = []
        for camp in campaigns:
            # Get insights for this campaign
            insights = camp.get_insights(fields=[
                "spend", "impressions", "clicks", "conversions",
            ], params={
                "date_preset": "last_30d",
            })
            
            insight_data = insights[0] if insights else {}
            
            result.append(Campaign(
                id=camp.get("id"),
                name=camp.get("name"),
                status=camp.get("status"),
                platform=self.PLATFORM_NAME,
                account_id=self._account_id,
                budget=float(camp.get("lifetime_budget", 0) or camp.get("daily_budget", 0)) / 100,
                spend=float(insight_data.get("spend", 0)),
                impressions=int(insight_data.get("impressions", 0)),
                clicks=int(insight_data.get("clicks", 0)),
                conversions=float(insight_data.get("conversions", 0)),
                start_date=datetime.fromisoformat(camp.get("start_time").replace("Z", "+00:00")) if camp.get("start_time") else None,
                end_date=datetime.fromisoformat(camp.get("stop_time").replace("Z", "+00:00")) if camp.get("stop_time") else None,
                objective=camp.get("objective"),
                extra={
                    "buying_type": camp.get("buying_type"),
                },
            ))
        
        return result
    
    def _get_performance_real(
        self,
        start_date: datetime,
        end_date: datetime,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """Fetch performance metrics from Meta Marketing API."""
        if not self._ad_account:
            raise RuntimeError("Account not initialized. Call test_connection() first.")
        
        params = {
            "time_range": {
                "since": start_date.strftime("%Y-%m-%d"),
                "until": end_date.strftime("%Y-%m-%d"),
            },
            "level": "account",
        }
        
        insights = self._ad_account.get_insights(
            fields=["spend", "impressions", "clicks", "conversions", "purchase_roas"],
            params=params,
        )
        
        data = insights[0] if insights else {}
        
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME,
            account_id=self._account_id,
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            spend=float(data.get("spend", 0)),
            impressions=int(data.get("impressions", 0)),
            clicks=int(data.get("clicks", 0)),
            conversions=float(data.get("conversions", 0)),
            revenue=float(data.get("spend", 0)) * float(data.get("purchase_roas", [{"value": 0}])[0].get("value", 0)),
        )
    
    def _get_mock_campaigns(self) -> List[Campaign]:
        """Return mock Meta Ads campaign data."""
        campaigns = []
        account = META_ADS_ACCOUNT
        
        for c in META_ADS_CAMPAIGNS:
            campaigns.append(Campaign(
                id=c["id"],
                name=c["name"],
                status=c["status"],
                platform=self.PLATFORM_NAME,
                account_id=account["id"],
                budget=c["budget"],
                spend=c["spend"],
                impressions=c["impressions"],
                clicks=c["clicks"],
                conversions=c["conversions"],
                start_date=datetime.strptime(c["start_date"], "%Y-%m-%d") if c.get("start_date") else None,
                end_date=datetime.strptime(c["end_date"], "%Y-%m-%d") if c.get("end_date") else None,
                objective=c["objective"],
                extra={
                    "optimization_goal": c["optimization_goal"],
                    "buying_type": c["buying_type"],
                },
            ))
        
        return campaigns
    
    def _get_mock_performance(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> PerformanceMetrics:
        """Return mock performance metrics."""
        data = get_mock_performance_data(self.PLATFORM_NAME, start_date, end_date)
        account = META_ADS_ACCOUNT
        
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME,
            account_id=account["id"],
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            spend=data["spend"],
            impressions=data["impressions"],
            clicks=data["clicks"],
            conversions=data["conversions"],
            revenue=data["revenue"],
        )
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        """Get accessible Meta Ads accounts."""
        if self.use_mock:
            return [META_ADS_ACCOUNT]
        
        if not self._ad_account:
            return []
        
        return [
            {
                "id": self._account_id,
                "name": self._account_name,
                "currency": "USD",
                "timezone": "America/Los_Angeles",
                "status": "ACTIVE",
            }
        ]
    
    def get_ad_sets(self, campaign_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ad sets, optionally filtered by campaign_id."""
        if self.use_mock:
            ad_sets = [
                # Campaign 001 - Lookalike Audiences
                {"id": "meta_as_001", "campaign_id": "meta_camp_001", "name": "LAL 1% Purchasers", "status": "ACTIVE", "optimization_goal": "CONVERSIONS", "billing_event": "IMPRESSIONS", "spend": 4500.25, "impressions": 1850000, "clicks": 55500, "conversions": 2850.0},
                {"id": "meta_as_002", "campaign_id": "meta_camp_001", "name": "LAL 3% Purchasers", "status": "ACTIVE", "optimization_goal": "CONVERSIONS", "billing_event": "IMPRESSIONS", "spend": 3200.50, "impressions": 1450000, "clicks": 43500, "conversions": 1950.0},
                {"id": "meta_as_003", "campaign_id": "meta_camp_001", "name": "Interest - Fashion", "status": "ACTIVE", "optimization_goal": "CONVERSIONS", "billing_event": "IMPRESSIONS", "spend": 2850.00, "impressions": 1200000, "clicks": 36000, "conversions": 1550.0},
                # Campaign 002 - Retargeting
                {"id": "meta_as_004", "campaign_id": "meta_camp_002", "name": "Website Visitors 7d", "status": "ACTIVE", "optimization_goal": "CONVERSIONS", "billing_event": "IMPRESSIONS", "spend": 3100.00, "impressions": 950000, "clicks": 47500, "conversions": 2850.0},
                {"id": "meta_as_005", "campaign_id": "meta_camp_002", "name": "Add to Cart 14d", "status": "ACTIVE", "optimization_goal": "CONVERSIONS", "billing_event": "IMPRESSIONS", "spend": 4200.75, "impressions": 1050000, "clicks": 63000, "conversions": 4200.0},
                # Campaign 003 - Stories
                {"id": "meta_as_006", "campaign_id": "meta_camp_003", "name": "Stories - 18-34", "status": "ACTIVE", "optimization_goal": "REACH", "billing_event": "IMPRESSIONS", "spend": 8500.00, "impressions": 4500000, "clicks": 135000, "conversions": 4500.0},
                {"id": "meta_as_007", "campaign_id": "meta_camp_003", "name": "Stories - 35-54", "status": "ACTIVE", "optimization_goal": "REACH", "billing_event": "IMPRESSIONS", "spend": 6250.50, "impressions": 3500000, "clicks": 105000, "conversions": 3500.0},
                # Campaign 004 - Lead Ads
                {"id": "meta_as_008", "campaign_id": "meta_camp_004", "name": "Lead Form - Newsletter", "status": "ACTIVE", "optimization_goal": "LEAD_GENERATION", "billing_event": "IMPRESSIONS", "spend": 2100.00, "impressions": 650000, "clicks": 13000, "conversions": 1950.0},
                {"id": "meta_as_009", "campaign_id": "meta_camp_004", "name": "Lead Form - Demo Request", "status": "ACTIVE", "optimization_goal": "LEAD_GENERATION", "billing_event": "IMPRESSIONS", "spend": 2650.25, "impressions": 750000, "clicks": 15000, "conversions": 2250.0},
                # Campaign 005 - Reels
                {"id": "meta_as_010", "campaign_id": "meta_camp_005", "name": "Reels - Gen Z", "status": "PAUSED", "optimization_goal": "VIDEO_VIEWS", "billing_event": "IMPRESSIONS", "spend": 5800.00, "impressions": 2800000, "clicks": 84000, "conversions": 2100.0},
            ]
            if campaign_id:
                ad_sets = [a for a in ad_sets if a["campaign_id"] == campaign_id]
            for a in ad_sets:
                a["ctr"] = round((a["clicks"] / a["impressions"] * 100) if a["impressions"] else 0, 2)
                a["cpc"] = round(a["spend"] / a["clicks"] if a["clicks"] else 0, 2)
                a["cpa"] = round(a["spend"] / a["conversions"] if a["conversions"] else 0, 2)
            return ad_sets
        return []
    
    def get_ads(self, campaign_id: Optional[str] = None, ad_set_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ads, optionally filtered by campaign_id or ad_set_id."""
        if self.use_mock:
            ads = [
                # Ad Set 001 - LAL 1%
                {"id": "meta_ad_001", "ad_set_id": "meta_as_001", "campaign_id": "meta_camp_001", "name": "Carousel - Product Showcase", "status": "ACTIVE", "format": "CAROUSEL", "spend": 2250.12, "impressions": 925000, "clicks": 27750, "conversions": 1425.0},
                {"id": "meta_ad_002", "ad_set_id": "meta_as_001", "campaign_id": "meta_camp_001", "name": "Video - Brand Story", "status": "ACTIVE", "format": "VIDEO", "spend": 2250.13, "impressions": 925000, "clicks": 27750, "conversions": 1425.0},
                # Ad Set 002 - LAL 3%
                {"id": "meta_ad_003", "ad_set_id": "meta_as_002", "campaign_id": "meta_camp_001", "name": "Single Image - Offer", "status": "ACTIVE", "format": "IMAGE", "spend": 3200.50, "impressions": 1450000, "clicks": 43500, "conversions": 1950.0},
                # Ad Set 003 - Interest Fashion
                {"id": "meta_ad_004", "ad_set_id": "meta_as_003", "campaign_id": "meta_camp_001", "name": "Collection - New Arrivals", "status": "ACTIVE", "format": "COLLECTION", "spend": 2850.00, "impressions": 1200000, "clicks": 36000, "conversions": 1550.0},
                # Ad Set 004 - Website Visitors
                {"id": "meta_ad_005", "ad_set_id": "meta_as_004", "campaign_id": "meta_camp_002", "name": "DPA - Viewed Products", "status": "ACTIVE", "format": "DYNAMIC_PRODUCT_AD", "spend": 3100.00, "impressions": 950000, "clicks": 47500, "conversions": 2850.0},
                # Ad Set 005 - Cart Abandoners
                {"id": "meta_ad_006", "ad_set_id": "meta_as_005", "campaign_id": "meta_camp_002", "name": "DPA - Cart Items", "status": "ACTIVE", "format": "DYNAMIC_PRODUCT_AD", "spend": 4200.75, "impressions": 1050000, "clicks": 63000, "conversions": 4200.0},
                # Ad Set 006 - Stories 18-34
                {"id": "meta_ad_007", "ad_set_id": "meta_as_006", "campaign_id": "meta_camp_003", "name": "Story - Full Screen Video", "status": "ACTIVE", "format": "STORY_VIDEO", "spend": 4250.00, "impressions": 2250000, "clicks": 67500, "conversions": 2250.0},
                {"id": "meta_ad_008", "ad_set_id": "meta_as_006", "campaign_id": "meta_camp_003", "name": "Story - Swipe Up Poll", "status": "ACTIVE", "format": "STORY_INTERACTIVE", "spend": 4250.00, "impressions": 2250000, "clicks": 67500, "conversions": 2250.0},
                # Ad Set 007 - Stories 35-54
                {"id": "meta_ad_009", "ad_set_id": "meta_as_007", "campaign_id": "meta_camp_003", "name": "Story - Testimonial", "status": "ACTIVE", "format": "STORY_VIDEO", "spend": 6250.50, "impressions": 3500000, "clicks": 105000, "conversions": 3500.0},
                # Ad Set 008 - Newsletter
                {"id": "meta_ad_010", "ad_set_id": "meta_as_008", "campaign_id": "meta_camp_004", "name": "Lead Ad - Newsletter Signup", "status": "ACTIVE", "format": "LEAD_AD", "spend": 2100.00, "impressions": 650000, "clicks": 13000, "conversions": 1950.0},
                # Ad Set 009 - Demo
                {"id": "meta_ad_011", "ad_set_id": "meta_as_009", "campaign_id": "meta_camp_004", "name": "Lead Ad - Book Demo", "status": "ACTIVE", "format": "LEAD_AD", "spend": 2650.25, "impressions": 750000, "clicks": 15000, "conversions": 2250.0},
                # Ad Set 010 - Reels
                {"id": "meta_ad_012", "ad_set_id": "meta_as_010", "campaign_id": "meta_camp_005", "name": "Reel - Trending Audio", "status": "PAUSED", "format": "REEL", "spend": 2900.00, "impressions": 1400000, "clicks": 42000, "conversions": 1050.0},
                {"id": "meta_ad_013", "ad_set_id": "meta_as_010", "campaign_id": "meta_camp_005", "name": "Reel - UGC Style", "status": "PAUSED", "format": "REEL", "spend": 2900.00, "impressions": 1400000, "clicks": 42000, "conversions": 1050.0},
            ]
            if ad_set_id:
                ads = [a for a in ads if a["ad_set_id"] == ad_set_id]
            elif campaign_id:
                ads = [a for a in ads if a["campaign_id"] == campaign_id]
            for ad in ads:
                ad["ctr"] = round((ad["clicks"] / ad["impressions"] * 100) if ad["impressions"] else 0, 2)
                ad["cpc"] = round(ad["spend"] / ad["clicks"] if ad["clicks"] else 0, 2)
                ad["cpa"] = round(ad["spend"] / ad["conversions"] if ad["conversions"] else 0, 2)
            return ads
        return []

