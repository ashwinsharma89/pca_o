"""
Google Ads API Connector.

Supports Google Ads API v17 with OAuth authentication.
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
    GOOGLE_ADS_CAMPAIGNS,
    GOOGLE_ADS_ACCOUNT,
    get_mock_performance_data,
)


class GoogleAdsConnector(BaseAdConnector):
    """
    Google Ads API connector.
    
    Supports:
    - OAuth 2.0 authentication
    - Customer ID based access
    - Search, Display, Video, Shopping, Performance Max campaigns
    
    Credentials needed:
    - GOOGLE_ADS_CUSTOMER_ID: Your Google Ads customer ID (format: 123-456-7890)
    - GOOGLE_ADS_DEVELOPER_TOKEN: Developer token from Google Ads
    - GOOGLE_ADS_REFRESH_TOKEN: OAuth refresh token
    - GOOGLE_ADS_CLIENT_ID: OAuth client ID
    - GOOGLE_ADS_CLIENT_SECRET: OAuth client secret
    """
    
    PLATFORM_NAME = "google_ads"
    
    def __init__(self, use_mock: bool = None, **credentials):
        """
        Initialize Google Ads connector.
        
        Args:
            use_mock: Use mock mode for testing.
            **credentials: Override credentials (otherwise uses env vars).
        """
        # Load credentials from env if not provided
        creds = {
            "customer_id": credentials.get("customer_id") or os.getenv("GOOGLE_ADS_CUSTOMER_ID"),
            "developer_token": credentials.get("developer_token") or os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
            "refresh_token": credentials.get("refresh_token") or os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
            "client_id": credentials.get("client_id") or os.getenv("GOOGLE_ADS_CLIENT_ID"),
            "client_secret": credentials.get("client_secret") or os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        }
        
        super().__init__(use_mock=use_mock, **creds)
        self._client = None
    
    def _validate_credentials(self) -> bool:
        """Check that all required credentials are present."""
        required = ["customer_id", "developer_token", "refresh_token", "client_id", "client_secret"]
        missing = [k for k in required if not self.credentials.get(k)]
        
        if missing:
            logger.warning(f"Missing Google Ads credentials: {missing}")
            return False
        return True
    
    def _connect_real(self) -> ConnectionResult:
        """Establish real connection to Google Ads API."""
        try:
            # Import Google Ads library
            from google.ads.googleads.client import GoogleAdsClient
            from google.ads.googleads.errors import GoogleAdsException
            
            # Build client configuration
            config = {
                "developer_token": self.credentials["developer_token"],
                "refresh_token": self.credentials["refresh_token"],
                "client_id": self.credentials["client_id"],
                "client_secret": self.credentials["client_secret"],
                "use_proto_plus": True,
            }
            
            self._client = GoogleAdsClient.load_from_dict(config)
            customer_id = self.credentials["customer_id"].replace("-", "")
            
            # Test connection by fetching customer info
            customer_service = self._client.get_service("CustomerService")
            customer = customer_service.get_customer(resource_name=f"customers/{customer_id}")
            
            self._account_id = customer_id
            self._account_name = customer.descriptive_name
            
            return ConnectionResult(
                success=True,
                status=ConnectorStatus.CONNECTED,
                message="Successfully connected to Google Ads",
                platform=self.PLATFORM_NAME,
                account_id=customer_id,
                account_name=customer.descriptive_name,
                permissions=["read_campaigns", "read_reports"],
            )
            
        except ImportError:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="google-ads library not installed",
                platform=self.PLATFORM_NAME,
                error_details="Install with: pip install google-ads",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="Failed to connect to Google Ads",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """Fetch campaigns from Google Ads API."""
        if not self._client:
            raise RuntimeError("Client not initialized. Call test_connection() first.")
        
        customer_id = self.credentials["customer_id"].replace("-", "")
        ga_service = self._client.get_service("GoogleAdsService")
        
        # Build GAQL query
        query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.bidding_strategy_type,
                campaign.start_date,
                campaign.end_date,
                campaign_budget.amount_micros,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions
            FROM campaign
            WHERE campaign.status != 'REMOVED'
        """
        
        campaigns = []
        response = ga_service.search(customer_id=customer_id, query=query)
        
        for row in response:
            campaigns.append(Campaign(
                id=str(row.campaign.id),
                name=row.campaign.name,
                status=row.campaign.status.name,
                platform=self.PLATFORM_NAME,
                account_id=customer_id,
                budget=row.campaign_budget.amount_micros / 1_000_000,
                spend=row.metrics.cost_micros / 1_000_000,
                impressions=row.metrics.impressions,
                clicks=row.metrics.clicks,
                conversions=row.metrics.conversions,
                start_date=datetime.strptime(row.campaign.start_date, "%Y-%m-%d") if row.campaign.start_date else None,
                end_date=datetime.strptime(row.campaign.end_date, "%Y-%m-%d") if row.campaign.end_date else None,
                objective=row.campaign.advertising_channel_type.name,
                extra={
                    "bidding_strategy": row.campaign.bidding_strategy_type.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                },
            ))
        
        return campaigns
    
    def _get_performance_real(
        self,
        start_date: datetime,
        end_date: datetime,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """Fetch performance metrics from Google Ads API."""
        if not self._client:
            raise RuntimeError("Client not initialized. Call test_connection() first.")
        
        customer_id = self.credentials["customer_id"].replace("-", "")
        ga_service = self._client.get_service("GoogleAdsService")
        
        # Build date filter
        date_filter = f"""
            segments.date >= '{start_date.strftime('%Y-%m-%d')}'
            AND segments.date <= '{end_date.strftime('%Y-%m-%d')}'
        """
        
        query = f"""
            SELECT
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.conversions_value
            FROM customer
            WHERE {date_filter}
        """
        
        response = ga_service.search(customer_id=customer_id, query=query)
        
        total_spend = 0
        total_impressions = 0
        total_clicks = 0
        total_conversions = 0
        total_revenue = 0
        
        for row in response:
            total_spend += row.metrics.cost_micros / 1_000_000
            total_impressions += row.metrics.impressions
            total_clicks += row.metrics.clicks
            total_conversions += row.metrics.conversions
            total_revenue += row.metrics.conversions_value
        
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME,
            account_id=customer_id,
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            spend=total_spend,
            impressions=total_impressions,
            clicks=total_clicks,
            conversions=total_conversions,
            revenue=total_revenue,
        )
    
    def _get_mock_campaigns(self) -> List[Campaign]:
        """Return mock Google Ads campaign data."""
        campaigns = []
        account = GOOGLE_ADS_ACCOUNT
        
        for c in GOOGLE_ADS_CAMPAIGNS:
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
                    "bidding_strategy": c["bidding_strategy"],
                    "ad_network": c["ad_network"],
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
        account = GOOGLE_ADS_ACCOUNT
        
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
        """Get accessible Google Ads accounts."""
        if self.use_mock:
            return [GOOGLE_ADS_ACCOUNT]
        
        if not self._client:
            return []
        
        # In real implementation, query accessible customers
        return [
            {
                "id": self._account_id,
                "name": self._account_name,
                "currency": "USD",
                "timezone": "America/New_York",
                "status": "active",
            }
        ]
    
    def get_ad_groups(self, campaign_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ad groups, optionally filtered by campaign_id."""
        if self.use_mock:
            ad_groups = [
                # Campaign 001 - Brand Awareness
                {"id": "ga_ag_001", "campaign_id": "ga_camp_001", "name": "Brand Keywords", "status": "ENABLED", "type": "SEARCH_STANDARD", "spend": 4200.25, "impressions": 850000, "clicks": 15500, "conversions": 420.0},
                {"id": "ga_ag_002", "campaign_id": "ga_camp_001", "name": "Competitor Keywords", "status": "ENABLED", "type": "SEARCH_STANDARD", "spend": 5100.50, "impressions": 980000, "clicks": 18200, "conversions": 510.0},
                {"id": "ga_ag_003", "campaign_id": "ga_camp_001", "name": "Generic Terms", "status": "ENABLED", "type": "SEARCH_STANDARD", "spend": 3150.00, "impressions": 620000, "clicks": 11500, "conversions": 320.5},
                # Campaign 002 - Lead Gen
                {"id": "ga_ag_004", "campaign_id": "ga_camp_002", "name": "Performance Max Asset Group", "status": "ENABLED", "type": "PERFORMANCE_MAX", "spend": 21875.50, "impressions": 1850000, "clicks": 72500, "conversions": 3420.0},
                # Campaign 003 - Retargeting
                {"id": "ga_ag_005", "campaign_id": "ga_camp_003", "name": "Website Visitors", "status": "ENABLED", "type": "DISPLAY", "spend": 3270.00, "impressions": 2600000, "clicks": 15600, "conversions": 445.0},
                {"id": "ga_ag_006", "campaign_id": "ga_camp_003", "name": "Cart Abandoners", "status": "ENABLED", "type": "DISPLAY", "spend": 3270.25, "impressions": 2600000, "clicks": 15600, "conversions": 445.0},
                # Campaign 004 - YouTube
                {"id": "ga_ag_007", "campaign_id": "ga_camp_004", "name": "In-Stream Skippable", "status": "PAUSED", "type": "VIDEO", "spend": 5625.00, "impressions": 445000, "clicks": 7800, "conversions": 222.5},
                {"id": "ga_ag_008", "campaign_id": "ga_camp_004", "name": "Discovery Ads", "status": "PAUSED", "type": "VIDEO", "spend": 5625.00, "impressions": 445000, "clicks": 7800, "conversions": 222.5},
                # Campaign 005 - Shopping
                {"id": "ga_ag_009", "campaign_id": "ga_camp_005", "name": "Electronics", "status": "ENABLED", "type": "SHOPPING", "spend": 14375.40, "impressions": 1600000, "clicks": 62500, "conversions": 4375.0},
                {"id": "ga_ag_010", "campaign_id": "ga_camp_005", "name": "Home & Garden", "status": "ENABLED", "type": "SHOPPING", "spend": 14375.40, "impressions": 1600000, "clicks": 62500, "conversions": 4375.0},
            ]
            if campaign_id:
                ad_groups = [ag for ag in ad_groups if ag["campaign_id"] == campaign_id]
            for ag in ad_groups:
                ag["ctr"] = round((ag["clicks"] / ag["impressions"] * 100) if ag["impressions"] else 0, 2)
                ag["cpc"] = round(ag["spend"] / ag["clicks"] if ag["clicks"] else 0, 2)
                ag["cpa"] = round(ag["spend"] / ag["conversions"] if ag["conversions"] else 0, 2)
            return ad_groups
        return []
    
    def get_ads(self, campaign_id: Optional[str] = None, ad_group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get ads, optionally filtered by campaign_id or ad_group_id."""
        if self.use_mock:
            ads = [
                # Ad Group 001 - Brand Keywords
                {"id": "ga_ad_001", "ad_group_id": "ga_ag_001", "campaign_id": "ga_camp_001", "name": "RSA - Brand Terms", "status": "ENABLED", "type": "RESPONSIVE_SEARCH_AD", "spend": 2100.12, "impressions": 425000, "clicks": 7750, "conversions": 210.0},
                {"id": "ga_ad_002", "ad_group_id": "ga_ag_001", "campaign_id": "ga_camp_001", "name": "RSA - Brand + Product", "status": "ENABLED", "type": "RESPONSIVE_SEARCH_AD", "spend": 2100.13, "impressions": 425000, "clicks": 7750, "conversions": 210.0},
                # Ad Group 002 - Competitor
                {"id": "ga_ad_003", "ad_group_id": "ga_ag_002", "campaign_id": "ga_camp_001", "name": "RSA - Competitor Compare", "status": "ENABLED", "type": "RESPONSIVE_SEARCH_AD", "spend": 5100.50, "impressions": 980000, "clicks": 18200, "conversions": 510.0},
                # Ad Group 003 - Generic
                {"id": "ga_ad_004", "ad_group_id": "ga_ag_003", "campaign_id": "ga_camp_001", "name": "RSA - Category Terms", "status": "ENABLED", "type": "RESPONSIVE_SEARCH_AD", "spend": 3150.00, "impressions": 620000, "clicks": 11500, "conversions": 320.5},
                # Ad Group 004 - PMax
                {"id": "ga_ad_005", "ad_group_id": "ga_ag_004", "campaign_id": "ga_camp_002", "name": "PMax Asset Group 1", "status": "ENABLED", "type": "PERFORMANCE_MAX", "spend": 10937.75, "impressions": 925000, "clicks": 36250, "conversions": 1710.0},
                {"id": "ga_ad_006", "ad_group_id": "ga_ag_004", "campaign_id": "ga_camp_002", "name": "PMax Asset Group 2", "status": "ENABLED", "type": "PERFORMANCE_MAX", "spend": 10937.75, "impressions": 925000, "clicks": 36250, "conversions": 1710.0},
                # Ad Group 005 - Website Visitors
                {"id": "ga_ad_007", "ad_group_id": "ga_ag_005", "campaign_id": "ga_camp_003", "name": "Display - Image 300x250", "status": "ENABLED", "type": "RESPONSIVE_DISPLAY_AD", "spend": 1635.00, "impressions": 1300000, "clicks": 7800, "conversions": 222.5},
                {"id": "ga_ad_008", "ad_group_id": "ga_ag_005", "campaign_id": "ga_camp_003", "name": "Display - Image 728x90", "status": "ENABLED", "type": "RESPONSIVE_DISPLAY_AD", "spend": 1635.00, "impressions": 1300000, "clicks": 7800, "conversions": 222.5},
                # Ad Group 006 - Cart Abandoners
                {"id": "ga_ad_009", "ad_group_id": "ga_ag_006", "campaign_id": "ga_camp_003", "name": "Dynamic Remarketing", "status": "ENABLED", "type": "RESPONSIVE_DISPLAY_AD", "spend": 3270.25, "impressions": 2600000, "clicks": 15600, "conversions": 445.0},
                # Ad Group 007 - In-Stream
                {"id": "ga_ad_010", "ad_group_id": "ga_ag_007", "campaign_id": "ga_camp_004", "name": "Video - 30s Skippable", "status": "PAUSED", "type": "VIDEO_AD", "spend": 5625.00, "impressions": 445000, "clicks": 7800, "conversions": 222.5},
                # Ad Group 009 - Shopping Electronics
                {"id": "ga_ad_011", "ad_group_id": "ga_ag_009", "campaign_id": "ga_camp_005", "name": "Product Listing - Electronics", "status": "ENABLED", "type": "SHOPPING_PRODUCT_AD", "spend": 14375.40, "impressions": 1600000, "clicks": 62500, "conversions": 4375.0},
            ]
            if ad_group_id:
                ads = [a for a in ads if a["ad_group_id"] == ad_group_id]
            elif campaign_id:
                ads = [a for a in ads if a["campaign_id"] == campaign_id]
            for ad in ads:
                ad["ctr"] = round((ad["clicks"] / ad["impressions"] * 100) if ad["impressions"] else 0, 2)
                ad["cpc"] = round(ad["spend"] / ad["clicks"] if ad["clicks"] else 0, 2)
                ad["cpa"] = round(ad["spend"] / ad["conversions"] if ad["conversions"] else 0, 2)
            return ads
        return []
