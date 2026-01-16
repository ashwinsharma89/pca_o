"""
Display & Video 360 (DV360) API Connector.

Supports DV360 API v2 with Service Account authentication.
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
    DV360_CAMPAIGNS,
    DV360_ACCOUNT,
    get_mock_performance_data,
)


class DV360Connector(BaseAdConnector):
    """
    Display & Video 360 API connector.
    
    Supports:
    - Service Account authentication
    - Partner and Advertiser access
    - Campaign, Insertion Order, and Line Item management
    
    Credentials needed:
    - GOOGLE_CLOUD_PROJECT_ID: GCP Project ID
    - GOOGLE_SERVICE_ACCOUNT_PATH: Path to service account JSON file
    - DV360_PARTNER_ID: DV360 Partner ID
    - DV360_ADVERTISER_ID: DV360 Advertiser ID (optional, for filtering)
    """
    
    PLATFORM_NAME = "dv360"
    API_VERSION = "v2"
    
    def __init__(self, use_mock: bool = None, **credentials):
        """
        Initialize DV360 connector.
        
        Args:
            use_mock: Use mock mode for testing.
            **credentials: Override credentials (otherwise uses env vars).
        """
        creds = {
            "project_id": credentials.get("project_id") or os.getenv("GOOGLE_CLOUD_PROJECT_ID"),
            "service_account_path": credentials.get("service_account_path") or os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH"),
            "partner_id": credentials.get("partner_id") or os.getenv("DV360_PARTNER_ID"),
            "advertiser_id": credentials.get("advertiser_id") or os.getenv("DV360_ADVERTISER_ID"),
        }
        
        super().__init__(use_mock=use_mock, **creds)
        self._service = None
        self._partner_id = None
        self._advertiser_id = None
    
    def _validate_credentials(self) -> bool:
        """Check that all required credentials are present."""
        has_service_account = self.credentials.get("service_account_path")
        has_adc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        if not (has_service_account or has_adc):
            logger.warning("Missing DV360 credentials: service_account_path or GOOGLE_APPLICATION_CREDENTIALS")
            return False
        
        if not self.credentials.get("partner_id") and not self.credentials.get("advertiser_id"):
            logger.warning("Missing DV360 credentials: partner_id or advertiser_id required")
            return False
        
        return True
    
    def _connect_real(self) -> ConnectionResult:
        """Establish real connection to DV360 API."""
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            
            SCOPES = ['https://www.googleapis.com/auth/display-video']
            
            # Try service account file first, fall back to ADC
            service_account_path = self.credentials.get("service_account_path")
            
            if service_account_path and os.path.exists(service_account_path):
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_path,
                    scopes=SCOPES
                )
            else:
                from google.auth import default
                credentials, _ = default(scopes=SCOPES)
            
            # Build the service
            self._service = build('displayvideo', self.API_VERSION, credentials=credentials)
            
            # Verify access by listing partners or advertisers
            partner_id = self.credentials.get("partner_id")
            advertiser_id = self.credentials.get("advertiser_id")
            
            if partner_id:
                # Verify partner access
                partner = self._service.partners().get(partnerId=partner_id).execute()
                self._partner_id = partner_id
                self._account_id = partner_id
                self._account_name = partner.get('displayName', 'DV360 Partner')
            elif advertiser_id:
                # Verify advertiser access
                advertiser = self._service.advertisers().get(advertiserId=advertiser_id).execute()
                self._advertiser_id = advertiser_id
                self._account_id = advertiser_id
                self._account_name = advertiser.get('displayName', 'DV360 Advertiser')
            
            return ConnectionResult(
                success=True,
                status=ConnectorStatus.CONNECTED,
                message="Successfully connected to DV360",
                platform=self.PLATFORM_NAME,
                account_id=self._account_id,
                account_name=self._account_name,
                permissions=["read_campaigns", "read_insertion_orders", "read_line_items"],
            )
            
        except ImportError as e:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="google-api-python-client library not installed",
                platform=self.PLATFORM_NAME,
                error_details=f"Install with: pip install google-api-python-client google-auth. Error: {e}",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message="Failed to connect to DV360",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """Fetch campaigns from DV360 API."""
        if not self._service:
            raise RuntimeError("Service not initialized. Call test_connection() first.")
        
        advertiser_id = self._advertiser_id or self.credentials.get("advertiser_id")
        
        if not advertiser_id:
            # List advertisers under partner and get campaigns from first one
            if self._partner_id:
                advertisers = self._service.advertisers().list(
                    partnerId=self._partner_id
                ).execute()
                if advertisers.get('advertisers'):
                    advertiser_id = advertisers['advertisers'][0]['advertiserId']
        
        if not advertiser_id:
            return []
        
        # List campaigns
        campaigns_response = self._service.advertisers().campaigns().list(
            advertiserId=advertiser_id
        ).execute()
        
        campaigns = []
        for camp in campaigns_response.get('campaigns', []):
            # Get campaign flight dates
            flight = camp.get('campaignFlight', {})
            planned_dates = flight.get('plannedDates', {})
            
            campaigns.append(Campaign(
                id=camp['campaignId'],
                name=camp['displayName'],
                status=camp.get('entityStatus', 'UNKNOWN'),
                platform=self.PLATFORM_NAME,
                account_id=advertiser_id,
                budget=0.0,  # Budget is at IO/LI level
                spend=0.0,   # Needs reporting API
                impressions=0,
                clicks=0,
                conversions=0.0,
                start_date=datetime.strptime(planned_dates.get('startDate', {}).get('year', ''), "%Y") if planned_dates.get('startDate') else None,
                end_date=None,
                objective=camp.get('campaignGoal', {}).get('campaignGoalType'),
                extra={
                    "advertiser_id": advertiser_id,
                    "campaign_goal": camp.get('campaignGoal'),
                },
            ))
        
        return campaigns
    
    def _get_performance_real(
        self,
        start_date: datetime,
        end_date: datetime,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """Fetch performance metrics from DV360 API."""
        # Note: DV360 uses a separate Reporting API for metrics
        # This would require setting up queries and polling for results
        
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME,
            account_id=self._account_id or "",
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            spend=0.0,
            impressions=0,
            clicks=0,
            conversions=0.0,
            revenue=0.0,
        )
    
    def _get_mock_campaigns(self) -> List[Campaign]:
        """Return mock DV360 campaign data."""
        campaigns = []
        account = DV360_ACCOUNT
        
        for c in DV360_CAMPAIGNS:
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
                objective=c["campaign_goal_type"],
                extra={
                    "partner_id": c["partner_id"],
                    "advertiser_id": c["advertiser_id"],
                    "insertion_order_id": c["insertion_order_id"],
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
        account = DV360_ACCOUNT
        
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
        """Get accessible DV360 accounts (partners/advertisers)."""
        if self.use_mock:
            return [DV360_ACCOUNT]
        
        if not self._service:
            return []
        
        accounts = []
        
        # List partners if we have access
        try:
            partners = self._service.partners().list().execute()
            for p in partners.get('partners', []):
                accounts.append({
                    "id": p['partnerId'],
                    "name": p.get('displayName', 'Unknown'),
                    "type": "partner",
                    "status": p.get('entityStatus', 'UNKNOWN'),
                })
        except Exception:
            pass
        
        # List advertisers
        if self._partner_id:
            try:
                advertisers = self._service.advertisers().list(
                    partnerId=self._partner_id
                ).execute()
                for a in advertisers.get('advertisers', []):
                    accounts.append({
                        "id": a['advertiserId'],
                        "name": a.get('displayName', 'Unknown'),
                        "type": "advertiser",
                        "status": a.get('entityStatus', 'UNKNOWN'),
                    })
            except Exception:
                pass
        
        return accounts
    
    def get_insertion_orders(self, campaign_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get insertion orders, optionally filtered by campaign."""
        if self.use_mock:
            return [
                {"id": "io_001", "name": "IO - Q4 Awareness", "status": "ACTIVE", "budget": 50000.0},
                {"id": "io_002", "name": "IO - Performance", "status": "ACTIVE", "budget": 30000.0},
            ]
        
        if not self._service:
            return []
        
        advertiser_id = self._advertiser_id or self.credentials.get("advertiser_id")
        if not advertiser_id:
            return []
        
        params = {"advertiserId": advertiser_id}
        if campaign_id:
            params["filter"] = f'campaignId="{campaign_id}"'
        
        ios = self._service.advertisers().insertionOrders().list(**params).execute()
        
        return [
            {
                "id": io['insertionOrderId'],
                "name": io.get('displayName', 'Unknown'),
                "status": io.get('entityStatus', 'UNKNOWN'),
                "campaign_id": io.get('campaignId'),
            }
            for io in ios.get('insertionOrders', [])
        ]
