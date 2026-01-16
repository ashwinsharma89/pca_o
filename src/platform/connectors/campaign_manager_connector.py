"""
Campaign Manager 360 (DCM) API Connector.

Supports Campaign Manager 360 Reporting API with Service Account authentication.
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
    CM360_CAMPAIGNS,
    CM360_ACCOUNT,
    get_mock_performance_data,
)


class CampaignManagerConnector(BaseAdConnector):
    """
    Campaign Manager 360 (DCM) API connector.
    
    Supports:
    - Service Account authentication
    - User profile access
    - Campaign and placement reporting
    
    Credentials needed:
    - GOOGLE_CLOUD_PROJECT_ID: GCP Project ID
    - GOOGLE_SERVICE_ACCOUNT_PATH: Path to service account JSON file
    - CM360_PROFILE_ID: Campaign Manager profile ID
    """
    
    PLATFORM_NAME = "campaign_manager"
    API_VERSION = "v4"
    
    def __init__(self, use_mock: bool = None, **credentials):
        """
        Initialize Campaign Manager 360 connector.
        
        Args:
            use_mock: Use mock mode for testing.
            **credentials: Override credentials (otherwise uses env vars).
        """
        creds = {
            "project_id": credentials.get("project_id") or os.getenv("GOOGLE_CLOUD_PROJECT_ID"),
            "service_account_path": credentials.get("service_account_path") or os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH"),
            "profile_id": credentials.get("profile_id") or os.getenv("CM360_PROFILE_ID"),
        }
        
        super().__init__(use_mock=use_mock, **creds)
        self._service = None
        self._profile_id = None
    
    def _validate_credentials(self) -> bool:
        """Check that all required credentials are present."""
        # Service account path or Application Default Credentials
        has_service_account = self.credentials.get("service_account_path")
        has_adc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        if not (has_service_account or has_adc):
            logger.warning("Missing Campaign Manager credentials: service_account_path or GOOGLE_APPLICATION_CREDENTIALS")
            return False
        return True
    
    def _connect_real(self) -> ConnectionResult:
        """Establish real connection to Campaign Manager 360 API."""
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            
            SCOPES = ['https://www.googleapis.com/auth/dfareporting']
            
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
            self._service = build('dfareporting', self.API_VERSION, credentials=credentials)
            
            # Get user profiles to verify access
            profiles = self._service.userProfiles().list().execute()
            
            if not profiles.get('items'):
                return ConnectionResult(
                    success=False,
                    status=ConnectorStatus.ERROR,
                    message="No CM360 profiles accessible",
                    platform=self.PLATFORM_NAME,
                    error_details="User has no accessible Campaign Manager profiles",
                )
            
            # Use first profile or specified profile
            profile = profiles['items'][0]
            if self.credentials.get("profile_id"):
                for p in profiles['items']:
                    if str(p['profileId']) == str(self.credentials['profile_id']):
                        profile = p
                        break
            
            self._profile_id = profile['profileId']
            self._account_id = str(profile['profileId'])
            self._account_name = profile.get('userName', 'CM360 User')
            
            return ConnectionResult(
                success=True,
                status=ConnectorStatus.CONNECTED,
                message="Successfully connected to Campaign Manager 360",
                platform=self.PLATFORM_NAME,
                account_id=self._account_id,
                account_name=self._account_name,
                permissions=["read_campaigns", "read_reports", "read_advertisers"],
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
                message="Failed to connect to Campaign Manager 360",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def _get_campaigns_real(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """Fetch campaigns from Campaign Manager 360 API."""
        if not self._service or not self._profile_id:
            raise RuntimeError("Service not initialized. Call test_connection() first.")
        
        # List campaigns
        campaigns_response = self._service.campaigns().list(
            profileId=self._profile_id
        ).execute()
        
        campaigns = []
        for camp in campaigns_response.get('campaigns', []):
            # Get basic metrics (would need report API for detailed metrics)
            campaigns.append(Campaign(
                id=str(camp['id']),
                name=camp['name'],
                status="ACTIVE" if camp.get('archived') is False else "ARCHIVED",
                platform=self.PLATFORM_NAME,
                account_id=self._account_id,
                budget=0.0,  # Budget not directly available, needs trafficking
                spend=0.0,   # Needs report
                impressions=0,
                clicks=0,
                conversions=0.0,
                start_date=datetime.strptime(camp['startDate'], "%Y-%m-%d") if camp.get('startDate') else None,
                end_date=datetime.strptime(camp['endDate'], "%Y-%m-%d") if camp.get('endDate') else None,
                objective=None,
                extra={
                    "advertiser_id": camp.get('advertiserId'),
                    "billing_invoice_code": camp.get('billingInvoiceCode'),
                },
            ))
        
        return campaigns
    
    def _get_performance_real(
        self,
        start_date: datetime,
        end_date: datetime,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """Fetch performance metrics from Campaign Manager 360 API."""
        if not self._service or not self._profile_id:
            raise RuntimeError("Service not initialized. Call test_connection() first.")
        
        # Create a report request
        report = {
            'name': 'API Performance Report',
            'type': 'STANDARD',
            'criteria': {
                'dateRange': {
                    'startDate': start_date.strftime("%Y-%m-%d"),
                    'endDate': end_date.strftime("%Y-%m-%d"),
                },
                'dimensions': [{'name': 'date'}],
                'metricNames': [
                    'impressions',
                    'clicks',
                    'totalConversions',
                    'mediaCost',
                ],
            }
        }
        
        # Insert and run the report
        inserted_report = self._service.reports().insert(
            profileId=self._profile_id,
            body=report
        ).execute()
        
        # Run the report
        file = self._service.reports().run(
            profileId=self._profile_id,
            reportId=inserted_report['id']
        ).execute()
        
        # Note: In real implementation, you'd poll for completion and download
        # For now, return placeholder
        return PerformanceMetrics(
            platform=self.PLATFORM_NAME,
            account_id=self._account_id,
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
        """Return mock Campaign Manager 360 campaign data."""
        campaigns = []
        account = CM360_ACCOUNT
        
        for c in CM360_CAMPAIGNS:
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
                objective=None,
                extra={
                    "advertiser_id": c["advertiser_id"],
                    "advertiser_name": c["advertiser_name"],
                    "billing_invoice_code": c["billing_invoice_code"],
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
        account = CM360_ACCOUNT
        
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
        """Get accessible Campaign Manager profiles."""
        if self.use_mock:
            return [CM360_ACCOUNT]
        
        if not self._service:
            return []
        
        profiles = self._service.userProfiles().list().execute()
        return [
            {
                "id": str(p['profileId']),
                "name": p.get('userName', 'Unknown'),
                "currency": "USD",
                "timezone": "America/Chicago",
                "status": "active",
            }
            for p in profiles.get('items', [])
        ]
    
    def get_advertisers(self) -> List[Dict[str, Any]]:
        """Get advertisers accessible to this profile."""
        if self.use_mock:
            return [
                {"id": "adv_001", "name": "Demo Brand Inc."},
                {"id": "adv_002", "name": "Secondary Brand LLC"},
            ]
        
        if not self._service or not self._profile_id:
            return []
        
        advertisers = self._service.advertisers().list(
            profileId=self._profile_id
        ).execute()
        
        return [
            {
                "id": str(a['id']),
                "name": a['name'],
                "status": a.get('status', 'UNKNOWN'),
            }
            for a in advertisers.get('advertisers', [])
        ]
