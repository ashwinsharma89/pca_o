"""
Base connector class for ad platform integrations.

Provides a unified interface for all ad platform connectors with
mock mode support for testing without real accounts.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
import os

from loguru import logger


class ConnectorStatus(str, Enum):
    """Connection status states."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    MOCK = "mock"
    AUTHENTICATING = "authenticating"


@dataclass
class ConnectionResult:
    """Result of a connection test."""
    success: bool
    status: ConnectorStatus
    message: str
    platform: str
    account_id: Optional[str] = None
    account_name: Optional[str] = None
    permissions: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    is_mock: bool = False
    error_details: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "status": self.status.value,
            "message": self.message,
            "platform": self.platform,
            "account_id": self.account_id,
            "account_name": self.account_name,
            "permissions": self.permissions,
            "timestamp": self.timestamp.isoformat(),
            "is_mock": self.is_mock,
            "error_details": self.error_details,
        }


@dataclass
class Campaign:
    """Unified campaign representation across platforms."""
    id: str
    name: str
    status: str
    platform: str
    account_id: str
    budget: float
    spend: float
    impressions: int
    clicks: int
    conversions: float
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    objective: Optional[str] = None
    
    # Platform-specific fields
    extra: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def ctr(self) -> float:
        """Click-through rate."""
        return (self.clicks / self.impressions * 100) if self.impressions > 0 else 0.0
    
    @property
    def cpc(self) -> float:
        """Cost per click."""
        return self.spend / self.clicks if self.clicks > 0 else 0.0
    
    @property
    def cpa(self) -> float:
        """Cost per acquisition."""
        return self.spend / self.conversions if self.conversions > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "platform": self.platform,
            "account_id": self.account_id,
            "budget": self.budget,
            "spend": self.spend,
            "impressions": self.impressions,
            "clicks": self.clicks,
            "conversions": self.conversions,
            "ctr": round(self.ctr, 2),
            "cpc": round(self.cpc, 2),
            "cpa": round(self.cpa, 2),
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "objective": self.objective,
            "extra": self.extra,
        }


@dataclass
class PerformanceMetrics:
    """Unified performance metrics across platforms."""
    platform: str
    account_id: str
    date_range: Dict[str, str]
    spend: float
    impressions: int
    clicks: int
    conversions: float
    revenue: float
    
    # Calculated metrics
    @property
    def ctr(self) -> float:
        return (self.clicks / self.impressions * 100) if self.impressions > 0 else 0.0
    
    @property
    def cpc(self) -> float:
        return self.spend / self.clicks if self.clicks > 0 else 0.0
    
    @property
    def roas(self) -> float:
        return self.revenue / self.spend if self.spend > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "platform": self.platform,
            "account_id": self.account_id,
            "date_range": self.date_range,
            "spend": self.spend,
            "impressions": self.impressions,
            "clicks": self.clicks,
            "conversions": self.conversions,
            "revenue": self.revenue,
            "ctr": round(self.ctr, 2),
            "cpc": round(self.cpc, 2),
            "roas": round(self.roas, 2),
        }


class BaseAdConnector(ABC):
    """
    Abstract base class for ad platform connectors.
    
    All platform-specific connectors inherit from this class and implement
    the abstract methods. Supports mock mode for testing without real credentials.
    """
    
    PLATFORM_NAME: str = "base"
    
    def __init__(self, use_mock: bool = None, **credentials):
        """
        Initialize connector.
        
        Args:
            use_mock: If True, use mock responses. If None, check env var AD_CONNECTORS_MOCK_MODE.
            **credentials: Platform-specific credentials.
        """
        # Determine mock mode from param or environment
        if use_mock is None:
            use_mock = os.getenv("AD_CONNECTORS_MOCK_MODE", "true").lower() == "true"
        
        self.use_mock = use_mock
        self.credentials = credentials
        self._connected = False
        self._account_id: Optional[str] = None
        self._account_name: Optional[str] = None
        
        logger.info(
            f"Initialized {self.PLATFORM_NAME} connector",
            mock_mode=self.use_mock,
            has_credentials=bool(credentials)
        )
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected or self.use_mock
    
    @abstractmethod
    def _validate_credentials(self) -> bool:
        """Validate that required credentials are present."""
        pass
    
    @abstractmethod
    def _connect_real(self) -> ConnectionResult:
        """Establish real connection to the platform API."""
        pass
    
    @abstractmethod
    def _get_campaigns_real(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """Fetch campaigns from real API."""
        pass
    
    @abstractmethod
    def _get_performance_real(
        self,
        start_date: datetime,
        end_date: datetime,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """Fetch performance metrics from real API."""
        pass
    
    @abstractmethod
    def _get_mock_campaigns(self) -> List[Campaign]:
        """Return mock campaign data."""
        pass
    
    @abstractmethod
    def _get_mock_performance(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> PerformanceMetrics:
        """Return mock performance metrics."""
        pass
    
    def test_connection(self) -> ConnectionResult:
        """
        Test connection to the ad platform.
        
        Returns:
            ConnectionResult with status and details.
        """
        if self.use_mock:
            logger.info(f"Testing {self.PLATFORM_NAME} connection (MOCK MODE)")
            self._connected = True
            return ConnectionResult(
                success=True,
                status=ConnectorStatus.MOCK,
                message=f"Mock connection to {self.PLATFORM_NAME} successful",
                platform=self.PLATFORM_NAME,
                account_id="mock_account_123",
                account_name=f"Mock {self.PLATFORM_NAME} Account",
                permissions=["read_campaigns", "read_reports", "read_insights"],
                is_mock=True,
            )
        
        # Validate credentials first
        if not self._validate_credentials():
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message=f"Missing required credentials for {self.PLATFORM_NAME}",
                platform=self.PLATFORM_NAME,
                error_details="Check that all required credentials are configured",
            )
        
        try:
            result = self._connect_real()
            self._connected = result.success
            return result
        except Exception as e:
            logger.error(f"Connection failed for {self.PLATFORM_NAME}: {e}")
            return ConnectionResult(
                success=False,
                status=ConnectorStatus.ERROR,
                message=f"Connection to {self.PLATFORM_NAME} failed",
                platform=self.PLATFORM_NAME,
                error_details=str(e),
            )
    
    def get_campaigns(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Campaign]:
        """
        Fetch campaigns from the platform.
        
        Args:
            start_date: Optional start date filter.
            end_date: Optional end date filter.
            
        Returns:
            List of Campaign objects.
        """
        if self.use_mock:
            logger.info(f"Fetching campaigns from {self.PLATFORM_NAME} (MOCK MODE)")
            return self._get_mock_campaigns()
        
        if not self.is_connected:
            raise RuntimeError(f"Not connected to {self.PLATFORM_NAME}. Call test_connection() first.")
        
        return self._get_campaigns_real(start_date, end_date)
    
    def get_performance(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        campaign_ids: Optional[List[str]] = None,
    ) -> PerformanceMetrics:
        """
        Fetch performance metrics from the platform.
        
        Args:
            start_date: Start date for metrics (default: 30 days ago).
            end_date: End date for metrics (default: today).
            campaign_ids: Optional list of campaign IDs to filter.
            
        Returns:
            PerformanceMetrics object.
        """
        # Default date range: last 30 days
        if end_date is None:
            end_date = datetime.utcnow()
        if start_date is None:
            start_date = end_date - timedelta(days=30)
        
        if self.use_mock:
            logger.info(f"Fetching performance from {self.PLATFORM_NAME} (MOCK MODE)")
            return self._get_mock_performance(start_date, end_date)
        
        if not self.is_connected:
            raise RuntimeError(f"Not connected to {self.PLATFORM_NAME}. Call test_connection() first.")
        
        return self._get_performance_real(start_date, end_date, campaign_ids)
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        """
        Get accessible ad accounts.
        
        Returns:
            List of account dictionaries.
        """
        if self.use_mock:
            return [
                {
                    "id": "mock_account_123",
                    "name": f"Mock {self.PLATFORM_NAME} Account",
                    "currency": "USD",
                    "timezone": "America/Los_Angeles",
                    "status": "active",
                }
            ]
        
        # Override in subclasses for real implementation
        return []
    
    def disconnect(self):
        """Disconnect from the platform."""
        self._connected = False
        logger.info(f"Disconnected from {self.PLATFORM_NAME}")
