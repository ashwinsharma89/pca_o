"""
Unit tests for Ad Platform Connectors.

Tests all 4 platform connectors and the unified manager
in both mock mode and credential validation.
"""

import pytest
from datetime import datetime, timedelta

from src.platform.connectors import (
    BaseAdConnector,
    GoogleAdsConnector,
    MetaAdsConnector,
    CampaignManagerConnector,
    DV360Connector,
    AdConnectorManager,
    ConnectorStatus,
)
from src.platform.connectors.base_connector import Campaign, PerformanceMetrics


class TestBaseConnector:
    """Tests for base connector functionality."""
    
    def test_campaign_metrics(self):
        """Test Campaign calculated metrics."""
        campaign = Campaign(
            id="test_001",
            name="Test Campaign",
            status="ENABLED",
            platform="test",
            account_id="acc_123",
            budget=10000.0,
            spend=5000.0,
            impressions=100000,
            clicks=2500,
            conversions=125.0,
        )
        
        # Test calculated metrics
        assert campaign.ctr == 2.5  # (2500/100000) * 100
        assert campaign.cpc == 2.0  # 5000/2500
        assert campaign.cpa == 40.0  # 5000/125
    
    def test_campaign_zero_division(self):
        """Test Campaign handles zero values gracefully."""
        campaign = Campaign(
            id="test_002",
            name="Empty Campaign",
            status="ENABLED",
            platform="test",
            account_id="acc_123",
            budget=10000.0,
            spend=0.0,
            impressions=0,
            clicks=0,
            conversions=0.0,
        )
        
        assert campaign.ctr == 0.0
        assert campaign.cpc == 0.0
        assert campaign.cpa == 0.0
    
    def test_performance_metrics(self):
        """Test PerformanceMetrics calculated fields."""
        metrics = PerformanceMetrics(
            platform="test",
            account_id="acc_123",
            date_range={"start": "2024-01-01", "end": "2024-01-31"},
            spend=10000.0,
            impressions=500000,
            clicks=15000,
            conversions=500.0,
            revenue=25000.0,
        )
        
        assert metrics.ctr == 3.0  # (15000/500000) * 100
        assert metrics.cpc == pytest.approx(0.67, 0.01)  # 10000/15000
        assert metrics.roas == 2.5  # 25000/10000


class TestGoogleAdsConnector:
    """Tests for Google Ads Connector."""
    
    def test_mock_connection(self):
        """Test connection in mock mode."""
        connector = GoogleAdsConnector(use_mock=True)
        result = connector.test_connection()
        
        assert result.success is True
        assert result.status == ConnectorStatus.MOCK
        assert result.is_mock is True
        assert result.platform == "google_ads"
        assert result.account_id is not None
    
    def test_mock_campaigns(self):
        """Test fetching campaigns in mock mode."""
        connector = GoogleAdsConnector(use_mock=True)
        campaigns = connector.get_campaigns()
        
        assert len(campaigns) > 0
        assert all(c.platform == "google_ads" for c in campaigns)
        assert all(isinstance(c, Campaign) for c in campaigns)
        
        # Check campaign has expected fields
        first = campaigns[0]
        assert first.id is not None
        assert first.name is not None
        assert first.spend >= 0
    
    def test_mock_performance(self):
        """Test fetching performance in mock mode."""
        connector = GoogleAdsConnector(use_mock=True)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)
        
        performance = connector.get_performance(start_date, end_date)
        
        assert isinstance(performance, PerformanceMetrics)
        assert performance.platform == "google_ads"
        assert performance.spend > 0
        assert performance.impressions > 0
    
    def test_mock_accounts(self):
        """Test fetching accounts in mock mode."""
        connector = GoogleAdsConnector(use_mock=True)
        accounts = connector.get_accounts()
        
        assert len(accounts) > 0
        assert "id" in accounts[0]
        assert "name" in accounts[0]
    
    def test_credential_validation_missing(self):
        """Test that missing credentials are properly detected."""
        connector = GoogleAdsConnector(use_mock=False)
        # _validate_credentials should return False with no creds
        assert connector._validate_credentials() is False


class TestMetaAdsConnector:
    """Tests for Meta Ads Connector."""
    
    def test_mock_connection(self):
        """Test connection in mock mode."""
        connector = MetaAdsConnector(use_mock=True)
        result = connector.test_connection()
        
        assert result.success is True
        assert result.status == ConnectorStatus.MOCK
        assert result.platform == "meta_ads"
    
    def test_mock_campaigns(self):
        """Test fetching campaigns in mock mode."""
        connector = MetaAdsConnector(use_mock=True)
        campaigns = connector.get_campaigns()
        
        assert len(campaigns) > 0
        assert all(c.platform == "meta_ads" for c in campaigns)
        
        # Check for Meta-specific fields
        for c in campaigns:
            assert "optimization_goal" in c.extra or "buying_type" in c.extra
    
    def test_mock_performance(self):
        """Test fetching performance in mock mode."""
        connector = MetaAdsConnector(use_mock=True)
        performance = connector.get_performance()
        
        assert performance.platform == "meta_ads"
        assert performance.spend > 0


class TestCampaignManagerConnector:
    """Tests for Campaign Manager 360 Connector."""
    
    def test_mock_connection(self):
        """Test connection in mock mode."""
        connector = CampaignManagerConnector(use_mock=True)
        result = connector.test_connection()
        
        assert result.success is True
        assert result.status == ConnectorStatus.MOCK
        assert result.platform == "campaign_manager"
    
    def test_mock_campaigns(self):
        """Test fetching campaigns in mock mode."""
        connector = CampaignManagerConnector(use_mock=True)
        campaigns = connector.get_campaigns()
        
        assert len(campaigns) > 0
        assert all(c.platform == "campaign_manager" for c in campaigns)
        
        # Check for CM360-specific fields
        for c in campaigns:
            assert "advertiser_id" in c.extra
    
    def test_mock_advertisers(self):
        """Test fetching advertisers in mock mode."""
        connector = CampaignManagerConnector(use_mock=True)
        advertisers = connector.get_advertisers()
        
        assert len(advertisers) > 0
        assert "id" in advertisers[0]
        assert "name" in advertisers[0]


class TestDV360Connector:
    """Tests for DV360 Connector."""
    
    def test_mock_connection(self):
        """Test connection in mock mode."""
        connector = DV360Connector(use_mock=True)
        result = connector.test_connection()
        
        assert result.success is True
        assert result.status == ConnectorStatus.MOCK
        assert result.platform == "dv360"
    
    def test_mock_campaigns(self):
        """Test fetching campaigns in mock mode."""
        connector = DV360Connector(use_mock=True)
        campaigns = connector.get_campaigns()
        
        assert len(campaigns) > 0
        assert all(c.platform == "dv360" for c in campaigns)
        
        # Check for DV360-specific fields
        for c in campaigns:
            assert "advertiser_id" in c.extra or "insertion_order_id" in c.extra
    
    def test_mock_insertion_orders(self):
        """Test fetching insertion orders in mock mode."""
        connector = DV360Connector(use_mock=True)
        ios = connector.get_insertion_orders()
        
        assert len(ios) > 0
        assert "id" in ios[0]


class TestAdConnectorManager:
    """Tests for the unified connector manager."""
    
    def test_init_all_platforms(self):
        """Test initializing manager with all platforms."""
        manager = AdConnectorManager(use_mock=True)
        
        assert manager.get_connector("google_ads") is not None
        assert manager.get_connector("meta_ads") is not None
        assert manager.get_connector("campaign_manager") is not None
        assert manager.get_connector("dv360") is not None
    
    def test_init_specific_platforms(self):
        """Test initializing manager with specific platforms only."""
        manager = AdConnectorManager(
            use_mock=True, 
            platforms=["google_ads", "meta_ads"]
        )
        
        assert manager.get_connector("google_ads") is not None
        assert manager.get_connector("meta_ads") is not None
        assert manager.get_connector("dv360") is None
    
    def test_test_all_connections(self):
        """Test connection testing for all platforms."""
        manager = AdConnectorManager(use_mock=True)
        results = manager.test_all_connections()
        
        assert len(results) == 12
        assert all(r.success for r in results.values())
        assert all(r.is_mock for r in results.values())
    
    def test_get_connection_status(self):
        """Test getting status summary."""
        manager = AdConnectorManager(use_mock=True)
        status = manager.get_connection_status()
        
        assert len(status) == 12
        assert all(s["connected"] for s in status.values())
        assert all(s["is_mock"] for s in status.values())
    
    def test_get_all_campaigns(self):
        """Test fetching campaigns from all platforms."""
        manager = AdConnectorManager(use_mock=True)
        campaigns = manager.get_all_campaigns()
        
        # Should have campaigns from all 4 platforms
        platforms = set(c.platform for c in campaigns)
        assert "google_ads" in platforms
        assert "meta_ads" in platforms
        assert "campaign_manager" in platforms
        assert "dv360" in platforms
    
    def test_get_campaigns_by_platform(self):
        """Test fetching campaigns grouped by platform."""
        manager = AdConnectorManager(use_mock=True)
        campaigns = manager.get_campaigns(["google_ads", "meta_ads"])
        
        assert "google_ads" in campaigns
        assert "meta_ads" in campaigns
        assert "dv360" not in campaigns
    
    def test_get_aggregated_performance(self):
        """Test aggregated performance metrics."""
        manager = AdConnectorManager(use_mock=True)
        performance = manager.get_aggregated_performance()
        
        assert "totals" in performance
        assert "by_platform" in performance
        assert performance["is_mock"] is True
        
        totals = performance["totals"]
        assert totals["spend"] > 0
        assert totals["impressions"] > 0
        assert "ctr" in totals
        assert "roas" in totals
    
    def test_disconnect_all(self):
        """Test disconnecting from all platforms."""
        manager = AdConnectorManager(use_mock=True)
        manager.disconnect_all()
        
        # Should not raise any errors
        assert True


class TestConnectorIntegration:
    """Integration tests for connector workflow."""
    
    def test_full_workflow(self):
        """Test complete workflow: connect, fetch, aggregate."""
        manager = AdConnectorManager(use_mock=True)
        
        # Step 1: Test all connections
        status = manager.get_connection_status()
        assert all(s["connected"] for s in status.values())
        
        # Step 2: Fetch campaigns
        campaigns = manager.get_all_campaigns()
        assert len(campaigns) > 0
        
        # Step 3: Get aggregated performance
        performance = manager.get_aggregated_performance()
        assert performance["totals"]["spend"] > 0
        
        # Step 4: Verify data consistency
        total_spend_from_campaigns = sum(c.spend for c in campaigns)
        assert total_spend_from_campaigns > 0
        
        # Step 5: Cleanup
        manager.disconnect_all()
    
    def test_campaign_to_dict(self):
        """Test campaign serialization."""
        manager = AdConnectorManager(use_mock=True)
        campaigns = manager.get_all_campaigns()
        
        for campaign in campaigns[:3]:  # Test first 3
            data = campaign.to_dict()
            
            assert "id" in data
            assert "name" in data
            assert "spend" in data
            assert "ctr" in data
            assert "platform" in data
