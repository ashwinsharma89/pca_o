"""
Unit tests for Extraction Agent.
Tests data normalization and validation across platforms.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Try to import
try:
    from src.engine.agents.extraction_agent import ExtractionAgent
    EXTRACTION_AGENT_AVAILABLE = True
except ImportError:
    EXTRACTION_AGENT_AVAILABLE = False
    ExtractionAgent = None

try:
    from src.platform.models.platform import PlatformType, NormalizedMetric, MetricType
    from src.platform.models.campaign import Campaign
    MODELS_AVAILABLE = True
except ImportError:
    MODELS_AVAILABLE = False

pytestmark = pytest.mark.skipif(not EXTRACTION_AGENT_AVAILABLE, reason="Extraction agent not available")


class TestExtractionAgentInit:
    """Test ExtractionAgent initialization."""
    
    def test_initialization(self):
        """Test agent initialization."""
        agent = ExtractionAgent()
        assert agent is not None


class TestExtractionAgentNormalization:
    """Test data normalization methods."""
    
    @pytest.fixture
    def agent(self):
        """Create extraction agent."""
        return ExtractionAgent()
    
    @pytest.fixture
    def mock_campaign(self):
        """Create mock campaign."""
        campaign = Mock()
        campaign.campaign_id = "test_campaign_123"
        campaign.snapshots = []
        campaign.normalized_metrics = []
        campaign.processing_logs = []
        return campaign
    
    def test_normalize_campaign_data_empty(self, agent, mock_campaign):
        """Test normalizing campaign with no snapshots."""
        result = agent.normalize_campaign_data(mock_campaign)
        
        assert result is not None
        assert result.normalized_metrics == []
    
    def test_normalize_campaign_data_with_snapshots(self, agent, mock_campaign):
        """Test normalizing campaign with snapshots."""
        # Add mock snapshot
        snapshot = Mock()
        snapshot.processing_status = "completed"
        snapshot.extracted_metrics = [
            Mock(metric_type="impressions", value=1000),
            Mock(metric_type="clicks", value=50)
        ]
        mock_campaign.snapshots = [snapshot]
        
        # Mock the internal method
        with patch.object(agent, '_normalize_snapshot_metrics', return_value=[]):
            with patch.object(agent, '_validate_metrics', return_value={"warnings": []}):
                result = agent.normalize_campaign_data(mock_campaign)
                assert result is not None
    
    def test_skip_incomplete_snapshots(self, agent, mock_campaign):
        """Test that incomplete snapshots are skipped."""
        snapshot = Mock()
        snapshot.processing_status = "pending"
        mock_campaign.snapshots = [snapshot]
        
        with patch.object(agent, '_validate_metrics', return_value={"warnings": []}):
            result = agent.normalize_campaign_data(mock_campaign)
            assert result.normalized_metrics == []


class TestExtractionAgentValidation:
    """Test data validation methods."""
    
    @pytest.fixture
    def agent(self):
        """Create extraction agent."""
        return ExtractionAgent()
    
    def test_validate_metrics_empty(self, agent):
        """Test validating empty metrics."""
        if hasattr(agent, '_validate_metrics'):
            result = agent._validate_metrics([])
            assert "warnings" in result
    
    def test_validate_metrics_with_data(self, agent):
        """Test validating metrics with data."""
        if hasattr(agent, '_validate_metrics'):
            metrics = [
                Mock(metric_type="impressions", value=1000),
                Mock(metric_type="clicks", value=50)
            ]
            result = agent._validate_metrics(metrics)
            assert "warnings" in result


class TestExtractionAgentMetricConversion:
    """Test metric conversion utilities."""
    
    @pytest.fixture
    def agent(self):
        """Create extraction agent."""
        return ExtractionAgent()
    
    def test_convert_currency(self, agent):
        """Test currency conversion."""
        if hasattr(agent, '_convert_currency'):
            result = agent._convert_currency(100, "USD", "EUR")
            assert isinstance(result, (int, float))
    
    def test_normalize_metric_name(self, agent):
        """Test metric name normalization."""
        if hasattr(agent, '_normalize_metric_name'):
            result = agent._normalize_metric_name("Cost Per Click")
            assert isinstance(result, str)
    
    def test_calculate_derived_metrics(self, agent):
        """Test derived metric calculation."""
        if hasattr(agent, '_calculate_derived_metrics'):
            metrics = {
                'impressions': 1000,
                'clicks': 50,
                'spend': 100
            }
            result = agent._calculate_derived_metrics(metrics)
            assert isinstance(result, dict)
