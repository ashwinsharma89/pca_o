
import pytest
import pandas as pd
from datetime import date
from unittest.mock import Mock, MagicMock
from src.engine.services.campaign_service import CampaignService

class TestCampaignServiceImport:
    """Test suite for CampaignService data import logic."""

    @pytest.fixture
    def campaign_service(self):
        """Create service with mocked repos."""
        mock_campaign_repo = Mock()
        mock_analysis_repo = Mock()
        mock_context_repo = Mock()
        mock_duckdb_repo = Mock()
        
        # Configure bulk create to return list of created items same length as input
        mock_campaign_repo.create_bulk.side_effect = lambda x: [Mock(id=i) for i in range(len(x))]
        
        return CampaignService(
            campaign_repo=mock_campaign_repo,
            analysis_repo=mock_analysis_repo,
            context_repo=mock_context_repo,
            duckdb_repo=mock_duckdb_repo
        )

    def test_import_from_dataframe_success(self, campaign_service):
        """Should successfully import standard campaign dataframe."""
        # Create valid dataframe
        df = pd.DataFrame({
            'Campaign Name': ['Test Campaign 1', 'Test Campaign 2'],
            'Platform': ['Google', 'Meta'],
            'Spend': [100.0, 200.0],
            'Impressions': [1000, 2000],
            'Clicks': [10, 20],
            'Conversions': [1, 2],
            'Date': ['2024-01-01', '2024-01-02']
        })
        
        result = campaign_service.import_from_dataframe(df)
        
        assert result['success'] is True
        assert result['imported_count'] == 2
        assert result['summary']['total_spend'] == 300.0
        
        # Verify repo calls
        campaign_service.campaign_repo.create_bulk.assert_called_once()
        campaign_service.campaign_repo.commit.assert_called_once()
        
        # Verify call args
        call_args = campaign_service.campaign_repo.create_bulk.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0]['campaign_name'] == 'Test Campaign 1'
        assert call_args[0]['platform'] == 'Google'

    def test_import_from_dataframe_aliasing(self, campaign_service):
        """Should handle column aliases correctly."""
        # DataFrame with weird column names
        df = pd.DataFrame({
            'Campaign': ['Alias Test'],
            'Total Spent': [50.0],  # Alias for Spend
            'Views': [500],         # Alias for Impressions
            'Link Clicks': [5],     # Alias for Clicks
            'Results': [1]          # Alias for Conversions
        })
        
        result = campaign_service.import_from_dataframe(df)
        
        assert result['success'] is True
        call_args = campaign_service.campaign_repo.create_bulk.call_args[0][0]
        assert call_args[0]['spend'] == 50.0
        assert call_args[0]['impressions'] == 500
        assert call_args[0]['clicks'] == 5
        assert call_args[0]['conversions'] == 1

    def test_import_from_dataframe_empty(self, campaign_service):
        """Should handle empty dataframe gracefully."""
        df = pd.DataFrame()
        result = campaign_service.import_from_dataframe(df)
        
        assert result['success'] is True
        assert result['imported_count'] == 0
        campaign_service.campaign_repo.create_bulk.assert_called_once_with([])

    def test_import_failure_rollback(self, campaign_service):
        """Should rollback transaction on failure."""
        df = pd.DataFrame({'Campaign': ['Fail']})
        
        # Simualte repo error
        campaign_service.campaign_repo.create_bulk.side_effect = Exception("DB Error")
        
        result = campaign_service.import_from_dataframe(df)
        
        assert result['success'] is False
        assert "DB Error" in result['message']
        campaign_service.campaign_repo.rollback.assert_called_once()
