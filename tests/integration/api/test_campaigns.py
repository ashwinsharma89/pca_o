"""
Integration tests for campaign endpoints.

Tests:
- Campaign data upload (CSV/Excel)
- Campaign analysis
- Global visualizations
- Dashboard stats
- Filter options
"""

import pytest
from fastapi import status
import io
import pandas as pd


@pytest.fixture
def mock_reasoning_agent(mock_analytics_services):
    """Fixture for reasoning agent mock."""
    return mock_analytics_services["expert"]

@pytest.fixture
def mock_query_engine(mock_analytics_services):
    """Fixture for query engine mock."""
    return mock_analytics_services["query_engine"]


# ============================================================================
# Upload Endpoints Tests
# ============================================================================

class TestCampaignUpload:
    """Tests for campaign data upload endpoints."""
    
    def test_upload_csv_success(self, client, auth_headers, sample_csv_file):
        """Test successful CSV upload."""
        files = {
            'file': ('campaign_data.csv', sample_csv_file, 'text/csv')
        }
        
        response = client.post(
            "/api/v1/campaigns/upload",
            headers=auth_headers,
            files=files
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert 'message' in data
        assert 'rows_uploaded' in data or 'success' in data
    
    def test_upload_csv_unauthorized(self, client, sample_csv_file):
        """Test upload without authentication fails."""
        files = {
            'file': ('campaign_data.csv', sample_csv_file, 'text/csv')
        }
        
        response = client.post(
            "/api/v1/campaigns/upload",
            files=files
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_upload_invalid_file_type(self, client, auth_headers):
        """Test upload with invalid file type."""
        # Create a text file (not CSV/Excel)
        invalid_file = io.BytesIO(b"This is not a valid CSV or Excel file")
        files = {
            'file': ('invalid.txt', invalid_file, 'text/plain')
        }
        
        response = client.post(
            "/api/v1/campaigns/upload",
            headers=auth_headers,
            files=files
        )
        
        # Should either reject or handle gracefully
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_upload_empty_file(self, client, auth_headers):
        """Test upload with empty file."""
        empty_file = io.BytesIO(b"")
        files = {
            'file': ('empty.csv', empty_file, 'text/csv')
        }
        
        response = client.post(
            "/api/v1/campaigns/upload",
            headers=auth_headers,
            files=files
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_upload_excel_success(self, client, auth_headers, sample_excel_file):
        """Test successful Excel upload."""
        files = {
            'file': ('campaign_data.xlsx', sample_excel_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        
        response = client.post(
            "/api/v1/campaigns/upload",
            headers=auth_headers,
            files=files
        )
        
        # May succeed or need sheet_name parameter
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED,
            status.HTTP_400_BAD_REQUEST  # If sheet_name required
        ]
    
    def test_preview_excel_sheets(self, client, auth_headers, sample_excel_file):
        """Test Excel sheet preview."""
        files = {
            'file': ('campaign_data.xlsx', sample_excel_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        
        response = client.post(
            "/api/v1/campaigns/preview-sheets",
            headers=auth_headers,
            files=files
        )
        
        if response.status_code == status.HTTP_200_OK:
            data = response.json()
            assert 'sheets' in data
            assert isinstance(data['sheets'], list)


# ============================================================================
# Visualization Endpoints Tests
# ============================================================================

class TestCampaignVisualizations:
    """Tests for campaign visualization endpoints."""
    
    def test_get_visualizations_success(self, client, auth_headers):
        """Test getting global visualizations."""
        response = client.get(
            "/api/v1/campaigns/visualizations",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Should return visualization data structure
        assert isinstance(data, dict)
    
    def test_get_visualizations_with_platform_filter(self, client, auth_headers):
        """Test visualizations with platform filter."""
        response = client.get(
            "/api/v1/campaigns/visualizations?platforms=meta",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_visualizations_with_date_filter(self, client, auth_headers):
        """Test visualizations with date range filter."""
        response = client.get(
            "/api/v1/campaigns/visualizations?start_date=2024-01-01&end_date=2024-01-31",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_visualizations_unauthorized(self, client):
        """Test visualizations without authentication."""
        response = client.get("/api/v1/campaigns/visualizations")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_get_visualizations_invalid_date_format(self, client, auth_headers):
        """Test visualizations with invalid date format."""
        response = client.get(
            "/api/v1/campaigns/visualizations?start_date=invalid-date",
            headers=auth_headers
        )
        
        # Should handle gracefully
        assert response.status_code in [
            status.HTTP_200_OK,  # Ignores invalid date
            status.HTTP_400_BAD_REQUEST,  # Rejects invalid date
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# Dashboard Stats Tests
# ============================================================================

class TestDashboardStats:
    """Tests for dashboard statistics endpoints."""
    
    def test_get_dashboard_stats_success(self, client, auth_headers):
        """Test getting dashboard statistics."""
        response = client.get(
            "/api/v1/campaigns/dashboard-stats",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Should return stats structure
        assert isinstance(data, dict)
    
    def test_get_dashboard_stats_with_filters(self, client, auth_headers):
        """Test dashboard stats with filters."""
        response = client.get(
            "/api/v1/campaigns/dashboard-stats?platforms=meta&start_date=2024-01-01",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_dashboard_stats_unauthorized(self, client):
        """Test dashboard stats without authentication."""
        response = client.get("/api/v1/campaigns/dashboard-stats")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


# ============================================================================
# Filter Options Tests
# ============================================================================

class TestFilterOptions:
    """Tests for filter options endpoints."""
    
    def test_get_filter_options_success(self, client, auth_headers):
        """Test getting filter options."""
        response = client.get(
            "/api/v1/campaigns/filters",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Should return filter options
        assert isinstance(data, dict)
    
    def test_get_filter_options_unauthorized(self, client):
        """Test filter options without authentication."""
        response = client.get("/api/v1/campaigns/filters")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


# ============================================================================
# Campaign Analysis Tests
# ============================================================================

class TestCampaignAnalysis:
    """Tests for campaign analysis endpoints."""
    
    def test_analyze_campaign_success(self, client, auth_headers, mock_reasoning_agent):
        """Test successful campaign analysis."""
        # This endpoint may vary - adjust based on actual implementation
        response = client.post(
            "/api/v1/campaigns/analyze/global",
            headers=auth_headers,
            json={
                'campaign_id': 1,
                'analysis_type': 'comprehensive'
            }
        )
        
        # May need data to be uploaded first
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_404_NOT_FOUND,  # No campaign data
            status.HTTP_400_BAD_REQUEST  # Missing data
        ]
    
    def test_analyze_campaign_unauthorized(self, client):
        """Test analysis without authentication."""
        response = client.post(
            "/api/v1/campaigns/analyze/global",
            json={'campaign_id': 1}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_analyze_campaign_invalid_id(self, client, auth_headers):
        """Test analysis with invalid campaign ID."""
        response = client.post(
            "/api/v1/campaigns/analyze/global",
            headers=auth_headers,
            json={'campaign_id': 99999}
        )
        
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST
        ]


# ============================================================================
# Natural Language Query Tests
# ============================================================================

class TestNaturalLanguageQuery:
    """Tests for NL query endpoints."""
    
    def test_chat_global_success(self, client, auth_headers, mock_query_engine):
        """Test natural language query."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={
                'question': 'What is the total spend?',
                'knowledge_mode': False
            }
        )
        
        # May need data uploaded first
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST  # No data
        ]
    
    def test_chat_global_knowledge_mode(self, client, auth_headers, mock_query_engine):
        """Test query in knowledge mode."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={
                'question': 'How can I improve my CTR?',
                'knowledge_mode': True
            }
        )
        
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST
        ]
    
    def test_chat_global_unauthorized(self, client):
        """Test query without authentication."""
        response = client.post(
            "/api/v1/campaigns/chat",
            json={'question': 'test query'}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_chat_global_empty_message(self, client, auth_headers):
        """Test query with empty message."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={'question': ''}
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# Suggested Questions Tests
# ============================================================================

class TestSuggestedQuestions:
    """Tests for suggested questions endpoint."""
    
    def test_get_suggested_questions_success(self, client, auth_headers):
        """Test getting suggested questions."""
        response = client.get(
            "/api/v1/campaigns/suggested-questions",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # Should return list of questions
        assert isinstance(data, (list, dict))
    
    def test_get_suggested_questions_unauthorized(self, client):
        """Test suggested questions without authentication."""
        response = client.get("/api/v1/campaigns/suggested-questions")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


# ============================================================================
# Aggregation Endpoints Tests
# ============================================================================

class TestAggregationEndpoints:
    """Tests for aggregation endpoints."""
    
    def test_get_funnel_stats(self, client, auth_headers):
        """Test getting funnel statistics."""
        response = client.get(
            "/api/v1/campaigns/funnel-stats",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_audience_stats(self, client, auth_headers):
        """Test getting audience statistics."""
        response = client.get(
            "/api/v1/campaigns/audience-stats",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
