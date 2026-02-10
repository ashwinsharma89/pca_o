"""
Tests for SQL repositories in repositories.py (Phase 3.2).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session
from datetime import datetime

from src.core.database.repositories import (
    BaseRepository, QueryHistoryRepository, LLMUsageRepository, 
    CampaignRepository, AnalysisRepository, CampaignContextRepository
)
from src.core.database.models import QueryHistory, LLMUsage, Campaign, Analysis, CampaignContext

class TestBaseRepository:
    def test_base_ops(self):
        mock_session = Mock(spec=Session)
        repo = BaseRepository(mock_session)
        
        repo.commit()
        mock_session.commit.assert_called_once()
        
        repo.rollback()
        mock_session.rollback.assert_called_once()
        
        repo.flush()
        mock_session.flush.assert_called_once()

class TestQueryHistoryRepository:
    def test_create_and_get(self):
        mock_session = MagicMock(spec=Session)
        repo = QueryHistoryRepository(mock_session)
        
        # Test create
        data = {"user_query": "test", "status": "success"}
        query = repo.create(data)
        assert isinstance(query, QueryHistory)
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()
        
        # Test get_by_id
        repo.get_by_id(1)
        mock_session.query.return_value.filter.return_value.first.assert_called_once()

    def test_search_and_status(self):
        mock_session = MagicMock(spec=Session)
        repo = QueryHistoryRepository(mock_session)
        
        repo.get_recent(limit=10)
        repo.search_by_text("foo")
        repo.get_by_status("failed")
        assert mock_session.query.call_count == 3

class TestLLMUsageRepository:
    def test_usage_aggregations(self):
        mock_session = MagicMock(spec=Session)
        repo = LLMUsageRepository(mock_session)
        
        # Mock result for aggregations
        mock_result = Mock()
        mock_result.total_tokens = 100
        mock_result.total_cost = 0.5
        mock_result.request_count = 10
        
        # Setup mock chain for with and without filter
        # .query().filter().first()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_result
        # .query().first()
        mock_session.query.return_value.first.return_value = mock_result
        
        stats = repo.get_usage_by_provider("openai")
        assert stats['total_tokens'] == 100
        assert stats['total_cost'] == 0.5
        
        stats_all = repo.get_total_usage()
        assert stats_all['total_cost'] == 0.5

class TestCampaignRepository:
    def test_bulk_create(self):
        mock_session = MagicMock(spec=Session)
        repo = CampaignRepository(mock_session)
        
        data_list = [{"campaign_id": "c1"}, {"campaign_id": "c2"}]
        repo.create_bulk(data_list, batch_size=1)
        
        assert mock_session.add_all.call_count == 2
        assert mock_session.commit.call_count == 2

    def test_search_filters(self):
        mock_session = MagicMock(spec=Session)
        repo = CampaignRepository(mock_session)
        
        repo.search(filters={"platform": "Google", "invalid_col": "x"})
        # Should filter by platform but ignore invalid_col
        # Verify query construction if possible, or just check call count
        assert mock_session.query.called

class TestCampaignContextRepository:
    def test_update_context(self):
        mock_session = MagicMock(spec=Session)
        repo = CampaignContextRepository(mock_session)
        
        # Case 1: Existing
        mock_context = Mock()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_context
        repo.update(1, {"a": 1})
        assert mock_context.context_data == {"a": 1}
        
        # Case 2: New
        mock_session.query.return_value.filter.return_value.first.return_value = None
        repo.update(2, {"b": 2})
        assert mock_session.add.called
