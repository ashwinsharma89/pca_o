"""
Tests for KG-RAG Ingestion and Normalization (Phase B.1/B.2).
Focuses on record normalization and orchestration logic.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.kg_rag.etl.ingestion import ingest_dataframe, _normalize_record

class TestIngestionLogic:
    """Unit tests for record normalization and ingestion flow."""

    def test_normalize_record_basic(self):
        """Verify basic key mapping and normalization."""
        raw = {
            "Campaign Name Full": "Summer Sale",
            "Total Spent": 150.5,
            "Site Visit": 30,
            "Device Type": "Mobile",
            "Date": "2024-01-01T00:00:00"
        }
        norm = _normalize_record(raw)
        
        assert norm['campaign_name'] == "Summer Sale"
        assert norm['spend'] == 150.5
        assert norm['conversions'] == 30
        assert norm['device_types'] == ["Mobile"] # Normalized to list
        assert norm['date'] == "2024-01-01"

    def test_normalize_record_date_fallback(self):
        """Verify date construction from year/month."""
        raw = {
            "Year": 2024,
            "Month": 5,
            "Campaign_ID": "C123"
        }
        norm = _normalize_record(raw)
        assert norm['date'] == "2024-05-01"

    def test_normalize_record_id_generation(self):
        """Verify campaign_id generation if missing."""
        raw = {
            "campaign_name": "Test",
            "platform": "Google"
        }
        norm = _normalize_record(raw)
        assert "campaign_id" in norm
        assert len(norm['campaign_id']) == 12

    def test_normalize_record_revenue_merge(self):
        """Verify revenue merging from multiple years."""
        raw = {
            "revenue_2024": 100,
            "revenue_2025": 50
        }
        norm = _normalize_record(raw)
        assert norm['revenue'] == 150.0

    @patch("src.kg_rag.etl.ingestion.Neo4jLoader")
    @patch("src.kg_rag.etl.ingestion.CampaignTransformer")
    def test_ingest_dataframe_flow(self, mock_campaign_trans_cls, mock_loader_cls):
        """Verify high-level ingestion orchestration."""
        # Setup mocks
        mock_loader = mock_loader_cls.return_value
        mock_loader.load_campaigns.return_value = {"nodes_created": 2}
        
        mock_trans = mock_campaign_trans_cls.return_value
        mock_trans.transform.return_value = [{"campaign_id": "1"}]
        
        # Test data
        df = pd.DataFrame([
            {"campaign_name": "C1", "platform": "Google"},
            {"campaign_name": "C2", "platform": "Meta"}
        ])
        
        # We need to mock other transformers if they are used
        with patch("src.kg_rag.etl.ingestion.MetricTransformer") as mock_metric_trans_cls:
            with patch("src.kg_rag.etl.ingestion.TargetingTransformer") as mock_target_trans_cls:
                mock_metric_trans_cls.return_value.transform.return_value = []
                mock_target_trans_cls.return_value.transform.return_value = []
                
                result = ingest_dataframe(df)
                
                assert result['total_records_processed'] == 2
                assert result['campaigns']['nodes_created'] == 2
                assert mock_loader.load_campaigns.called
