"""
Tests for CampaignTransformer (Phase B.3).
Verifies normalization and ID generation logic.
"""

import pytest
import hashlib
from datetime import date, datetime
from src.kg_rag.etl.transformers.campaign_transformer import CampaignTransformer

class TestCampaignTransformer:
    """Unit tests for CampaignTransformer."""

    def test_normalize_platform(self):
        """Verify platform normalization mappings."""
        transformer = CampaignTransformer()
        assert transformer._normalize_platform("Google Ads") == "google_ads"
        assert transformer._normalize_platform("Facebook") == "meta"
        assert transformer._normalize_platform("TTD") == "trade_desk"
        assert transformer._normalize_platform("Unknown New") == "unknown_new"

    def test_normalize_objective(self):
        """Verify objective normalization mappings."""
        transformer = CampaignTransformer()
        assert transformer._normalize_objective("Link Clicks") == "traffic"
        assert transformer._normalize_objective("Sales") == "conversions"
        assert transformer._normalize_objective("Lead Generation") == "leads"
        assert transformer._normalize_objective("Custom") == "Custom"

    def test_normalize_status(self):
        """Verify status normalization mappings."""
        transformer = CampaignTransformer()
        assert transformer._normalize_status("Enabled") == "active"
        assert transformer._normalize_status("Paused") == "paused"
        assert transformer._normalize_status("Removed") == "removed"

    def test_get_campaign_id_explicit(self):
        """Verify explicit ID extraction."""
        transformer = CampaignTransformer()
        assert transformer._get_campaign_id({"campaign_id": "123"}) == "123"
        assert transformer._get_campaign_id({"id": "456"}) == "456"

    def test_get_campaign_id_generated(self):
        """Verify generated ID from name/platform."""
        transformer = CampaignTransformer()
        record = {"campaign_name": "Summer", "platform": "Google"}
        gen_id = transformer._get_campaign_id(record)
        
        expected = hashlib.md5("Google:Summer".encode()).hexdigest()[:12]
        assert gen_id == expected

    def test_to_date_parsing(self):
        """Verify flexible date parsing."""
        transformer = CampaignTransformer()
        assert transformer._to_date(date(2024, 1, 1)) == "2024-01-01"
        assert transformer._to_date("2024-01-01") == "2024-01-01"
        assert transformer._to_date("01/01/2024") == "2024-01-01"
        assert transformer._to_date("not-a-date") == "not-a-date"

    def test_transform_record_full(self):
        """Verify full record transformation into node properties."""
        transformer = CampaignTransformer(default_platform="default")
        record = {
            "campaign_id": "C1",
            "campaign_name": "Test Cam",
            "platform": "Meta",
            "objective": "Sales",
            "budget": "1000.50",
            "start_date": "2024-05-01"
        }
        transformed = transformer._transform_record(record)
        
        assert transformed['id'] == "C1"
        assert transformed['platform_id'] == "meta"
        assert transformed['name'] == "Test Cam"
        assert transformed['objective'] == "conversions"
        assert transformed['budget'] == 1000.50
        assert transformed['start_date'] == "2024-05-01"
        assert transformed['spend_total'] == 0.0

    def test_transform_batch_with_errors(self):
        """Verify batch transformation continues on single errors."""
        transformer = CampaignTransformer()
        records = [
            {"campaign_id": "valid", "campaign_name": "V"},
            {"campaign_id": None}, # Should skip
            {"campaign_id": "other", "campaign_name": "O"}
        ]
        transformed = transformer.transform(records)
        assert len(transformed) == 2
        assert transformed[0]['id'] == "valid"
        assert transformed[1]['id'] == "other"
