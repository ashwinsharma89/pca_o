"""
Tests for Metric and Targeting Transformers (Phase B.3).
Verifies detailed metric extraction and targeting normalization.
"""

import pytest
import hashlib
from src.kg_rag.etl.transformers.metric_transformer import MetricTransformer
from src.kg_rag.etl.transformers.targeting_transformer import TargetingTransformer

class TestMetricTransformer:
    """Unit tests for MetricTransformer."""

    def test_transform_record_with_dimensions(self):
        """Verify metric ID generation and dimension mapping."""
        transformer = MetricTransformer()
        record = {
            "campaign_id": "C1",
            "date": "2024-01-01",
            "platform": "Google",
            "spend": "100.50",
            "clicks": 50,
            "impressions": 1000
        }
        transformed = transformer._transform_record(record, hashlib)
        
        assert "C1_2024-01-01_" in transformed['id']
        assert transformed['spend'] == 100.5
        assert transformed['clicks'] == 50
        assert transformed['platform'] == "Google"

    def test_aggregate_for_campaign(self):
        """Verify aggregation logic for campaign totals."""
        transformer = MetricTransformer()
        metrics = [
            {"impressions": 100, "spend": 10.0},
            {"impressions": 200, "spend": 20.0}
        ]
        totals = transformer.aggregate_for_campaign(metrics)
        assert totals['impressions_total'] == 300
        assert totals['spend_total'] == 30.0

class TestTargetingTransformer:
    """Unit tests for TargetingTransformer."""

    def test_platform_field_aliases(self):
        """Verify platform-specific field mapping (Meta)."""
        # Meta: targeting_age -> age_range
        transformer = TargetingTransformer(platform="meta")
        record = {
            "campaign_id": "C1",
            "targeting_age": "25-34"
        }
        transformed = transformer._transform_record(record)
        assert transformed['age_range'] == "25-34"

    def test_completeness_score(self):
        """Verify completeness score calculation."""
        transformer = TargetingTransformer()
        # Provide 2 fields out of many
        record = {
            "campaign_id": "C1",
            "geo_countries": "US",
            "device_types": "Mobile"
        }
        transformed = transformer._transform_record(record)
        assert transformed['completeness_score'] > 0
        assert "geo_countries" in transformed['available_fields']

    def test_process_value_list_expansion(self):
        """Verify comma-separated string expansion to list."""
        transformer = TargetingTransformer()
        val = transformer._process_value("geo_countries", "US, UK, DE")
        assert val == ["US", "UK", "DE"]

    def test_nested_targeting_extraction(self):
        """Verify extraction from nested 'targeting' object."""
        transformer = TargetingTransformer()
        record = {
            "campaign_id": "C1",
            "targeting": {
                "interests": "Coffee, Tea"
            }
        }
        transformed = transformer._transform_record(record)
        assert transformed['interests'] == ["Coffee", "Tea"]
