
import pytest
from datetime import date, datetime
import math
import hashlib
from src.kg_rag.etl.transformers.campaign_transformer import CampaignTransformer
from src.kg_rag.etl.transformers.metric_transformer import MetricTransformer
from src.kg_rag.etl.transformers.targeting_transformer import TargetingTransformer
from unittest.mock import patch

# --- CampaignTransformer Tests ---

class TestCampaignTransformer:
    
    @pytest.fixture
    def transformer(self):
        return CampaignTransformer(default_platform="unknown")

    def test_transform_valid(self, transformer):
        records = [{
            "campaign_id": "123",
            "campaign_name": "Test Camp",
            "platform": "Google Ads",
            "status": "ENABLEd",
            "budget": "100.50",
            "start_date": "2023-01-01"
        }]
        
        results = transformer.transform(records)
        assert len(results) == 1
        c = results[0]
        assert c['id'] == "123"
        assert c['platform_id'] == "google_ads"
        assert c['status'] == "active" # defined in normalization
        assert c['budget'] == 100.5
        assert c['start_date'] == "2023-01-01"

    def test_id_generation_fallback(self, transformer):
        """Test ID generation from naming convention when ID is missing."""
        records = [{
            "campaign_name": "My Campaign",
            "platform": "Meta"
        }]
        results = transformer.transform(records)
        assert len(results) == 1
        # Expect hash
        expected_hash = hashlib.md5("Meta:My Campaign".encode()).hexdigest()[:12]
        assert results[0]['id'] == expected_hash

    def test_transform_skips_invalid(self, transformer):
        """Test skipping records without ID or identifiable info."""
        records = [{"garbage": "data"}]
        results = transformer.transform(records)
        assert len(results) == 0

    def test_normalization(self, transformer):
        # Platform
        assert transformer._normalize_platform("Facebook") == "meta"
        assert transformer._normalize_platform(None) == "unknown"
        
        # Objective
        assert transformer._normalize_objective("Lead Generation") == "leads"
        assert transformer._normalize_objective(None) is None
        
        # Status
        assert transformer._normalize_status("Running") == "active"
        assert transformer._normalize_status("Deleted") == "removed"

    def test_date_parsing(self, transformer):
        assert transformer._to_date("2023-01-01") == "2023-01-01"
        assert transformer._to_date(datetime(2023, 1, 1)) == "2023-01-01"
        assert transformer._to_date("01/01/2023") == "2023-01-01"
        assert transformer._to_date("invalid") == "invalid" # fallback to return as-is
        # Test the "looks like ISO" fallback (only relevant if logic existed, but here keeps behavior)
        assert transformer._to_date("2023-99-99") == "2023-99-99" 

    def test_transform_loop_exception(self, transformer):
        """Test exception handling in the main transform loop."""
        records = [{"id": "1"}]
        # Mock _transform_record to raise exception
        # We need to patch the method on the class or instance
        with patch.object(transformer, '_transform_record', side_effect=Exception("Fail")):
             results = transformer.transform(records)
             assert len(results) == 0

    def test_conversion_errors(self, transformer):
        assert transformer._to_float("abc") is None
        assert transformer._to_float(None) is None
        
    def test_missing_platform_warning(self, transformer):
        """Test warning log when platform is missing."""
        # Use a fresh transformer with no default
        t_strict = CampaignTransformer(default_platform=None)
        records = [{"campaign_id": "1"}]
        res = t_strict.transform(records)
        assert len(res) == 1
        assert res[0]['platform_id'] is None



# --- MetricTransformer Tests ---

class TestMetricTransformer:
    
    @pytest.fixture
    def transformer(self):
        return MetricTransformer()

    def test_transform_valid(self, transformer):
        records = [{
            "campaign_id": "123",
            "date": "2023-01-01",
            "impressions": "1000",
            "clicks": 50,
            "spend": 10.5,
            "platform": "meta" # dimension
        }]
        
        results = transformer.transform(records)
        assert len(results) == 1
        m = results[0]
        assert m['campaign_id'] == "123"
        assert m['impressions'] == 1000
        assert m['platform'] == "meta"
        assert 'id' in m

    def test_id_generation_consistency(self, transformer):
        """Verify ID generation is deterministic based on dimensions."""
        rec1 = {"campaign_id": "1", "date": "2023-01-01", "platform": "meta"}
        rec2 = {"campaign_id": "1", "date": "2023-01-01", "platform": "meta"}
        
        res1 = transformer.transform([rec1])[0]
        res2 = transformer.transform([rec2])[0]
        assert res1['id'] == res2['id']
        
        # Changing dimension changes ID
        rec3 = {"campaign_id": "1", "date": "2023-01-01", "platform": "google"}
        res3 = transformer.transform([rec3])[0]
        assert res1['id'] != res3['id']

    def test_aggregation(self, transformer):
        metrics = [
            {"impressions": 100, "spend": 10.0},
            {"impressions": 200, "spend": 20.0, "conversions": 1.0}
        ]
        total = transformer.aggregate_for_campaign(metrics)
        assert total['impressions_total'] == 300
        assert total['spend_total'] == 30.0
        assert total['conversions_total'] == 1.0

    def test_dimension_list_handling(self, transformer):
        """Test that list dimensions are handled (sorted/joined)."""
        rec = {
            "campaign_id": "1", "date": "2023-01-01",
            "device_types": ["mobile", "desktop"] # list dimension
        }
        res = transformer.transform([rec])[0]
        # Should be stored as provided in the dict, but used in ID generation
        assert res['device_types'] == ["mobile", "desktop"]

    def test_transform_error(self, transformer):
        """Test error handling in loop."""
        # Missing campaign_id should skip
        res = transformer.transform([{"date": "2023-01-01"}])
        assert len(res) == 0

    def test_float_int_conversions(self, transformer):
        # Int
        assert transformer._to_int("10") == 10
        assert transformer._to_int("10.5") == 10
        assert transformer._to_int("abc") is None
        assert transformer._to_int(None) is None
        
        # Float
        assert transformer._to_float("10.5") == 10.5
        assert transformer._to_float("abc") is None
        assert transformer._to_float(float('nan')) is None
        assert transformer._to_float(float('inf')) is None

    def test_transform_exception_logging(self, transformer):
        """Test that individual record failure doesn't stop batch."""
        # Cause an error in _transform_record by mocking
        # We need to match the signature or accept any args
        recs = [{}, {"id": "ok"}]
        with patch.object(transformer, '_transform_record', side_effect=[Exception("Fail"), {"id": "ok"}]):
             res = transformer.transform(recs)
             assert len(res) == 1


# --- TargetingTransformer Tests ---

class TestTargetingTransformer:
    
    @pytest.fixture
    def transformer(self):
        return TargetingTransformer(platform="meta")

    def test_transform_aliases(self, transformer):
        """Test platform-specific field mapping."""
        # Meta alias: targeting_age -> age_range
        records = [{
            "id": "123",
            "targeting_age": "18-65" 
        }]
        
        results = transformer.transform(records)
        assert len(results) == 1
        t = results[0]
        assert t['age_range'] == "18-65"
        assert t['campaign_id'] == "123"

    def test_nested_targeting(self, transformer):
        """Test extracting from 'targeting' nested dict."""
        records = [{
            "id": "123",
            "targeting": {
                 "age_range": "25-34",
                 "gender": "female"
            }
        }]
        results = transformer.transform(records)
        assert results[0]['age_range'] == "25-34"
        assert results[0]['gender'] == "female"

    def test_process_value_normalization(self, transformer):
        # JSON String
        assert transformer._process_value("interests", '["Shoes"]') == ["Shoes"]
        
        # List String Split
        assert transformer._process_value("geo_countries", "US, CA") == ["US", "CA"]
        
        # Boolean
        assert transformer._process_value("flag", "true") is True
        assert transformer._process_value("flag", "0") is False
        
        # Numeric
        assert transformer._process_value("bid_amount", "5.5") == 5.5
        assert transformer._process_value("bid_amount", "invalid") is None

    def test_completeness_score(self, transformer):
        """Verify completeness score calculation."""
        # Only 1 field present out of many
        records = [{"id": "1", "gender": "male"}]
        t = transformer.transform(records)[0]
        assert t['completeness_score'] > 0.0
        assert t['completeness_score'] < 1.0
        assert "gender" in t['available_fields']

    def test_extract_from_campaign(self, transformer):
        """Test the helper method extract_from_campaign."""
        camp = {"id": "1", "gender": "f", "targeting": {"age_range": "20"}}
        res = transformer.extract_from_campaign(camp)
        assert res['gender'] == "f"
        assert res['age_range'] == "20"

    def test_missing_campaign_id(self, transformer):
        records = [{"gender": "male"}] # No ID
        res = transformer.transform(records)
        assert len(res) == 0

    def test_json_parsing_error(self, transformer):
        # Invalid JSON string should be treated as string or ignored?
        # Logic says pass exception -> so it remains string?
        # Code: except json.JSONDecodeError: pass
        val = transformer._process_value("interests", '{invalid')
        # Since it's passed as is if not json, and interests is a list field:
        # It hits list normalization -> split by comma -> ["{invalid"]
        assert val == ["{invalid"] 
        
    def test_int_conversion_failure(self, transformer):
        assert transformer._process_value("retargeting_window_days", "abc") is None

    def test_float_conversion_failure(self, transformer):
        assert transformer._process_value("bid_amount", "abc") is None

    def test_nested_targeting_not_dict(self, transformer):
        # Targeting field exists but is not dict
        rec = {"id": "1", "targeting": "some_string"}
        # This SHOULD work and return a record with just ID and defaults
        res = transformer.transform([rec])
        assert len(res) == 1 
        assert res[0]['campaign_id'] == "1"

    def test_transform_loop_exception(self, transformer):
        # Mock class-level to avoid instance patching issues if needed, 
        # or use patch.object on the instance
        rec = [{"id": "1"}]
        with patch.object(transformer, '_transform_record', side_effect=Exception("Fail")):
            res = transformer.transform(rec)
            assert len(res) == 0

    def test_extract_from_campaign_collision(self, transformer):
        # Result key already exists (should NOT overwrite if not None?)
        # Logic: if key not in result or result[key] is None: result[key] = value
        
        # Scenario: Top level has "gender", nested "targeting" also has "gender"
        # Since we copy top level FIRST, result has "gender".
        # If top level is not None, nested should be ignored.
        camp = {"id": "1", "gender": "male", "targeting": {"gender": "female"}}
        res = transformer.extract_from_campaign(camp)
        assert res['gender'] == "male"
        
        # If top level is None (or missing), nested should take over.
        camp2 = {"id": "2", "targeting": {"gender": "female"}}
        res2 = transformer.extract_from_campaign(camp2)
        assert res2['gender'] == "female"
