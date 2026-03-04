
import pytest
import pandas as pd
import numpy as np
import unittest.mock as mock
from loguru import logger
import sys
from src.platform.ingestion.normalizer import DataNormalizer, SchemaEnforcer, normalize_dataframe

@pytest.fixture(autouse=True)
def setup_logging():
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    yield

@pytest.fixture
def normalizer():
    return DataNormalizer(log_mappings=True)

class TestDataNormalizer:
    def test_normalize_empty(self, normalizer):
        df = pd.DataFrame()
        result = normalizer.normalize(df)
        assert result.empty
        assert 'spend' in result.columns

    def test_map_columns(self, normalizer):
        df = pd.DataFrame({
            'Campaign Name': ['A'],
            'Amount Spent': [100.0],
            'Day': ['2024-01-01'],
            'Unmapped Column': ['Test']
        })
        result = normalizer.normalize(df)
        assert 'campaign' in result.columns
        assert 'spend' in result.columns
        assert 'extra_unmapped_column' in result.columns

    def test_convert_types(self, normalizer):
        df = pd.DataFrame({
            'spend': ['100.50'],
            'platform': [123], # Coerced to '123' if it's already string or '123.0' if float
            'channel': ['Social'],
            'date': ['2024-01-01']
        })
        result = normalizer.normalize(df)
        # Note: if the input is 123 (int), and the column becomes object/string, astype(str) makes it '123'
        # But if it was float it would be '123.0'. 
        assert result['platform'].iloc[0] in ['123', '123.0']

    def test_conversion_failure_branch(self, normalizer):
        df = pd.DataFrame({'spend': [100], 'date': ['2024-01-01']})
        with mock.patch('pandas.to_numeric', side_effect=ValueError("Mock fail")):
            result = normalizer.normalize(df)
            assert not result.empty

    def test_strict_mode_branches(self):
        strict_norm = DataNormalizer(strict_mode=True)
        df = pd.DataFrame({'spend': [100]})
        with pytest.raises(ValueError, match="Required column 'date' is missing"):
            strict_norm.normalize(df)
            
        df_null = pd.DataFrame({
            'spend': [None], 
            'date': [pd.Timestamp('2024-01-01')],
            'platform': ['FB']
        })
        DataNormalizer(strict_mode=False).normalize(df_null)

class TestSchemaEnforcer:
    def test_enforce_constraints(self):
        df = pd.DataFrame({
            'spend': [-10],
            'impressions': [50],
            'clicks': [60],
            'conversions': [70],
            'date': [pd.Timestamp.now() + pd.Timedelta(days=1)]
        })
        cleaned_df, violations = SchemaEnforcer.enforce_constraints(df)
        assert len(violations) >= 4

def test_normalize_dataframe_conv():
    df = pd.DataFrame({'spend': [100], 'date': ['2024-01-01']})
    result = normalize_dataframe(df)
    assert not result.empty
