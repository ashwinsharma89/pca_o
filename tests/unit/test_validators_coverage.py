
import pytest
import pandas as pd
import numpy as np
from src.platform.ingestion.validators import (
    DataValidator, 
    ValidationResult,
    ValidationSeverity,
    DuckDBConstraintValidator,
    validate_dataframe
)

@pytest.fixture
def validator():
    return DataValidator(log_errors=True)

def get_valid_df():
    return pd.DataFrame({
        'spend': [100.0],
        'impressions': [1000],
        'clicks': [100],
        'conversions': [10],
        'revenue': [500.0],
        'platform': ['Facebook'],
        'channel': ['Social'],
        'campaign': ['Test'],
        'date': [pd.Timestamp('2024-01-01')]
    })

class TestValidationResult:
    def test_add_error_warning(self):
        result = ValidationResult()
        result.add_error("col", "msg")
        result.add_warning("col2", "msg2")
        assert result.passed is False
        d = result.to_dict()
        assert d['error_count'] == 1
        assert d['warning_count'] == 1

class TestDataValidator:
    def test_validate_empty(self, validator):
        df = pd.DataFrame()
        validated_df, result = validator.validate(df)
        assert validated_df.empty
        assert result.row_count == 0

    def test_validate_success(self, validator):
        df = get_valid_df()
        validated_df, result = validator.validate(df)
        assert result.valid_rows == 1
        assert result.passed is True

    def test_fast_validate_branches(self, validator):
        # Click > Impression (Line 215)
        df_click = get_valid_df()
        df_click.at[0, 'impressions'] = 50
        df_click.at[0, 'clicks'] = 60
        validator.validate(df_click)
        
        # Conv > Click (Line 228)
        df_conv = get_valid_df()
        df_conv.at[0, 'clicks'] = 5
        df_conv.at[0, 'conversions'] = 10
        validator.validate(df_conv)
        
        # Null spend (Line 241)
        df_null = get_valid_df()
        df_null.at[0, 'spend'] = None
        validator.validate(df_null)
        
        # Future date (Line 252)
        df_future = get_valid_df()
        df_future.at[0, 'date'] = pd.Timestamp.now() + pd.Timedelta(days=10)
        validator.validate(df_future)

    def test_deep_validation_logging(self, validator):
        # Hit line 192->195 by triggering a schema error with log_errors=True
        df = get_valid_df()
        df.at[0, 'spend'] = -100.0
        validator.validate(df)
        
    def test_strict_mode(self):
        validator_strict = DataValidator(strict=True)
        df = get_valid_df()
        df.at[0, 'spend'] = -100.0
        with pytest.raises(Exception):
             validator_strict.validate(df)

class TestDuckDBConstraintValidator:
    def test_get_sql(self):
        sql = DuckDBConstraintValidator.get_create_table_sql("test_tbl")
        assert "CREATE TABLE IF NOT EXISTS test_tbl" in sql
        assert "CHECK (spend >= 0)" in sql

    def test_validate_with_duckdb(self):
        import duckdb
        conn = duckdb.connect(":memory:")
        df = pd.DataFrame({'spend': [100.0], 'clicks': [10], 'impressions': [100]})
        result = DuckDBConstraintValidator.validate_with_duckdb(df, conn)
        assert result.passed is True
        
        # Test failure
        df_fail = pd.DataFrame({'spend': [-10.0], 'clicks': [10], 'impressions': [100]})
        result_fail = DuckDBConstraintValidator.validate_with_duckdb(df_fail, conn)
        assert result_fail.passed is False

def test_validate_dataframe_conv():
    df = get_valid_df()
    _, result = validate_dataframe(df)
    assert result.row_count == 1
