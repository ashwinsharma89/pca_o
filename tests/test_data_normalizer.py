"""
Tests for Data Normalizer Module

Tests cover:
1. Column name normalization and fuzzy matching
2. Data type detection
3. Date parsing (messy formats)
4. Numeric value cleaning
5. Column alias matching
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import directly from the module file to avoid settings chain
import importlib.util
spec = importlib.util.spec_from_file_location(
    "data_normalizer", 
    PROJECT_ROOT / "src" / "utils" / "data_normalizer.py"
)
data_normalizer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_normalizer)

normalize_column_name = data_normalizer.normalize_column_name
get_canonical_name = data_normalizer.get_canonical_name
similarity_score = data_normalizer.similarity_score
find_best_match = data_normalizer.find_best_match
auto_map_columns = data_normalizer.auto_map_columns
detect_column_type = data_normalizer.detect_column_type
parse_date = data_normalizer.parse_date
normalize_date_column = data_normalizer.normalize_date_column
clean_numeric_value = data_normalizer.clean_numeric_value
normalize_numeric_column = data_normalizer.normalize_numeric_column
normalize_dataframe = data_normalizer.normalize_dataframe
validate_data_quality = data_normalizer.validate_data_quality
get_column_mapping_report = data_normalizer.get_column_mapping_report


# =============================================================================
# COLUMN NAME NORMALIZATION TESTS
# =============================================================================

class TestColumnNameNormalization:
    """Tests for column name normalization."""
    
    def test_normalize_basic(self):
        """Test basic normalization."""
        assert normalize_column_name("Campaign_Name") == "campaign name"
        assert normalize_column_name("campaign-name") == "campaign name"
        assert normalize_column_name("CampaignName") == "campaignname"
        assert normalize_column_name("IMPRESSIONS") == "impressions"
    
    def test_normalize_special_chars(self):
        """Test removal of special characters."""
        assert normalize_column_name("Spend ($)") == "spend"
        assert normalize_column_name("CTR_%") == "ctr"
        assert normalize_column_name("Revenue (USD)") == "revenue usd"
    
    def test_normalize_whitespace(self):
        """Test whitespace handling."""
        assert normalize_column_name("  Campaign   Name  ") == "campaign name"
        assert normalize_column_name("Campaign\tName") == "campaign name"


class TestCanonicalNames:
    """Tests for canonical name detection."""
    
    def test_exact_canonical(self):
        """Test exact canonical name matches."""
        assert get_canonical_name("impressions") == "impressions"
        assert get_canonical_name("clicks") == "clicks"
        assert get_canonical_name("spend") == "spend"
    
    def test_alias_canonical(self):
        """Test alias to canonical mapping."""
        assert get_canonical_name("cost") == "spend"
        assert get_canonical_name("amount_spent") == "spend"
        assert get_canonical_name("impr") == "impressions"
        assert get_canonical_name("conv") == "conversions"
        assert get_canonical_name("sales") == "revenue"
    
    def test_no_canonical(self):
        """Test unknown column names."""
        assert get_canonical_name("xyz_unknown") is None
        assert get_canonical_name("random_column") is None


class TestSimilarityScore:
    """Tests for similarity scoring."""
    
    def test_exact_match(self):
        """Test exact match score."""
        assert similarity_score("impressions", "impressions") == 1.0
        assert similarity_score("Impressions", "impressions") == 1.0
    
    def test_partial_match(self):
        """Test partial match scores."""
        score = similarity_score("impression", "impressions")
        assert 0.9 < score < 1.0
        
        score = similarity_score("impr", "impressions")
        assert 0.4 < score < 0.7
    
    def test_no_match(self):
        """Test low similarity scores."""
        score = similarity_score("xyz", "impressions")
        assert score < 0.3


class TestFuzzyMatching:
    """Tests for fuzzy column matching."""
    
    def test_exact_match(self):
        """Test exact match."""
        columns = ['Date', 'Platform', 'Impressions', 'Clicks']
        match, score = find_best_match('Impressions', columns)
        assert match == 'Impressions'
        assert score == 1.0
    
    def test_case_insensitive_match(self):
        """Test case-insensitive matching."""
        columns = ['Date', 'Platform', 'Impressions', 'Clicks']
        match, score = find_best_match('impressions', columns)
        assert match == 'Impressions'
        assert score == 1.0
    
    def test_alias_match(self):
        """Test alias matching."""
        columns = ['Date', 'Platform', 'Cost', 'Clicks']
        match, score = find_best_match('Spend', columns)
        assert match == 'Cost'
        assert score >= 0.9
    
    def test_typo_match(self):
        """Test typo tolerance."""
        columns = ['Date', 'Platform', 'Impressions', 'Clicks']
        match, score = find_best_match('Impresions', columns)  # typo
        assert match == 'Impressions'
        assert score >= 0.8
    
    def test_abbreviation_match(self):
        """Test abbreviation matching."""
        columns = ['Date', 'Platform', 'Impressions', 'Clicks']
        match, score = find_best_match('Impr', columns)
        assert match == 'Impressions'
        assert score >= 0.6
    
    def test_no_match_below_threshold(self):
        """Test no match when below threshold."""
        columns = ['Date', 'Platform', 'Impressions', 'Clicks']
        match, score = find_best_match('XYZ_Unknown', columns, threshold=0.6)
        assert match is None
        assert score < 0.6
    
    def test_underscore_vs_space(self):
        """Test matching with different separators."""
        columns = ['Campaign_Name', 'Ad_Group', 'Spend_USD']
        match, score = find_best_match('Campaign Name', columns)
        assert match == 'Campaign_Name'
        assert score == 1.0


class TestAutoMapColumns:
    """Tests for automatic column mapping."""
    
    def test_auto_map_exact(self):
        """Test auto-mapping with exact matches."""
        source = ['Date', 'Platform', 'Impressions']
        target = ['Date', 'Platform', 'Impressions']
        
        mappings = auto_map_columns(source, target)
        
        assert mappings['Date']['target'] == 'Date'
        assert mappings['Platform']['target'] == 'Platform'
        assert mappings['Impressions']['target'] == 'Impressions'
    
    def test_auto_map_aliases(self):
        """Test auto-mapping with aliases."""
        source = ['Date', 'Cost', 'Sales']
        target = ['Date', 'Spend', 'Revenue']
        
        mappings = auto_map_columns(source, target)
        
        assert mappings['Cost']['target'] == 'Spend'
        assert mappings['Sales']['target'] == 'Revenue'
    
    def test_auto_map_partial(self):
        """Test auto-mapping with partial matches."""
        source = ['Date', 'Unknown_Column']
        target = ['Date', 'Platform']
        
        mappings = auto_map_columns(source, target)
        
        assert mappings['Date']['target'] == 'Date'
        assert mappings['Unknown_Column']['target'] is None


# =============================================================================
# DATA TYPE DETECTION TESTS
# =============================================================================

class TestDataTypeDetection:
    """Tests for column type detection."""
    
    def test_detect_date_column(self):
        """Test date column detection."""
        series = pd.Series(['2024-01-01', '2024-01-02', '2024-01-03'])
        assert detect_column_type(series) == 'date'
        
        series = pd.Series(['01/15/2024', '02/20/2024', '03/25/2024'])
        assert detect_column_type(series) == 'date'
    
    def test_detect_numeric_column(self):
        """Test numeric column detection."""
        series = pd.Series([100, 200, 300])
        assert detect_column_type(series) == 'numeric'
        
        series = pd.Series([1.5, 2.5, 3.5])
        assert detect_column_type(series) == 'numeric'
    
    def test_detect_currency_column(self):
        """Test currency column detection."""
        series = pd.Series([100.0, 200.0, 300.0], name='Spend_USD')
        assert detect_column_type(series) == 'currency'
        
        series = pd.Series(['$100', '$200', '$300'])
        assert detect_column_type(series) == 'currency'
    
    def test_detect_percentage_column(self):
        """Test percentage column detection."""
        series = pd.Series([1.5, 2.0, 2.5], name='CTR_%')
        assert detect_column_type(series) == 'percentage'
        
        series = pd.Series(['1.5%', '2.0%', '2.5%'])
        assert detect_column_type(series) == 'percentage'
    
    def test_detect_text_column(self):
        """Test text column detection."""
        series = pd.Series(['Google Ads', 'Meta Ads', 'Snapchat'])
        assert detect_column_type(series) == 'text'


# =============================================================================
# DATE PARSING TESTS
# =============================================================================

class TestDateParsing:
    """Tests for date parsing."""
    
    def test_parse_iso_format(self):
        """Test ISO date format."""
        result = parse_date('2024-01-15')
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_us_format(self):
        """Test US date format (MM/DD/YYYY)."""
        result = parse_date('01/15/2024')
        assert result is not None
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_european_format(self):
        """Test European date format (DD/MM/YYYY)."""
        result = parse_date('15-01-2024')
        assert result is not None
        # Note: pandas may interpret this as Jan 15 or 15th of Jan depending on settings
    
    def test_parse_text_format(self):
        """Test text date formats."""
        result = parse_date('Jan 15, 2024')
        assert result is not None
        assert result.month == 1
        
        result = parse_date('15 Jan 2024')
        assert result is not None
    
    def test_parse_compact_format(self):
        """Test compact date format (YYYYMMDD)."""
        result = parse_date('20240115')
        assert result is not None
        assert result.year == 2024
    
    def test_parse_null(self):
        """Test null handling."""
        assert parse_date(None) is None
        assert parse_date('') is None
        assert parse_date(np.nan) is None
    
    def test_parse_datetime_passthrough(self):
        """Test datetime passthrough."""
        dt = datetime(2024, 1, 15)
        result = parse_date(dt)
        assert result == dt
    
    def test_normalize_date_column(self):
        """Test date column normalization."""
        series = pd.Series(['2024-01-01', '01/15/2024', 'Jan 20, 2024', None])
        result = normalize_date_column(series)
        
        assert pd.notna(result.iloc[0])
        assert pd.notna(result.iloc[1])
        assert pd.notna(result.iloc[2])
        assert pd.isna(result.iloc[3])


# =============================================================================
# NUMERIC VALUE CLEANING TESTS
# =============================================================================

class TestNumericCleaning:
    """Tests for numeric value cleaning."""
    
    def test_clean_plain_number(self):
        """Test plain number cleaning."""
        assert clean_numeric_value(100) == 100.0
        assert clean_numeric_value(100.5) == 100.5
        assert clean_numeric_value('100') == 100.0
    
    def test_clean_currency_symbol(self):
        """Test currency symbol removal."""
        assert clean_numeric_value('$100') == 100.0
        assert clean_numeric_value('€100') == 100.0
        assert clean_numeric_value('£100.50') == 100.50
        assert clean_numeric_value('$1,234.56') == 1234.56
    
    def test_clean_thousand_separator(self):
        """Test thousand separator handling."""
        assert clean_numeric_value('1,000') == 1000.0
        assert clean_numeric_value('1,234,567') == 1234567.0
        assert clean_numeric_value('1,234.56') == 1234.56
    
    def test_clean_percentage(self):
        """Test percentage sign removal."""
        assert clean_numeric_value('5%') == 5.0
        assert clean_numeric_value('2.5%') == 2.5
    
    def test_clean_negative_parentheses(self):
        """Test negative values in parentheses."""
        assert clean_numeric_value('(100)') == -100.0
        assert clean_numeric_value('($1,234.56)') == -1234.56
    
    def test_clean_whitespace(self):
        """Test whitespace handling."""
        assert clean_numeric_value('  100  ') == 100.0
        assert clean_numeric_value('$ 100') == 100.0
    
    def test_clean_null(self):
        """Test null handling."""
        assert clean_numeric_value(None) is None
        assert clean_numeric_value('') is None
        assert clean_numeric_value(np.nan) is None
    
    def test_normalize_numeric_column(self):
        """Test numeric column normalization."""
        series = pd.Series(['$1,000', '€2,000', '3000', None])
        result = normalize_numeric_column(series)
        
        assert result.iloc[0] == 1000.0
        assert result.iloc[1] == 2000.0
        assert result.iloc[2] == 3000.0
        assert pd.isna(result.iloc[3])


# =============================================================================
# DATAFRAME NORMALIZATION TESTS
# =============================================================================

class TestDataFrameNormalization:
    """Tests for DataFrame normalization."""
    
    def test_normalize_mixed_dataframe(self):
        """Test normalization of mixed data types."""
        df = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'Platform': ['Google', 'Meta', 'Snap'],
            'Spend_USD': ['$1,000', '$2,000', '$3,000'],
            'CTR_%': ['1.5%', '2.0%', '2.5%'],
        })
        
        normalized_df, column_types = normalize_dataframe(df)
        
        assert column_types['Date'] == 'date'
        assert column_types['Platform'] == 'text'
        assert column_types['Spend_USD'] == 'currency'
        assert column_types['CTR_%'] == 'percentage'
        
        # Check values were cleaned
        assert normalized_df['Spend_USD'].iloc[0] == 1000.0
        assert normalized_df['CTR_%'].iloc[0] == 1.5
    
    def test_normalize_with_explicit_columns(self):
        """Test normalization with explicit column types."""
        df = pd.DataFrame({
            'col1': ['2024-01-01', '2024-01-02'],
            'col2': ['100', '200'],
        })
        
        normalized_df, column_types = normalize_dataframe(
            df,
            date_columns=['col1'],
            numeric_columns=['col2']
        )
        
        assert column_types['col1'] == 'date'
        assert column_types['col2'] == 'numeric'


# =============================================================================
# DATA QUALITY VALIDATION TESTS
# =============================================================================

class TestDataQualityValidation:
    """Tests for data quality validation."""
    
    def test_validate_clean_data(self):
        """Test validation of clean data."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Value': range(10),
        })
        
        report = validate_data_quality(df)
        
        assert report['total_rows'] == 10
        assert report['total_columns'] == 2
        assert len(report['issues']) == 0
    
    def test_validate_null_warning(self):
        """Test warning for high null rate."""
        df = pd.DataFrame({
            'col1': [1, 2, None, None, None, None, None, None, None, None],
        })
        
        report = validate_data_quality(df)
        
        assert any('null' in w.lower() for w in report['warnings'])
    
    def test_validate_all_null_issue(self):
        """Test issue for entirely null column."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [None, None, None],
        })
        
        report = validate_data_quality(df)
        
        assert any('entirely null' in i.lower() for i in report['issues'])
    
    def test_validate_duplicates_warning(self):
        """Test warning for duplicate rows."""
        df = pd.DataFrame({
            'col1': [1, 1, 2],
            'col2': ['a', 'a', 'b'],
        })
        
        report = validate_data_quality(df)
        
        assert any('duplicate' in w.lower() for w in report['warnings'])


# =============================================================================
# COLUMN MAPPING REPORT TESTS
# =============================================================================

class TestColumnMappingReport:
    """Tests for column mapping report generation."""
    
    def test_generate_mapping_report(self):
        """Test mapping report generation."""
        source_df = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Cost': [100],
            'Impr': [1000],
        })
        target_columns = ['Date', 'Spend', 'Impressions']
        
        report = get_column_mapping_report(source_df, target_columns)
        
        assert len(report) == 3
        assert 'Source Column' in report.columns
        assert 'Target Column' in report.columns
        assert 'Match Score' in report.columns


# =============================================================================
# EDGE CASES
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases."""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        normalized_df, column_types = normalize_dataframe(df)
        assert len(normalized_df) == 0
    
    def test_single_column(self):
        """Test handling of single column."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        normalized_df, column_types = normalize_dataframe(df)
        assert 'col1' in column_types
    
    def test_unicode_column_names(self):
        """Test handling of unicode column names."""
        df = pd.DataFrame({'日付': ['2024-01-01'], '金額': [100]})
        normalized_df, column_types = normalize_dataframe(df)
        assert len(column_types) == 2
    
    def test_very_long_column_name(self):
        """Test handling of very long column names."""
        long_name = 'a' * 200
        df = pd.DataFrame({long_name: [1, 2, 3]})
        normalized_df, column_types = normalize_dataframe(df)
        assert long_name in column_types


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
