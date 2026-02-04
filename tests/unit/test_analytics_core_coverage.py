import pytest
import pandas as pd
import numpy as np
import polars as pl
import json
from datetime import datetime, timedelta
from src.engine.analytics.text_cleaner import TextCleaner
from src.engine.analytics.data_quality import DataQualityAnalyzer, generate_data_quality_report
from src.engine.analytics.data_prep_layer import DataPrepLayer, get_data_prep_layer

# ==============================================================================
# TEXT CLEANER TESTS
# ==============================================================================

class TestTextCleaner:
    def test_strip_italics_basic(self):
        text = "***Bold and Italics*** and __Underline style__"
        cleaned = TextCleaner.strip_italics(text)
        assert cleaned == "Bold and Italics and Underline style"
        
    def test_strip_italics_dashes(self):
        text = "En–dash and Em—dash"
        cleaned = TextCleaner.strip_italics(text)
        assert cleaned == "En - dash and Em - dash"
        
    def test_strip_italics_spacing(self):
        # Number-letter spacing
        assert TextCleaner.strip_italics("39.05CPA") == "39.05 CPA"
        assert TextCleaner.strip_italics("100clicks") == "100 clicks"
        assert TextCleaner.strip_italics("Clicks100") == "Clicks 100"
        
    def test_strip_italics_dictionary_fixes(self):
        text = "campaignson platformsgenerating conversionsat CPAfrom comparedto"
        cleaned = TextCleaner.strip_italics(text)
        assert cleaned == "campaigns on platforms generating conversions at CPA from compared to"
        
    def test_strip_italics_punctuation(self):
        text = "Hello.World!How?Are,You:Today;Fine"
        cleaned = TextCleaner.strip_italics(text)
        assert cleaned == "Hello. World! How? Are, You: Today; Fine"
        
    def test_strip_italics_headers(self):
        text = "[OVERALL SUMMARY] text [CHANNEL SUMMARY] more text"
        cleaned = TextCleaner.strip_italics(text)
        assert "OVERALL SUMMARY: text CHANNEL SUMMARY: more text" in cleaned
        
    def test_strip_italics_whitespace(self):
        text = "Too   many    spaces\n\n\n\nToo many newlines"
        cleaned = TextCleaner.strip_italics(text)
        assert cleaned == "Too many spaces\n\nToo many newlines"
        
    def test_strip_italics_non_string(self):
        assert TextCleaner.strip_italics(123) == 123
        
    def test_extract_json_array_empty(self):
        with pytest.raises(ValueError, match="Empty response"):
            TextCleaner.extract_json_array("")
            
    def test_extract_json_array_code_blocks(self):
        text = "Here is the data: ```json\n[{\"id\": 1}]\n```"
        result = TextCleaner.extract_json_array(text)
        assert result == [{"id": 1}]
        
        # Test code block without "json" tag
        text_no_tag = "```\n[{\"id\": 2}]\n```"
        assert TextCleaner.extract_json_array(text_no_tag) == [{"id": 2}]
        
    def test_extract_json_array_raw_brackets(self):
        # Brackets at specific positions
        text = "Random text [{\"a\": 1}, {\"b\": 2}] random text"
        result = TextCleaner.extract_json_array(text)
        assert result == [{"a": 1}, {"b": 2}]
        
        # Array at the very end
        text_end = "Start here: [1, 2, 3]"
        assert TextCleaner.extract_json_array(text_end) == [1, 2, 3]

    def test_extract_json_array_complex_text(self):
        # Multiple code blocks, should find the first valid array one
        text = "Ignore this ```text``` and use this ```[{\"x\": 10}]```"
        assert TextCleaner.extract_json_array(text) == [{"x": 10}]
        
    def test_extract_json_array_invalid(self):
        text = "[{invalid json}]"
        result = TextCleaner.extract_json_array(text)
        assert result == []

# ==============================================================================
# DATA QUALITY ANALYZER TESTS
# ==============================================================================

class TestDataQualityAnalyzer:
    @pytest.fixture
    def robust_df(self):
        return pd.DataFrame({
            'Platform': ['Meta', 'Google', 'LinkedIn'] * 40,
            'Spend': np.random.uniform(10, 100, 120),
            'Revenue': np.random.uniform(50, 500, 120),
            'Impressions': np.random.uniform(1000, 10000, 120),
            'Clicks': np.random.uniform(10, 100, 120),
            'Conversions': np.random.uniform(1, 10, 120)
        })

    def test_validate_input_robust(self, robust_df):
        res = DataQualityAnalyzer.validate_input(robust_df)
        assert res['completeness_score'] == 100
        assert res['has_revenue'] is True
        assert res['has_funnel'] is True
        assert len(res['platforms']) == 3
        
    def test_validate_input_missing_revenue(self):
        df = pd.DataFrame({
            'Platform': ['Meta'] * 100,
            'Spend': [100] * 100,
            'Impressions': [1000] * 100,
            'Clicks': [100] * 100,
            'Conversions': [10] * 100
        })
        res = DataQualityAnalyzer.validate_input(df)
        assert res['has_revenue'] is False
        assert res['completeness_score'] < 100
        assert any("Revenue/ROAS data missing" in w for w in res['warnings'])
        
    def test_validate_input_missing_funnel(self):
        df = pd.DataFrame({
            'Platform': ['Meta'] * 100,
            'Spend': [100] * 100,
            'Revenue': [200] * 100
        })
        res = DataQualityAnalyzer.validate_input(df)
        assert res['has_funnel'] is False
        assert any("Full funnel data incomplete" in w for w in res['warnings'])
        
    def test_validate_input_small_sample(self):
        df = pd.DataFrame({
            'Platform': ['Meta'] * 10,
            'Spend': [100] * 10,
            'Revenue': [200] * 10,
            'Impressions': [1000] * 10,
            'Clicks': [100] * 10,
            'Conversions': [10] * 10
        })
        res = DataQualityAnalyzer.validate_input(df)
        assert res['sample_size'] == 10
        assert any("Small sample size" in w for w in res['warnings'])
        
    def test_validate_input_polars(self):
        df_pl = pl.DataFrame({
            'Platform': ['Meta'],
            'Spend': [100],
            'Revenue': [200],
            'Impressions': [1000],
            'Clicks': [100],
            'Conversions': [10]
        })
        res = DataQualityAnalyzer.validate_input(df_pl)
        assert res['sample_size'] == 1
        
    def test_validate_input_no_platforms(self):
        df = pd.DataFrame({'Spend': [100], 'Date': ['2024-01-01']})
        res = DataQualityAnalyzer.validate_input(df)
        assert any("No Platform column found" in w for w in res['warnings'])
        assert res['completeness_score'] < 100

    def test_validate_input_pure_pandas(self):
        # To hit line 24 (pl.from_pandas(df)), we need an object that has 'columns' but not 'to_pandas'
        df_pd = pd.DataFrame({'Platform': ['meta'], 'Spend': [100]})
        # We can't easily remove 'to_pandas' from a real DF, so let's use a class that Polars accepts
        class PolarsCompatible:
            def __init__(self, df):
                self.columns = df.columns.tolist()
                self._df = df
            def __getitem__(self, key): return self._df[key]
            def to_dict(self, orient=None): return self._df.to_dict(orient=orient)
            @property
            def empty(self): return self._df.empty
            @property
            def iloc(self): return self._df.iloc
            @property
            def shape(self): return self._df.shape
        
        # Actually pl.from_pandas explicitly checks for pandas.DataFrame
        # But we can test the logic path if we find a way. 
        # For now, let's just use a dict-like mock for the scoring logic.
        res = DataQualityAnalyzer.validate_input(df_pd)
        assert res['sample_size'] == 1

    def test_generate_report_high_confidence(self, robust_df):
        report = generate_data_quality_report(robust_df, {"campaign_name": "Test Campaign"})
        assert "Data Quality Report - Test Campaign" in report
        assert "Analysis Confidence: HIGH" in report
        assert "All key fields present" in report
        
    def test_generate_report_low_confidence(self):
        df = pd.DataFrame({'Spend': [100]})
        report = generate_data_quality_report(df)
        assert "Analysis Confidence: LOW" in report
        assert "Recommendations to Improve Data Quality" in report
        assert "Track Impressions and Clicks" in report
        assert "Implement Revenue/Value tracking" in report
        assert "Aggregate more historical data" in report

# ==============================================================================
# DATA PREP LAYER TESTS
# ==============================================================================

class TestDataPrepLayer:
    @pytest.fixture
    def sample_raw_df(self):
        """Create a raw dataframe with platform-specific columns and messy data."""
        return pd.DataFrame({
            'platform': ['meta', 'google_ads', 'dv360', 'linkedin', 'snapcap', 'cm360', 'unknown'],
            'campaign_name': ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'],
            'spend_amt': [100, 200, 300, 400, 500, 600, 700], # Avoid mapping collision initially
            'impressions': [1000, 2000, 3000, 4000, 5000, 6000, 7000],
            'clicks': [100, 200, 300, 400, 500, 600, 700],
            'conversions': [10, 20, 30, 40, 50, 60, 70],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07']
        })

    def test_init_defaults(self):
        dpl = DataPrepLayer()
        assert dpl.outlier_method == 'iqr'
        assert dpl.vif_threshold == 10.0
        
    def test_get_data_prep_layer(self):
        dpl1 = get_data_prep_layer()
        dpl2 = get_data_prep_layer()
        assert dpl1 is dpl2

    def test_prepare_empty_df(self):
        dpl = DataPrepLayer()
        df, meta = dpl.prepare(pd.DataFrame())
        assert df.empty
        assert "error" in meta

    def test_prepare_platform_mapping_all(self):
        # Test CM360 and other mappings to ensure branch coverage
        platforms = ['google_ads', 'dv360', 'linkedin', 'snapchat', 'cm360']
        df_raw = pd.DataFrame({
            'platform': platforms,
            'campaign': ['c1'] * 5, # for google_ads
            'insertion_order': ['i1'] * 5, # for dv360
            'media_cost': [100] * 5, # for cm360
            'leads': [5] * 5, # for linkedin
            'swipes': [50] * 5, # for snapchat
            'date': ['2024-01-01'] * 5
        })
        dpl = DataPrepLayer()
        df, meta = dpl.prepare(df_raw)
        assert 'spend' in df.columns
        assert 'conversions' in df.columns
        # CM360: media_cost -> spend
        # LinkedIn: leads -> conversions
        assert df['spend'].notna().any()
        assert df['conversions'].notna().any()

    def test_prepare_no_platform_col(self):
        dpl = DataPrepLayer()
        df_raw = pd.DataFrame({'spend': [100], 'date': ['2024-01-01']})
        df, meta = dpl.prepare(df_raw, platform_col='missing_platform')
        assert df['spend'].iloc[0] == 100

    def test_clean_data_invalid_dates(self):
        dpl = DataPrepLayer()
        df_invalid = pd.DataFrame({
            'platform': ['meta', 'meta'],
            'date': ['invalid-date', '2024-01-01'],
            'spend': [100, 200]
        })
        df, meta = dpl.prepare(df_invalid)
        assert df['date'].isna().any()

    def test_handle_missing_values_roas_skip(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({
            'platform': ['meta'] * 5,
            'roas': [1.0, np.nan, 3.0, 4.0, 5.0],
            'date': pd.date_range('2024-01-01', periods=5)
        })
        df_out, meta = dpl.prepare(df)
        # ROAS strategy is 'skip', but Step 6 fills with 0 if it's numeric
        assert 'roas' in df_out.columns

    def test_handle_missing_values_target_protection(self):
        dpl = DataPrepLayer(missing_threshold=0.1) # Aggressive threshold
        df = pd.DataFrame({
            'platform': ['meta'] * 4,
            'target': [1, np.nan, np.nan, 4], # 50% missing > 10%
            'date': pd.date_range('2024-01-01', periods=4)
        })
        # If target_col is protected, it should stay
        df_out, meta = dpl.prepare(df, target_col='target')
        assert 'target' in df_out.columns

    def test_handle_outliers_non_numeric(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({
            'platform': ['meta'] * 5,
            'spend': ['not', 'a', 'number', '!', '?'],
            'date': ['2024-01-01'] * 5
        })
        # Should not crash and should skip numeric outlier check
        df_out, meta = dpl.prepare(df)
        assert len(meta['outlier_report']) == 0

    def test_engineer_features_variants(self):
        dpl = DataPrepLayer()
        # Test case where impressions are missing but spend exists (cpm check branch)
        df_missing_impr = pd.DataFrame({
            'spend': [100, 200],
            'impressions': [0, 0], # Will be replaced by nan then 0
            'clicks': [10, 20],
            'platform': ['meta', 'google'],
            'date': ['2024-01-01', '2024-01-02']
        })
        df, meta = dpl.prepare(df_missing_impr)
        assert df['cpm'].iloc[0] == 0
        
    def test_prepare_for_modeling_no_scaling(self):
        dpl = DataPrepLayer(enable_scaling=False)
        df = pd.DataFrame({
            'spend': np.arange(10),
            'target': np.arange(10),
            'platform': ['meta'] * 10,
            'date': pd.date_range('2024-01-01', periods=10)
        })
        # Use time_aware to ensure deterministic split without shuffling
        res = dpl.prepare_for_modeling(df, 'target', ['spend'], test_size=0.2, val_size=0.2, time_aware=True)
        assert res['scaler'] is None
        assert res['X_train']['spend'].iloc[0] == 0
        
    def test_clean_data_no_dedupe_cols(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({'other': [1, 2]})
        # Should not crash when platform/date/campaign are all missing
        df_out, meta = dpl.prepare(df, time_col='missing', platform_col='missing', group_col=None)
        assert len(df_out) == 2

    def test_engineer_features_missing_cols(self):
        # Test feature engineering with missing standard columns to hit branches
        dpl = DataPrepLayer(enable_feature_engineering=True)
        df_minimal = pd.DataFrame({
            'impressions': [1000],
            'clicks': [100],
            'platform': ['meta']
            # missing spend and date
        })
        df_out, meta = dpl.prepare(df_minimal)
        assert 'ctr' in df_out.columns
        assert 'cpm' not in df_out.columns # Requires spend
        assert 'day_of_week' not in df_out.columns # Requires date
        
    def test_handle_missing_values(self):
        dpl = DataPrepLayer()
        df_missing = pd.DataFrame({
            'platform': ['meta'] * 6,
            'spend': [100, np.nan, 300, 400, 500, 600], # strategy: zero
            'impressions': [1000, np.nan, 3000, 4000, 5000, 6000], # strategy: forward_fill
            'engagement': [0.2, np.nan, 0.4, 0.4, 0.5, 0.5], # strategy: mean
            'categorical': ['A', np.nan, 'B', 'C', 'D', 'E'], # strategy: mode
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06']
        })
        
        df, meta = dpl.prepare(df_missing)
        assert df['spend'].iloc[1] == 0
        # Mean of [0.2, 0.4, 0.4, 0.5, 0.5] is 2.0 / 5 = 0.4
        assert df['engagement'].iloc[1] == pytest.approx(0.4)
        assert df['categorical'].iloc[1] in ['A', 'B', 'C', 'D', 'E']

    def test_handle_outliers_iqr(self):
        dpl = DataPrepLayer(outlier_method='iqr')
        df_outliers = pd.DataFrame({
            'spend': [10, 12, 11, 13, 1000],
            'platform': ['meta'] * 5,
            'campaign': ['c1', 'c2', 'c3', 'c4', 'c5'], # Unique campaigns to avoid deduplication
            'date': ['2024-01-01'] * 5
        })
        df, meta = dpl.prepare(df_outliers)
        assert len(df) == 5
        assert df['spend'].iloc[4] < 1000
        assert meta['outlier_report']['spend'] == 1

    def test_handle_outliers_zscore(self):
        dpl = DataPrepLayer(outlier_method='zscore')
        df_outliers = pd.DataFrame({
            'spend': [10] * 100 + [10000],
            'platform': ['meta'] * 101,
            'campaign': [f'c{i}' for i in range(101)],
            'date': ['2024-01-01'] * 101
        })
        df, meta = dpl.prepare(df_outliers)
        assert df['spend'].iloc[100] < 10000

    def test_engineer_features(self):
        dpl = DataPrepLayer(enable_feature_engineering=True)
        df_base = pd.DataFrame({
            'spend': [100, 200],
            'impressions': [1000, 2000],
            'clicks': [50, 100],
            'conversions': [5, 10],
            'platform': ['meta', 'google'],
            'date': ['2024-01-01', '2024-01-02']
        })
        df, meta = dpl.prepare(df_base)
        assert 'ctr' in df.columns
        assert 'platform_meta' in df.columns.str.lower()
        assert 'day_of_week' in df.columns

    def test_prepare_for_modeling_stratified(self):
        dpl = DataPrepLayer(enable_scaling=True, enable_vif_check=True)
        df = pd.DataFrame({
            'spend': np.random.uniform(10, 100, 100),
            'ctr': np.random.uniform(1, 5, 100),
            'platform': ['Meta', 'Google'] * 50,
            'target': np.random.uniform(0, 10, 100),
            'date': pd.date_range('2024-01-01', periods=100)
        })
        
        prep_res = dpl.prepare_for_modeling(
            df, 
            target_col='target', 
            feature_cols=['spend', 'ctr'],
            test_size=0.2,
            val_size=0.2
        )
        
        assert 'X_train' in prep_res
        assert prep_res['X_train'].shape[1] == 2
        assert prep_res['scaler'] is not None

    def test_prepare_for_modeling_time_aware(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({
            'spend': np.arange(100),
            'target': np.arange(100),
            'date': pd.date_range('2024-01-01', periods=100)
        })
        
        prep_res = dpl.prepare_for_modeling(
            df, 
            target_col='target', 
            feature_cols=['spend'],
            time_aware=True,
            test_size=0.2,
            val_size=0.2
        )
        assert prep_res['X_train']['spend'].max() < prep_res['X_val']['spend'].min()

    def test_vif_check(self):
        dpl = DataPrepLayer(enable_vif_check=True, vif_threshold=5.0)
        x = np.linspace(0, 10, 100)
        df = pd.DataFrame({
            'f1': x,
            'f2': x * 2 + np.random.normal(0, 0.001, 100),
            'target': x * 3
        })
        
        prep_res = dpl.prepare_for_modeling(
            df, 
            target_col='target', 
            feature_cols=['f1', 'f2']
        )
        assert len(prep_res['feature_names']) == 1

    def test_prepare_for_modeling_errors(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({'f1': [1, 2, 3]})
        assert "error" in dpl.prepare_for_modeling(df, 'target', ['invalid'])
        assert "error" in dpl.prepare_for_modeling(df, 'target', ['f1'])

    def test_get_full_report(self):
        dpl = DataPrepLayer()
        df = pd.DataFrame({
            'spend': [100, 200],
            'platform': ['meta', 'google'],
            'date': ['2024-01-01', '2024-01-02']
        })
        dpl.prepare(df)
        report = dpl.get_full_report()
        assert 'missing' in report
        assert 'outliers' in report
