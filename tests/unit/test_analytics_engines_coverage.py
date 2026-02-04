import pytest
import pandas as pd
import numpy as np
import polars as pl
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from src.engine.analytics.anomaly_detector import AnomalyDetector
from src.engine.analytics.pacing_analyzer import PacingAnalyzer
from src.engine.analytics.causal_analysis import (
    CausalAnalysisEngine,
    CausalAnalysisResult,
    ComponentContribution,
    DecompositionMethod
)
import sys
import importlib

# ==============================================================================
# ANOMALY DETECTOR TESTS
# ==============================================================================

class TestAnomalyDetector:
    def test_detect_anomalies_basic(self):
        # Create data with an extreme outlier (> 2 SD)
        # Using [1, 1, 1, 1, 1, 100] ensures z-score > 2.0
        df = pl.DataFrame({
            'cpc': [1.0, 1.0, 1.0, 1.0, 1.0, 100.0]
        })
        results = AnomalyDetector.detect_anomalies(df, metric='cpc')
        assert len(results) == 1
        assert results[0]['cpc'] == 100.0

    def test_detect_anomalies_empty_missing(self):
        assert AnomalyDetector.detect_anomalies(pl.DataFrame(), 'cpc') == []
        df = pl.DataFrame({'spend': [100]})
        assert AnomalyDetector.detect_anomalies(df, 'missing') == []

# ==============================================================================
# PACING ANALYZER TESTS
# ==============================================================================

class TestPacingAnalyzer:
    def test_analyze_pacing_on_track(self):
        df = pl.DataFrame({'spend': [50, 50]}) # Total 100
        # 100 budget, 10/20 days elapsed -> expected 50. 100 spend -> 200% pacing (overspending)
        # Let's adjust for on-track (100%): 100 spend, 10/10 days, 100 budget
        res = PacingAnalyzer.analyze_pacing(df, total_budget=100.0, days_elapsed=10, total_days=10)
        assert res['pacing_percent'] == 100.0
        assert res['status'] == 'on_track'

    def test_analyze_pacing_overspending(self):
        df = pl.DataFrame({'spend': [120]})
        res = PacingAnalyzer.analyze_pacing(df, total_budget=100.0, days_elapsed=5, total_days=10)
        # Expected spend = 100 / 10 * 5 = 50. Actual = 120. Pacing = 240%
        assert res['pacing_percent'] == 240.0
        assert res['status'] == 'overspending'

    def test_analyze_pacing_underspending(self):
        df = pl.DataFrame({'spend': [20]})
        res = PacingAnalyzer.analyze_pacing(df, total_budget=100.0, days_elapsed=5, total_days=10)
        # Expected 50. Actual 20. Pacing 40%
        assert res['status'] == 'underspending'

    def test_analyze_pacing_empty_edge(self):
        assert PacingAnalyzer.analyze_pacing(pl.DataFrame(), 100, 1, 10) == {}
        res = PacingAnalyzer.analyze_pacing(pl.DataFrame({'spend': [10]}), 100, 1, 0)
        assert res['pacing_percent'] == 0

# ==============================================================================
# CAUSAL ANALYSIS ENGINE TESTS
# ==============================================================================

class TestCausalAnalysisEngine:
    @pytest.fixture
    def sample_causal_df(self):
        dates = pd.date_range('2024-01-01', periods=10)
        return pd.DataFrame({
            'Date': dates,
            'Spend': [100, 110, 105, 100, 110, 200, 210, 205, 200, 210],
            'Revenue': [500, 550, 525, 500, 550, 800, 840, 820, 800, 840],
            'Conversions': [10, 11, 10, 10, 11, 15, 16, 15, 15, 16],
            'Clicks': [100, 110, 105, 100, 110, 150, 160, 155, 150, 160],
            'Impressions': [1000, 1100, 1050, 1000, 1100, 1500, 1600, 1550, 1500, 1600],
            'Platform': ['Meta', 'Google', 'Meta', 'Google', 'Meta', 'Meta', 'Google', 'Meta', 'Google', 'Meta'],
            'Channel': ['Social', 'Search', 'Social', 'Search', 'Social', 'Social', 'Search', 'Social', 'Search', 'Social']
        })

    def test_analyze_roas_basic(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        result = engine.analyze(
            sample_causal_df, 
            metric='ROAS', 
            split_date='2024-01-06'
        )
        assert isinstance(result, CausalAnalysisResult)
        assert result.total_change != 0
        assert len(result.contributions) > 0
        assert result.primary_driver is not None
        assert "ROAS" in result.insights[0]

    def test_analyze_cpa(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        result = engine.analyze(sample_causal_df, metric='CPA', split_date='2024-01-06')
        assert result.metric == 'CPA'
        assert any("CPC" in c.component for c in result.contributions)

    def test_analyze_ctr_cvr(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        res_ctr = engine.analyze(sample_causal_df, metric='CTR', split_date='2024-01-06')
        assert res_ctr.metric == 'CTR'
        res_cvr = engine.analyze(sample_causal_df, metric='CVR', split_date='2024-01-06')
        assert res_cvr.metric == 'CVR'

    def test_analyze_cpc_cpm_revenue_spend(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        for m in ['CPC', 'CPM', 'Revenue', 'Spend']:
            res = engine.analyze(sample_causal_df, metric=m, split_date='2024-01-06', include_attribution=True)
            assert res.metric == m
            assert len(res.channel_attribution) > 0
            assert len(res.platform_attribution) > 0

    def test_analyze_insufficient_data(self):
        engine = CausalAnalysisEngine()
        df = pd.DataFrame({'Date': ['2024-01-01'], 'Spend': [100]})
        result = engine.analyze(df, 'ROAS')
        assert "Insufficient data" in result.insights[0]

    def test_analyze_ml_integration(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', True):
            # Mock XGBoost and SHAP if they would fail or take long
            # However, the logic checks SHAP_AVAILABLE first.
            # Let's mock the internal _ml_causal_impact to verify it's called
            with patch.object(engine, '_ml_causal_impact') as mock_ml:
                mock_ml.return_value = ({'f1': 0.5}, {'f1': 0.1})
                result = engine.analyze(sample_causal_df, 'ROAS', include_ml=True)
                assert result.ml_drivers == {'f1': 0.5}
                assert result.shap_values == {'f1': 0.1}

    def test_analyze_shapley_method(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        result = engine.analyze(
            sample_causal_df, 
            metric='ROAS', 
            method=DecompositionMethod.SHAPLEY
        )
        assert result.method == DecompositionMethod.SHAPLEY.value

    def test_analyze_kb_integration(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        with patch('src.engine.analytics.causal_analysis.KB_AVAILABLE', True):
            mock_kb = MagicMock()
            mock_kb.enhance_causal_result.return_value = {
                "enhanced_recommendations": ["KB Rec 1"],
                "interpretation": {"insights": ["KB Insight 1"]},
                "pitfall_warnings": [{"pitfall": "P1", "solution": "S1"}]
            }
            with patch('src.engine.analytics.causal_analysis.get_knowledge_base', return_value=mock_kb):
                result = engine.analyze(sample_causal_df, 'ROAS')
                # The strings from the mock should be present in the result lists
                assert any("KB Rec 1" in r for r in result.recommendations)
                assert any("KB Insight 1" in i for i in result.insights)
                assert any("P1" in i and "S1" in i for i in result.insights)

    def test_calculate_confidence(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # Mocking values to ensure t-test runs
        conf = engine._calculate_confidence(
            sample_causal_df.head(5), 
            sample_causal_df.tail(5), 
            'ROAS'
        )
        assert 0 <= conf <= 1

    def test_create_empty_result(self):
        engine = CausalAnalysisEngine()
        res = engine._create_empty_result('CTR')
        assert res.metric == 'CTR'
        assert res.before_value == 0.0

    def test_decompose_generic(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # Test with a metric that doesn't have a specific decomposer
        res = engine.analyze(sample_causal_df, metric='UnknownMetric')
        assert any("Unknown Factor" == c.component for c in res.contributions)

    def test_ml_causal_impact_real_path(self):
        # Create a larger dataset to pass the row count check (>= 20)
        dates = pd.date_range('2024-01-01', periods=30)
        df_large = pd.DataFrame({
            'Date': dates,
            'Spend': np.random.rand(30) * 100,
            'Revenue': np.random.rand(30) * 500,
            'Conversions': np.random.randint(1, 10, 30),
            'Clicks': np.random.randint(10, 100, 30),
            'Impressions': np.random.randint(100, 1000, 30)
        })
        engine = CausalAnalysisEngine()
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', True):
            # Mock XGBoost and SHAP to avoid dependency issues in dev env
            mock_model = MagicMock()
            mock_model.feature_importances_ = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
            with patch('xgboost.XGBRegressor', return_value=mock_model):
                with patch('shap.TreeExplainer') as mock_explainer:
                    mock_explainer.return_value.shap_values.return_value = np.random.rand(30, 5)
                    # Revenue has Spend, Conversions, Clicks, Impressions as features
                    imp, shap = engine._ml_causal_impact(df_large, 'Revenue', pd.Timestamp('2024-01-06'))
                    assert imp is not None
                    assert shap is not None

    def test_generate_recommendations_branches(self):
        engine = CausalAnalysisEngine()
        # Create 4 contributions with negative impact for different components
        # to ensure we hit the loop-back and the 3-item limit
        contribs = [
            ComponentContribution("CPC", -1.0, 25, 1.0, 2.0, 1.0, 100, "negative", "high"),
            ComponentContribution("CVR", -1.0, 25, 0.05, 0.02, -0.03, -60, "negative", "high"),
            ComponentContribution("Spend", -1.0, 25, 100, 200, 100, 100, "negative", "high"),
            ComponentContribution("Success", 1.0, 25, 100, 200, 100, 100, "positive", "high")
        ]
        recs = engine._generate_recommendations("ROAS", contribs, {}, {"Meta": -10.0})
        # Use simple 'in' checks for substrings
        recs_str = " ".join(recs)
        assert "Reduce CPC" in recs_str
        assert "Improve Conversion Rate" in recs_str
        # Only top 3 from 'actionable' loop are taken
        # So Spend or Success might be there depending on sort.
        # But we hit the branches during the loop.
        assert "Review Meta" in recs_str

    def test_component_contribution_str(self):
        c = ComponentContribution("Test", 5.0, 50.0, 10, 15, 5, 50, "positive", "high")
        assert "+$5.00 (50.0%)" in str(c)
        c2 = ComponentContribution("Test", -5.0, 50.0, 15, 10, -5, -33, "negative", "high")
        assert "-$5.00 (50.0%)" in str(c2)

    def test_attribution_missing_columns(self):
        engine = CausalAnalysisEngine()
        df = pd.DataFrame({'Date': pd.date_range('2024-01-01', periods=4), 'Spend': [100]*4})
        # Should return empty dicts without crashing
        assert engine._calculate_channel_attribution(df, df, 'Spend') == {}
        assert engine._calculate_platform_attribution(df, df, 'Spend') == {}

    def test_ml_causal_impact_failures(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', False):
            assert engine._ml_causal_impact(sample_causal_df, 'ROAS', None) == (None, None)
        
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', True):
            # Test insufficient features
            tiny_df = pd.DataFrame({'ROAS': [1, 2], 'A': [3, 4]})
            # No split_date needed for this check
            assert engine._ml_causal_impact(tiny_df, 'ROAS', pd.Timestamp('2024-01-01')) == (None, None)
            
            # Test insufficient rows
            assert engine._ml_causal_impact(sample_causal_df, 'ROAS', pd.Timestamp('2024-01-01')) == (None, None)

    def test_availability_flags_coverage(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # Test paths where KB_AVAILABLE is False
        with patch('src.engine.analytics.causal_analysis.KB_AVAILABLE', False):
            res = engine.analyze(sample_causal_df, 'ROAS')
            assert res.confidence > 0
            
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', False):
            res = engine.analyze(sample_causal_df, 'ROAS', include_ml=True)
            assert res.ml_drivers is None

    def test_calculate_confidence_exception(self):
        engine = CausalAnalysisEngine()
        # Trigger exception by passing something that self._calculate_metric will fail on
        # inside the list comprehension.
        conf = engine._calculate_confidence(pd.DataFrame({'A': [1]}), pd.DataFrame({'A': [1]}), 'ROAS')
        # Since 'Revenue'/'Spend' are missing, _calculate_metric returns 0. ttest_ind of [0], [0] will fail or return nan
        assert 0 <= conf <= 1

    def test_kb_enhancement_error(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        with patch('src.engine.analytics.causal_analysis.KB_AVAILABLE', True):
            with patch('src.engine.analytics.causal_analysis.get_knowledge_base', side_effect=Exception("KB Error")):
                res = engine._create_empty_result('ROAS')
                enhanced = engine._enhance_with_knowledge_base(res, {})
                assert enhanced == res # Should return original result on error

    def test_import_errors_real_reload(self):
        # This is a bit advanced: reload module with missing dependencies
        # to hit lines 26-28 and 34-35
        with patch.dict('sys.modules', {'xgboost': None, 'shap': None, 'src.platform.knowledge.causal_kb_rag': None}):
            # We must be careful not to break other tests
            # Reloading in a separate test might be okay
            import src.engine.analytics.causal_analysis as ca
            importlib.reload(ca)
            assert ca.SHAP_AVAILABLE is False
            assert ca.KB_AVAILABLE is False
        # Restore for other tests
        importlib.reload(ca)

    def test_all_decomposers_zero_contribution(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        df = sample_causal_df.copy()
        for metric in ['CTR', 'CVR', 'CPC', 'CPM', 'Revenue']:
            res = engine._decompose_metric(df, df, metric, DecompositionMethod.HYBRID)
            assert all(c.absolute_change == 0 for c in res)

    def test_generate_insights_zero_platform_attr(self):
        engine = CausalAnalysisEngine()
        # To hit 1052->1059, we need platform_attr to be truthy but sorted_platforms to be falsy
        # This is hard with a dict. But we can mock platform_attr.items()
        mock_attr = MagicMock()
        mock_attr.__len__.return_value = 1 # Truthy
        mock_attr.items.return_value = [] # Sorted will be empty
        insights = engine._generate_insights("ROAS", 1.0, 10.0, [], {}, mock_attr)
        assert len(insights) == 1

    def test_import_errors_coverage(self):
        # We need to test the logic where KB_AVAILABLE or SHAP_AVAILABLE is False
        # These are usually set at module level, so we patch them in the module
        with patch('src.engine.analytics.causal_analysis.KB_AVAILABLE', False):
            engine = CausalAnalysisEngine()
            # This should skip the KB enhancement logic
            res = engine._create_empty_result('ROAS')
            enhanced = engine._enhance_with_knowledge_base(res, {})
            assert enhanced == res

    def test_zero_contribution_branches(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # Create a cases where metrics are identical before and after -> 0 contribution
        df_identical = sample_causal_df.copy()
        # Force a case where Spend is identical
        contribs = engine._decompose_spend(df_identical, df_identical)
        # total_abs_contribution will be 0, hits branch skip
        assert all(c.absolute_change == 0 for c in contribs)
        
        # Test other decomposing functions for 0 contribution branches
        # ROAS
        res = engine._decompose_roas(df_identical, df_identical)
        assert all(c.absolute_change == 0 for c in res)
        # CPA
        res = engine._decompose_cpa(df_identical, df_identical)
        assert all(c.absolute_change == 0 for c in res)

    def test_decompose_spend_no_platform_branch(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # df without 'Platform' column to hit the branch skip at 799
        df_no_plat = sample_causal_df.drop(columns=['Platform'])
        contribs = engine._decompose_spend(df_no_plat, df_no_plat)
        assert len(contribs) == 0

    def test_generate_insights_no_platforms_branch(self):
        engine = CausalAnalysisEngine()
        # Hits line 1052 skip branch
        insights = engine._generate_insights("ROAS", 1.0, 10.0, [], {}, {})
        assert len(insights) == 1

    def test_analyze_exception(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        # Mock pd.to_datetime to raise an error inside analyze
        with patch('pandas.to_datetime', side_effect=Exception("Date Error")):
            res = engine.analyze(sample_causal_df, 'ROAS')
            assert res.insights == ["Insufficient data for causal analysis"]

    def test_ml_causal_impact_exception(self):
        engine = CausalAnalysisEngine()
        df = pd.DataFrame({
            'A': np.random.rand(30),
            'B': np.random.rand(30),
            'Revenue': np.random.rand(30)
        })
        with patch('src.engine.analytics.causal_analysis.SHAP_AVAILABLE', True):
            with patch.object(engine.scaler, 'fit_transform', side_effect=ValueError("Scale Error")):
                imp, shap = engine._ml_causal_impact(df, 'Revenue', pd.Timestamp('2024-01-01'))
                assert imp is None
                assert shap is None

    def test_calculate_confidence_ttest_error(self, sample_causal_df):
        engine = CausalAnalysisEngine()
        with patch('scipy.stats.ttest_ind', side_effect=Exception("Stats Error")):
            conf = engine._calculate_confidence(sample_causal_df.head(5), sample_causal_df.tail(5), 'ROAS')
            assert conf == 0.5

    def test_generate_insights_empty_branches(self):
        engine = CausalAnalysisEngine()
        # No contributions, no platform_attr
        insights = engine._generate_insights("ROAS", 1.0, 10.0, [], {}, {})
        assert len(insights) == 1 # Only overall change

    def test_decompose_spend_zero_base(self):
        engine = CausalAnalysisEngine()
        # Case where before_spend is 0 to hit line 814 division check
        before = pd.DataFrame({'Platform': ['A'], 'Spend': [0]})
        after = pd.DataFrame({'Platform': ['A'], 'Spend': [100]})
        contribs = engine._decompose_spend(before, after)
        assert contribs[0].delta_pct == 0

    def test_enhance_kb_no_pitfalls(self):
        engine = CausalAnalysisEngine()
        res = engine._create_empty_result('ROAS')
        with patch('src.engine.analytics.causal_analysis.KB_AVAILABLE', True):
            mock_kb = MagicMock()
            mock_kb.enhance_causal_result.return_value = {
                "enhanced_recommendations": [],
                "interpretation": {},
                "pitfall_warnings": [] # Empty pitfalls
            }
            with patch('src.engine.analytics.causal_analysis.get_knowledge_base', return_value=mock_kb):
                enhanced = engine._enhance_with_knowledge_base(res, {})
                assert len(enhanced.insights) == 1 # Just the default empty insights
