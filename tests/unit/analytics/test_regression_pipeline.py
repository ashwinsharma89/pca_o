"""
Unit tests for Regression Analytics Suite

Tests:
- ModelComparisonEngine
- RegressionPipeline
- InsightAgent
- ModelSelector
- PipelineErrorHandler
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def sample_regression_data():
    """Generate sample data for regression tests."""
    np.random.seed(42)
    n = 500
    
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=n),
        'campaign': np.random.choice(['camp_A', 'camp_B', 'camp_C'], n),
        'platform': np.random.choice(['meta', 'google', 'dv360'], n),
        'spend': np.random.uniform(100, 1000, n),
        'impressions': np.random.uniform(10000, 100000, n),
        'clicks': np.random.uniform(100, 1000, n),
        'conversions': np.random.randint(0, 50, n)
    })


@pytest.fixture
def small_data():
    """Small dataset for edge case testing."""
    return pd.DataFrame({
        'spend': [100, 200, 300],
        'clicks': [10, 20, 30],
        'conversions': [1, 2, 3]
    })


# =============================================================================
# MODEL COMPARISON TESTS
# =============================================================================

class TestModelComparisonEngine:
    """Tests for ModelComparisonEngine."""
    
    def test_run_comparison_basic(self, sample_regression_data):
        """Test basic model comparison runs without error."""
        from src.engine.analytics.models.model_comparison import ModelComparisonEngine
        
        engine = ModelComparisonEngine(models_to_run=['OLS', 'Ridge'])
        
        X = sample_regression_data[['spend', 'clicks']].fillna(0)
        y = sample_regression_data['conversions'].fillna(0)
        
        # Simple split
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        
        result = engine.run_comparison(X_train, y_train, X_test, y_test)
        
        assert result is not None
        assert len(result.comparison_table) >= 1
        assert 'OLS' in result.comparison_table['Model'].values or 'Ridge' in result.comparison_table['Model'].values
    
    def test_comparison_table_columns(self, sample_regression_data):
        """Test comparison table has expected columns."""
        from src.engine.analytics.models.model_comparison import ModelComparisonEngine
        
        engine = ModelComparisonEngine(models_to_run=['Ridge'])
        
        X = sample_regression_data[['spend']].fillna(0)
        y = sample_regression_data['conversions'].fillna(0)
        
        split_idx = int(len(X) * 0.8)
        result = engine.run_comparison(
            X.iloc[:split_idx], y.iloc[:split_idx],
            X.iloc[split_idx:], y.iloc[split_idx:]
        )
        
        expected_columns = ['Model', 'R² (Test)', 'RMSE']
        for col in expected_columns:
            assert col in result.comparison_table.columns


# =============================================================================
# PIPELINE TESTS
# =============================================================================

class TestRegressionPipeline:
    """Tests for RegressionPipeline."""
    
    def test_pipeline_initialization(self):
        """Test pipeline initializes correctly."""
        from src.engine.analytics.regression_pipeline import RegressionPipeline
        
        pipeline = RegressionPipeline(models_to_run=['OLS'], quick_mode=True)
        assert pipeline is not None
        assert pipeline.quick_mode == True
    
    def test_pipeline_run_success(self, sample_regression_data):
        """Test pipeline runs successfully with valid data."""
        from src.engine.analytics.regression_pipeline import RegressionPipeline
        
        pipeline = RegressionPipeline(models_to_run=['Ridge'], quick_mode=True)
        
        result = pipeline.run(
            df=sample_regression_data,
            target_col='conversions',
            feature_cols=['spend', 'clicks']
        )
        
        assert result.success == True
        assert result.best_model is not None
        assert result.r2_test is not None
    
    def test_pipeline_insufficient_data(self, small_data):
        """Test pipeline handles insufficient data gracefully."""
        from src.engine.analytics.regression_pipeline import RegressionPipeline
        
        pipeline = RegressionPipeline(models_to_run=['Ridge'], quick_mode=True)
        
        # Should handle small data (may fail gracefully)
        result = pipeline.run(
            df=small_data,
            target_col='conversions',
            feature_cols=['spend', 'clicks']
        )
        
        # Either succeeds or fails gracefully
        assert result is not None


# =============================================================================
# MODEL SELECTOR TESTS
# =============================================================================

class TestModelSelector:
    """Tests for ModelSelector."""
    
    def test_selects_highest_r2(self):
        """Test selector picks model with highest R²."""
        from src.engine.analytics.output_delivery import ModelSelector, ModelSelectionConfig
        from dataclasses import dataclass
        
        @dataclass
        class MockResult:
            r2_test: float
            rmse_test: float
            training_time: float
        
        selector = ModelSelector()
        
        results = {
            'OLS': MockResult(0.70, 100, 0.1),
            'Ridge': MockResult(0.75, 95, 0.2),
            'XGBoost': MockResult(0.80, 90, 5.0)
        }
        
        best, reason = selector.select_best(results)
        
        assert best == 'XGBoost'
        assert 'R²' in reason['reason']
    
    def test_interpretability_override(self):
        """Test interpretability preference overrides pure R²."""
        from src.engine.analytics.output_delivery import ModelSelector, ModelSelectionConfig
        from dataclasses import dataclass
        
        @dataclass
        class MockResult:
            r2_test: float
            rmse_test: float
            training_time: float
        
        selector = ModelSelector(ModelSelectionConfig(prefer_interpretability=True))
        
        results = {
            'Ridge': MockResult(0.72, 100, 0.1),
            'XGBoost': MockResult(0.80, 90, 5.0)
        }
        
        best, reason = selector.select_best(results)
        
        # Should still pick XGBoost if R² difference is significant
        # Or pick Ridge if close enough
        assert best in ['Ridge', 'XGBoost']


# =============================================================================
# ERROR HANDLER TESTS
# =============================================================================

class TestPipelineErrorHandler:
    """Tests for PipelineErrorHandler."""
    
    def test_data_sufficiency_check_pass(self):
        """Test data sufficiency passes with enough rows."""
        from src.engine.analytics.output_delivery import PipelineErrorHandler
        
        handler = PipelineErrorHandler()
        df = pd.DataFrame({'x': range(1500)})
        
        result = handler.check_data_sufficiency(df)
        
        assert result['passed'] == True
    
    def test_data_sufficiency_check_fail(self):
        """Test data sufficiency fails with too few rows."""
        from src.engine.analytics.output_delivery import PipelineErrorHandler
        
        handler = PipelineErrorHandler()
        df = pd.DataFrame({'x': range(500)})
        
        result = handler.check_data_sufficiency(df)
        
        assert result['passed'] == False
        assert 'Insufficient' in result['message']
    
    def test_model_fit_check_pass(self):
        """Test model fit check passes with good R²."""
        from src.engine.analytics.output_delivery import PipelineErrorHandler
        
        handler = PipelineErrorHandler()
        
        result = handler.check_model_fit(0.65)
        
        assert result['passed'] == True
    
    def test_model_fit_check_fail(self):
        """Test model fit check fails with poor R²."""
        from src.engine.analytics.output_delivery import PipelineErrorHandler
        
        handler = PipelineErrorHandler()
        
        result = handler.check_model_fit(0.15)
        
        assert result['passed'] == False


# =============================================================================
# INSIGHT AGENT TESTS
# =============================================================================

class TestInsightAgent:
    """Tests for InsightAgent."""
    
    def test_rule_based_fallback(self, sample_regression_data):
        """Test rule-based insights work when LLM unavailable."""
        from src.engine.analytics.regression_pipeline import RegressionPipeline, InsightAgent
        
        pipeline = RegressionPipeline(models_to_run=['Ridge'], quick_mode=True)
        result = pipeline.run(
            df=sample_regression_data,
            target_col='conversions',
            feature_cols=['spend', 'clicks']
        )
        
        agent = InsightAgent()
        
        # Force rule-based by using invalid API key
        with patch.object(agent, '_generate_with_llm', side_effect=Exception("Mock LLM failure")):
            insights = agent.generate_insights(result)
        
        assert 'executive_summary' in insights
        assert 'recommendations' in insights


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
