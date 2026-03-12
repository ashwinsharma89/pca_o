import pytest
pytest.skip("Orphaned test - src.engine.analytics.regression module removed", allow_module_level=True)
"""
Unit Tests for Regression Module - ModelEvaluator

Tests comprehensive metrics computation:
- R2, MAE, RMSE, MAPE, SMAPE
- Residual diagnostics
- Prediction intervals
"""


import pytest
import numpy as np
from src.engine.analytics.regression.model_evaluator import (
    ModelEvaluator,
    ModelMetrics,
    ResidualDiagnostics
)


@pytest.mark.skip(reason="Orphaned test - src.engine.analytics.regression module removed")
class TestModelEvaluator:
    """Test ModelEvaluator functionality."""
    
    def setup_method(self):
        """Setup test data."""
        np.random.seed(42)
        self.n = 100
        
        # Perfect predictions (R² = 1.0)
        self.y_train_perfect = np.array([1, 2, 3, 4, 5])
        self.y_train_pred_perfect = np.array([1, 2, 3, 4, 5])
        
        # Realistic predictions
        self.y_train = np.random.uniform(10, 100, 80)
        self.y_test = np.random.uniform(10, 100, 20)
        self.y_train_pred = self.y_train + np.random.normal(0, 5, 80)
        self.y_test_pred = self.y_test + np.random.normal(0, 10, 20)
    
    def test_evaluate_returns_metrics(self):
        """Test that evaluate returns ModelMetrics object."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        assert isinstance(metrics, ModelMetrics)
        assert hasattr(metrics, 'r2_train')
        assert hasattr(metrics, 'r2_test')
        assert hasattr(metrics, 'mae')
        assert hasattr(metrics, 'rmse')
        assert hasattr(metrics, 'mape')
        assert hasattr(metrics, 'smape')
    
    def test_perfect_predictions(self):
        """Test metrics with perfect predictions."""
        metrics = ModelEvaluator.evaluate(
            self.y_train_perfect, self.y_train_perfect,
            self.y_train_pred_perfect, self.y_train_pred_perfect
        )
        
        assert metrics.r2_train == pytest.approx(1.0, abs=0.01)
        assert metrics.r2_test == pytest.approx(1.0, abs=0.01)
        assert metrics.mae == pytest.approx(0.0, abs=0.01)
        assert metrics.rmse == pytest.approx(0.0, abs=0.01)
    
    def test_r2_bounds(self):
        """Test that R² is within valid bounds."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        # R² can be negative for very poor models, but should be < 1
        assert metrics.r2_train <= 1.0
        assert metrics.r2_test <= 1.0
    
    def test_mae_positive(self):
        """Test that MAE is always positive."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        assert metrics.mae >= 0
        assert metrics.rmse >= 0
    
    def test_rmse_greater_than_mae(self):
        """Test that RMSE >= MAE (mathematical property)."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        assert metrics.rmse >= metrics.mae
    
    def test_train_test_gap(self):
        """Test train-test gap calculation."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        expected_gap = metrics.r2_train - metrics.r2_test
        assert metrics.train_test_gap == pytest.approx(expected_gap, abs=0.01)
    
    def test_metrics_to_dict(self):
        """Test conversion to dictionary."""
        metrics = ModelEvaluator.evaluate(
            self.y_train, self.y_test,
            self.y_train_pred, self.y_test_pred
        )
        
        result = metrics.to_dict()
        
        assert 'performance' in result
        assert 'diagnostics' in result
        assert 'interpretation' in result
        
        assert 'r2_test' in result['performance']
        assert 'mae' in result['performance']
        assert 'rmse' in result['performance']
    
    def test_overfitting_assessment(self):
        """Test overfitting risk assessment."""
        # Low gap (good)
        metrics_good = ModelMetrics(
            r2_train=0.80, r2_test=0.78, mae=10, rmse=12,
            mape=15, smape=14, train_test_gap=0.02,
            residual_std=5, n_train=80, n_test=20
        )
        assert metrics_good._assess_overfitting() == "Low"
        
        # High gap (overfitting)
        metrics_bad = ModelMetrics(
            r2_train=0.95, r2_test=0.60, mae=10, rmse=12,
            mape=15, smape=14, train_test_gap=0.35,
            residual_std=5, n_train=80, n_test=20
        )
        assert metrics_bad._assess_overfitting() == "High"
    
    def test_quality_assessment(self):
        """Test overall quality assessment."""
        # Excellent model
        metrics_excellent = ModelMetrics(
            r2_train=0.80, r2_test=0.78, mae=10, rmse=12,
            mape=15, smape=14, train_test_gap=0.02,
            residual_std=5, n_train=80, n_test=20
        )
        assert metrics_excellent._assess_quality() == "Excellent"
        
        # Poor model
        metrics_poor = ModelMetrics(
            r2_train=0.25, r2_test=0.20, mae=50, rmse=60,
            mape=80, smape=75, train_test_gap=0.05,
            residual_std=30, n_train=80, n_test=20
        )
        assert metrics_poor._assess_quality() == "Poor"


class TestResidualDiagnostics:
    """Test residual analysis."""
    
    def setup_method(self):
        """Setup test data."""
        np.random.seed(42)
        self.y_true = np.random.uniform(10, 100, 100)
        self.y_pred = self.y_true + np.random.normal(0, 5, 100)
    
    def test_analyze_residuals(self):
        """Test residual analysis."""
        diagnostics = ModelEvaluator.analyze_residuals(self.y_true, self.y_pred)
        
        assert isinstance(diagnostics, ResidualDiagnostics)
        assert hasattr(diagnostics, 'mean')
        assert hasattr(diagnostics, 'std')
        assert hasattr(diagnostics, 'shapiro_p_value')
    
    def test_residual_mean_near_zero(self):
        """Test that residual mean is near zero for unbiased predictions."""
        diagnostics = ModelEvaluator.analyze_residuals(self.y_true, self.y_pred)
        
        # Mean should be close to 0 for unbiased predictions
        assert abs(diagnostics.mean) < 5
    
    def test_outlier_detection(self):
        """Test outlier detection."""
        # Add some outliers
        y_pred_with_outliers = self.y_pred.copy()
        y_pred_with_outliers[0] = self.y_true[0] + 100  # Large error
        
        diagnostics = ModelEvaluator.analyze_residuals(self.y_true, y_pred_with_outliers)
        
        assert diagnostics.outlier_count > 0
    
    def test_normality_test(self):
        """Test Shapiro-Wilk normality test."""
        diagnostics = ModelEvaluator.analyze_residuals(self.y_true, self.y_pred)
        
        # p-value should be between 0 and 1
        assert 0 <= diagnostics.shapiro_p_value <= 1


class TestPredictionIntervals:
    """Test prediction interval generation."""
    
    def test_prediction_intervals(self):
        """Test that prediction intervals are generated correctly."""
        y_pred = np.array([10, 20, 30, 40, 50])
        residual_std = 5.0
        
        lower, upper = ModelEvaluator.generate_prediction_intervals(
            y_pred, residual_std, confidence=0.95
        )
        
        # Check shapes
        assert lower.shape == y_pred.shape
        assert upper.shape == y_pred.shape
        
        # Check bounds
        assert np.all(lower < y_pred)
        assert np.all(upper > y_pred)
        
        # Check symmetry
        margin = upper - y_pred
        assert np.allclose(y_pred - lower, margin)
    
    def test_confidence_level_affects_width(self):
        """Test that higher confidence = wider intervals."""
        y_pred = np.array([50])
        residual_std = 10.0
        
        lower_95, upper_95 = ModelEvaluator.generate_prediction_intervals(
            y_pred, residual_std, confidence=0.95
        )
        lower_80, upper_80 = ModelEvaluator.generate_prediction_intervals(
            y_pred, residual_std, confidence=0.80
        )
        
        width_95 = upper_95[0] - lower_95[0]
        width_80 = upper_80[0] - lower_80[0]
        
        assert width_95 > width_80


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
