"""
Model Evaluator - Comprehensive Metrics for Production ML

Computes all metrics required for stakeholder-facing ML:
- Performance: R², MAE, RMSE, MAPE, SMAPE
- Diagnostics: Train-test gap, overfitting detection
- Interpretations: Plain English explanations

Author: Senior ML Expert (Google Ads Platform experience)
"""

from dataclasses import dataclass, asdict
from typing import Dict, Optional, List
import numpy as np
from scipy import stats
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error


@dataclass
class ModelMetrics:
    """Complete model performance metrics with interpretations."""
    
    # Core Performance
    r2_train: float
    r2_test: float
    mae: float
    rmse: float
    mape: float
    smape: float
    
    # Diagnostics
    train_test_gap: float
    residual_std: float
    
    # Sample info
    n_train: int
    n_test: int
    
    def to_dict(self) -> Dict:
        """Convert to dict with rounded values and interpretation."""
        return {
            "performance": {
                "r2_train": round(self.r2_train, 3),
                "r2_test": round(self.r2_test, 3),
                "mae": round(self.mae, 2),
                "rmse": round(self.rmse, 2),
                "mape": round(self.mape, 2),
                "smape": round(self.smape, 2)
            },
            "diagnostics": {
                "train_test_gap": round(self.train_test_gap, 3),
                "residual_std": round(self.residual_std, 2),
                "overfitting_risk": self._assess_overfitting(),
                "model_quality": self._assess_quality()
            },
            "sample_size": {
                "train": self.n_train,
                "test": self.n_test,
                "total": self.n_train + self.n_test
            },
            "interpretation": self._generate_interpretation()
        }
    
    def _assess_overfitting(self) -> str:
        """Assess overfitting risk based on train-test gap."""
        if self.train_test_gap < 0.05:
            return "Low"
        elif self.train_test_gap < 0.10:
            return "Moderate"
        else:
            return "High"
    
    def _assess_quality(self) -> str:
        """Overall model quality assessment."""
        if self.r2_test >= 0.7 and self.train_test_gap < 0.10:
            return "Excellent"
        elif self.r2_test >= 0.5 and self.train_test_gap < 0.15:
            return "Good"
        elif self.r2_test >= 0.3:
            return "Fair"
        else:
            return "Poor"
    
    def _generate_interpretation(self) -> str:
        """Generate plain English interpretation for stakeholders."""
        r2_pct = int(self.r2_test * 100)
        gap_pct = int(self.train_test_gap * 100)
        
        # Base interpretation
        interp = f"Model explains {r2_pct}% of variance in test data. "
        
        # Overfitting assessment
        if gap_pct < 5:
            interp += f"Excellent generalization ({gap_pct}% train-test gap). "
        elif gap_pct < 10:
            interp += f"Good generalization ({gap_pct}% train-test gap). "
        else:
            interp += f"⚠️ Possible overfitting ({gap_pct}% train-test gap > 10% threshold). "
        
        # Error magnitude
        interp += f"Typical prediction error is ±{self.rmse:.1f} (RMSE) or {self.mape:.1f}% (MAPE)."
        
        return interp


@dataclass
class ResidualDiagnostics:
    """Residual analysis for model validation."""
    
    mean: float
    std: float
    skewness: float
    kurtosis: float
    shapiro_p_value: float
    outlier_count: int
    outlier_threshold: float
    
    def to_dict(self) -> Dict:
        """Convert to dict with interpretation."""
        return {
            "distribution": {
                "mean": round(self.mean, 3),
                "std": round(self.std, 2),
                "skewness": round(self.skewness, 3),
                "kurtosis": round(self.kurtosis, 3)
            },
            "normality_test": {
                "shapiro_p_value": round(self.shapiro_p_value, 4),
                "is_normal": self.shapiro_p_value > 0.05,
                "interpretation": "Residuals are normally distributed" if self.shapiro_p_value > 0.05 
                                 else "⚠️ Residuals not normally distributed (consider non-linear model)"
            },
            "outliers": {
                "count": self.outlier_count,
                "threshold": round(self.outlier_threshold, 2),
                "percentage": round(self.outlier_count / self._total_samples * 100, 1) if hasattr(self, '_total_samples') else None
            }
        }


class ModelEvaluator:
    """
    Comprehensive model evaluation for production ML.
    
    Computes all metrics needed for:
    1. Technical validation (R², RMSE, residuals)
    2. Stakeholder communication (MAE, MAPE, interpretations)
    3. Model diagnostics (overfitting, normality, outliers)
    """
    
    @staticmethod
    def evaluate(
        y_train: np.ndarray,
        y_test: np.ndarray,
        y_train_pred: np.ndarray,
        y_test_pred: np.ndarray
    ) -> ModelMetrics:
        """
        Compute comprehensive model metrics.
        
        Args:
            y_train: Training labels
            y_test: Test labels
            y_train_pred: Training predictions
            y_test_pred: Test predictions
            
        Returns:
            ModelMetrics with all performance and diagnostic metrics
        """
        # Core metrics
        r2_train = r2_score(y_train, y_train_pred)
        r2_test = r2_score(y_test, y_test_pred)
        mae = mean_absolute_error(y_test, y_test_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        
        # Percentage errors (handle division by zero)
        mape = ModelEvaluator._compute_mape(y_test, y_test_pred)
        smape = ModelEvaluator._compute_smape(y_test, y_test_pred)
        
        # Diagnostics
        residuals = y_test - y_test_pred
        residual_std = np.std(residuals)
        train_test_gap = r2_train - r2_test
        
        return ModelMetrics(
            r2_train=r2_train,
            r2_test=r2_test,
            mae=mae,
            rmse=rmse,
            mape=mape,
            smape=smape,
            train_test_gap=train_test_gap,
            residual_std=residual_std,
            n_train=len(y_train),
            n_test=len(y_test)
        )
    
    @staticmethod
    def analyze_residuals(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        outlier_threshold: float = 3.0
    ) -> ResidualDiagnostics:
        """
        Analyze residual distribution and detect outliers.
        
        Args:
            y_true: True values
            y_pred: Predicted values
            outlier_threshold: Number of std devs for outlier detection
            
        Returns:
            ResidualDiagnostics with distribution analysis
        """
        residuals = y_true - y_pred
        
        # Distribution stats
        mean = np.mean(residuals)
        std = np.std(residuals)
        skewness = stats.skew(residuals)
        kurtosis = stats.kurtosis(residuals)
        
        # Normality test
        shapiro_stat, shapiro_p = stats.shapiro(residuals[:5000])  # Limit to 5000 for performance
        
        # Outlier detection
        z_scores = np.abs((residuals - mean) / std)
        outlier_count = np.sum(z_scores > outlier_threshold)
        
        diagnostics = ResidualDiagnostics(
            mean=mean,
            std=std,
            skewness=skewness,
            kurtosis=kurtosis,
            shapiro_p_value=shapiro_p,
            outlier_count=int(outlier_count),
            outlier_threshold=outlier_threshold
        )
        diagnostics._total_samples = len(residuals)
        
        return diagnostics
    
    @staticmethod
    def _compute_mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute MAPE, handling zero values."""
        # Avoid division by zero
        mask = y_true != 0
        if not np.any(mask):
            return 0.0
        
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        return min(mape, 999.9)  # Cap at 999.9%
    
    @staticmethod
    def _compute_smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute Symmetric MAPE (better for zero/low values)."""
        numerator = np.abs(y_true - y_pred)
        denominator = (np.abs(y_true) + np.abs(y_pred)) / 2
        
        # Avoid division by zero
        mask = denominator != 0
        if not np.any(mask):
            return 0.0
        
        smape = np.mean(numerator[mask] / denominator[mask]) * 100
        return min(smape, 999.9)  # Cap at 999.9%
    
    @staticmethod
    def generate_prediction_intervals(
        y_pred: np.ndarray,
        residual_std: float,
        confidence: float = 0.95
    ) -> tuple:
        """
        Generate prediction intervals for confidence bounds.
        
        Args:
            y_pred: Point predictions
            residual_std: Standard deviation of residuals
            confidence: Confidence level (default 95%)
            
        Returns:
            (lower_bound, upper_bound) arrays
        """
        # Z-score for confidence level
        z_score = stats.norm.ppf((1 + confidence) / 2)
        
        margin = z_score * residual_std
        lower_bound = y_pred - margin
        upper_bound = y_pred + margin
        
        return lower_bound, upper_bound
