"""
Bayesian Regression

Provides uncertainty estimates (confidence intervals) for predictions and coefficients.

Features:
- Posterior distributions for coefficients (mean + std)
- Prediction intervals (credible intervals)
- Interval calibration analysis
- Uncertainty quantification for decision-making
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger

from sklearn.linear_model import BayesianRidge
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from scipy import stats


@dataclass
class BayesianResult:
    """Result container for Bayesian regression."""
    success: bool
    coefficients: List[Dict[str, Any]]
    metrics: Dict[str, float]
    uncertainty: Dict[str, Any]
    calibration: Dict[str, Any]
    warnings: List[Dict[str, str]]
    predictions: Optional[np.ndarray] = None
    prediction_intervals: Optional[Dict[str, np.ndarray]] = None


class BayesianRegressionModel:
    """
    Bayesian Regression with uncertainty quantification.
    
    Features:
    - Posterior distributions for coefficients
    - 95% credible intervals for predictions
    - Interval calibration checking
    """
    
    # Default hyperparameters
    DEFAULT_ALPHA_1 = 1e-6
    DEFAULT_ALPHA_2 = 1e-6
    DEFAULT_LAMBDA_1 = 1e-6
    DEFAULT_LAMBDA_2 = 1e-6
    
    def __init__(
        self,
        alpha_1: float = DEFAULT_ALPHA_1,
        alpha_2: float = DEFAULT_ALPHA_2,
        lambda_1: float = DEFAULT_LAMBDA_1,
        lambda_2: float = DEFAULT_LAMBDA_2,
        n_iter: int = 300,
        fit_intercept: bool = True,
        credible_interval: float = 0.95
    ):
        """
        Initialize Bayesian Regression.
        
        Args:
            alpha_1: Prior precision for coefficients (shape)
            alpha_2: Prior precision for coefficients (rate)
            lambda_1: Prior precision for noise (shape)
            lambda_2: Prior precision for noise (rate)
            n_iter: Maximum iterations for optimization
            fit_intercept: Whether to fit intercept
            credible_interval: Interval width (default 0.95 = 95%)
        """
        self.alpha_1 = alpha_1
        self.alpha_2 = alpha_2
        self.lambda_1 = lambda_1
        self.lambda_2 = lambda_2
        self.n_iter = n_iter
        self.fit_intercept = fit_intercept
        self.credible_interval = credible_interval
        
        self.model = None
        self.feature_names: List[str] = []
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None
    ) -> BayesianResult:
        """
        Fit Bayesian regression model.
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            # Clean data
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            # Fit Bayesian Ridge
            logger.info(f"BayesianRidge: Fitting with {len(X_train)} samples, {len(self.feature_names)} features")
            
            self.model = BayesianRidge(
                alpha_1=self.alpha_1,
                alpha_2=self.alpha_2,
                lambda_1=self.lambda_1,
                lambda_2=self.lambda_2,
                max_iter=self.n_iter,
                fit_intercept=self.fit_intercept,
                compute_score=True
            )
            self.model.fit(X_train_clean, y_train_clean)
            
            # Extract coefficients with uncertainty
            coefficients = self._extract_coefficients()
            
            # Training predictions with intervals
            train_pred, train_std = self.model.predict(X_train_clean, return_std=True)
            
            # Metrics
            metrics = {
                'r2_train': float(r2_score(y_train_clean, train_pred)),
                'rmse_train': float(np.sqrt(mean_squared_error(y_train_clean, train_pred))),
                'mae_train': float(mean_absolute_error(y_train_clean, train_pred)),
                'alpha_posterior': float(self.model.alpha_),  # Posterior noise precision
                'lambda_posterior': float(self.model.lambda_),  # Posterior coef precision
                'log_marginal_likelihood': float(self.model.scores_[-1]) if len(self.model.scores_) > 0 else None,
                'n_samples': len(X_train)
            }
            
            # Test evaluation with calibration
            calibration = {}
            prediction_intervals = None
            
            if X_test is not None and y_test is not None:
                X_test_clean = X_test.fillna(0).replace([np.inf, -np.inf], 0)
                y_test_clean = y_test.fillna(0)
                
                test_pred, test_std = self.model.predict(X_test_clean, return_std=True)
                
                metrics['r2_test'] = float(r2_score(y_test_clean, test_pred))
                metrics['rmse_test'] = float(np.sqrt(mean_squared_error(y_test_clean, test_pred)))
                metrics['mae_test'] = float(mean_absolute_error(y_test_clean, test_pred))
                metrics['mean_prediction_std'] = float(test_std.mean())
                
                # Compute prediction intervals
                z_score = stats.norm.ppf((1 + self.credible_interval) / 2)
                lower = test_pred - z_score * test_std
                upper = test_pred + z_score * test_std
                
                prediction_intervals = {
                    'predictions': test_pred,
                    'std': test_std,
                    'lower': lower,
                    'upper': upper
                }
                
                # Calibration check
                calibration = self._check_calibration(y_test_clean.values, test_pred, test_std)
            
            # Uncertainty summary
            uncertainty = self._build_uncertainty_summary(coefficients)
            
            # Warnings
            warnings = self._generate_warnings(metrics, calibration, uncertainty)
            
            logger.info(f"BayesianRidge: R²={metrics['r2_train']:.4f}, α={metrics['alpha_posterior']:.4f}")
            
            return BayesianResult(
                success=True,
                coefficients=coefficients,
                metrics=metrics,
                uncertainty=uncertainty,
                calibration=calibration,
                warnings=warnings,
                predictions=train_pred,
                prediction_intervals=prediction_intervals
            )
            
        except Exception as e:
            logger.error(f"Bayesian fit failed: {e}")
            return BayesianResult(
                success=False,
                coefficients=[],
                metrics={},
                uncertainty={},
                calibration={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _extract_coefficients(self) -> List[Dict[str, Any]]:
        """Extract coefficients with posterior uncertainty."""
        coefficients = []
        
        # Get coefficient standard deviations from posterior covariance
        # sigma_ is the posterior covariance matrix
        if hasattr(self.model, 'sigma_') and self.model.sigma_ is not None:
            coef_std = np.sqrt(np.diag(self.model.sigma_))
        else:
            coef_std = np.zeros(len(self.model.coef_))
        
        # Intercept
        if self.fit_intercept:
            coefficients.append({
                'feature': 'intercept',
                'mean': float(self.model.intercept_),
                'std': 0.0,  # Intercept std not directly available
                'ci_lower': float(self.model.intercept_),
                'ci_upper': float(self.model.intercept_),
                'significant': True
            })
        
        # Feature coefficients
        z_score = stats.norm.ppf((1 + self.credible_interval) / 2)
        
        for i, (name, coef) in enumerate(zip(self.feature_names, self.model.coef_)):
            std = coef_std[i] if i < len(coef_std) else 0.0
            ci_lower = coef - z_score * std
            ci_upper = coef + z_score * std
            
            # Significant if CI doesn't include zero
            is_significant = (ci_lower > 0 and ci_upper > 0) or (ci_lower < 0 and ci_upper < 0)
            
            coefficients.append({
                'feature': name,
                'mean': float(coef),
                'std': float(std),
                'ci_lower': float(ci_lower),
                'ci_upper': float(ci_upper),
                'significant': is_significant,
                'impact': 'positive' if coef > 0 else 'negative'
            })
        
        # Sort by absolute mean
        non_intercept = [c for c in coefficients if c['feature'] != 'intercept']
        intercept = [c for c in coefficients if c['feature'] == 'intercept']
        non_intercept.sort(key=lambda x: abs(x['mean']), reverse=True)
        
        return intercept + non_intercept
    
    def _check_calibration(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_std: np.ndarray
    ) -> Dict[str, Any]:
        """Check interval calibration."""
        # Coverage at different interval widths
        coverages = {}
        for width in [0.50, 0.80, 0.90, 0.95, 0.99]:
            z = stats.norm.ppf((1 + width) / 2)
            lower = y_pred - z * y_std
            upper = y_pred + z * y_std
            in_interval = ((y_true >= lower) & (y_true <= upper)).mean()
            coverages[f'{int(width*100)}%'] = float(in_interval)
        
        # Sharpness (average interval width at 95%)
        z95 = stats.norm.ppf(0.975)
        interval_widths = 2 * z95 * y_std
        
        return {
            'expected_coverage': self.credible_interval,
            'actual_coverage': coverages.get(f'{int(self.credible_interval*100)}%', 0),
            'coverage_by_width': coverages,
            'mean_interval_width': float(interval_widths.mean()),
            'std_interval_width': float(interval_widths.std()),
            'is_calibrated': abs(coverages.get('95%', 0) - 0.95) < 0.05
        }
    
    def _build_uncertainty_summary(self, coefficients: List[Dict]) -> Dict[str, Any]:
        """Summarize uncertainty in coefficients."""
        non_intercept = [c for c in coefficients if c['feature'] != 'intercept']
        
        if not non_intercept:
            return {}
        
        stds = [c['std'] for c in non_intercept]
        significant_count = sum(1 for c in non_intercept if c['significant'])
        
        return {
            'mean_coefficient_std': float(np.mean(stds)),
            'max_coefficient_std': float(np.max(stds)),
            'min_coefficient_std': float(np.min(stds)),
            'n_significant': significant_count,
            'n_total': len(non_intercept),
            'pct_significant': float(significant_count / len(non_intercept) * 100),
            'most_uncertain_feature': non_intercept[np.argmax(stds)]['feature'] if stds else None,
            'most_certain_feature': non_intercept[np.argmin(stds)]['feature'] if stds else None
        }
    
    def _generate_warnings(
        self,
        metrics: Dict[str, float],
        calibration: Dict[str, Any],
        uncertainty: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Generate warnings."""
        warnings = []
        
        # 1. Poor calibration
        if calibration:
            actual = calibration.get('actual_coverage', 0)
            expected = calibration.get('expected_coverage', 0.95)
            if actual < expected - 0.05:
                warnings.append({
                    'type': 'calibration',
                    'severity': 'high',
                    'message': f"Interval coverage {actual*100:.0f}% < expected {expected*100:.0f}%. Model is overconfident."
                })
            elif actual > expected + 0.10:
                warnings.append({
                    'type': 'calibration',
                    'severity': 'low',
                    'message': f"Interval coverage {actual*100:.0f}% > expected. Intervals may be too wide."
                })
        
        # 2. Wide intervals
        mean_width = calibration.get('mean_interval_width', 0)
        if mean_width > metrics.get('rmse_test', float('inf')) * 5:
            warnings.append({
                'type': 'wide_intervals',
                'severity': 'medium',
                'message': f"Very wide prediction intervals ({mean_width:.2f}). High uncertainty - collect more data."
            })
        
        # 3. Low significance rate
        pct_sig = uncertainty.get('pct_significant', 100)
        if pct_sig < 30:
            warnings.append({
                'type': 'low_significance',
                'severity': 'medium',
                'message': f"Only {pct_sig:.0f}% coefficients significant. High coefficient uncertainty."
            })
        
        return warnings
    
    def predict(
        self,
        X: pd.DataFrame,
        return_interval: bool = True
    ) -> Tuple[np.ndarray, Optional[Dict[str, np.ndarray]]]:
        """
        Make predictions with optional credible intervals.
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        pred, std = self.model.predict(X_clean, return_std=True)
        
        if return_interval:
            z = stats.norm.ppf((1 + self.credible_interval) / 2)
            return pred, {
                'std': std,
                'lower': pred - z * std,
                'upper': pred + z * std
            }
        
        return pred, None
    
    def get_coefficient_credible_intervals(self) -> pd.DataFrame:
        """Get coefficient credible intervals as DataFrame."""
        if self.model is None:
            return pd.DataFrame()
        
        coefs = self._extract_coefficients()
        return pd.DataFrame(coefs)


def run_bayesian_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> BayesianResult:
    """Convenience function for Bayesian regression."""
    model = BayesianRegressionModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
