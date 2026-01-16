"""
Ridge Regression (L2 Regularization)

Reduces coefficient magnitude to prevent overfitting and handles multicollinearity.

Features:
- Cross-validated alpha selection (log scale 0.001 to 1000)
- Coefficient shrinkage tracking
- Train/test gap analysis for overfitting detection
- Coefficient stability analysis
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger

from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler


@dataclass
class RidgeResult:
    """Result container for Ridge regression."""
    success: bool
    optimal_alpha: float
    coefficients: List[Dict[str, Any]]
    metrics: Dict[str, float]
    cv_results: Dict[str, Any]
    warnings: List[Dict[str, str]]
    residuals: Optional[np.ndarray] = None
    predictions: Optional[np.ndarray] = None


class RidgeRegressionModel:
    """
    Ridge Regression with L2 regularization and cross-validated alpha selection.
    
    Features:
    - Automatic alpha tuning via 5-fold CV
    - Coefficient shrinkage tracking
    - Train/test performance gap analysis
    - Overfitting/under-regularization warnings
    """
    
    # Default alpha search space (log scale)
    DEFAULT_ALPHAS = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
    
    # Thresholds
    TEST_R2_LOW_THRESHOLD = 0.40
    TRAIN_TEST_GAP_THRESHOLD = 0.15
    COEF_NEAR_ZERO_THRESHOLD = 0.001
    
    def __init__(
        self,
        alphas: Optional[List[float]] = None,
        cv_folds: int = 5,
        fit_intercept: bool = True
    ):
        """
        Initialize Ridge model.
        
        Args:
            alphas: List of alpha values to try (default: log scale 0.001-1000)
            cv_folds: Number of CV folds (default: 5)
            fit_intercept: Whether to fit intercept term
        """
        self.alphas = alphas or self.DEFAULT_ALPHAS
        self.cv_folds = cv_folds
        self.fit_intercept = fit_intercept
        
        self.model = None
        self.optimal_alpha = None
        self.scaler = None
        self.feature_names: List[str] = []
        self.cv_scores: Dict[float, Dict] = {}
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None
    ) -> RidgeResult:
        """
        Fit Ridge model with cross-validated alpha selection.
        
        Args:
            X_train: Training features
            y_train: Training target
            X_test: Optional test features for evaluation
            y_test: Optional test target
            feature_names: Optional feature names
            
        Returns:
            RidgeResult with optimal alpha, coefficients, and metrics
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            # Clean data
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            # Step 1: Cross-validated alpha selection
            logger.info(f"Ridge: Testing {len(self.alphas)} alpha values with {self.cv_folds}-fold CV")
            
            self.cv_scores = self._cross_validate_alphas(X_train_clean, y_train_clean)
            self.optimal_alpha = self._select_optimal_alpha()
            
            # Step 2: Fit final model with optimal alpha
            self.model = Ridge(alpha=self.optimal_alpha, fit_intercept=self.fit_intercept)
            self.model.fit(X_train_clean, y_train_clean)
            
            # Step 3: Extract coefficients
            coefficients = self._extract_coefficients()
            
            # Step 4: Compute metrics
            train_pred = self.model.predict(X_train_clean)
            metrics = {
                'optimal_alpha': self.optimal_alpha,
                'r2_train': float(r2_score(y_train_clean, train_pred)),
                'rmse_train': float(np.sqrt(mean_squared_error(y_train_clean, train_pred))),
                'mae_train': float(mean_absolute_error(y_train_clean, train_pred)),
                'n_features': len(self.feature_names),
                'n_samples': len(X_train)
            }
            
            # Step 5: Test set evaluation if provided
            if X_test is not None and y_test is not None:
                X_test_clean = X_test.fillna(0).replace([np.inf, -np.inf], 0)
                y_test_clean = y_test.fillna(0)
                test_pred = self.model.predict(X_test_clean)
                
                metrics['r2_test'] = float(r2_score(y_test_clean, test_pred))
                metrics['rmse_test'] = float(np.sqrt(mean_squared_error(y_test_clean, test_pred)))
                metrics['mae_test'] = float(mean_absolute_error(y_test_clean, test_pred))
                metrics['train_test_r2_gap'] = metrics['r2_train'] - metrics['r2_test']
            
            # Step 6: Build CV results summary
            cv_results = self._build_cv_summary()
            
            # Step 7: Generate warnings
            warnings = self._generate_warnings(coefficients, metrics)
            
            logger.info(f"Ridge: Optimal α={self.optimal_alpha}, R²={metrics['r2_train']:.4f}")
            
            return RidgeResult(
                success=True,
                optimal_alpha=self.optimal_alpha,
                coefficients=coefficients,
                metrics=metrics,
                cv_results=cv_results,
                warnings=warnings,
                residuals=y_train_clean.values - train_pred,
                predictions=train_pred
            )
            
        except Exception as e:
            logger.error(f"Ridge fit failed: {e}")
            return RidgeResult(
                success=False,
                optimal_alpha=0,
                coefficients=[],
                metrics={},
                cv_results={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _cross_validate_alphas(
        self,
        X: pd.DataFrame,
        y: pd.Series
    ) -> Dict[float, Dict]:
        """Run cross-validation for each alpha value."""
        cv_scores = {}
        kf = KFold(n_splits=self.cv_folds, shuffle=True, random_state=42)
        
        for alpha in self.alphas:
            model = Ridge(alpha=alpha, fit_intercept=self.fit_intercept)
            
            # Negative MSE scores (sklearn convention)
            scores = cross_val_score(model, X, y, cv=kf, scoring='neg_mean_squared_error')
            rmse_scores = np.sqrt(-scores)
            
            # Also get R² scores
            r2_scores = cross_val_score(model, X, y, cv=kf, scoring='r2')
            
            cv_scores[alpha] = {
                'rmse_mean': float(rmse_scores.mean()),
                'rmse_std': float(rmse_scores.std()),
                'r2_mean': float(r2_scores.mean()),
                'r2_std': float(r2_scores.std())
            }
        
        return cv_scores
    
    def _select_optimal_alpha(self) -> float:
        """Select alpha with lowest CV RMSE."""
        best_alpha = min(self.cv_scores, key=lambda a: self.cv_scores[a]['rmse_mean'])
        return best_alpha
    
    def _extract_coefficients(self) -> List[Dict[str, Any]]:
        """Extract Ridge coefficients."""
        coefficients = []
        
        # Intercept
        if self.fit_intercept:
            coefficients.append({
                'feature': 'intercept',
                'coefficient': float(self.model.intercept_),
                'abs_coefficient': abs(float(self.model.intercept_)),
                'shrunk': False
            })
        
        # Feature coefficients
        for name, coef in zip(self.feature_names, self.model.coef_):
            coefficients.append({
                'feature': name,
                'coefficient': float(coef),
                'abs_coefficient': abs(float(coef)),
                'shrunk': abs(coef) < self.COEF_NEAR_ZERO_THRESHOLD,
                'impact': 'positive' if coef > 0 else 'negative'
            })
        
        # Sort by absolute coefficient
        non_intercept = [c for c in coefficients if c['feature'] != 'intercept']
        intercept = [c for c in coefficients if c['feature'] == 'intercept']
        non_intercept.sort(key=lambda x: x['abs_coefficient'], reverse=True)
        
        return intercept + non_intercept
    
    def _build_cv_summary(self) -> Dict[str, Any]:
        """Build CV results summary."""
        cv_curve = [
            {'alpha': alpha, **scores}
            for alpha, scores in sorted(self.cv_scores.items())
        ]
        
        return {
            'alphas_tested': len(self.alphas),
            'cv_folds': self.cv_folds,
            'optimal_alpha': self.optimal_alpha,
            'optimal_rmse': self.cv_scores[self.optimal_alpha]['rmse_mean'],
            'optimal_r2': self.cv_scores[self.optimal_alpha]['r2_mean'],
            'cv_curve': cv_curve
        }
    
    def _generate_warnings(
        self,
        coefficients: List[Dict],
        metrics: Dict[str, float]
    ) -> List[Dict[str, str]]:
        """Generate warnings based on model quality."""
        warnings = []
        
        # 1. Low test R²
        if metrics.get('r2_test', 1.0) < self.TEST_R2_LOW_THRESHOLD:
            warnings.append({
                'type': 'predictive_power',
                'severity': 'high',
                'message': f"Low test R² ({metrics.get('r2_test', 0):.3f}): Poor predictive power. Need better features."
            })
        
        # 2. Overfitting (train >> test)
        gap = metrics.get('train_test_r2_gap', 0)
        if gap > self.TRAIN_TEST_GAP_THRESHOLD:
            warnings.append({
                'type': 'overfitting',
                'severity': 'high',
                'message': f"Train R² >> Test R² (gap={gap:.3f}): Overfitting detected. Increase alpha."
            })
        
        # 3. Over-regularization (all coefficients near zero)
        non_intercept = [c for c in coefficients if c['feature'] != 'intercept']
        shrunk_count = sum(1 for c in non_intercept if c.get('shrunk', False))
        if len(non_intercept) > 0 and shrunk_count / len(non_intercept) > 0.8:
            warnings.append({
                'type': 'over_regularization',
                'severity': 'medium',
                'message': f"Most coefficients near zero ({shrunk_count}/{len(non_intercept)}): Over-regularized. Decrease alpha."
            })
        
        # 4. Alpha at boundary
        if self.optimal_alpha == min(self.alphas):
            warnings.append({
                'type': 'alpha_boundary',
                'severity': 'low',
                'message': "Optimal alpha at lower boundary. Consider testing smaller alpha values."
            })
        elif self.optimal_alpha == max(self.alphas):
            warnings.append({
                'type': 'alpha_boundary',
                'severity': 'low',
                'message': "Optimal alpha at upper boundary. Consider testing larger alpha values."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions using fitted model."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        return self.model.predict(X_clean)
    
    def coefficient_path(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """
        Compute coefficient paths across all alpha values.
        Useful for visualization of regularization effect.
        """
        paths = {name: [] for name in self.feature_names}
        alphas_used = []
        
        for alpha in sorted(self.alphas):
            model = Ridge(alpha=alpha, fit_intercept=self.fit_intercept)
            model.fit(X.fillna(0), y.fillna(0))
            
            alphas_used.append(alpha)
            for name, coef in zip(self.feature_names, model.coef_):
                paths[name].append(float(coef))
        
        return {
            'alphas': alphas_used,
            'paths': paths
        }


def run_ridge_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> RidgeResult:
    """
    Convenience function to run Ridge analysis.
    """
    model = RidgeRegressionModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
