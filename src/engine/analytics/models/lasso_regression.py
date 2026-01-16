"""
Lasso Regression (L1 Regularization)

Performs feature selection by driving irrelevant coefficients to exactly zero.

Features:
- Cross-validated alpha selection (log scale 0.0001 to 10)
- Automatic feature selection (sparse coefficients)
- Feature path tracking across alpha values
- Sparsity analysis and ranking
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger

from sklearn.linear_model import Lasso, LassoCV
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


@dataclass
class LassoResult:
    """Result container for Lasso regression."""
    success: bool
    optimal_alpha: float
    selected_features: List[str]
    coefficients: List[Dict[str, Any]]
    metrics: Dict[str, float]
    cv_results: Dict[str, Any]
    warnings: List[Dict[str, str]]
    feature_path: Optional[Dict[str, Any]] = None
    residuals: Optional[np.ndarray] = None
    predictions: Optional[np.ndarray] = None


class LassoRegressionModel:
    """
    Lasso Regression with L1 regularization for feature selection.
    
    Features:
    - Automatic alpha tuning via CV
    - Sparse coefficient extraction
    - Feature ranking by importance
    - Coefficient path visualization data
    """
    
    # Default alpha search space (log scale, smaller than Ridge)
    DEFAULT_ALPHAS = [0.0001, 0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
    
    # Thresholds
    MIN_SELECTED_FEATURES = 2
    MAX_SELECTED_FEATURES = 50
    TEST_R2_LOW_THRESHOLD = 0.40
    COEF_ZERO_THRESHOLD = 1e-6
    
    def __init__(
        self,
        alphas: Optional[List[float]] = None,
        cv_folds: int = 5,
        max_iter: int = 10000,
        fit_intercept: bool = True
    ):
        """
        Initialize Lasso model.
        
        Args:
            alphas: List of alpha values to try
            cv_folds: Number of CV folds
            max_iter: Maximum iterations for convergence
            fit_intercept: Whether to fit intercept term
        """
        self.alphas = alphas or self.DEFAULT_ALPHAS
        self.cv_folds = cv_folds
        self.max_iter = max_iter
        self.fit_intercept = fit_intercept
        
        self.model = None
        self.optimal_alpha = None
        self.feature_names: List[str] = []
        self.selected_features: List[str] = []
        self.cv_scores: Dict[float, Dict] = {}
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None,
        compute_path: bool = True
    ) -> LassoResult:
        """
        Fit Lasso model with cross-validated alpha selection.
        
        Args:
            X_train: Training features
            y_train: Training target
            X_test: Optional test features
            y_test: Optional test target
            feature_names: Optional feature names
            compute_path: Whether to compute coefficient path
            
        Returns:
            LassoResult with selected features, coefficients, and metrics
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            # Clean data
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            # Step 1: Cross-validated alpha selection
            logger.info(f"Lasso: Testing {len(self.alphas)} alpha values with {self.cv_folds}-fold CV")
            
            self.cv_scores = self._cross_validate_alphas(X_train_clean, y_train_clean)
            self.optimal_alpha = self._select_optimal_alpha()
            
            # Step 2: Fit final model
            self.model = Lasso(
                alpha=self.optimal_alpha,
                fit_intercept=self.fit_intercept,
                max_iter=self.max_iter
            )
            self.model.fit(X_train_clean, y_train_clean)
            
            # Step 3: Extract selected features and coefficients
            coefficients = self._extract_coefficients()
            self.selected_features = [
                c['feature'] for c in coefficients 
                if c['feature'] != 'intercept' and not c.get('zero', False)
            ]
            
            # Step 4: Compute metrics
            train_pred = self.model.predict(X_train_clean)
            n_features = len(self.feature_names)
            n_selected = len(self.selected_features)
            
            metrics = {
                'optimal_alpha': self.optimal_alpha,
                'r2_train': float(r2_score(y_train_clean, train_pred)),
                'rmse_train': float(np.sqrt(mean_squared_error(y_train_clean, train_pred))),
                'mae_train': float(mean_absolute_error(y_train_clean, train_pred)),
                'n_features_total': n_features,
                'n_features_selected': n_selected,
                'sparsity_pct': float((n_features - n_selected) / n_features * 100),
                'n_samples': len(X_train)
            }
            
            # Step 5: Test evaluation
            if X_test is not None and y_test is not None:
                X_test_clean = X_test.fillna(0).replace([np.inf, -np.inf], 0)
                y_test_clean = y_test.fillna(0)
                test_pred = self.model.predict(X_test_clean)
                
                metrics['r2_test'] = float(r2_score(y_test_clean, test_pred))
                metrics['rmse_test'] = float(np.sqrt(mean_squared_error(y_test_clean, test_pred)))
                metrics['mae_test'] = float(mean_absolute_error(y_test_clean, test_pred))
            
            # Step 6: Compute coefficient path
            feature_path = None
            if compute_path:
                feature_path = self._compute_coefficient_path(X_train_clean, y_train_clean)
            
            # Step 7: CV summary
            cv_results = self._build_cv_summary()
            
            # Step 8: Warnings
            warnings = self._generate_warnings(coefficients, metrics)
            
            logger.info(f"Lasso: α={self.optimal_alpha}, Selected {n_selected}/{n_features} features, R²={metrics['r2_train']:.4f}")
            
            return LassoResult(
                success=True,
                optimal_alpha=self.optimal_alpha,
                selected_features=self.selected_features,
                coefficients=coefficients,
                metrics=metrics,
                cv_results=cv_results,
                warnings=warnings,
                feature_path=feature_path,
                residuals=y_train_clean.values - train_pred,
                predictions=train_pred
            )
            
        except Exception as e:
            logger.error(f"Lasso fit failed: {e}")
            return LassoResult(
                success=False,
                optimal_alpha=0,
                selected_features=[],
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
            model = Lasso(alpha=alpha, fit_intercept=self.fit_intercept, max_iter=self.max_iter)
            
            try:
                scores = cross_val_score(model, X, y, cv=kf, scoring='neg_mean_squared_error')
                rmse_scores = np.sqrt(-scores)
                r2_scores = cross_val_score(model, X, y, cv=kf, scoring='r2')
                
                cv_scores[alpha] = {
                    'rmse_mean': float(rmse_scores.mean()),
                    'rmse_std': float(rmse_scores.std()),
                    'r2_mean': float(r2_scores.mean()),
                    'r2_std': float(r2_scores.std())
                }
            except Exception as e:
                cv_scores[alpha] = {
                    'rmse_mean': float('inf'),
                    'rmse_std': 0,
                    'r2_mean': 0,
                    'r2_std': 0,
                    'error': str(e)
                }
        
        return cv_scores
    
    def _select_optimal_alpha(self) -> float:
        """Select alpha with lowest CV RMSE."""
        valid_scores = {a: s for a, s in self.cv_scores.items() if s['rmse_mean'] != float('inf')}
        if not valid_scores:
            return self.alphas[len(self.alphas) // 2]  # Middle alpha
        return min(valid_scores, key=lambda a: valid_scores[a]['rmse_mean'])
    
    def _extract_coefficients(self) -> List[Dict[str, Any]]:
        """Extract Lasso coefficients with zero/non-zero status."""
        coefficients = []
        
        # Intercept
        if self.fit_intercept:
            coefficients.append({
                'feature': 'intercept',
                'coefficient': float(self.model.intercept_),
                'abs_coefficient': abs(float(self.model.intercept_)),
                'zero': False,
                'rank': 0
            })
        
        # Feature coefficients with ranking
        coef_list = []
        for name, coef in zip(self.feature_names, self.model.coef_):
            is_zero = abs(coef) < self.COEF_ZERO_THRESHOLD
            coef_list.append({
                'feature': name,
                'coefficient': float(coef),
                'abs_coefficient': abs(float(coef)),
                'zero': is_zero,
                'selected': not is_zero,
                'impact': 'positive' if coef > 0 else 'negative' if coef < 0 else 'none'
            })
        
        # Sort by absolute coefficient and assign ranks
        coef_list.sort(key=lambda x: x['abs_coefficient'], reverse=True)
        for rank, c in enumerate(coef_list, 1):
            c['rank'] = rank if not c['zero'] else None
        
        return coefficients + coef_list
    
    def _compute_coefficient_path(
        self,
        X: pd.DataFrame,
        y: pd.Series
    ) -> Dict[str, Any]:
        """Compute coefficient path across alpha values."""
        paths = {name: [] for name in self.feature_names}
        alphas_used = []
        n_selected = []
        
        for alpha in sorted(self.alphas):
            model = Lasso(alpha=alpha, fit_intercept=self.fit_intercept, max_iter=self.max_iter)
            try:
                model.fit(X, y)
                alphas_used.append(alpha)
                
                selected = 0
                for name, coef in zip(self.feature_names, model.coef_):
                    paths[name].append(float(coef))
                    if abs(coef) > self.COEF_ZERO_THRESHOLD:
                        selected += 1
                
                n_selected.append(selected)
            except:
                pass
        
        return {
            'alphas': alphas_used,
            'paths': paths,
            'n_selected': n_selected
        }
    
    def _build_cv_summary(self) -> Dict[str, Any]:
        """Build CV results summary."""
        cv_curve = [
            {'alpha': alpha, **scores}
            for alpha, scores in sorted(self.cv_scores.items())
            if 'error' not in scores
        ]
        
        return {
            'alphas_tested': len(self.alphas),
            'cv_folds': self.cv_folds,
            'optimal_alpha': self.optimal_alpha,
            'optimal_rmse': self.cv_scores.get(self.optimal_alpha, {}).get('rmse_mean', 0),
            'optimal_r2': self.cv_scores.get(self.optimal_alpha, {}).get('r2_mean', 0),
            'cv_curve': cv_curve
        }
    
    def _generate_warnings(
        self,
        coefficients: List[Dict],
        metrics: Dict[str, float]
    ) -> List[Dict[str, str]]:
        """Generate warnings based on Lasso results."""
        warnings = []
        n_selected = metrics.get('n_features_selected', 0)
        
        # 1. Over-regularized (too few features)
        if n_selected < self.MIN_SELECTED_FEATURES:
            warnings.append({
                'type': 'over_regularization',
                'severity': 'high',
                'message': f"Only {n_selected} features selected. Over-regularized - decrease alpha."
            })
        
        # 2. Under-regularized (too many features)
        if n_selected > self.MAX_SELECTED_FEATURES:
            warnings.append({
                'type': 'under_regularization',
                'severity': 'medium',
                'message': f"{n_selected} features selected. Under-regularized - increase alpha for simpler model."
            })
        
        # 3. Low test R²
        if metrics.get('r2_test', 1.0) < self.TEST_R2_LOW_THRESHOLD:
            warnings.append({
                'type': 'predictive_power',
                'severity': 'high',
                'message': f"Low test R² ({metrics.get('r2_test', 0):.3f}): Poor predictions. Need better features."
            })
        
        # 4. Alpha at boundary
        if self.optimal_alpha == min(self.alphas):
            warnings.append({
                'type': 'alpha_boundary',
                'severity': 'low',
                'message': "Optimal alpha at lower boundary. Consider smaller alpha values."
            })
        elif self.optimal_alpha == max(self.alphas):
            warnings.append({
                'type': 'alpha_boundary',
                'severity': 'low',
                'message': "Optimal alpha at upper boundary. Consider larger alpha values."
            })
        
        # 5. High sparsity information
        sparsity = metrics.get('sparsity_pct', 0)
        if sparsity > 80:
            warnings.append({
                'type': 'sparsity',
                'severity': 'info',
                'message': f"High sparsity ({sparsity:.0f}%): Model is very interpretable with few key features."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions using fitted model."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        return self.model.predict(X_clean)
    
    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance ranking."""
        if self.model is None:
            return pd.DataFrame()
        
        data = []
        for name, coef in zip(self.feature_names, self.model.coef_):
            data.append({
                'feature': name,
                'coefficient': coef,
                'abs_importance': abs(coef),
                'selected': abs(coef) > self.COEF_ZERO_THRESHOLD
            })
        
        df = pd.DataFrame(data)
        return df.sort_values('abs_importance', ascending=False)


def run_lasso_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> LassoResult:
    """Convenience function to run Lasso analysis."""
    model = LassoRegressionModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
