"""
Elastic Net Regression (L1 + L2 Regularization)

Combines Ridge and Lasso: handles multicollinearity while performing feature selection.

Features:
- 2D grid search over alpha and l1_ratio
- Cross-validated hyperparameter selection
- Feature selection with grouped correlated features
- Comparison metrics against pure Ridge/Lasso
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger

from sklearn.linear_model import ElasticNet, ElasticNetCV
from sklearn.model_selection import cross_val_score, KFold, GridSearchCV
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


@dataclass
class ElasticNetResult:
    """Result container for Elastic Net regression."""
    success: bool
    optimal_alpha: float
    optimal_l1_ratio: float
    selected_features: List[str]
    coefficients: List[Dict[str, Any]]
    metrics: Dict[str, float]
    grid_results: Dict[str, Any]
    warnings: List[Dict[str, str]]
    residuals: Optional[np.ndarray] = None
    predictions: Optional[np.ndarray] = None


class ElasticNetModel:
    """
    Elastic Net with combined L1 + L2 regularization.
    
    Features:
    - 2D grid search over alpha and l1_ratio
    - Best for correlated feature groups
    - Feature selection with stability
    """
    
    # Default grids
    DEFAULT_ALPHAS = [0.001, 0.01, 0.1, 1.0, 10.0]
    DEFAULT_L1_RATIOS = [0.1, 0.3, 0.5, 0.7, 0.9]
    
    # Thresholds
    COEF_ZERO_THRESHOLD = 1e-6
    L1_RATIO_BOUNDARY = 0.05  # Close to 0 or 1
    
    def __init__(
        self,
        alphas: Optional[List[float]] = None,
        l1_ratios: Optional[List[float]] = None,
        cv_folds: int = 5,
        max_iter: int = 10000,
        fit_intercept: bool = True
    ):
        """
        Initialize Elastic Net model.
        
        Args:
            alphas: List of alpha values to try
            l1_ratios: List of l1_ratio values (0=Ridge, 1=Lasso)
            cv_folds: Number of CV folds
            max_iter: Maximum iterations
            fit_intercept: Whether to fit intercept
        """
        self.alphas = alphas or self.DEFAULT_ALPHAS
        self.l1_ratios = l1_ratios or self.DEFAULT_L1_RATIOS
        self.cv_folds = cv_folds
        self.max_iter = max_iter
        self.fit_intercept = fit_intercept
        
        self.model = None
        self.optimal_alpha = None
        self.optimal_l1_ratio = None
        self.feature_names: List[str] = []
        self.selected_features: List[str] = []
        self.grid_scores: Dict[Tuple[float, float], float] = {}
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None
    ) -> ElasticNetResult:
        """
        Fit Elastic Net with grid search over alpha and l1_ratio.
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            # Clean data
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            # Step 1: Grid search
            logger.info(f"ElasticNet: Grid search {len(self.alphas)}×{len(self.l1_ratios)} = {len(self.alphas)*len(self.l1_ratios)} combinations")
            
            self._grid_search(X_train_clean, y_train_clean)
            
            # Step 2: Fit final model
            self.model = ElasticNet(
                alpha=self.optimal_alpha,
                l1_ratio=self.optimal_l1_ratio,
                fit_intercept=self.fit_intercept,
                max_iter=self.max_iter
            )
            self.model.fit(X_train_clean, y_train_clean)
            
            # Step 3: Extract coefficients
            coefficients = self._extract_coefficients()
            self.selected_features = [
                c['feature'] for c in coefficients 
                if c['feature'] != 'intercept' and not c.get('zero', False)
            ]
            
            # Step 4: Metrics
            train_pred = self.model.predict(X_train_clean)
            n_features = len(self.feature_names)
            n_selected = len(self.selected_features)
            
            metrics = {
                'optimal_alpha': self.optimal_alpha,
                'optimal_l1_ratio': self.optimal_l1_ratio,
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
            
            # Step 6: Grid results
            grid_results = self._build_grid_summary()
            
            # Step 7: Warnings
            warnings = self._generate_warnings(coefficients, metrics)
            
            logger.info(f"ElasticNet: α={self.optimal_alpha}, l1={self.optimal_l1_ratio}, Selected {n_selected}/{n_features}, R²={metrics['r2_train']:.4f}")
            
            return ElasticNetResult(
                success=True,
                optimal_alpha=self.optimal_alpha,
                optimal_l1_ratio=self.optimal_l1_ratio,
                selected_features=self.selected_features,
                coefficients=coefficients,
                metrics=metrics,
                grid_results=grid_results,
                warnings=warnings,
                residuals=y_train_clean.values - train_pred,
                predictions=train_pred
            )
            
        except Exception as e:
            logger.error(f"ElasticNet fit failed: {e}")
            return ElasticNetResult(
                success=False,
                optimal_alpha=0,
                optimal_l1_ratio=0,
                selected_features=[],
                coefficients=[],
                metrics={},
                grid_results={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _grid_search(self, X: pd.DataFrame, y: pd.Series):
        """Perform 2D grid search over alpha and l1_ratio."""
        kf = KFold(n_splits=self.cv_folds, shuffle=True, random_state=42)
        best_score = float('inf')
        
        for alpha in self.alphas:
            for l1_ratio in self.l1_ratios:
                model = ElasticNet(
                    alpha=alpha,
                    l1_ratio=l1_ratio,
                    fit_intercept=self.fit_intercept,
                    max_iter=self.max_iter
                )
                
                try:
                    scores = cross_val_score(model, X, y, cv=kf, scoring='neg_mean_squared_error')
                    rmse = np.sqrt(-scores.mean())
                    self.grid_scores[(alpha, l1_ratio)] = rmse
                    
                    if rmse < best_score:
                        best_score = rmse
                        self.optimal_alpha = alpha
                        self.optimal_l1_ratio = l1_ratio
                except:
                    self.grid_scores[(alpha, l1_ratio)] = float('inf')
        
        logger.debug(f"Grid search complete: best α={self.optimal_alpha}, l1={self.optimal_l1_ratio}, RMSE={best_score:.4f}")
    
    def _extract_coefficients(self) -> List[Dict[str, Any]]:
        """Extract Elastic Net coefficients."""
        coefficients = []
        
        if self.fit_intercept:
            coefficients.append({
                'feature': 'intercept',
                'coefficient': float(self.model.intercept_),
                'abs_coefficient': abs(float(self.model.intercept_)),
                'zero': False,
                'rank': 0
            })
        
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
        
        coef_list.sort(key=lambda x: x['abs_coefficient'], reverse=True)
        for rank, c in enumerate(coef_list, 1):
            c['rank'] = rank if not c['zero'] else None
        
        return coefficients + coef_list
    
    def _build_grid_summary(self) -> Dict[str, Any]:
        """Build grid search summary."""
        # Create heatmap data
        heatmap = []
        for (alpha, l1), rmse in self.grid_scores.items():
            heatmap.append({
                'alpha': alpha,
                'l1_ratio': l1,
                'rmse': rmse if rmse != float('inf') else None
            })
        
        return {
            'alphas_tested': self.alphas,
            'l1_ratios_tested': self.l1_ratios,
            'total_combinations': len(self.alphas) * len(self.l1_ratios),
            'optimal_alpha': self.optimal_alpha,
            'optimal_l1_ratio': self.optimal_l1_ratio,
            'optimal_rmse': self.grid_scores.get((self.optimal_alpha, self.optimal_l1_ratio), None),
            'heatmap': heatmap
        }
    
    def _generate_warnings(
        self,
        coefficients: List[Dict],
        metrics: Dict[str, float]
    ) -> List[Dict[str, str]]:
        """Generate warnings."""
        warnings = []
        
        # 1. l1_ratio at boundary (suggests pure Ridge or Lasso)
        if self.optimal_l1_ratio <= self.L1_RATIO_BOUNDARY:
            warnings.append({
                'type': 'l1_ratio_boundary',
                'severity': 'medium',
                'message': f"l1_ratio={self.optimal_l1_ratio:.2f} near 0: Consider pure Ridge regression."
            })
        elif self.optimal_l1_ratio >= 1 - self.L1_RATIO_BOUNDARY:
            warnings.append({
                'type': 'l1_ratio_boundary',
                'severity': 'medium',
                'message': f"l1_ratio={self.optimal_l1_ratio:.2f} near 1: Consider pure Lasso regression."
            })
        
        # 2. Feature selection info
        n_selected = metrics.get('n_features_selected', 0)
        n_total = metrics.get('n_features_total', 1)
        if n_selected == n_total:
            warnings.append({
                'type': 'no_selection',
                'severity': 'info',
                'message': "All features retained. Increase alpha or l1_ratio for feature selection."
            })
        elif n_selected < 3:
            warnings.append({
                'type': 'over_regularization',
                'severity': 'medium',
                'message': f"Only {n_selected} features selected. May be over-regularized."
            })
        
        # 3. Model performance
        r2_test = metrics.get('r2_test', 1.0)
        if r2_test < 0.4:
            warnings.append({
                'type': 'low_r2',
                'severity': 'high',
                'message': f"Low test R² ({r2_test:.3f}). Need better features."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        return self.model.predict(X_clean)
    
    def compare_to_baselines(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: pd.DataFrame,
        y_test: pd.Series
    ) -> Dict[str, Dict[str, float]]:
        """Compare Elastic Net to pure Ridge and Lasso."""
        from sklearn.linear_model import Ridge, Lasso
        
        results = {}
        X_tr = X_train.fillna(0)
        X_te = X_test.fillna(0)
        y_tr = y_train.fillna(0)
        y_te = y_test.fillna(0)
        
        # Elastic Net (already fitted)
        en_pred = self.model.predict(X_te)
        results['elastic_net'] = {
            'r2': r2_score(y_te, en_pred),
            'rmse': np.sqrt(mean_squared_error(y_te, en_pred)),
            'n_features': len(self.selected_features)
        }
        
        # Ridge
        ridge = Ridge(alpha=self.optimal_alpha)
        ridge.fit(X_tr, y_tr)
        ridge_pred = ridge.predict(X_te)
        results['ridge'] = {
            'r2': r2_score(y_te, ridge_pred),
            'rmse': np.sqrt(mean_squared_error(y_te, ridge_pred)),
            'n_features': len(self.feature_names)
        }
        
        # Lasso
        lasso = Lasso(alpha=self.optimal_alpha, max_iter=self.max_iter)
        lasso.fit(X_tr, y_tr)
        lasso_pred = lasso.predict(X_te)
        lasso_selected = sum(1 for c in lasso.coef_ if abs(c) > self.COEF_ZERO_THRESHOLD)
        results['lasso'] = {
            'r2': r2_score(y_te, lasso_pred),
            'rmse': np.sqrt(mean_squared_error(y_te, lasso_pred)),
            'n_features': lasso_selected
        }
        
        return results


def run_elastic_net_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> ElasticNetResult:
    """Convenience function to run Elastic Net analysis."""
    model = ElasticNetModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
