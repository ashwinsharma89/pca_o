"""
XGBoost Regression (Gradient Boosting)

State-of-the-art gradient boosting with built-in regularization.

Features:
- Randomized hyperparameter search
- Early stopping to prevent overfitting
- Multiple feature importance types (gain, cover, weight)
- L1/L2 regularization
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("XGBoost not installed. Install with: pip install xgboost")

from sklearn.model_selection import RandomizedSearchCV, KFold
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


@dataclass
class XGBoostResult:
    """Result container for XGBoost regression."""
    success: bool
    best_params: Dict[str, Any]
    feature_importance: Dict[str, List[Dict[str, Any]]]
    metrics: Dict[str, float]
    cv_results: Dict[str, Any]
    warnings: List[Dict[str, str]]
    best_iteration: Optional[int] = None
    predictions: Optional[np.ndarray] = None


class XGBoostModel:
    """
    XGBoost Regression with hyperparameter tuning and early stopping.
    
    Features:
    - Randomized search over comprehensive hyperparameter space
    - Early stopping with validation set
    - Multiple feature importance metrics
    """
    
    DEFAULT_PARAM_GRID = {
        'n_estimators': [100, 300, 500],
        'learning_rate': [0.01, 0.05, 0.1],
        'max_depth': [3, 5, 7],
        'min_child_weight': [1, 3, 5],
        'subsample': [0.7, 0.8, 1.0],
        'colsample_bytree': [0.7, 0.8, 1.0],
        'gamma': [0, 0.1, 0.2],
        'reg_alpha': [0, 0.1, 1],
        'reg_lambda': [1, 10, 100]
    }
    
    def __init__(
        self,
        param_grid: Optional[Dict] = None,
        n_iter: int = 20,
        cv_folds: int = 5,
        early_stopping_rounds: int = 50,
        n_jobs: int = -1,
        random_state: int = 42
    ):
        """
        Initialize XGBoost model.
        
        Args:
            param_grid: Hyperparameter search space
            n_iter: Number of random search iterations
            cv_folds: Number of CV folds
            early_stopping_rounds: Patience for early stopping
            n_jobs: Parallel jobs
            random_state: Random seed
        """
        if not XGB_AVAILABLE:
            raise ImportError("XGBoost not installed. Install with: pip install xgboost")
        
        self.param_grid = param_grid or self.DEFAULT_PARAM_GRID
        self.n_iter = n_iter
        self.cv_folds = cv_folds
        self.early_stopping_rounds = early_stopping_rounds
        self.n_jobs = n_jobs
        self.random_state = random_state
        
        self.model = None
        self.best_params = {}
        self.feature_names: List[str] = []
        self.best_iteration = None
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None
    ) -> XGBoostResult:
        """
        Fit XGBoost with randomized search and early stopping.
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            logger.info(f"XGBoost: Randomized search with {self.n_iter} iterations")
            
            # Randomized search (without early stopping for CV)
            base_model = xgb.XGBRegressor(
                random_state=self.random_state,
                n_jobs=self.n_jobs,
                verbosity=0
            )
            
            search = RandomizedSearchCV(
                base_model,
                self.param_grid,
                n_iter=self.n_iter,
                cv=self.cv_folds,
                scoring='neg_mean_squared_error',
                random_state=self.random_state,
                n_jobs=1  # XGBoost handles parallelism internally
            )
            
            search.fit(X_train_clean, y_train_clean)
            self.best_params = search.best_params_
            
            # Retrain with early stopping if validation set provided
            if X_test is not None and y_test is not None:
                X_test_clean = X_test.fillna(0).replace([np.inf, -np.inf], 0)
                y_test_clean = y_test.fillna(0)
                
                self.model = xgb.XGBRegressor(
                    **self.best_params,
                    random_state=self.random_state,
                    n_jobs=self.n_jobs,
                    verbosity=0,
                    early_stopping_rounds=self.early_stopping_rounds
                )
                
                self.model.fit(
                    X_train_clean, y_train_clean,
                    eval_set=[(X_test_clean, y_test_clean)],
                    verbose=False
                )
                
                self.best_iteration = self.model.best_iteration
            else:
                self.model = search.best_estimator_
            
            # Feature importance (multiple types)
            feature_importance = self._extract_feature_importance()
            
            # Metrics
            train_pred = self.model.predict(X_train_clean)
            
            metrics = {
                'r2_train': float(r2_score(y_train_clean, train_pred)),
                'rmse_train': float(np.sqrt(mean_squared_error(y_train_clean, train_pred))),
                'mae_train': float(mean_absolute_error(y_train_clean, train_pred)),
                'best_iteration': self.best_iteration,
                'n_samples': len(X_train)
            }
            
            if X_test is not None and y_test is not None:
                test_pred = self.model.predict(X_test_clean)
                metrics['r2_test'] = float(r2_score(y_test_clean, test_pred))
                metrics['rmse_test'] = float(np.sqrt(mean_squared_error(y_test_clean, test_pred)))
                metrics['mae_test'] = float(mean_absolute_error(y_test_clean, test_pred))
            
            # CV summary
            cv_results = {
                'n_iterations': self.n_iter,
                'best_cv_rmse': float(np.sqrt(-search.best_score_)),
                'best_params': self.best_params
            }
            
            # Warnings
            warnings = self._generate_warnings(metrics)
            
            logger.info(f"XGBoost: Best iteration={self.best_iteration}, R²={metrics['r2_train']:.4f}")
            
            return XGBoostResult(
                success=True,
                best_params=self.best_params,
                feature_importance=feature_importance,
                metrics=metrics,
                cv_results=cv_results,
                warnings=warnings,
                best_iteration=self.best_iteration,
                predictions=train_pred
            )
            
        except Exception as e:
            logger.error(f"XGBoost fit failed: {e}")
            return XGBoostResult(
                success=False,
                best_params={},
                feature_importance={},
                metrics={},
                cv_results={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _extract_feature_importance(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract multiple feature importance types."""
        importance_dict = {}
        
        for imp_type in ['gain', 'cover', 'weight']:
            try:
                booster = self.model.get_booster()
                scores = booster.get_score(importance_type=imp_type)
                
                imp_list = []
                total = sum(scores.values()) if scores else 1
                
                for i, name in enumerate(self.feature_names):
                    # XGBoost uses f0, f1, etc. as feature names internally
                    key = f'f{i}'
                    score = scores.get(key, scores.get(name, 0))
                    
                    imp_list.append({
                        'feature': name,
                        'importance': float(score),
                        'importance_pct': float(score / total * 100) if total > 0 else 0
                    })
                
                imp_list.sort(key=lambda x: x['importance'], reverse=True)
                for rank, item in enumerate(imp_list, 1):
                    item['rank'] = rank
                
                importance_dict[imp_type] = imp_list
            except:
                importance_dict[imp_type] = []
        
        return importance_dict
    
    def _generate_warnings(self, metrics: Dict[str, float]) -> List[Dict[str, str]]:
        """Generate warnings."""
        warnings = []
        
        # Overfitting
        r2_train = metrics.get('r2_train', 0)
        r2_test = metrics.get('r2_test', r2_train)
        gap = r2_train - r2_test
        
        if gap > 0.15:
            warnings.append({
                'type': 'overfitting',
                'severity': 'high',
                'message': f"Train R² >> Test R² (gap={gap:.3f}). Increase regularization (reg_alpha, reg_lambda)."
            })
        
        # Early stopping not triggered
        best_iter = metrics.get('best_iteration')
        n_est = self.best_params.get('n_estimators', 100)
        if best_iter and best_iter >= n_est - 10:
            warnings.append({
                'type': 'no_early_stop',
                'severity': 'medium',
                'message': f"Early stopping near max ({best_iter}/{n_est}). Consider more trees."
            })
        
        # Perfect train fit (suspicious)
        if r2_train > 0.999:
            warnings.append({
                'type': 'perfect_fit',
                'severity': 'high',
                'message': "Near-perfect train fit. Likely overfitting or data leakage."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        return self.model.predict(X_clean)


def run_xgboost_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> XGBoostResult:
    """Convenience function for XGBoost analysis."""
    model = XGBoostModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
