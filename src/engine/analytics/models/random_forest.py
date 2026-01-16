"""
Random Forest Regression

Ensemble of decision trees capturing non-linear relationships and interactions.

Features:
- Randomized hyperparameter search (faster than grid search)
- Feature importance (Gini-based)
- Out-of-bag (OOB) scoring
- No scaling required
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, cross_val_score, KFold
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


@dataclass
class RandomForestResult:
    """Result container for Random Forest regression."""
    success: bool
    best_params: Dict[str, Any]
    feature_importance: List[Dict[str, Any]]
    metrics: Dict[str, float]
    cv_results: Dict[str, Any]
    warnings: List[Dict[str, str]]
    predictions: Optional[np.ndarray] = None


class RandomForestModel:
    """
    Random Forest Regression with hyperparameter tuning.
    
    Features:
    - Randomized search over hyperparameter space
    - Gini feature importance
    - OOB score for unbiased estimate
    """
    
    # Default hyperparameter space
    DEFAULT_PARAM_GRID = {
        'n_estimators': [100, 300, 500],
        'max_depth': [10, 20, 30, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['sqrt', 'log2', 0.3]
    }
    
    def __init__(
        self,
        param_grid: Optional[Dict] = None,
        n_iter: int = 20,
        cv_folds: int = 5,
        n_jobs: int = -1,
        random_state: int = 42
    ):
        """
        Initialize Random Forest model.
        
        Args:
            param_grid: Hyperparameter search space
            n_iter: Number of random search iterations
            cv_folds: Number of CV folds
            n_jobs: Parallel jobs (-1 = all cores)
            random_state: Random seed
        """
        self.param_grid = param_grid or self.DEFAULT_PARAM_GRID
        self.n_iter = n_iter
        self.cv_folds = cv_folds
        self.n_jobs = n_jobs
        self.random_state = random_state
        
        self.model = None
        self.best_params = {}
        self.feature_names: List[str] = []
        self.search_results = None
        
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        feature_names: Optional[List[str]] = None
    ) -> RandomForestResult:
        """
        Fit Random Forest with randomized hyperparameter search.
        """
        try:
            self.feature_names = feature_names or list(X_train.columns)
            
            # Clean data (RF handles NaN but we fill for consistency)
            X_train_clean = X_train.fillna(0).replace([np.inf, -np.inf], 0)
            y_train_clean = y_train.fillna(0)
            
            # Randomized search
            logger.info(f"RandomForest: Searching {self.n_iter} random combinations")
            
            base_model = RandomForestRegressor(
                random_state=self.random_state,
                oob_score=True,
                n_jobs=self.n_jobs
            )
            
            search = RandomizedSearchCV(
                base_model,
                self.param_grid,
                n_iter=self.n_iter,
                cv=self.cv_folds,
                scoring='neg_mean_squared_error',
                random_state=self.random_state,
                n_jobs=self.n_jobs
            )
            
            search.fit(X_train_clean, y_train_clean)
            
            self.model = search.best_estimator_
            self.best_params = search.best_params_
            self.search_results = search.cv_results_
            
            # Feature importance
            feature_importance = self._extract_feature_importance()
            
            # Metrics
            train_pred = self.model.predict(X_train_clean)
            
            metrics = {
                'r2_train': float(r2_score(y_train_clean, train_pred)),
                'rmse_train': float(np.sqrt(mean_squared_error(y_train_clean, train_pred))),
                'mae_train': float(mean_absolute_error(y_train_clean, train_pred)),
                'oob_score': float(self.model.oob_score_),
                'n_estimators': self.model.n_estimators,
                'max_depth_actual': self._get_tree_stats(),
                'n_samples': len(X_train)
            }
            
            # Test evaluation
            if X_test is not None and y_test is not None:
                X_test_clean = X_test.fillna(0).replace([np.inf, -np.inf], 0)
                y_test_clean = y_test.fillna(0)
                test_pred = self.model.predict(X_test_clean)
                
                metrics['r2_test'] = float(r2_score(y_test_clean, test_pred))
                metrics['rmse_test'] = float(np.sqrt(mean_squared_error(y_test_clean, test_pred)))
                metrics['mae_test'] = float(mean_absolute_error(y_test_clean, test_pred))
                metrics['oob_test_gap'] = metrics['oob_score'] - metrics['r2_test']
            
            # CV results summary
            cv_results = self._build_cv_summary()
            
            # Warnings
            warnings = self._generate_warnings(feature_importance, metrics)
            
            logger.info(f"RandomForest: Best params found, OOB={metrics['oob_score']:.4f}, R²={metrics['r2_train']:.4f}")
            
            return RandomForestResult(
                success=True,
                best_params=self.best_params,
                feature_importance=feature_importance,
                metrics=metrics,
                cv_results=cv_results,
                warnings=warnings,
                predictions=train_pred
            )
            
        except Exception as e:
            logger.error(f"RandomForest fit failed: {e}")
            return RandomForestResult(
                success=False,
                best_params={},
                feature_importance=[],
                metrics={},
                cv_results={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _extract_feature_importance(self) -> List[Dict[str, Any]]:
        """Extract Gini feature importance."""
        importance_list = []
        total_importance = sum(self.model.feature_importances_)
        
        for name, imp in zip(self.feature_names, self.model.feature_importances_):
            importance_list.append({
                'feature': name,
                'importance': float(imp),
                'importance_pct': float(imp / total_importance * 100) if total_importance > 0 else 0,
                'rank': 0  # Will be assigned after sorting
            })
        
        # Sort and assign ranks
        importance_list.sort(key=lambda x: x['importance'], reverse=True)
        for rank, item in enumerate(importance_list, 1):
            item['rank'] = rank
        
        return importance_list
    
    def _get_tree_stats(self) -> Dict[str, Any]:
        """Get statistics about tree depths."""
        depths = [tree.tree_.max_depth for tree in self.model.estimators_]
        return {
            'mean_depth': float(np.mean(depths)),
            'max_depth': int(np.max(depths)),
            'min_depth': int(np.min(depths))
        }
    
    def _build_cv_summary(self) -> Dict[str, Any]:
        """Build CV results summary."""
        if self.search_results is None:
            return {}
        
        return {
            'n_iterations': self.n_iter,
            'cv_folds': self.cv_folds,
            'best_cv_score': float(-self.search_results['mean_test_score'][self.search_results['rank_test_score'] == 1][0]),
            'best_cv_std': float(self.search_results['std_test_score'][self.search_results['rank_test_score'] == 1][0]),
            'best_params': self.best_params
        }
    
    def _generate_warnings(
        self,
        feature_importance: List[Dict],
        metrics: Dict[str, float]
    ) -> List[Dict[str, str]]:
        """Generate warnings."""
        warnings = []
        
        # 1. Overfitting (OOB >> test)
        gap = metrics.get('oob_test_gap', 0)
        if gap > 0.1:
            warnings.append({
                'type': 'overfitting',
                'severity': 'high',
                'message': f"OOB score >> Test R² (gap={gap:.3f}). Overfitting - reduce max_depth."
            })
        
        # 2. Single feature dominates
        if feature_importance:
            top_imp = feature_importance[0]['importance_pct']
            if top_imp > 70:
                warnings.append({
                    'type': 'feature_dominance',
                    'severity': 'medium',
                    'message': f"'{feature_importance[0]['feature']}' dominates ({top_imp:.0f}%). Check for data leakage."
                })
        
        # 3. Very deep trees
        depth_stats = metrics.get('max_depth_actual', {})
        max_depth = depth_stats.get('max_depth', 0) if isinstance(depth_stats, dict) else 0
        if max_depth > 40:
            warnings.append({
                'type': 'deep_trees',
                'severity': 'medium',
                'message': f"Very deep trees (max={max_depth}). May be overfitting - constrain max_depth."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions."""
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        return self.model.predict(X_clean)
    
    def get_feature_importance_df(self) -> pd.DataFrame:
        """Get feature importance as DataFrame."""
        if self.model is None:
            return pd.DataFrame()
        
        imp = self._extract_feature_importance()
        return pd.DataFrame(imp)


def run_random_forest_analysis(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[pd.Series] = None,
    **kwargs
) -> RandomForestResult:
    """Convenience function for Random Forest analysis."""
    model = RandomForestModel(**kwargs)
    return model.fit(X_train, y_train, X_test, y_test)
