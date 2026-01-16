"""
Model Trainer - Production ML Models for Marketing Mix

Only 3 models (opinionated selection):
1. Ridge Regression (Primary) - Handles multicollinearity, interpretable
2. Random Forest (Secondary) - Non-linear interactions
3. XGBoost (Tertiary) - Best-in-class for tabular data

Removed: OLS, Lasso, Elastic Net, Bayesian (redundant/unnecessary)

Author: Senior ML Expert (15 years Google/Meta Ads experience)
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, LinearRegression, ElasticNet, SGDRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.compose import TransformedTargetRegressor
from sklearn.preprocessing import StandardScaler
from loguru import logger

try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    logger.warning("XGBoost not installed. Install with: pip install xgboost")


@dataclass
class TrainedModel:
    """Trained model with metadata."""
    name: str
    model: object
    training_time: float
    hyperparameters: Dict
    feature_importance: Optional[Dict[str, float]] = None


class ModelTrainer:
    """
    Train production-ready models for marketing mix modeling.
    
    Model Selection Philosophy:
    - Ridge: Default choice. Handles correlated features (spend/impressions/clicks).
             Fast, stable, interpretable coefficients.
    - Random Forest: Use when R² improvement > 10% over Ridge.
                     Captures non-linear effects (e.g., diminishing returns).
    - XGBoost: Use when R² improvement > 15% over Ridge.
               Best for large datasets (>50K rows). Watch for overfitting.
    """
    
    def __init__(self, quick_mode: bool = False, random_state: int = 42):
        """
        Initialize model trainer.
        
        Args:
            quick_mode: If True, use reduced hyperparameter search (faster)
            random_state: Random seed for reproducibility
        """
        self.quick_mode = quick_mode
        self.random_state = random_state
    
    def train_ridge(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        alphas: Optional[list] = None
    ) -> TrainedModel:
        """
        Train Ridge regression with cross-validated alpha selection.
        
        Ridge is the PRIMARY model for marketing mix:
        - Handles multicollinearity (spend ↔ impressions ↔ clicks)
        - Stable coefficients
        - Interpretable for stakeholders
        
        Args:
            X_train: Training features
            y_train: Training labels
            alphas: Alpha values to try (default: [0.1, 1, 10, 100])
            
        Returns:
            TrainedModel with best Ridge model
        """
        import time
        start_time = time.time()
        
        if alphas is None:
            alphas = [0.1, 1.0, 10.0, 100.0] if self.quick_mode else [0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
        
        from sklearn.model_selection import cross_val_score
        
        best_alpha = 1.0  # Default
        best_score = 0.0
        
        # Try cross-validation, but don't fail if it doesn't work
        try:
            for alpha in alphas:
                model = Ridge(alpha=alpha, random_state=self.random_state)
                try:
                    scores = cross_val_score(model, X_train, y_train, cv=3, scoring='r2')  # Reduced CV folds
                    mean_score = scores.mean()
                    
                    if mean_score > best_score:
                        best_score = mean_score
                        best_alpha = alpha
                except Exception as cv_error:
                    logger.debug(f"CV failed for alpha={alpha}: {cv_error}")
                    continue
        except Exception as e:
            logger.warning(f"Cross-validation failed completely: {e}. Using default alpha=1.0")
        
        # Train final model with best alpha
        final_model = Ridge(alpha=best_alpha, random_state=self.random_state)
        final_model.fit(X_train, y_train)
        
        training_time = time.time() - start_time
        
        logger.info(f"Ridge: Best α={best_alpha}, CV R²={best_score:.4f}, Time={training_time:.2f}s")
        
        return TrainedModel(
            name="Ridge",
            model=final_model,
            training_time=training_time,
            hyperparameters={"alpha": best_alpha, "cv_r2": best_score}
        )
    
    def train_random_forest(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        feature_names: Optional[list] = None
    ) -> TrainedModel:
        """
        Train Random Forest with hyperparameter tuning.
        
        Use when:
        - Ridge R² < 0.7 (linear model insufficient)
        - Suspect non-linear interactions
        - Need feature importance rankings
        
        Args:
            X_train: Training features
            y_train: Training labels
            feature_names: Feature names for importance ranking
            
        Returns:
            TrainedModel with tuned Random Forest
        """
        import time
        start_time = time.time()
        
        # Hyperparameter grid
        if self.quick_mode:
            param_dist = {
                'n_estimators': [50, 100],
                'max_depth': [10, 20],
                'min_samples_split': [10],
                'min_samples_leaf': [5]
            }
            n_iter = 4
        else:
            param_dist = {
                'n_estimators': [100, 200, 300],
                'max_depth': [10, 20, 30, None],
                'min_samples_split': [5, 10, 20],
                'min_samples_leaf': [2, 5, 10]
            }
            n_iter = 10
        
        rf = RandomForestRegressor(
            random_state=self.random_state,
            n_jobs=-1,
            oob_score=True
        )
        
        search = RandomizedSearchCV(
            rf,
            param_distributions=param_dist,
            n_iter=n_iter,
            cv=3,
            scoring='r2',
            random_state=self.random_state,
            n_jobs=-1
        )
        
        search.fit(X_train, y_train)
        best_model = search.best_estimator_
        
        training_time = time.time() - start_time
        
        # Extract feature importance
        feature_importance = None
        if feature_names:
            importances = best_model.feature_importances_
            feature_importance = dict(zip(feature_names, importances.astype(float)))
        
        logger.info(f"Random Forest: Best params={search.best_params_}, "
                   f"CV R²={search.best_score_:.4f}, Time={training_time:.2f}s")
        
        return TrainedModel(
            name="Random Forest",
            model=best_model,
            training_time=training_time,
            hyperparameters=search.best_params_,
            feature_importance=feature_importance
        )
    
    def train_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        feature_names: Optional[list] = None
    ) -> Optional[TrainedModel]:
        """
        Train XGBoost with hyperparameter tuning.
        
        Use when:
        - Dataset > 50K rows
        - Ridge/RF R² < 0.6
        - Need best possible accuracy
        
        WARNING: Prone to overfitting on small datasets (<10K rows)
        
        Args:
            X_train: Training features
            y_train: Training labels
            feature_names: Feature names for importance ranking
            
        Returns:
            TrainedModel with tuned XGBoost, or None if XGBoost not installed
        """
        if not HAS_XGBOOST:
            logger.warning("XGBoost not available. Skipping.")
            return None
        
        import time
        start_time = time.time()
        
        # Hyperparameter grid
        if self.quick_mode:
            param_dist = {
                'max_depth': [3, 5],
                'learning_rate': [0.1],
                'n_estimators': [100],
                'subsample': [0.8]
            }
            n_iter = 2
        else:
            param_dist = {
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.1, 0.3],
                'n_estimators': [100, 200, 300],
                'subsample': [0.7, 0.8, 0.9],
                'colsample_bytree': [0.7, 0.8, 0.9]
            }
            n_iter = 10
        
        xgb_model = xgb.XGBRegressor(
            random_state=self.random_state,
            n_jobs=-1,
            objective='reg:squarederror'
        )
        
        search = RandomizedSearchCV(
            xgb_model,
            param_distributions=param_dist,
            n_iter=n_iter,
            cv=3,
            scoring='r2',
            random_state=self.random_state,
            n_jobs=-1
        )
        
        search.fit(X_train, y_train)
        best_model = search.best_estimator_
        
        training_time = time.time() - start_time
        
        # Extract feature importance
        feature_importance = None
        if feature_names:
            importances = best_model.feature_importances_
            feature_importance = dict(zip(feature_names, importances.astype(float)))
        
        logger.info(f"XGBoost: Best params={search.best_params_}, "
                   f"CV R²={search.best_score_:.4f}, Time={training_time:.2f}s")
        
        return TrainedModel(
            name="XGBoost",
            model=best_model,
            training_time=training_time,
            hyperparameters=search.best_params_,
            feature_importance=feature_importance
        )

    def train_ols(self, X_train: np.ndarray, y_train: np.ndarray) -> TrainedModel:
        """Train Ordinary Least Squares (Linear Regression)."""
        import time
        start_time = time.time()
        
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        training_time = time.time() - start_time
        
        return TrainedModel(
            name="OLS (Linear Regression)",
            model=model,
            training_time=training_time,
            hyperparameters={},
            feature_importance=None  # Coeffs handled separately
        )
        
    def train_elastic_net(self, X_train: np.ndarray, y_train: np.ndarray) -> TrainedModel:
        """Train Elastic Net (L1 + L2 regularization)."""
        import time
        start_time = time.time()
        
        from sklearn.linear_model import ElasticNet
        
        # Simple grid search if not provided via CV
        best_model = ElasticNet(random_state=self.random_state)
        best_model.fit(X_train, y_train) # Using defaults for simplicity/speed or use ElasticNetCV for better results
        
        # Proper implementation with CV similar to Ridge
        from sklearn.model_selection import GridSearchCV
        
        param_grid = {
            'alpha': [0.1, 1.0, 10.0] if self.quick_mode else [0.01, 0.1, 1.0, 10.0],
            'l1_ratio': [0.5] if self.quick_mode else [0.1, 0.5, 0.9]
        }
        
        search = GridSearchCV(
            ElasticNet(random_state=self.random_state),
            param_grid,
            cv=3,
            scoring='r2',
            error_score='raise'
        )
        
        try:
            search.fit(X_train, y_train)
            best_model = search.best_estimator_
            params = search.best_params_
        except Exception as e:
            logger.warning(f"ElasticNet CV failed: {e}. Using default.")
            best_model = ElasticNet(random_state=self.random_state)
            best_model.fit(X_train, y_train)
            params = {}
            
        training_time = time.time() - start_time
        
        return TrainedModel(
            name="Elastic Net",
            model=best_model,
            training_time=training_time,
            hyperparameters=params
        )

    def train_bayesian_ridge(self, X_train: np.ndarray, y_train: np.ndarray) -> TrainedModel:
        """Train Bayesian Ridge regression with automatic relevance determination.
        
        Benefits:
        - Probabilistic approach with uncertainty quantification
        - Automatic regularization (no hyperparameter tuning needed)
        - Robust to multicollinearity
        """
        import time
        start_time = time.time()
        
        from sklearn.linear_model import BayesianRidge
        
        # BayesianRidge has built-in hyperparameter estimation
        model = BayesianRidge(
            n_iter=300,
            tol=1e-3,
            alpha_1=1e-6,
            alpha_2=1e-6,
            lambda_1=1e-6,
            lambda_2=1e-6,
            compute_score=True  # For model selection
        )
        
        try:
            model.fit(X_train, y_train)
            logger.info(f"Bayesian Ridge trained: alpha={model.alpha_:.4f}, lambda={model.lambda_:.4f}")
        except Exception as e:
            logger.warning(f"Bayesian Ridge training error: {e}")
            # Fallback to defaults
            model = BayesianRidge()
            model.fit(X_train, y_train)
        
        training_time = time.time() - start_time
        
        return TrainedModel(
            name="Bayesian Ridge",
            model=model,
            training_time=training_time,
            hyperparameters={"alpha": model.alpha_, "lambda": model.lambda_}
        )

    def train_sgd(self, X_train: np.ndarray, y_train: np.ndarray) -> TrainedModel:
        """Train Stochastic Gradient Descent Regressor with Target Scaling."""
        import time
        start_time = time.time()
        
        # SGD requires scaled target to converge effectively
        # We use TransformedTargetRegressor to handle scaling/inverse_scaling automatically
        base_estimator = SGDRegressor(
            max_iter=5000, 
            tol=1e-4, 
            random_state=self.random_state,
            early_stopping=True,
            n_iter_no_change=10
        )
        
        model = TransformedTargetRegressor(
            regressor=base_estimator,
            transformer=StandardScaler()
        )
        
        try:
            model.fit(X_train, y_train)
        except Exception as e:
            logger.warning(f"SGD training failed: {e}. Falling back to default params.")
            base_estimator = SGDRegressor(random_state=self.random_state)
            model = TransformedTargetRegressor(regressor=base_estimator, transformer=StandardScaler())
            model.fit(X_train, y_train)
        
        training_time = time.time() - start_time
        
        return TrainedModel(
            name="Gradient Descent (SGD)",
            model=model,
            training_time=training_time,
            hyperparameters={"max_iter": 5000, "scaled_target": True},
            feature_importance=None
        )
    
    def train_all(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        feature_names: Optional[list] = None,
        models_to_run: Optional[list] = None
    ) -> Dict[str, TrainedModel]:
        """
        Train all requested models and return results.
        
        Args:
            X_train: Training features
            y_train: Training labels
            feature_names: Feature names for importance
            models_to_run: List of model names (default: all 3)
            
        Returns:
            Dict mapping model name to TrainedModel
        """
        if models_to_run is None:
            models_to_run = ["Ridge", "Random Forest", "XGBoost"]
        
        results = {}
        
        if "Ridge" in models_to_run:
            results["Ridge"] = self.train_ridge(X_train, y_train)
        
        if "Random Forest" in models_to_run:
            results["Random Forest"] = self.train_random_forest(X_train, y_train, feature_names)
        
        if "XGBoost" in models_to_run and HAS_XGBOOST:
            xgb_result = self.train_xgboost(X_train, y_train, feature_names)
            if xgb_result:
                results["XGBoost"] = xgb_result
                
        if "OLS" in models_to_run or "Linear Regression" in models_to_run:
            results["OLS"] = self.train_ols(X_train, y_train)
            
        if "Elastic Net" in models_to_run:
            results["Elastic Net"] = self.train_elastic_net(X_train, y_train)
            
        if "Gradient Descent" in models_to_run or "SGD" in models_to_run:
            results["Gradient Descent"] = self.train_sgd(X_train, y_train)
        
        if "Bayesian Ridge" in models_to_run:
            results["Bayesian Ridge"] = self.train_bayesian_ridge(X_train, y_train)
        
        return results
