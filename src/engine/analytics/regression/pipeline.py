"""
Unified Regression Pipeline - Production ML Orchestrator

Coordinates all regression components:
1. Feature Analysis (VIF, correlation)
2. Model Training (Ridge, RF, XGBoost)
3. Model Evaluation (MAE, RMSE, MAPE, residuals)
4. Explainability (coefficient translation, insights)

Replaces 5000+ lines of scattered code with clean, modular architecture.

Author: Senior ML Expert (Google Ads Platform experience)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from loguru import logger

from .model_trainer import ModelTrainer, TrainedModel
from .model_evaluator import ModelEvaluator, ModelMetrics, ResidualDiagnostics
from .feature_analyzer import FeatureAnalyzer
from .coefficient_explainer import CoefficientExplainer
from .shap_explainer import ShapExplainer

# Phase 4: Observability
from src.core.utils.opentelemetry_config import get_tracer
from src.core.utils.observability import metrics

tracer = get_tracer(__name__)



@dataclass
class RegressionResult:
    """Complete regression analysis result."""
    
    # Model selection
    best_model_name: str
    best_model: object
    all_models: Dict[str, TrainedModel]
    
    # Performance metrics
    metrics: ModelMetrics
    residual_diagnostics: ResidualDiagnostics
    
    # Feature analysis
    vif_analysis: Dict
    correlation_analysis: Dict
    feature_coverage: Dict
    
    # Explainability
    coefficient_insights: List[Dict]
    executive_summary: str
    
    # Predictions
    predictions: pd.DataFrame
    prediction_intervals: Tuple[np.ndarray, np.ndarray]
    
    # Raw data
    coefficients: Dict[str, float] = field(default_factory=dict)
    feature_importance: Optional[Dict[str, float]] = None
    shap_data: Optional[Dict[str, Any]] = None
    target_col: str = ""



class RegressionPipeline:
    """
    Production-ready regression pipeline for marketing mix modeling.
    
    Key Features:
    - Only 3 models (Ridge, RF, XGBoost) - no redundant models
    - Comprehensive metrics (MAE, RMSE, MAPE, R², residuals)
    - Multicollinearity detection (VIF, correlation)
    - Stakeholder-friendly explanations
    - Prediction intervals for confidence
    
    Usage:
        pipeline = RegressionPipeline(quick_mode=True)
        result = pipeline.run(df, target='conversions', features=['spend', 'impressions'])
        print(result.executive_summary)
    """
    
    def __init__(
        self,
        models_to_run: Optional[List[str]] = None,
        quick_mode: bool = False,
        random_state: int = 42
    ):
        """
        Initialize regression pipeline.
        
        Args:
            models_to_run: Models to train (default: ["Ridge", "Random Forest", "XGBoost"])
            quick_mode: Use reduced hyperparameter search (faster)
            random_state: Random seed for reproducibility
        """
        self.models_to_run = models_to_run or ["Ridge", "Random Forest", "XGBoost"]
        self.quick_mode = quick_mode
        self.random_state = random_state
        
        self.trainer = ModelTrainer(quick_mode=quick_mode, random_state=random_state)
    
    def run(
        self,
        df: pd.DataFrame,
        target: str,
        features: List[str],
        test_size: float = 0.2,
        encode_dimensions: Optional[List[str]] = None
    ) -> RegressionResult:
        """
        Run complete regression analysis.
        
        Args:
            df: Input dataframe
            target: Target column name
            features: Feature column names
            test_size: Test set proportion
            encode_dimensions: Categorical dimensions to one-hot encode
            
        Returns:
            RegressionResult with all analysis outputs
        """
        if tracer:
            with tracer.start_as_current_span("RegressionPipeline.run") as span:
                span.set_attribute("regression.target", target)
                span.set_attribute("regression.features_count", len(features))
                return self._run(df, target, features, test_size, encode_dimensions)
        else:
            return self._run(df, target, features, test_size, encode_dimensions)

    def _run(
        self,
        df: pd.DataFrame,
        target: str,
        features: List[str],
        test_size: float = 0.2,
        encode_dimensions: Optional[List[str]] = None
    ) -> RegressionResult:
        """Internal execution of regression analysis."""
        import time
        start_pipeline = time.time()
        logger.info(f"Starting regression pipeline: target={target}, features={features}")
        
        # Step 1: Prepare data
        with tracer.start_as_current_span("prepare_data") if tracer else open("/dev/null", "w") as _:
            X, y, feature_names = self._prepare_data(df, target, features, encode_dimensions)

        
        # Step 2: Train-test split
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=self.random_state
        )
        
        logger.info(f"Data split: {len(X_train)} train, {len(X_test)} test")
        
        # Step 2.5: Scale features (important for Ridge regression)
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        logger.info("Features scaled (StandardScaler)")
        
        # Step 3: Feature analysis (on scaled data)
        logger.info("Analyzing features (VIF, correlation, coverage)...")
        vif_analysis = FeatureAnalyzer.compute_vif(pd.DataFrame(X_train_scaled, columns=feature_names))
        correlation_analysis = FeatureAnalyzer.analyze_correlation(pd.DataFrame(X_train_scaled, columns=feature_names))
        feature_coverage = FeatureAnalyzer.check_feature_coverage(
            pd.DataFrame(X_train_scaled, columns=feature_names),
            pd.DataFrame(X_test_scaled, columns=feature_names)
        )
        
        # Step 4: Train models (on scaled data)
        logger.info(f"Training models: {self.models_to_run}")
        with tracer.start_as_current_span("train_models") if tracer else open("/dev/null", "w") as _:
            trained_models = self.trainer.train_all(X_train_scaled, y_train, feature_names, self.models_to_run)

        
        # Step 5: Select best model
        best_model_name, best_model = self._select_best_model(trained_models, X_test_scaled, y_test)
        logger.info(f"Best model: {best_model_name}")
        
        # Step 6: Evaluate best model (on scaled data)
        logger.info("Evaluating model performance...")
        y_train_pred = best_model.model.predict(X_train_scaled)
        y_test_pred = best_model.model.predict(X_test_scaled)
        
        evaluation_metrics = ModelEvaluator.evaluate(y_train, y_test, y_train_pred, y_test_pred)
        residual_diagnostics = ModelEvaluator.analyze_residuals(y_test, y_test_pred)
        
        # Step 7: Generate predictions with intervals
        prediction_intervals = ModelEvaluator.generate_prediction_intervals(
            y_test_pred, evaluation_metrics.residual_std
        )
        
        predictions_df = pd.DataFrame({
            'actual': y_test,
            'predicted': y_test_pred,
            'residual': y_test - y_test_pred,
            'lower_bound': prediction_intervals[0],
            'upper_bound': prediction_intervals[1]
        })
        
        # Step 8: Extract coefficients and generate insights
        coefficients = self._extract_coefficients(best_model, feature_names)
        coefficient_insights = CoefficientExplainer.rank_features_by_impact(coefficients, top_n=10)
        
        executive_summary = CoefficientExplainer.generate_executive_summary(
            model_name=best_model_name,
            r2_test=evaluation_metrics.r2_test,
            mae=evaluation_metrics.mae,
            top_drivers=coefficient_insights,
            target_name=target
        )

        # Step 9: Compute SHAP values
        shap_data = {}
        try:
            # Determine model type
            model_type = "linear"
            if best_model_name in ["Random Forest", "XGBoost"]:
                model_type = "tree"
            
            shap_data = ShapExplainer.compute_shap_values(
                model=best_model.model,
                X_background=X_train_scaled,
                X_explain=X_test_scaled,
                feature_names=feature_names,
                model_type=model_type
            )
        except Exception as e:
            logger.warning(f"Failed to compute SHAP: {e}")
        
        # Phase 4 Metrics: Performance and Quality
        pipeline_duration = time.time() - start_pipeline
        metrics.record_time("regression_pipeline_duration_ms", pipeline_duration * 1000)
        metrics.set_gauge("regression_best_model_r2", evaluation_metrics.r2_test)
        metrics.increment("regression_runs_total", labels={"target": target, "best_model": best_model_name})

        logger.info(f"Pipeline complete in {pipeline_duration:.2f}s!")

        
        return RegressionResult(
            best_model_name=best_model_name,
            best_model=best_model.model,
            all_models=trained_models,
            metrics=evaluation_metrics,
            residual_diagnostics=residual_diagnostics,
            vif_analysis=vif_analysis,
            correlation_analysis=correlation_analysis,
            feature_coverage=feature_coverage,
            coefficient_insights=coefficient_insights,
            executive_summary=executive_summary,
            predictions=predictions_df,
            prediction_intervals=prediction_intervals,
            coefficients=coefficients,
            feature_importance=best_model.feature_importance,
            shap_data=shap_data,
            target_col=target
        )
    
    def _prepare_data(
        self,
        df: pd.DataFrame,
        target: str,
        features: List[str],
        encode_dimensions: Optional[List[str]] = None
    ) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare features and target."""
        cols_to_check = [target] + features
        if encode_dimensions:
            cols_to_check += encode_dimensions
            
        # 1. Clean data - drop rows with NaNs in relevant columns
        initial_len = len(df)
        df_clean = df.dropna(subset=[c for c in cols_to_check if c in df.columns]).copy()
        
        if len(df_clean) < initial_len:
            logger.warning(f"Regression Data Prep: Dropped {initial_len - len(df_clean)} rows containing NaNs")
            
        if len(df_clean) == 0:
            raise ValueError(f"Regression Data Prep Error: All {initial_len} rows contain NaNs in required columns ({cols_to_check})")

        # 2. Extract target
        y = df_clean[target].values
        
        # 3. Extract features
        X_df = df_clean[features].copy()
        
        # 4. One-hot encode dimensions if requested
        if encode_dimensions:
            for dim in encode_dimensions:
                if dim in df_clean.columns:
                    # Use get_dummies on the cleaned categorical column
                    dummies = pd.get_dummies(df_clean[dim], prefix=dim, drop_first=True)
                    # Convert booleans to ints (0/1) for better compatibility with all models
                    dummies = dummies.astype(int)
                    X_df = pd.concat([X_df, dummies], axis=1)
                    
                    # Remove original categorical column if it was in features
                    if dim in X_df.columns:
                        X_df = X_df.drop(columns=[dim])
        
        feature_names = X_df.columns.tolist()
        X = X_df.values
        
        return X, y, feature_names
    
    def _select_best_model(
        self,
        trained_models: Dict[str, TrainedModel],
        X_test: np.ndarray,
        y_test: np.ndarray
    ) -> Tuple[str, TrainedModel]:
        """Select best model based on test R²."""
        from sklearn.metrics import r2_score, mean_absolute_error
        from types import SimpleNamespace
        
        best_name = None
        best_model = None
        best_r2 = -np.inf
        
        for name, model in trained_models.items():
            y_pred = model.model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            
            # Attach metrics for backend comparison
            model.metrics = SimpleNamespace(r2_test=float(r2), mae=float(mae))
            
            if r2 > best_r2:
                best_r2 = r2
                best_name = name
                best_model = model
        
        return best_name, best_model
    
    def _extract_coefficients(
        self,
        model: TrainedModel,
        feature_names: List[str]
    ) -> Dict[str, float]:
        """Extract coefficients from model."""
        estimator = model.model
        
        # Unwrap TransformedTargetRegressor (used for SGD)
        if hasattr(estimator, 'regressor_'):
            estimator = estimator.regressor_
            
        if hasattr(estimator, 'coef_'):
            # Linear model
            return dict(zip(feature_names, estimator.coef_.astype(float)))
        elif model.feature_importance:
            # Tree-based model
            return model.feature_importance
        else:
            return {}
