"""
Model Comparison Engine

Runs all regression models, generates comparison table, and produces business insights.

Features:
- Run all 7 models (OLS, Ridge, Lasso, Elastic Net, Bayesian, RF, XGBoost)
- Generate comprehensive comparison table
- Ranked feature importance across models
- Predictions DataFrame with residuals and confidence intervals
- Auto-generated business insights
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger
import time

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

# Import all models
from src.engine.analytics.models.ols_regression import OLSRegressionModel
from src.engine.analytics.models.ridge_regression import RidgeRegressionModel
from src.engine.analytics.models.lasso_regression import LassoRegressionModel
from src.engine.analytics.models.elastic_net import ElasticNetModel
from src.engine.analytics.models.bayesian_regression import BayesianRegressionModel
from src.engine.analytics.models.random_forest import RandomForestModel

try:
    from src.engine.analytics.models.xgboost_model import XGBoostModel
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False


@dataclass
class ModelResult:
    """Individual model result."""
    name: str
    r2_test: float
    rmse_test: float
    mae_test: float
    mape_test: Optional[float] = None
    training_time: float = 0.0
    interpretability: str = 'High'
    feature_importance: List[Dict] = field(default_factory=list)
    predictions: Optional[np.ndarray] = None
    prediction_intervals: Optional[Dict] = None
    best_params: Optional[Dict] = None
    warnings: List[Dict] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Complete comparison result."""
    success: bool
    comparison_table: pd.DataFrame
    best_model: str
    feature_importance: pd.DataFrame
    predictions_df: pd.DataFrame
    business_insights: List[Dict[str, str]]
    model_results: Dict[str, ModelResult]
    target_type: str  # 'conversions' or 'roas'
    recommendations: List[str]


class ModelComparisonEngine:
    """
    Unified engine to run and compare all regression models.
    """
    
    MODEL_INTERPRETABILITY = {
        'OLS': 'High',
        'Ridge': 'High', 
        'Lasso': 'Very High',
        'Elastic Net': 'High',
        'Bayesian': 'High',
        'Random Forest': 'Medium',
        'XGBoost': 'Medium'
    }
    
    # Target thresholds
    GOOD_R2_CONVERSIONS = (0.65, 0.85)
    GOOD_R2_ROAS = (0.50, 0.75)
    IDEAL_MAPE = 0.15
    GOOD_DIRECTIONAL_ACCURACY = 0.80
    
    def __init__(
        self,
        models_to_run: Optional[List[str]] = None,
        quick_mode: bool = False
    ):
        """
        Initialize comparison engine.
        
        Args:
            models_to_run: List of model names to run (default: all)
            quick_mode: Use reduced hyperparameter grids for faster testing
        """
        self.models_to_run = models_to_run or [
            'OLS', 'Ridge', 'Lasso', 'Elastic Net', 
            'Bayesian', 'Random Forest', 'XGBoost'
        ]
        self.quick_mode = quick_mode
        self.model_results: Dict[str, ModelResult] = {}
        
    def run_comparison(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        target_type: str = 'conversions',
        campaign_ids: Optional[pd.Series] = None,
        platforms: Optional[pd.Series] = None
    ) -> ComparisonResult:
        """
        Run all models and generate comprehensive comparison.
        """
        logger.info(f"ModelComparison: Running {len(self.models_to_run)} models")
        
        feature_names = list(X_train.columns)
        all_predictions = {}
        
        # Run each model
        for model_name in self.models_to_run:
            try:
                result = self._run_model(
                    model_name, X_train, y_train, X_test, y_test, feature_names
                )
                self.model_results[model_name] = result
                all_predictions[model_name] = result.predictions
                logger.info(f"  {model_name}: R²={result.r2_test:.4f}, Time={result.training_time:.2f}s")
            except Exception as e:
                logger.error(f"  {model_name} failed: {e}")
        
        # Generate comparison table
        comparison_table = self._build_comparison_table()
        
        # Determine best model
        best_model = self._select_best_model(target_type)
        
        # Aggregate feature importance
        feature_importance_df = self._aggregate_feature_importance()
        
        # Build predictions DataFrame
        predictions_df = self._build_predictions_df(
            X_test, y_test, all_predictions, campaign_ids, platforms
        )
        
        # Generate business insights
        business_insights = self._generate_business_insights(
            target_type, feature_importance_df, predictions_df
        )
        
        # Recommendations
        recommendations = self._generate_recommendations(target_type)
        
        return ComparisonResult(
            success=True,
            comparison_table=comparison_table,
            best_model=best_model,
            feature_importance=feature_importance_df,
            predictions_df=predictions_df,
            business_insights=business_insights,
            model_results=self.model_results,
            target_type=target_type,
            recommendations=recommendations
        )
    
    def _run_model(
        self,
        model_name: str,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        feature_names: List[str]
    ) -> ModelResult:
        """Run a single model and extract results."""
        start_time = time.time()
        
        if model_name == 'OLS':
            model = OLSRegressionModel()
            result = model.fit(X_train, y_train)
            predictions = model.predict(X_test)
            feature_imp = [{'feature': c['feature'], 'importance': abs(c['coefficient'])} 
                          for c in result.coefficients if c['feature'] != 'const']
            
        elif model_name == 'Ridge':
            model = RidgeRegressionModel()
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions = model.predict(X_test)
            feature_imp = [{'feature': c['feature'], 'importance': c['abs_coefficient']} 
                          for c in result.coefficients if c['feature'] != 'intercept']
            
        elif model_name == 'Lasso':
            model = LassoRegressionModel()
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions = model.predict(X_test)
            feature_imp = [{'feature': c['feature'], 'importance': c['abs_coefficient']} 
                          for c in result.coefficients if c['feature'] != 'intercept']
            
        elif model_name == 'Elastic Net':
            model = ElasticNetModel()
            if self.quick_mode:
                model.alphas = [0.1, 1.0]
                model.l1_ratios = [0.5]
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions = model.predict(X_test)
            feature_imp = [{'feature': c['feature'], 'importance': c['abs_coefficient']} 
                          for c in result.coefficients if c['feature'] != 'intercept']
            
        elif model_name == 'Bayesian':
            model = BayesianRegressionModel()
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions, intervals = model.predict(X_test)
            feature_imp = [{'feature': c['feature'], 'importance': abs(c['mean'])} 
                          for c in result.coefficients if c['feature'] != 'intercept']
            
        elif model_name == 'Random Forest':
            param_grid = {
                'n_estimators': [50, 100],
                'max_depth': [10, 20],
                'max_features': ['sqrt']
            } if self.quick_mode else None
            model = RandomForestModel(param_grid=param_grid, n_iter=5 if self.quick_mode else 20)
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions = model.predict(X_test)
            feature_imp = result.feature_importance
            
        elif model_name == 'XGBoost':
            if not XGBOOST_AVAILABLE:
                raise ImportError("XGBoost not available")
            param_grid = {
                'n_estimators': [50, 100],
                'max_depth': [3, 5],
                'learning_rate': [0.1]
            } if self.quick_mode else None
            model = XGBoostModel(param_grid=param_grid, n_iter=5 if self.quick_mode else 20)
            result = model.fit(X_train, y_train, X_test, y_test)
            predictions = model.predict(X_test)
            feature_imp = result.feature_importance.get('gain', [])
        else:
            raise ValueError(f"Unknown model: {model_name}")
        
        training_time = time.time() - start_time
        
        # Calculate metrics
        y_test_clean = y_test.fillna(0).values
        r2 = r2_score(y_test_clean, predictions)
        rmse = np.sqrt(mean_squared_error(y_test_clean, predictions))
        mae = mean_absolute_error(y_test_clean, predictions)
        
        # MAPE (avoid division by zero)
        non_zero_mask = y_test_clean != 0
        mape = np.mean(np.abs((y_test_clean[non_zero_mask] - predictions[non_zero_mask]) / y_test_clean[non_zero_mask])) if non_zero_mask.sum() > 0 else None
        
        return ModelResult(
            name=model_name,
            r2_test=r2,
            rmse_test=rmse,
            mae_test=mae,
            mape_test=mape,
            training_time=training_time,
            interpretability=self.MODEL_INTERPRETABILITY.get(model_name, 'Medium'),
            feature_importance=feature_imp,
            predictions=predictions,
            prediction_intervals=intervals if model_name == 'Bayesian' else None,
            warnings=getattr(result, 'warnings', [])
        )
    
    def _build_comparison_table(self) -> pd.DataFrame:
        """Build model comparison table."""
        rows = []
        for name, result in self.model_results.items():
            rows.append({
                'Model': name,
                'R² (Test)': round(result.r2_test, 3),
                'RMSE': round(result.rmse_test, 1),
                'MAE': round(result.mae_test, 1),
                'MAPE': f"{result.mape_test*100:.1f}%" if result.mape_test else 'N/A',
                'Training Time': f"{result.training_time:.1f}s",
                'Interpretability': result.interpretability
            })
        
        df = pd.DataFrame(rows)
        df = df.sort_values('R² (Test)', ascending=False)
        return df
    
    def _select_best_model(self, target_type: str) -> str:
        """Select best model based on target type."""
        if not self.model_results:
            return 'None'
        
        # Score each model
        scores = {}
        for name, result in self.model_results.items():
            # Weighted scoring: R² (50%), interpretability (30%), speed (20%)
            r2_score_val = result.r2_test
            interp_score = {'Very High': 1.0, 'High': 0.8, 'Medium': 0.5, 'Low': 0.2}.get(result.interpretability, 0.5)
            speed_score = 1.0 / (1.0 + result.training_time / 10)  # Normalize
            
            scores[name] = 0.5 * r2_score_val + 0.3 * interp_score + 0.2 * speed_score
        
        return max(scores, key=scores.get)
    
    def _aggregate_feature_importance(self) -> pd.DataFrame:
        """Aggregate feature importance across models."""
        all_features = {}
        
        for name, result in self.model_results.items():
            for feat_info in result.feature_importance:
                feat = feat_info.get('feature', '')
                imp = feat_info.get('importance', feat_info.get('importance_pct', 0))
                
                if feat not in all_features:
                    all_features[feat] = {'feature': feat}
                all_features[feat][name] = imp
        
        df = pd.DataFrame(list(all_features.values()))
        
        # Calculate average importance
        model_cols = [c for c in df.columns if c != 'feature']
        if model_cols:
            df['avg_importance'] = df[model_cols].mean(axis=1)
            df = df.sort_values('avg_importance', ascending=False)
            df['rank'] = range(1, len(df) + 1)
        
        return df
    
    def _build_predictions_df(
        self,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        predictions: Dict[str, np.ndarray],
        campaign_ids: Optional[pd.Series],
        platforms: Optional[pd.Series]
    ) -> pd.DataFrame:
        """Build predictions DataFrame."""
        # Use best model predictions
        best_model = self._select_best_model('conversions')
        best_preds = predictions.get(best_model, np.zeros(len(y_test)))
        
        df = pd.DataFrame({
            'actual': y_test.values,
            'predicted': best_preds,
            'residual': y_test.values - best_preds,
            'abs_error': np.abs(y_test.values - best_preds),
        })
        
        # MAPE per row
        df['pct_error'] = np.where(
            df['actual'] != 0,
            np.abs(df['residual'] / df['actual']) * 100,
            np.nan
        )
        
        # Add identifiers if provided
        if campaign_ids is not None:
            df['campaign_id'] = campaign_ids.values
        if platforms is not None:
            df['platform'] = platforms.values
        
        # Add Bayesian intervals if available
        bayesian_result = self.model_results.get('Bayesian')
        if bayesian_result and bayesian_result.prediction_intervals:
            df['ci_lower'] = bayesian_result.prediction_intervals.get('lower')
            df['ci_upper'] = bayesian_result.prediction_intervals.get('upper')
        
        return df
    
    def _generate_business_insights(
        self,
        target_type: str,
        feature_importance: pd.DataFrame,
        predictions_df: pd.DataFrame
    ) -> List[Dict[str, str]]:
        """Generate actionable business insights."""
        insights = []
        
        # Top 5 drivers
        top_features = feature_importance.head(5)
        if len(top_features) > 0:
            drivers = [f"{row['feature']} (rank #{int(row['rank'])})" 
                      for _, row in top_features.iterrows()]
            insights.append({
                'type': 'drivers',
                'title': f'Top Drivers of {target_type.title()}',
                'content': f"Primary drivers: {', '.join(drivers)}"
            })
        
        # Platform comparison (if available)
        if 'platform' in predictions_df.columns:
            platform_perf = predictions_df.groupby('platform').agg({
                'actual': 'mean',
                'pct_error': 'mean'
            }).round(2)
            
            if len(platform_perf) > 1:
                best_platform = platform_perf['actual'].idxmax()
                worst_platform = platform_perf['actual'].idxmin()
                diff = ((platform_perf.loc[best_platform, 'actual'] / 
                        platform_perf.loc[worst_platform, 'actual']) - 1) * 100
                
                insights.append({
                    'type': 'platform',
                    'title': 'Platform Comparison',
                    'content': f"{best_platform} drives {diff:.0f}% more {target_type} than {worst_platform}"
                })
        
        # Underperforming campaigns
        worst_campaigns = predictions_df.nlargest(10, 'residual')
        if 'campaign_id' in worst_campaigns.columns and len(worst_campaigns) > 0:
            insights.append({
                'type': 'underperforming',
                'title': 'Underperforming Campaigns',
                'content': f"Top underperformers by residual: {worst_campaigns['campaign_id'].tolist()[:5]}"
            })
        
        # Model accuracy
        best_model = self._select_best_model(target_type)
        best_r2 = self.model_results.get(best_model, {})
        if hasattr(best_r2, 'r2_test'):
            insights.append({
                'type': 'accuracy',
                'title': 'Model Accuracy',
                'content': f"Best model ({best_model}) explains {best_r2.r2_test*100:.1f}% of {target_type} variance"
            })
        
        return insights
    
    def _generate_recommendations(self, target_type: str) -> List[str]:
        """Generate model recommendations."""
        recs = []
        
        if not self.model_results:
            return ['No models successfully trained']
        
        # Best for accuracy
        best_r2_model = max(self.model_results, key=lambda x: self.model_results[x].r2_test)
        recs.append(f"Best accuracy: {best_r2_model} (R²={self.model_results[best_r2_model].r2_test:.3f})")
        
        # Best for interpretability
        linear_models = ['OLS', 'Ridge', 'Lasso', 'Elastic Net']
        linear_results = {k: v for k, v in self.model_results.items() if k in linear_models}
        if linear_results:
            best_linear = max(linear_results, key=lambda x: linear_results[x].r2_test)
            recs.append(f"Best interpretable: {best_linear} (R²={linear_results[best_linear].r2_test:.3f})")
        
        # Feature selection
        if 'Lasso' in self.model_results:
            lasso_feats = len([f for f in self.model_results['Lasso'].feature_importance 
                              if f.get('importance', 0) > 0.001])
            recs.append(f"Feature selection: Lasso selected {lasso_feats} key features")
        
        return recs


def run_full_comparison(
    df: pd.DataFrame,
    target_col: str,
    feature_cols: List[str],
    test_size: float = 0.2,
    **kwargs
) -> ComparisonResult:
    """Convenience function to run full model comparison."""
    X = df[feature_cols].fillna(0)
    y = df[target_col].fillna(0)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    
    engine = ModelComparisonEngine(**kwargs)
    return engine.run_comparison(X_train, y_train, X_test, y_test)
