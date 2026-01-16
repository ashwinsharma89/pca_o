"""
Model Selection, Error Handling, and Output Delivery

Features:
- Intelligent model selection with tiebreakers
- Comprehensive error handling
- Excel dashboard export
- Structured API response
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger
from pathlib import Path
import json
from datetime import datetime

try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False


# =============================================================================
# 5.3 MODEL SELECTION LOGIC
# =============================================================================

@dataclass
class ModelSelectionConfig:
    """Configuration for model selection."""
    primary_metric: str = 'r2_test'
    tiebreaker_1: str = 'rmse_test'
    tiebreaker_2: str = 'training_time'
    r2_tolerance: float = 0.02  # 2% tolerance for tiebreaker
    prefer_interpretability: bool = False
    interpretable_models: List[str] = field(default_factory=lambda: [
        'OLS', 'Ridge', 'Lasso', 'Elastic Net', 'Bayesian'
    ])


class ModelSelector:
    """
    Intelligent model selection with configurable logic.
    
    Selection Order:
    1. Primary: Highest test R²
    2. Tiebreaker 1: Lowest RMSE (if R² within tolerance)
    3. Tiebreaker 2: Lowest training time (if still tied)
    4. Override: Prefer interpretable model if configured
    """
    
    def __init__(self, config: Optional[ModelSelectionConfig] = None):
        self.config = config or ModelSelectionConfig()
    
    def select_best(
        self,
        model_results: Dict[str, Any],
        prefer_interpretability: Optional[bool] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Select best model from results.
        
        Returns:
            Tuple of (best_model_name, selection_reasoning)
        """
        if not model_results:
            return 'None', {'reason': 'No models available'}
        
        prefer_interp = prefer_interpretability if prefer_interpretability is not None else self.config.prefer_interpretability
        
        # Extract metrics
        metrics = []
        for name, result in model_results.items():
            metrics.append({
                'name': name,
                'r2': getattr(result, 'r2_test', 0),
                'rmse': getattr(result, 'rmse_test', float('inf')),
                'time': getattr(result, 'training_time', 0),
                'interpretable': name in self.config.interpretable_models
            })
        
        # Sort by primary metric (R²) descending
        metrics.sort(key=lambda x: x['r2'], reverse=True)
        
        # Apply interpretability override
        if prefer_interp:
            interpretable = [m for m in metrics if m['interpretable']]
            if interpretable:
                best_interp = interpretable[0]
                best_overall = metrics[0]
                
                # Only override if interpretable model is reasonably close
                if best_overall['r2'] - best_interp['r2'] <= self.config.r2_tolerance * 2:
                    return best_interp['name'], {
                        'reason': 'Interpretability preferred',
                        'selected_r2': best_interp['r2'],
                        'best_r2': best_overall['r2'],
                        'r2_trade_off': best_overall['r2'] - best_interp['r2']
                    }
        
        # Check for ties within tolerance
        best = metrics[0]
        tied = [m for m in metrics if best['r2'] - m['r2'] <= self.config.r2_tolerance]
        
        if len(tied) > 1:
            # Tiebreaker 1: Lowest RMSE
            tied.sort(key=lambda x: x['rmse'])
            
            # Check for still-tied
            rmse_tied = [m for m in tied if m['rmse'] == tied[0]['rmse']]
            
            if len(rmse_tied) > 1:
                # Tiebreaker 2: Lowest training time
                rmse_tied.sort(key=lambda x: x['time'])
                best = rmse_tied[0]
                reason = 'Selected by training time (tiebreaker 2)'
            else:
                best = tied[0]
                reason = 'Selected by RMSE (tiebreaker 1)'
        else:
            reason = 'Highest R² (primary metric)'
        
        return best['name'], {
            'reason': reason,
            'r2': best['r2'],
            'rmse': best['rmse'],
            'time': best['time']
        }


# =============================================================================
# 5.4 ERROR HANDLING
# =============================================================================

@dataclass
class ErrorHandlingConfig:
    """Configuration for error handling."""
    min_rows: int = 1000
    max_vif: float = 100.0
    min_r2: float = 0.2
    max_error_pct: float = 0.30  # 30% rows with large errors


class PipelineErrorHandler:
    """
    Handles errors and edge cases in the regression pipeline.
    """
    
    ERROR_TYPES = {
        'insufficient_data': 'Insufficient data rows after cleaning',
        'perfect_multicollinearity': 'Perfect multicollinearity detected',
        'convergence_failure': 'Model failed to converge',
        'poor_fit': 'Model has poor predictive power',
        'prediction_errors': 'Too many large prediction errors'
    }
    
    def __init__(self, config: Optional[ErrorHandlingConfig] = None):
        self.config = config or ErrorHandlingConfig()
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
    
    def check_data_sufficiency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check if data is sufficient for regression."""
        n_rows = len(df)
        
        if n_rows < self.config.min_rows:
            self.errors.append({
                'type': 'insufficient_data',
                'message': f"Only {n_rows} rows (min: {self.config.min_rows})",
                'action': 'Return descriptive stats only'
            })
            return {
                'passed': False,
                'rows': n_rows,
                'min_required': self.config.min_rows,
                'action': 'descriptive_stats_only'
            }
        
        return {'passed': True, 'rows': n_rows}
    
    def check_multicollinearity(
        self,
        vif_scores: Dict[str, float]
    ) -> Tuple[bool, List[str]]:
        """Check for perfect multicollinearity."""
        problematic = []
        
        for feature, vif in vif_scores.items():
            if vif > self.config.max_vif:
                problematic.append(feature)
                self.errors.append({
                    'type': 'perfect_multicollinearity',
                    'message': f"VIF({feature}) = {vif:.1f} > {self.config.max_vif}",
                    'action': f"Drop '{feature}' and rerun"
                })
        
        return len(problematic) == 0, problematic
    
    def check_model_fit(self, r2: float) -> Dict[str, Any]:
        """Check if model has acceptable fit."""
        if r2 < self.config.min_r2:
            self.warnings.append({
                'type': 'poor_fit',
                'message': f"R² = {r2:.3f} < {self.config.min_r2}",
                'action': 'Add more features or try non-linear models'
            })
            return {
                'passed': False,
                'r2': r2,
                'action': 'recommend_more_features'
            }
        
        return {'passed': True, 'r2': r2}
    
    def check_prediction_errors(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        threshold: float = 0.5  # 50% error
    ) -> Dict[str, Any]:
        """Check for excessive prediction errors."""
        # Calculate relative errors
        non_zero = y_true != 0
        rel_errors = np.zeros(len(y_true))
        rel_errors[non_zero] = np.abs((y_true[non_zero] - y_pred[non_zero]) / y_true[non_zero])
        
        large_error_pct = (rel_errors > threshold).mean()
        
        if large_error_pct > self.config.max_error_pct:
            self.warnings.append({
                'type': 'prediction_errors',
                'message': f"{large_error_pct*100:.1f}% rows have > {threshold*100}% error",
                'action': 'Review outliers and feature scaling'
            })
            return {
                'passed': False,
                'large_error_pct': large_error_pct,
                'action': 'review_outliers'
            }
        
        return {'passed': True, 'large_error_pct': large_error_pct}
    
    def get_summary(self) -> Dict[str, Any]:
        """Get error handling summary."""
        return {
            'errors': self.errors,
            'warnings': self.warnings,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'can_proceed': len(self.errors) == 0
        }


# =============================================================================
# 5.6 OUTPUT DELIVERY
# =============================================================================

class ExcelDashboardExporter:
    """
    Export regression results to Excel dashboard.
    
    Tabs:
    1. Model Comparison
    2. Feature Importance
    3. Predictions with Actuals
    4. Underperforming Campaigns
    """
    
    def __init__(self):
        if not EXCEL_AVAILABLE:
            logger.warning("openpyxl not installed. Excel export disabled.")
    
    def export(
        self,
        pipeline_result: Any,
        insights: Dict[str, Any],
        output_path: str
    ) -> bool:
        """Export results to Excel."""
        if not EXCEL_AVAILABLE:
            logger.error("Cannot export: openpyxl not installed")
            return False
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Tab 1: Model Comparison
                if hasattr(pipeline_result, 'model_comparison'):
                    pipeline_result.model_comparison.to_excel(
                        writer, sheet_name='Model Comparison', index=False
                    )
                
                # Tab 2: Feature Importance
                if hasattr(pipeline_result, 'feature_importance'):
                    pipeline_result.feature_importance.to_excel(
                        writer, sheet_name='Feature Importance', index=False
                    )
                
                # Tab 3: Predictions
                if hasattr(pipeline_result, 'predictions_sample'):
                    pipeline_result.predictions_sample.to_excel(
                        writer, sheet_name='Predictions', index=False
                    )
                
                # Tab 4: Insights Summary
                insights_df = pd.DataFrame([
                    {'Category': 'Executive Summary', 'Content': insights.get('executive_summary', '')},
                    *[{'Category': f'Recommendation {i+1}', 'Content': rec} 
                      for i, rec in enumerate(insights.get('recommendations', []))],
                    {'Category': 'Platform Insights', 'Content': insights.get('platform_insights', '')}
                ])
                insights_df.to_excel(writer, sheet_name='Insights', index=False)
            
            logger.info(f"Excel dashboard exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Excel export failed: {e}")
            return False


class APIResponseBuilder:
    """
    Build structured API response for downstream systems.
    """
    
    @staticmethod
    def build(
        pipeline_result: Any,
        insights: Dict[str, Any],
        include_predictions: bool = True
    ) -> Dict[str, Any]:
        """Build complete API response."""
        
        response = {
            'success': pipeline_result.success,
            'timestamp': datetime.now().isoformat(),
            
            # Model metadata
            'model': {
                'type': pipeline_result.best_model,
                'r2': pipeline_result.r2_test,
                'rmse': pipeline_result.rmse_test,
                'target': pipeline_result.target_col,
                'target_type': pipeline_result.target_type,
                'validation_passed': pipeline_result.validation_passed
            },
            
            # Feature importance
            'features': {
                'top_drivers': pipeline_result.top_drivers,
                'coefficients': pipeline_result.coefficients
            },
            
            # Business insights
            'insights': {
                'executive_summary': insights.get('executive_summary', ''),
                'recommendations': insights.get('recommendations', []),
                'platform_insights': insights.get('platform_insights'),
                'driver_insights': insights.get('driver_insights', [])
            },
            
            # Warnings
            'warnings': pipeline_result.validation_warnings
        }
        
        # Include predictions if requested
        if include_predictions and hasattr(pipeline_result, 'predictions_sample'):
            predictions_df = pipeline_result.predictions_sample
            if not predictions_df.empty:
                response['predictions'] = predictions_df.to_dict(orient='records')
        
        # Platform comparison
        if pipeline_result.platform_comparison:
            response['platform_comparison'] = pipeline_result.platform_comparison
        
        return response
    
    @staticmethod
    def to_json(response: Dict, indent: int = 2) -> str:
        """Convert response to JSON string."""
        return json.dumps(response, indent=indent, default=str)


# =============================================================================
# INTEGRATED PIPELINE ENHANCEMENTS
# =============================================================================

class EnhancedRegressionPipeline:
    """
    Enhanced pipeline with model selection, error handling, and output delivery.
    """
    
    def __init__(
        self,
        prefer_interpretability: bool = False,
        min_rows: int = 1000,
        quick_mode: bool = False
    ):
        self.selector = ModelSelector(ModelSelectionConfig(
            prefer_interpretability=prefer_interpretability
        ))
        self.error_handler = PipelineErrorHandler(ErrorHandlingConfig(
            min_rows=min_rows
        ))
        self.excel_exporter = ExcelDashboardExporter()
        self.quick_mode = quick_mode
    
    def run_with_outputs(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str],
        output_excel: Optional[str] = None,
        **kwargs
    ) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
        """
        Run pipeline with full outputs.
        
        Returns:
            Tuple of (pipeline_result, insights, api_response)
        """
        # Import here to avoid circular imports
        from src.engine.analytics.regression_pipeline import (
            RegressionPipeline, InsightAgent
        )
        
        # Check data sufficiency
        data_check = self.error_handler.check_data_sufficiency(df)
        if not data_check['passed']:
            # Return descriptive stats only
            return self._descriptive_stats_only(df, target_col, feature_cols)
        
        # Run main pipeline
        pipeline = RegressionPipeline(quick_mode=self.quick_mode)
        result = pipeline.run(df, target_col, feature_cols, **kwargs)
        
        # Check model fit
        if result.success:
            self.error_handler.check_model_fit(result.r2_test)
        
        # Generate insights
        agent = InsightAgent()
        insights = agent.generate_insights(result)
        
        # Build API response
        api_response = APIResponseBuilder.build(result, insights)
        
        # Add error handler summary
        api_response['error_handling'] = self.error_handler.get_summary()
        
        # Export Excel if requested
        if output_excel:
            self.excel_exporter.export(result, insights, output_excel)
        
        return result, insights, api_response
    
    def _descriptive_stats_only(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str]
    ) -> Tuple[None, Dict, Dict]:
        """Return descriptive stats when regression not possible."""
        stats = df[feature_cols + [target_col]].describe().to_dict()
        
        insights = {
            'executive_summary': f"Insufficient data for regression ({len(df)} rows). Showing descriptive statistics only.",
            'recommendations': ['Collect more data before running regression analysis'],
            'driver_insights': [],
            'platform_insights': None
        }
        
        api_response = {
            'success': False,
            'error': 'insufficient_data',
            'message': f"Need at least 1000 rows, got {len(df)}",
            'descriptive_stats': stats,
            'insights': insights
        }
        
        return None, insights, api_response
