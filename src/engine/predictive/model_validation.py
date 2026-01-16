"""
Model Validation Module for Digital Marketing ML Models
Provides comprehensive validation metrics, cross-validation, and feature importance analysis
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from sklearn.model_selection import cross_val_score, KFold, StratifiedKFold
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, mean_absolute_percentage_error
)
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Comprehensive model validation report"""
    model_name: str
    model_type: str  # 'regression' or 'classification'
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # General metrics
    sample_size: int = 0
    train_size: int = 0
    test_size: int = 0
    
    # Regression metrics
    r2: Optional[float] = None
    mae: Optional[float] = None
    rmse: Optional[float] = None
    mape: Optional[float] = None
    
    # Classification metrics
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1: Optional[float] = None
    auc_roc: Optional[float] = None
    
    # Cross-validation
    cv_scores: List[float] = field(default_factory=list)
    cv_mean: Optional[float] = None
    cv_std: Optional[float] = None
    cv_confidence_interval: Tuple[float, float] = (0.0, 0.0)
    
    # Feature importance
    feature_importance: Dict[str, float] = field(default_factory=dict)
    
    # Marketing-specific
    prediction_bias: Optional[float] = None  # Over/under prediction tendency
    within_10pct_accuracy: Optional[float] = None  # % predictions within 10% of actual
    
    def to_dict(self) -> Dict:
        return {
            "model_name": self.model_name,
            "model_type": self.model_type,
            "timestamp": self.timestamp.isoformat(),
            "sample_size": self.sample_size,
            "train_size": self.train_size,
            "test_size": self.test_size,
            "metrics": self._get_metrics_dict(),
            "cross_validation": {
                "cv_mean": self.cv_mean,
                "cv_std": self.cv_std,
                "cv_confidence_interval": self.cv_confidence_interval,
                "cv_scores": self.cv_scores
            },
            "feature_importance": self.feature_importance,
            "marketing_metrics": {
                "prediction_bias": self.prediction_bias,
                "within_10pct_accuracy": self.within_10pct_accuracy
            }
        }
    
    def _get_metrics_dict(self) -> Dict:
        if self.model_type == 'regression':
            return {"r2": self.r2, "mae": self.mae, "rmse": self.rmse, "mape": self.mape}
        else:
            return {
                "accuracy": self.accuracy, "precision": self.precision,
                "recall": self.recall, "f1": self.f1, "auc_roc": self.auc_roc
            }


class ModelValidator:
    """
    Validates ML models with digital marketing-specific metrics
    
    Supports:
    - Regression models (ROAS prediction, spend forecasting)
    - Classification models (success prediction, anomaly detection)
    """
    
    MARKETING_METRICS = {
        'spend': {'direction': 'lower_better', 'tolerance': 0.1},
        'roas': {'direction': 'higher_better', 'tolerance': 0.15},
        'cpa': {'direction': 'lower_better', 'tolerance': 0.1},
        'conversions': {'direction': 'higher_better', 'tolerance': 0.1},
        'ctr': {'direction': 'higher_better', 'tolerance': 0.2}
    }
    
    def __init__(self, model, model_name: str = "unnamed_model"):
        """
        Initialize validator with a trained model
        
        Args:
            model: Trained sklearn-compatible model
            model_name: Human-readable model name
        """
        self.model = model
        self.model_name = model_name
        self._is_classifier = hasattr(model, 'predict_proba')
    
    def validate_regression(
        self,
        X_test: np.ndarray,
        y_test: np.ndarray,
        X_train: Optional[np.ndarray] = None,
        y_train: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
        n_cv_folds: int = 5
    ) -> ValidationReport:
        """
        Validate a regression model with comprehensive metrics
        
        Args:
            X_test: Test features
            y_test: Test target values
            X_train: Training features (for cross-validation)
            y_train: Training target values
            feature_names: Names of features for importance reporting
            n_cv_folds: Number of cross-validation folds
            
        Returns:
            ValidationReport with all metrics
        """
        report = ValidationReport(
            model_name=self.model_name,
            model_type='regression',
            test_size=len(y_test),
            train_size=len(y_train) if y_train is not None else 0,
            sample_size=(len(y_test) + len(y_train)) if y_train is not None else len(y_test)
        )
        
        # Make predictions
        y_pred = self.model.predict(X_test)
        
        # Core regression metrics
        report.r2 = float(r2_score(y_test, y_pred))
        report.mae = float(mean_absolute_error(y_test, y_pred))
        report.rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        
        # MAPE - handle zeros
        try:
            non_zero_mask = y_test != 0
            if np.sum(non_zero_mask) > 0:
                report.mape = float(mean_absolute_percentage_error(
                    y_test[non_zero_mask], y_pred[non_zero_mask]
                ))
        except Exception:
            report.mape = None
        
        # Marketing-specific metrics
        report.prediction_bias = float(np.mean(y_pred - y_test))
        
        # Within 10% accuracy
        if len(y_test) > 0:
            relative_errors = np.abs(y_pred - y_test) / (np.abs(y_test) + 1e-10)
            report.within_10pct_accuracy = float(np.mean(relative_errors <= 0.1))
        
        # Cross-validation
        if X_train is not None and y_train is not None:
            X_combined = np.vstack([X_train, X_test])
            y_combined = np.concatenate([y_train, y_test])
            
            try:
                cv = KFold(n_splits=n_cv_folds, shuffle=True, random_state=42)
                cv_scores = cross_val_score(self.model, X_combined, y_combined, cv=cv, scoring='r2')
                report.cv_scores = cv_scores.tolist()
                report.cv_mean = float(np.mean(cv_scores))
                report.cv_std = float(np.std(cv_scores))
                
                # 95% confidence interval
                ci_margin = 1.96 * (report.cv_std / np.sqrt(n_cv_folds))
                report.cv_confidence_interval = (
                    report.cv_mean - ci_margin,
                    report.cv_mean + ci_margin
                )
            except Exception as e:
                logger.warning(f"Cross-validation failed: {e}")
        
        # Feature importance
        if hasattr(self.model, 'feature_importances_') and feature_names:
            importances = self.model.feature_importances_
            report.feature_importance = {
                name: float(imp) 
                for name, imp in sorted(
                    zip(feature_names, importances),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]  # Top 10
            }
        
        logger.info(f"Validation complete for {self.model_name}: R²={report.r2:.4f}, MAE={report.mae:.4f}")
        return report
    
    def validate_classification(
        self,
        X_test: np.ndarray,
        y_test: np.ndarray,
        X_train: Optional[np.ndarray] = None,
        y_train: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
        n_cv_folds: int = 5
    ) -> ValidationReport:
        """
        Validate a classification model with comprehensive metrics
        
        Args:
            X_test: Test features
            y_test: Test target values (binary: 0/1)
            X_train: Training features (for cross-validation)
            y_train: Training target values
            feature_names: Names of features for importance reporting
            n_cv_folds: Number of cross-validation folds
            
        Returns:
            ValidationReport with all metrics
        """
        report = ValidationReport(
            model_name=self.model_name,
            model_type='classification',
            test_size=len(y_test),
            train_size=len(y_train) if y_train is not None else 0,
            sample_size=(len(y_test) + len(y_train)) if y_train is not None else len(y_test)
        )
        
        # Make predictions
        y_pred = self.model.predict(X_test)
        
        # Core classification metrics
        report.accuracy = float(accuracy_score(y_test, y_pred))
        report.precision = float(precision_score(y_test, y_pred, zero_division=0))
        report.recall = float(recall_score(y_test, y_pred, zero_division=0))
        report.f1 = float(f1_score(y_test, y_pred, zero_division=0))
        
        # AUC-ROC (requires probability predictions)
        if hasattr(self.model, 'predict_proba'):
            try:
                y_proba = self.model.predict_proba(X_test)[:, 1]
                report.auc_roc = float(roc_auc_score(y_test, y_proba))
            except Exception:
                report.auc_roc = None
        
        # Cross-validation
        if X_train is not None and y_train is not None:
            X_combined = np.vstack([X_train, X_test])
            y_combined = np.concatenate([y_train, y_test])
            
            try:
                cv = StratifiedKFold(n_splits=n_cv_folds, shuffle=True, random_state=42)
                cv_scores = cross_val_score(self.model, X_combined, y_combined, cv=cv, scoring='accuracy')
                report.cv_scores = cv_scores.tolist()
                report.cv_mean = float(np.mean(cv_scores))
                report.cv_std = float(np.std(cv_scores))
                
                # 95% confidence interval
                ci_margin = 1.96 * (report.cv_std / np.sqrt(n_cv_folds))
                report.cv_confidence_interval = (
                    report.cv_mean - ci_margin,
                    report.cv_mean + ci_margin
                )
            except Exception as e:
                logger.warning(f"Cross-validation failed: {e}")
        
        # Feature importance
        if hasattr(self.model, 'feature_importances_') and feature_names:
            importances = self.model.feature_importances_
            report.feature_importance = {
                name: float(imp) 
                for name, imp in sorted(
                    zip(feature_names, importances),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]
            }
        
        logger.info(f"Validation complete for {self.model_name}: Accuracy={report.accuracy:.4f}, F1={report.f1:.4f}")
        return report
    
    def validate_marketing_forecast(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        metric_name: str = 'roas'
    ) -> Dict[str, Any]:
        """
        Validate a marketing metric forecast with domain-specific analysis
        
        Args:
            actual: Actual metric values
            predicted: Predicted metric values
            metric_name: Name of the metric being predicted
            
        Returns:
            Dict with marketing-specific validation results
        """
        tolerance = self.MARKETING_METRICS.get(metric_name, {}).get('tolerance', 0.1)
        direction = self.MARKETING_METRICS.get(metric_name, {}).get('direction', 'higher_better')
        
        errors = predicted - actual
        abs_errors = np.abs(errors)
        rel_errors = abs_errors / (np.abs(actual) + 1e-10)
        
        results = {
            "metric_name": metric_name,
            "sample_size": len(actual),
            "mae": float(np.mean(abs_errors)),
            "rmse": float(np.sqrt(np.mean(errors ** 2))),
            "mean_bias": float(np.mean(errors)),
            "median_bias": float(np.median(errors)),
            "within_tolerance": float(np.mean(rel_errors <= tolerance)),
            "over_predictions": float(np.mean(errors > 0)),
            "under_predictions": float(np.mean(errors < 0)),
            "directional_accuracy": None,
            "forecast_quality": "unknown"
        }
        
        # Directional accuracy (for time series)
        if len(actual) > 1:
            actual_direction = np.sign(np.diff(actual))
            predicted_direction = np.sign(np.diff(predicted))
            results["directional_accuracy"] = float(np.mean(actual_direction == predicted_direction))
        
        # Forecast quality rating
        within_tol = results["within_tolerance"]
        if within_tol >= 0.8:
            results["forecast_quality"] = "excellent"
        elif within_tol >= 0.6:
            results["forecast_quality"] = "good"
        elif within_tol >= 0.4:
            results["forecast_quality"] = "fair"
        else:
            results["forecast_quality"] = "poor"
        
        logger.info(f"Marketing forecast validation: {metric_name} - Quality: {results['forecast_quality']}")
        return results


def validate_campaign_predictor(
    predictor,
    test_data: pd.DataFrame,
    success_column: str = 'is_successful'
) -> ValidationReport:
    """
    Convenience function to validate the CampaignSuccessPredictor
    
    Args:
        predictor: Trained CampaignSuccessPredictor instance
        test_data: DataFrame with test campaigns
        success_column: Column name for success labels
        
    Returns:
        ValidationReport
    """
    if not hasattr(predictor, 'model') or predictor.model is None:
        raise ValueError("Predictor model is not trained")
    
    # Engineer features (using predictor's method)
    X_test = predictor._engineer_features(test_data)
    y_test = test_data[success_column].values
    
    validator = ModelValidator(predictor.model, model_name="CampaignSuccessPredictor")
    return validator.validate_classification(
        X_test=X_test.values if hasattr(X_test, 'values') else X_test,
        y_test=y_test,
        feature_names=list(X_test.columns) if hasattr(X_test, 'columns') else None
    )
