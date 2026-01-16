"""
Regression Module - Production ML for Marketing Mix Modeling

Clean, modular architecture replacing 5000+ lines of scattered code.

Components:
- ModelTrainer: Ridge, Random Forest, XGBoost (opinionated selection)
- ModelEvaluator: MAE, RMSE, MAPE, SMAPE, R², residuals
- FeatureAnalyzer: VIF, correlation, coverage analysis
- CoefficientExplainer: Stakeholder-friendly translations
- Pipeline: Unified orchestrator

Usage:
    from src.engine.analytics.regression import RegressionPipeline
    
    pipeline = RegressionPipeline(quick_mode=True)
    result = pipeline.run(df, target='conversions', features=['spend', 'impressions'])
    
    print(result.executive_summary)
    print(result.metrics.to_dict())
    print(result.vif_analysis)

Author: Senior ML Expert (15 years Google/Meta Ads experience)
"""

from .pipeline import RegressionPipeline, RegressionResult
from .model_trainer import ModelTrainer, TrainedModel
from .model_evaluator import ModelEvaluator, ModelMetrics, ResidualDiagnostics
from .feature_analyzer import FeatureAnalyzer
from .coefficient_explainer import CoefficientExplainer, CoefficientInsight

__all__ = [
    'RegressionPipeline',
    'RegressionResult',
    'ModelTrainer',
    'TrainedModel',
    'ModelEvaluator',
    'ModelMetrics',
    'ResidualDiagnostics',
    'FeatureAnalyzer',
    'CoefficientExplainer',
    'CoefficientInsight',
]
