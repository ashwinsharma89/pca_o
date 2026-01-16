"""
Predictive Analytics Module for PCA Agent
Transform from retrospective reporting to forward-looking strategic planning
"""

from .early_performance_indicators import EarlyPerformanceIndicators
from .budget_optimizer import BudgetAllocationOptimizer
from .campaign_success_predictor import CampaignSuccessPredictor
from .model_validation import ModelValidator, ValidationReport
from .marketing_statistics import MarketingStatistics, CorrelationResult, ABTestResult
from .model_monitor import ModelMonitor, get_model_monitor

__all__ = [
    'EarlyPerformanceIndicators',
    'BudgetAllocationOptimizer',
    'CampaignSuccessPredictor',
    'ModelValidator',
    'ValidationReport',
    'MarketingStatistics',
    'CorrelationResult',
    'ABTestResult',
    'ModelMonitor',
    'get_model_monitor'
]

__version__ = '1.1.0'

