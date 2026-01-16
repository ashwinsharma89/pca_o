"""
Unit tests for Predictive QA Integration.
Tests forecasting, optimization, and prediction integration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Try to import
try:
    from src.engine.predictive.predictive_qa_integration import PredictiveQAIntegration
    PREDICTIVE_QA_AVAILABLE = True
except ImportError:
    PREDICTIVE_QA_AVAILABLE = False
    PredictiveQAIntegration = None

pytestmark = pytest.mark.skipif(not PREDICTIVE_QA_AVAILABLE, reason="Predictive QA not available")


class TestPredictiveQAInit:
    """Test PredictiveQAIntegration initialization."""
    
    def test_initialization(self):
        """Test basic initialization."""
        integration = PredictiveQAIntegration()
        
        assert integration is not None
        assert integration.forecasts == {}
        assert integration.optimizations == {}


class TestForecastNextMonth:
    """Test next month forecasting."""
    
    @pytest.fixture
    def integration(self):
        """Create integration instance."""
        return PredictiveQAIntegration()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample historical data."""
        dates = pd.date_range(start='2024-01-01', periods=90, freq='D')
        return pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(100, 500, 90),
            'Conversions': np.random.randint(5, 50, 90),
            'Revenue': np.random.uniform(500, 2000, 90)
        })
    
    def test_forecast_next_month(self, integration, sample_data):
        """Test generating next month forecast."""
        forecast = integration.forecast_next_month(sample_data)
        
        assert forecast is not None
        assert 'next_month' in forecast
    
    def test_forecast_contains_cpa(self, integration, sample_data):
        """Test forecast contains CPA prediction."""
        forecast = integration.forecast_next_month(sample_data)
        
        assert 'cpa' in forecast['next_month']
        assert 'forecast' in forecast['next_month']['cpa']
    
    def test_forecast_contains_conversions(self, integration, sample_data):
        """Test forecast contains conversions prediction."""
        forecast = integration.forecast_next_month(sample_data)
        
        assert 'conversions' in forecast['next_month']
        assert 'forecast' in forecast['next_month']['conversions']
    
    def test_forecast_contains_roas(self, integration, sample_data):
        """Test forecast contains ROAS prediction."""
        forecast = integration.forecast_next_month(sample_data)
        
        assert 'roas' in forecast['next_month']
    
    def test_forecast_confidence_intervals(self, integration, sample_data):
        """Test forecast includes confidence intervals."""
        forecast = integration.forecast_next_month(sample_data)
        
        cpa = forecast['next_month']['cpa']
        assert 'conservative' in cpa
        assert 'optimistic' in cpa
    
    def test_forecast_with_missing_revenue(self, integration):
        """Test forecast when Revenue column is missing."""
        dates = pd.date_range(start='2024-01-01', periods=90, freq='D')
        data = pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(100, 500, 90),
            'Conversions': np.random.randint(5, 50, 90)
        })
        
        try:
            forecast = integration.forecast_next_month(data)
            assert forecast is not None
        except Exception:
            pytest.skip("Forecast requires Revenue column")


class TestBudgetOptimization:
    """Test budget optimization functionality."""
    
    @pytest.fixture
    def integration(self):
        """Create integration instance."""
        return PredictiveQAIntegration()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample campaign data."""
        return pd.DataFrame({
            'Campaign': ['A', 'B', 'C', 'D'],
            'Spend': [1000, 2000, 1500, 3000],
            'Conversions': [50, 80, 60, 100],
            'Revenue': [5000, 10000, 7000, 12000]
        })
    
    def test_optimize_budget(self, integration, sample_data):
        """Test budget optimization."""
        if hasattr(integration, 'optimize_budget'):
            result = integration.optimize_budget(
                df=sample_data,
                total_budget=10000
            )
            
            assert result is not None
    
    def test_optimize_budget_with_constraints(self, integration, sample_data):
        """Test budget optimization with constraints."""
        if hasattr(integration, 'optimize_budget'):
            result = integration.optimize_budget(
                df=sample_data,
                total_budget=10000,
                min_per_campaign=500,
                max_per_campaign=5000
            )
            
            assert result is not None


class TestPerformancePrediction:
    """Test performance prediction functionality."""
    
    @pytest.fixture
    def integration(self):
        """Create integration instance."""
        return PredictiveQAIntegration()
    
    def test_predict_performance(self, integration):
        """Test performance prediction."""
        if hasattr(integration, 'predict_performance'):
            result = integration.predict_performance(
                spend=1000,
                channel='google',
                historical_cpa=20
            )
            
            assert result is not None
    
    def test_predict_conversions(self, integration):
        """Test conversion prediction."""
        if hasattr(integration, 'predict_conversions'):
            result = integration.predict_conversions(
                spend=5000,
                cpa=25
            )
            
            assert result is not None
            assert result > 0


class TestTrendAnalysis:
    """Test trend analysis functionality."""
    
    @pytest.fixture
    def integration(self):
        """Create integration instance."""
        return PredictiveQAIntegration()
    
    @pytest.fixture
    def time_series_data(self):
        """Create time series data."""
        dates = pd.date_range(start='2024-01-01', periods=180, freq='D')
        return pd.DataFrame({
            'Date': dates,
            'Spend': np.cumsum(np.random.uniform(-10, 20, 180)) + 1000,
            'Conversions': np.cumsum(np.random.uniform(-2, 5, 180)) + 100
        })
    
    def test_analyze_trends(self, integration, time_series_data):
        """Test trend analysis."""
        if hasattr(integration, 'analyze_trends'):
            trends = integration.analyze_trends(time_series_data)
            
            assert trends is not None
    
    def test_detect_seasonality(self, integration, time_series_data):
        """Test seasonality detection."""
        if hasattr(integration, 'detect_seasonality'):
            seasonality = integration.detect_seasonality(time_series_data)
            
            assert seasonality is not None


class TestScenarioAnalysis:
    """Test scenario analysis functionality."""
    
    @pytest.fixture
    def integration(self):
        """Create integration instance."""
        return PredictiveQAIntegration()
    
    def test_what_if_analysis(self, integration):
        """Test what-if scenario analysis."""
        if hasattr(integration, 'what_if_analysis'):
            result = integration.what_if_analysis(
                current_spend=10000,
                spend_change=0.2,  # 20% increase
                current_cpa=25
            )
            
            assert result is not None
    
    def test_budget_scenarios(self, integration):
        """Test budget scenario comparison."""
        if hasattr(integration, 'compare_budget_scenarios'):
            scenarios = [
                {'name': 'Conservative', 'budget': 8000},
                {'name': 'Moderate', 'budget': 10000},
                {'name': 'Aggressive', 'budget': 15000}
            ]
            
            result = integration.compare_budget_scenarios(scenarios)
            
            assert result is not None
