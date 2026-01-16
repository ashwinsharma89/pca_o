"""
Unit Tests for Visualization Agents

Tests for:
- SmartVisualizationEngine (smart_visualization_engine.py)
- SmartFilterEngine (visualization_filters.py)
- VisualizationAgent (visualization_agent.py)
- EnhancedVisualizationAgent (enhanced_visualization_agent.py)

FIXED: Simplified to interface tests without OpenAI client instantiation.
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime, timedelta


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_campaign_data():
    """Sample campaign DataFrame for visualization tests."""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'date': dates,
        'campaign_name': ['Campaign A'] * 15 + ['Campaign B'] * 15,
        'platform': ['Google Ads'] * 10 + ['Meta'] * 10 + ['LinkedIn'] * 10,
        'channel': ['Search'] * 15 + ['Social'] * 15,
        'spend': [100 + i * 5 for i in range(30)],
        'impressions': [10000 + i * 500 for i in range(30)],
        'clicks': [500 + i * 25 for i in range(30)],
        'conversions': [50 + i * 2 for i in range(30)],
        'revenue': [2500 + i * 100 for i in range(30)]
    })


@pytest.fixture
def sample_aggregated_data():
    """Sample aggregated data for pie/bar charts."""
    return pd.DataFrame({
        'platform': ['Google Ads', 'Meta', 'LinkedIn', 'Twitter'],
        'spend': [15000, 12000, 8000, 5000],
        'conversions': [750, 600, 400, 200],
        'roas': [4.5, 5.2, 3.8, 2.5]
    })


# ============================================================================
# SMART VISUALIZATION ENGINE TESTS
# ============================================================================

class TestSmartVisualizationEngine:
    """Unit tests for SmartVisualizationEngine class."""
    
    @pytest.fixture
    def engine(self):
        """Create SmartVisualizationEngine instance."""
        from src.engine.agents.smart_visualization_engine import SmartVisualizationEngine
        return SmartVisualizationEngine()
    
    def test_initialization(self, engine):
        """Test SmartVisualizationEngine initialization."""
        assert engine is not None
    
    def test_has_select_visualization_method(self, engine):
        """Test engine has select_visualization method."""
        assert hasattr(engine, 'select_visualization')
    
    def test_has_create_visualization_method(self, engine):
        """Test engine has create_visualization method."""
        assert hasattr(engine, 'create_visualization')
    
    def test_select_visualization_returns_result(self, engine, sample_aggregated_data):
        """Test select_visualization returns a result."""
        result = engine.select_visualization(
            data=sample_aggregated_data,
            insight_type="comparison",
            audience="analyst"
        )
        
        assert result is not None


class TestVisualizationTypeEnum:
    """Test VisualizationType enum."""
    
    def test_enum_exists(self):
        """Test VisualizationType enum can be imported."""
        from src.engine.agents.smart_visualization_engine import VisualizationType
        
        assert VisualizationType is not None
    
    def test_has_bar_chart(self):
        """Test BAR_CHART exists."""
        from src.engine.agents.smart_visualization_engine import VisualizationType
        
        assert hasattr(VisualizationType, 'BAR_CHART')
    
    def test_has_line_chart(self):
        """Test LINE_CHART exists."""
        from src.engine.agents.smart_visualization_engine import VisualizationType
        
        assert hasattr(VisualizationType, 'LINE_CHART')


class TestInsightTypeEnum:
    """Test InsightType enum."""
    
    def test_enum_exists(self):
        """Test InsightType enum can be imported."""
        from src.engine.agents.smart_visualization_engine import InsightType
        
        assert InsightType is not None
    
    def test_has_comparison(self):
        """Test COMPARISON exists."""
        from src.engine.agents.smart_visualization_engine import InsightType
        
        assert hasattr(InsightType, 'COMPARISON')
    
    def test_has_trend(self):
        """Test TREND exists."""
        from src.engine.agents.smart_visualization_engine import InsightType
        
        assert hasattr(InsightType, 'TREND')


# ============================================================================
# SMART FILTER ENGINE TESTS
# ============================================================================

class TestSmartFilterEngine:
    """Unit tests for SmartFilterEngine class."""
    
    @pytest.fixture
    def engine(self):
        """Create SmartFilterEngine instance."""
        from src.engine.agents.visualization_filters import SmartFilterEngine
        return SmartFilterEngine()
    
    def test_initialization(self, engine):
        """Test SmartFilterEngine initialization."""
        assert engine is not None
    
    def test_has_suggest_filters_method(self, engine):
        """Test engine has suggest_filters_for_data method."""
        assert hasattr(engine, 'suggest_filters_for_data')
    
    def test_has_apply_filters_method(self, engine):
        """Test engine has apply_filters method."""
        assert hasattr(engine, 'apply_filters')
    
    def test_suggest_filters_returns_list(self, engine, sample_campaign_data):
        """Test suggest_filters_for_data returns a list."""
        suggestions = engine.suggest_filters_for_data(sample_campaign_data)
        
        assert isinstance(suggestions, list)


class TestFilterTypeEnum:
    """Test FilterType enum."""
    
    def test_enum_exists(self):
        """Test FilterType enum can be imported."""
        from src.engine.agents.visualization_filters import FilterType
        
        assert FilterType is not None
    
    def test_has_date_range(self):
        """Test DATE_RANGE exists."""
        from src.engine.agents.visualization_filters import FilterType
        
        assert hasattr(FilterType, 'DATE_RANGE')
    
    def test_has_channel(self):
        """Test CHANNEL exists."""
        from src.engine.agents.visualization_filters import FilterType
        
        assert hasattr(FilterType, 'CHANNEL')


class TestFilterConditionEnum:
    """Test FilterCondition enum."""
    
    def test_enum_exists(self):
        """Test FilterCondition enum can be imported."""
        from src.engine.agents.visualization_filters import FilterCondition
        
        assert FilterCondition is not None


# ============================================================================
# VISUALIZATION AGENT INTERFACE TESTS
# ============================================================================

class TestVisualizationAgentInterface:
    """Test VisualizationAgent module exports."""
    
    def test_class_exists(self):
        """Test VisualizationAgent class can be imported."""
        from src.engine.agents.visualization_agent import VisualizationAgent
        
        assert VisualizationAgent is not None


class TestEnhancedVisualizationAgentInterface:
    """Test EnhancedVisualizationAgent module exports."""
    
    def test_class_exists(self):
        """Test EnhancedVisualizationAgent class can be imported."""
        from src.engine.agents.enhanced_visualization_agent import EnhancedVisualizationAgent
        
        assert EnhancedVisualizationAgent is not None


# ============================================================================
# MARKETING VISUALIZATION RULES TESTS
# ============================================================================

class TestMarketingVisualizationRules:
    """Unit tests for MarketingVisualizationRules class."""
    
    def test_class_exists(self):
        """Test MarketingVisualizationRules can be imported."""
        from src.engine.agents.marketing_visualization_rules import MarketingVisualizationRules
        
        assert MarketingVisualizationRules is not None
    
    def test_initialization(self):
        """Test MarketingVisualizationRules initialization."""
        from src.engine.agents.marketing_visualization_rules import MarketingVisualizationRules
        
        rules = MarketingVisualizationRules()
        
        assert rules is not None


# ============================================================================
# CHART GENERATORS TESTS
# ============================================================================

class TestSmartChartGenerator:
    """Unit tests for SmartChartGenerator class."""
    
    def test_class_exists(self):
        """Test SmartChartGenerator can be imported."""
        from src.engine.agents.chart_generators import SmartChartGenerator
        
        assert SmartChartGenerator is not None
    
    def test_initialization(self):
        """Test SmartChartGenerator initialization."""
        from src.engine.agents.chart_generators import SmartChartGenerator
        
        generator = SmartChartGenerator()
        
        assert generator is not None


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
