"""
Tests for EnhancedReasoningAgent and PatternDetector (Phase C.1).
Verifies detection of trends, anomalies, fatigue, and pacing.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent, PatternDetector

@pytest.fixture
def base_data():
    """Create a base dataframe with 14 days of stable performance."""
    dates = [datetime.now() - timedelta(days=i) for i in range(14)]
    return pd.DataFrame({
        'Date': sorted(dates),
        'Spend': [100.0] * 14,
        'Impressions': [1000] * 14,
        'Clicks': [50] * 14,
        'Conversions': [5] * 14,
        'CTR': [0.05] * 14,
        'CPC': [2.0] * 14,
        'Campaign': ['Test Campaign'] * 14,
        'Platform': ['Google Search'] * 14
    })

class TestPatternDetector:
    """Unit tests for PatternDetector branch coverage."""

    def test_detect_trends_improving(self, base_data):
        """Verify detection of improving trends."""
        detector = PatternDetector()
        data = base_data.copy()
        # Improving CTR: 0.05 -> 0.10
        data['CTR'] = np.linspace(0.05, 0.1, 14)
        # Improving CPC: 2.0 -> 1.0 (declining slope is improvement for CPC)
        data['CPC'] = np.linspace(2.0, 1.0, 14)
        
        result = detector._detect_trends(data)
        assert result['detected'] is True
        assert result['direction'] == 'improving'

    def test_detect_anomalies_high(self, base_data):
        """Verify detection of high-severity anomalies."""
        detector = PatternDetector()
        data = base_data.copy()
        # Single massive spike in Spend
        data.loc[7, 'Spend'] = 5000.0
        
        result = detector._detect_anomalies(data)
        assert result['detected'] is True
        assert any(a['metric'] == 'Spend' for a in result['anomalies'])

    def test_detect_creative_fatigue(self, base_data):
        """Verify detection of creative fatigue (declining CTR + high frequency)."""
        detector = PatternDetector()
        data = base_data.copy()
        data['Frequency'] = 8.0 # Above threshold 7
        # Declining CTR: 0.10 -> 0.05
        data['CTR'] = np.linspace(0.1, 0.05, 14)
        
        result = detector._detect_creative_fatigue(data)
        assert result['detected'] is True
        assert result['severity'] == 'high'

    def test_analyze_budget_pacing_accelerating(self, base_data):
        """Verify detection of accelerating spend velocity."""
        detector = PatternDetector()
        data = base_data.copy()
        # Accelerating spend: 100 -> 500
        # avg = 300, 10% = 30. Slope = 400/13 = 30.7 > 30.
        data['Spend'] = np.linspace(100, 500, 14)
        
        result = detector._analyze_budget_pacing(data)
        assert result['detected'] is True
        assert result['status'] == 'accelerating'

class TestEnhancedReasoningAgent:
    """Unit tests for EnhancedReasoningAgent orchestration."""

    def test_analyze_full_flow(self, base_data):
        """Verify full analysis orchestration with insights and recommendations."""
        agent = EnhancedReasoningAgent()
        # Create data with multiple issues
        data = base_data.copy()
        data['Frequency'] = 9.0
        data['CTR'] = np.linspace(0.1, 0.02, 14) # Sharp decline
        
        result = agent.analyze(data)
        
        assert 'insights' in result
        assert 'patterns' in result
        assert 'recommendations' in result
        
        # Check specific fatigue recommendation
        recs = result['recommendations']
        assert any(r['category'] == 'Creative' for r in recs)

    def test_detect_platform_logic(self):
        """Verify platform detection from dataframe columns."""
        agent = EnhancedReasoningAgent()
        df = pd.DataFrame({'Platform': ['LinkedIn Ads'], 'Spend': [100]})
        assert agent._detect_platform(df) == 'linkedin'
        
        df2 = pd.DataFrame({'Platform': ['Facebook Business'], 'Spend': [100]})
        assert agent._detect_platform(df2) == 'meta'
