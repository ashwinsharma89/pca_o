"""
Test Fixtures for Agent Testing

Provides reusable test data and mock objects.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any
from unittest.mock import Mock, MagicMock


@pytest.fixture
def sample_campaign_data():
    """Sample campaign data for testing"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Campaign_A'] * 14,
        'Platform': ['Facebook'] * 14,
        'Spend': np.random.uniform(80, 120, 14),
        'Impressions': np.random.randint(8000, 12000, 14),
        'Clicks': np.random.randint(200, 400, 14),
        'Conversions': np.random.randint(10, 30, 14),
        'CTR': np.random.uniform(2.0, 3.5, 14),
        'CPC': np.random.uniform(0.30, 0.50, 14),
        'Frequency': np.random.uniform(3.0, 8.0, 14),
        'Reach': np.random.randint(2000, 4000, 14)
    })


@pytest.fixture
def campaign_data_with_creative_fatigue():
    """Campaign data showing creative fatigue pattern"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Declining CTR over time
    ctr_values = np.linspace(3.0, 1.8, 14)  # 40% decline
    
    # Increasing frequency
    frequency_values = np.linspace(4.0, 8.5, 14)
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Campaign_Fatigued'] * 14,
        'Platform': ['Facebook'] * 14,
        'Spend': [100] * 14,  # Stable spend
        'Impressions': [10000] * 14,  # Stable impressions
        'Clicks': (ctr_values / 100 * 10000).astype(int),
        'Conversions': np.random.randint(15, 25, 14),
        'CTR': ctr_values,
        'CPC': np.random.uniform(0.35, 0.45, 14),
        'Frequency': frequency_values,
        'Reach': [2500] * 14
    })


@pytest.fixture
def campaign_data_with_audience_saturation():
    """Campaign data showing audience saturation"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Declining reach
    reach_values = np.linspace(4000, 2500, 14)
    
    # Stable/increasing spend
    spend_values = np.linspace(100, 110, 14)
    
    # Increasing frequency
    frequency_values = np.linspace(3.0, 6.5, 14)
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Campaign_Saturated'] * 14,
        'Platform': ['Facebook'] * 14,
        'Spend': spend_values,
        'Impressions': np.random.randint(8000, 10000, 14),
        'Clicks': np.random.randint(200, 300, 14),
        'Conversions': np.random.randint(10, 20, 14),
        'CTR': np.random.uniform(2.0, 3.0, 14),
        'CPC': np.random.uniform(0.40, 0.55, 14),
        'Frequency': frequency_values,
        'Reach': reach_values
    })


@pytest.fixture
def campaign_data_with_trend():
    """Campaign data with clear upward trend"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Improving CTR
    ctr_values = np.linspace(2.0, 3.5, 14)
    
    # Declining CPC (good)
    cpc_values = np.linspace(0.50, 0.35, 14)
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Campaign_Improving'] * 14,
        'Platform': ['Google'] * 14,
        'Spend': np.random.uniform(90, 110, 14),
        'Impressions': np.random.randint(9000, 11000, 14),
        'Clicks': (ctr_values / 100 * 10000).astype(int),
        'Conversions': np.random.randint(20, 35, 14),
        'CTR': ctr_values,
        'CPC': cpc_values,
        'Frequency': np.random.uniform(3.0, 5.0, 14),
        'Reach': np.random.randint(2500, 3500, 14)
    })


@pytest.fixture
def campaign_data_with_anomalies():
    """Campaign data with anomalies/outliers"""
    dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
    
    # Normal CPC with outliers
    cpc_values = np.random.uniform(0.35, 0.45, 14)
    cpc_values[5] = 1.50  # Anomaly
    cpc_values[10] = 1.80  # Anomaly
    
    return pd.DataFrame({
        'Date': dates,
        'Campaign': ['Campaign_Anomaly'] * 14,
        'Platform': ['Google'] * 14,
        'Spend': np.random.uniform(90, 110, 14),
        'Impressions': np.random.randint(9000, 11000, 14),
        'Clicks': np.random.randint(250, 350, 14),
        'Conversions': np.random.randint(15, 25, 14),
        'CTR': np.random.uniform(2.5, 3.5, 14),
        'CPC': cpc_values,
        'Frequency': np.random.uniform(3.0, 5.0, 14),
        'Reach': np.random.randint(2500, 3500, 14)
    })


@pytest.fixture
def multi_campaign_data():
    """Multiple campaigns with varying performance"""
    dates = pd.date_range(start='2024-01-01', periods=7, freq='D')
    
    campaigns = []
    
    # High performer
    for date in dates:
        campaigns.append({
            'Date': date,
            'Campaign': 'High_Performer',
            'Spend': 100,
            'Conversions': 50,
            'ROAS': 5.0,
            'CTR': 3.5,
            'CPC': 0.30
        })
    
    # Medium performer
    for date in dates:
        campaigns.append({
            'Date': date,
            'Campaign': 'Medium_Performer',
            'Spend': 100,
            'Conversions': 25,
            'ROAS': 2.5,
            'CTR': 2.5,
            'CPC': 0.40
        })
    
    # Low performer
    for date in dates:
        campaigns.append({
            'Date': date,
            'Campaign': 'Low_Performer',
            'Spend': 100,
            'Conversions': 10,
            'ROAS': 1.0,
            'CTR': 1.5,
            'CPC': 0.60
        })
    
    return pd.DataFrame(campaigns)


@pytest.fixture
def mock_llm_client(monkeypatch):
    """Mock LLM client that returns predefined responses"""
    from tests.mocks.llm_mocks import MockLLMResponse
    
    mock_client = Mock()
    
    # Mock completion method
    def mock_completion(*args, **kwargs):
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Mocked LLM response"
        return mock_response
    
    mock_client.chat.completions.create = mock_completion
    
    return mock_client


@pytest.fixture
def mock_rag_retriever():
    """Mock RAG retriever for testing"""
    mock_rag = Mock()
    
    def mock_retrieve(query, filters=None, top_k=5):
        return {
            'documents': [
                {
                    'content': 'Creative fatigue optimization strategy',
                    'metadata': {'category': 'optimization', 'priority': 1}
                },
                {
                    'content': 'Audience expansion best practices',
                    'metadata': {'category': 'optimization', 'priority': 1}
                }
            ],
            'query': query
        }
    
    mock_rag.retrieve = mock_retrieve
    
    return mock_rag


@pytest.fixture
def mock_benchmark_engine():
    """Mock benchmark engine for testing"""
    mock_benchmarks = Mock()
    
    def mock_get_benchmarks(channel, business_model, industry, objective, region):
        return {
            'benchmarks': {
                'ctr': {'poor': 1.0, 'average': 2.0, 'good': 3.0, 'excellent': 4.0},
                'cpc': {'poor': 1.0, 'average': 0.50, 'good': 0.30, 'excellent': 0.20},
                'conversion_rate': {'poor': 1.0, 'average': 2.0, 'good': 3.0, 'excellent': 5.0}
            },
            'context': {
                'channel': channel,
                'business_model': business_model,
                'industry': industry
            }
        }
    
    mock_benchmarks.get_contextual_benchmarks = mock_get_benchmarks
    
    return mock_benchmarks


@pytest.fixture
def sample_patterns():
    """Sample detected patterns for testing"""
    return {
        'trends': {
            'detected': True,
            'direction': 'declining',
            'description': '2 metrics declining',
            'metrics': {
                'ctr': {'slope': -0.05, 'r_squared': 0.85, 'direction': 'declining'},
                'cpc': {'slope': 0.03, 'r_squared': 0.78, 'direction': 'declining'}
            }
        },
        'creative_fatigue': {
            'detected': True,
            'severity': 'high',
            'evidence': {
                'frequency': 7.5,
                'ctr_decline': -0.35,
                'recommendation': 'Refresh creative within 48 hours'
            }
        },
        'audience_saturation': {
            'detected': False
        },
        'anomalies': {
            'detected': False
        }
    }


@pytest.fixture
def campaign_context():
    """Mock campaign context for testing"""
    from unittest.mock import MagicMock
    
    context = MagicMock()
    context.business_model.value = 'ecommerce'
    context.industry_vertical = 'retail'
    context.geographic_focus = ['US', 'CA']
    context.target_audience = 'B2C'
    
    return context
