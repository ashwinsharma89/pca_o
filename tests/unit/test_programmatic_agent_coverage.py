"""
Tests for ProgrammaticAgent (Phase C.2).
Verifies programmatic-specific metrics like Viewability and Brand Safety.
"""

import pytest
import pandas as pd
import numpy as np
from src.engine.agents.channel_specialists.programmatic_agent import ProgrammaticAgent, ProgrammaticBenchmarks

@pytest.fixture
def programmatic_data():
    """Create mock programmatic data for DV360."""
    return pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=14),
        'Platform': ['DV360'] * 14,
        'Spend': [100.0] * 14,
        'Impressions': [10000] * 14,
        'Clicks': [50] * 14,
        'Conversions': [5] * 14,
        'CTR': [0.005] * 14,
        'CPM': [10.0] * 14,
        'Viewability_Rate': [0.75] * 14,
        'Brand_Safety_Score': [0.98] * 14,
        'Invalid_Traffic_Impressions': [10] * 14,
        'Placement': ['Site A'] * 14,
        'Video_Completion_Rate': [0.8] * 14
    })

class TestProgrammaticAgent:
    """Unit tests for ProgrammaticAgent."""

    def test_programmatic_benchmarks(self):
        """Verify benchmark lookups."""
        assert ProgrammaticBenchmarks.get_benchmark("viewability") == 0.70
        assert ProgrammaticBenchmarks.get_benchmark("ctr") == 0.0046

    def test_analyze_viewability_critical(self, programmatic_data):
        """Verify detection of critical low viewability."""
        agent = ProgrammaticAgent()
        data = programmatic_data.copy()
        data['Viewability_Rate'] = 0.4 # Threshold < 50%
        
        result = agent._analyze_viewability(data)
        assert result['status'] in ['critical', 'poor']
        assert "Critical" in result['findings'][0] or "Below standard" in result['findings'][0]

    def test_check_brand_safety_excellent(self, programmatic_data):
        """Verify brand safety analysis."""
        agent = ProgrammaticAgent()
        result = agent._check_brand_safety(programmatic_data)
        assert result['status'] == 'excellent'
        assert result['brand_safety_score'] == 98.0

    def test_detect_invalid_traffic_concerning(self, programmatic_data):
        """Verify detection of elevated IVT."""
        agent = ProgrammaticAgent()
        data = programmatic_data.copy()
        # 140000 total impressions. Let's make 5000 IVT
        # Rate = 5000 / 140000 = 3.57% (Threshold 2% concerning, 5% critical)
        data['Invalid_Traffic_Impressions'] = 5000 / 14 
        
        result = agent._detect_invalid_traffic(data)
        assert result['status'] == 'concerning'
        assert result['ivt_rate'] > 2

    def test_analyze_video_metrics_vcr(self, programmatic_data):
        """Verify video completion rate analysis."""
        agent = ProgrammaticAgent()
        result = agent._analyze_video_metrics(programmatic_data)
        assert result['video_completion_rate'] == 80.0
        assert "✅ Good video completion" in result['findings'][0]

    def test_overall_health_calculation(self):
        """Verify weighted health score aggregation."""
        agent = ProgrammaticAgent()
        insights = {
            'viewability_analysis': {'status': 'excellent'}, # Critical metric (weight 2)
            'brand_safety': {'status': 'excellent'}, # Critical metric (weight 2)
            'placement_performance': {'status': 'good'} # Non-critical (weight 1)
        }
        health = agent._calculate_overall_health(insights)
        assert health == 'excellent'

    def test_detect_invalid_traffic_none(self, programmatic_data):
        """Verify behavior when IVT data is missing."""
        agent = ProgrammaticAgent()
        data = programmatic_data.drop(columns=['Invalid_Traffic_Impressions'])
        result = agent._detect_invalid_traffic(data)
        assert result['status'] == 'unavailable'
