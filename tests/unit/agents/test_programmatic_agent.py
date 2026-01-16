import pytest
import pandas as pd
from src.engine.agents.channel_specialists.programmatic_agent import ProgrammaticAgent

class TestProgrammaticAgent:
    @pytest.fixture
    def agent(self):
        return ProgrammaticAgent()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'Campaign_ID': ['P1', 'P2'],
            'Campaign_Name': ['Prog_Display_DV360', 'Prog_Video_CM360'],
            'Platform': ['DV360', 'CM360'],
            'Spend': [10000, 5000],
            'Impressions': [1000000, 500000],
            'Clicks': [4000, 1000],
            'Conversions': [50, 10],
            'Viewability': [0.75, 0.45], # P1 good, P2 poor
            'Measurable_Impressions': [900000, 400000],
            'Safety_Score': [0.98, 0.88],
            'Invalid_Traffic': [10000, 20000],
            'VCR': [0.0, 0.72] # P2 has video completion
        })

    def test_analyze_structure(self, agent, sample_data):
        """Test that analysis returns programmatic-specific keys."""
        results = agent.analyze(sample_data)
        assert 'viewability_analysis' in results
        assert 'brand_safety' in results
        assert 'fraud_detection' in results
        assert 'overall_health' in results

    def test_viewability_analysis(self, agent, sample_data):
        """Test viewability logic."""
        results = agent.analyze(sample_data)
        viewability = results['viewability_analysis']
        assert viewability['average_viewability'] == 60.0 # (75+45)/2
        assert viewability['status'] == 'average'

    def test_fraud_detection(self, agent, sample_data):
        """Test IVT/fraud logic."""
        results = agent.analyze(sample_data)
        fraud = results['fraud_detection']
        assert 'ivt_rate' in fraud
        # total_ivt = 0.01*1M + 0.04*500k = 10k + 20k = 30k
        # total_imp = 1.5M
        # ivt_rate = 30k / 1.5M = 2%
        assert fraud['ivt_rate'] == 2.0
