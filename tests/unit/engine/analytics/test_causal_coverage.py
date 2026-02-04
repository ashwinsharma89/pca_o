import unittest
import pandas as pd
import numpy as np
from src.engine.analytics.causal_analysis import CausalAnalysisEngine, DecompositionMethod, CausalAnalysisResult

class TestCausalCoverage(unittest.TestCase):
    def setUp(self):
        # Create synthetic time-series data
        dates = pd.date_range('2023-01-01', periods=60, freq='D')
        self.df = pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(100, 200, 60),
            'Revenue': np.random.uniform(300, 600, 60),
            'Conversions': np.random.randint(5, 20, 60),
            'Clicks': np.random.randint(100, 200, 60),
            'Impressions': np.random.randint(1000, 2000, 60),
            'Platform': np.random.choice(['Meta', 'Google'], 60)
        })
        self.engine = CausalAnalysisEngine()

    def test_roas_decomposition(self):
        # Force a change between periods
        self.df.loc[30:, 'Revenue'] *= 1.5
        self.df.loc[30:, 'Spend'] *= 1.2
        
        result = self.engine.analyze(self.df, 'ROAS', split_date=self.df['Date'].iloc[30])
        self.assertIsInstance(result, CausalAnalysisResult)
        self.assertEqual(result.metric, 'ROAS')
        self.assertTrue(len(result.contributions) > 0)
        
        # Verify specific components exist for ROAS
        components = [c.component for c in result.contributions]
        self.assertIn("Spend Level", components)
        self.assertIn("Conversion Volume", components)

    def test_cpa_decomposition(self):
        result = self.engine.analyze(self.df, 'CPA', split_date=self.df['Date'].iloc[30])
        self.assertEqual(result.metric, 'CPA')
        components = [c.component for c in result.contributions]
        self.assertIn("Cost Per Click (CPC)", components)
        self.assertIn("Conversion Rate (CVR)", components)

    def test_insufficient_data_handling(self):
        empty_df = pd.DataFrame(columns=self.df.columns)
        result = self.engine.analyze(empty_df, 'ROAS')
        self.assertEqual(len(result.contributions), 0)
        self.assertTrue(result.primary_driver is None)

    def test_insights_generation(self):
        self.df.loc[30:, 'Revenue'] *= 2.0
        result = self.engine.analyze(self.df, 'ROAS')
        self.assertTrue(len(result.insights) > 0)
        self.assertTrue(len(result.recommendations) > 0)

    def test_generic_decomposition_fallback(self):
        # Test a metric that doesn't have a specific decomposer
        self.df['CustomMetric'] = self.df['Revenue'] / 10
        result = self.engine.analyze(self.df, 'CustomMetric')
        self.assertIsInstance(result, CausalAnalysisResult)

if __name__ == "__main__":
    unittest.main()
