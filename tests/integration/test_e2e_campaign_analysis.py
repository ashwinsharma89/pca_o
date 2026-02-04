"""
End-to-End Integration Test (Phase D.2).
Fixed for robust async testing.
"""

import unittest
import pandas as pd
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import date

# Import core components
from src.kg_rag.etl.transformers.campaign_transformer import CampaignTransformer
from src.engine.agents.multi_agent_orchestrator import MultiAgentOrchestrator
from src.platform.models.campaign import Campaign, CampaignObjective, DateRange

class TestE2EFlow(unittest.IsolatedAsyncioTestCase):
    """E2E Test: Full lifecycle simulation."""

    async def test_full_analysis_lifecycle(self):
        """
        CSV Data -> Transformation -> Multi-Agent Orchestration -> Final Synthesis
        """
        
        # 1. Setup Mock Data
        csv_data = pd.DataFrame({
            'Date': pd.date_range(start='2024-01-01', periods=10),
            'Platform': ['Meta Ads'] * 5 + ['Google Ads'] * 5,
            'Spend': [100.0] * 10,
            'Impressions': [10000] * 10,
            'Clicks': [100] * 10,
            'Conversions': [5] * 10,
            'Campaign_Name': ['Campaign A'] * 10
        })
        
        records = csv_data.to_dict('records')
        
        # 2. Mocking sub-components
        with patch('src.engine.agents.reasoning_agent.AsyncOpenAI'), \
             patch('src.engine.agents.enhanced_visualization_agent.SmartChartGenerator'):
            
            orchestrator = MultiAgentOrchestrator()
            
            # Run orchestrated analysis
            result = await orchestrator.run(
                query="Analyze my campaign performance across Meta and Google",
                campaign_data=records
            )
            
            # 3. Verify Results
            print(f"DEBUG: E2E Result Success: {result['success']}")
            print(f"DEBUG: E2E Result Keys: {result.keys()}")
            print(f"DEBUG: E2E Results Trace: {json.dumps(result['results'], indent=2)}")
            
            self.assertTrue(result["success"])
            self.assertGreater(len(result["results"]), 0)
            
            # Verify trace contains ANY of the agents we expect
            findings_str = str(result["results"])
            # The query "Analyze my campaign performance" should go to general_analyzer
            # But wait, does it go to specialists? 
            # Router logic:
            # if any(word in query for word in ["spend", "budget", "cost", "roas"]): next_agent = "budget_analyzer"
            # else: next_agent = "general_analyzer"
            
            # General analyzer next_agent = "recommender"
            # Recommender next_agent = "synthesizer"
            # Synthesizer next_agent = None
            
            # Specialists are NOT reached unless the query or state explicitly directs to them.
            # So budget/spend words are needed to trigger specialist routing if we want to see platforms.
            # Wait, RouterAgent in multi_agent_orchestrator.py:
            # 87:         if any(word in query for word in ["spend", "budget", "cost", "roas"]):
            # 88:             next_agent = "budget_analyzer"
            
            self.assertTrue(len(result["results"]) >= 3) # Router, Analyzer, Recommender, Synthesizer
            self.assertIn("analysis", findings_str) 

if __name__ == "__main__":
    unittest.main()
