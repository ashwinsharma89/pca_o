"""
Tests for MultiAgentOrchestrator (Phase C.3).
Verifies routing, execution loops, and state accumulation using unittest.
"""

import unittest
import asyncio
from src.engine.agents.multi_agent_orchestrator import (
    MultiAgentOrchestrator, RouterAgent, AnalyzerAgent, 
    SpecialistAgent, RecommenderAgent, SynthesizerAgent
)

class TestMultiAgentOrchestrator(unittest.IsolatedAsyncioTestCase):
    """Unit tests for multi-agent orchestration."""

    async def test_router_logic(self):
        """Verify router identifies the correct next agent."""
        router = RouterAgent()
        
        # Budget query
        res1 = await router.execute({"query": "What is our spend?"})
        self.assertEqual(res1.next_agent, "budget_analyzer")
        
        # Creative query
        res2 = await router.execute({"query": "Which ad copy is best?"})
        self.assertEqual(res2.next_agent, "creative_analyzer")
        
        # Audience query
        res3 = await router.execute({"query": "Are we reaching the right audience?"})
        self.assertEqual(res3.next_agent, "audience_analyzer")

    async def test_specialist_metrics(self):
        """Verify specialist returns platform-specific metrics."""
        meta_spec = SpecialistAgent("meta")
        res = await meta_spec.execute({})
        self.assertIn("Relevance Score", res.output["specialist_insights"]["platform_specific_metrics"])
        
        google_spec = SpecialistAgent("google")
        res2 = await google_spec.execute({})
        self.assertIn("Quality Score", res2.output["specialist_insights"]["platform_specific_metrics"])

    async def test_simple_orchestration_loop(self):
        """Verify the full execution chain from router to end."""
        orchestrator = MultiAgentOrchestrator()
        
        result = await orchestrator.run("What's the spend?", campaign_data=[{"spend": 100}])
        
        self.assertTrue(result["success"])
        # Should have results from: router, budget_analyzer, recommender, synthesizer
        self.assertGreaterEqual(len(result["results"]), 4)
        self.assertTrue(any("total_spend" in str(r) for r in result["results"]))
        self.assertGreater(len(result["recommendations"]), 0)

    async def test_error_handling_missing_agent(self):
        """Verify orchestrator handles missing agent gracefully."""
        orchestrator = MultiAgentOrchestrator()
        # Manually set an invalid starting agent
        res = await orchestrator._simple_orchestrate({
            "current_agent": "non_existent_agent",
            "iteration": 0,
            "max_iterations": 10,
            "errors": [],
            "analysis_results": [],
            "recommendations": []
        })
        self.assertFalse(res["success"])
        self.assertIn("Agent not found", res["errors"][0])

    async def test_max_iterations_cap(self):
        """Verify orchestrator respects max iterations limit."""
        orchestrator = MultiAgentOrchestrator()
        # Set max_iterations to 1
        res = await orchestrator.run("test", max_iterations=1)
        # Should only run the router
        self.assertEqual(len(res["results"]), 1)

if __name__ == "__main__":
    unittest.main()
