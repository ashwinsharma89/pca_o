"""
Tests for ReasoningAgent (Phase D.1).
Fixed for robust async testing and model validation.
"""

import unittest
from datetime import date
from unittest.mock import MagicMock, AsyncMock, patch
from src.engine.agents.reasoning_agent import ReasoningAgent
from src.platform.models.campaign import Campaign, CampaignObjective, DateRange
from src.platform.models.platform import PlatformType, MetricType, NormalizedMetric

class TestReasoningAgent(unittest.IsolatedAsyncioTestCase):
    """Unit tests for ReasoningAgent core logic."""

    async def asyncSetUp(self):
        """Setup agent with mocked LLM."""
        with patch('src.engine.agents.reasoning_agent.AsyncOpenAI'):
            self.agent = ReasoningAgent(provider="openai")
            self.agent._call_llm = AsyncMock()

    def test_calculate_performance_score(self):
        """Verify performance score calculation for different objectives."""
        perf = MagicMock()
        perf.total_impressions = 500_000 # 50% of 1M -> 15 points
        perf.ctr = 1.0 # 50% of 2% -> 15 points
        perf.roas = 1.5 # 50% of 3.0 -> 20 points
        
        # Combined Awareness + Consideration + Conversion
        objectives = [CampaignObjective.AWARENESS, CampaignObjective.CONSIDERATION, CampaignObjective.CONVERSION]
        score = self.agent._calculate_performance_score(perf, objectives)
        # Expected: (15 + 15 + 20) / (30 + 30 + 40) * 100 = 50%
        self.assertEqual(score, 50.0)

    def test_rank_channels(self):
        """Verify ranking of channels by performance score."""
        p1 = MagicMock(performance_score=90.0)
        p2 = MagicMock(performance_score=50.0)
        p3 = MagicMock(performance_score=75.0)
        
        ranked = self.agent._rank_channels([p1, p2, p3])
        self.assertEqual(ranked[0].performance_score, 90.0)
        self.assertEqual(ranked[1].performance_score, 75.0)
        self.assertEqual(ranked[2].performance_score, 50.0)
        self.assertEqual(ranked[0].efficiency_rank, 1)
        self.assertEqual(ranked[2].efficiency_rank, 3)

    async def test_generate_channel_insights_json_parsing(self):
        """Verify LLM response parsing and fallback."""
        # Success case
        self.agent._call_llm.return_value = '{"strengths": ["test"], "weaknesses": [], "opportunities": []}'
        res = await self.agent._generate_channel_insights(MagicMock(), {}, [])
        self.assertEqual(res["strengths"], ["test"])
        
        # Failure case (invalid JSON)
        self.agent._call_llm.return_value = 'INVALID'
        res2 = await self.agent._generate_channel_insights(MagicMock(), {}, [])
        self.assertEqual(res2["strengths"], [])

    async def test_analyze_campaign_flow(self):
        """Verify full analysis flow with mocked LLM calls."""
        campaign = Campaign(
            campaign_id="test_id",
            campaign_name="Test Campaign",
            objectives=[CampaignObjective.CONVERSION],
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 14)),
            normalized_metrics=[
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0),
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CONVERSIONS, value=10.0),
                NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.SPEND, value=200.0)
            ]
        )
        
        # Mocking sub-parts to focus on flow
        self.agent._generate_channel_insights = AsyncMock(return_value={"strengths": ["S"]})
        self.agent._call_llm.side_effect = [
            '[]', # cross-channel insights
            '[]', # achievements
            '["R1"]' # recommendations
        ]
        
        result = await self.agent.analyze_campaign(campaign)
        
        self.assertEqual(len(result.insights["channel_performances"]), 2)
        self.assertEqual(len(result.recommendations), 1)
        self.assertEqual(result.recommendations[0], "R1")
