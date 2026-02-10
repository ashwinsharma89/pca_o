
import pytest
import pandas as pd
import polars as pl
import numpy as np
import os
from unittest.mock import MagicMock, patch
from src.engine.analytics.metrics_calculator import MetricsCalculator
from src.engine.analytics.business_rules import BusinessRules
from src.engine.analytics.recommendations import RecommendationEngine
from src.engine.analytics.llm_service import LLMService
from src.engine.analytics.orchestrator import AnalyticsOrchestrator
from src.engine.analytics.auto_insights import MediaAnalyticsExpert

# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def sample_polars_df():
    return pl.DataFrame({
        "campaign": ["Camp A"], "platform": ["Meta"], "spend": [100.0],
        "impressions": [1000], "clicks": [10], "conversions": [1], "revenue": [100.0]
    })

# ==============================================================================
# TESTS
# ==============================================================================

class TestMetricsCalculator:
    def test_core(self, sample_polars_df):
        res = MetricsCalculator.calculate_core_metrics(sample_polars_df)
        assert res[0, "ctr"] == 1.0
        assert MetricsCalculator.calculate_core_metrics(pl.DataFrame({"spend": [0], "impressions": [0], "clicks": [0], "conversions": [0], "revenue": [0]}))[0, "ctr"] == 0.0
    def test_agg(self, sample_polars_df):
        assert MetricsCalculator.calculate_aggregated_metrics(sample_polars_df, ["platform"]).height == 1

class TestBusinessRules:
    def test_eval(self):
        for c, r, expected_c, expected_r in [(0.1, 0.5, "poor", "critical"), (1.0, 2.0, "average", "good"), (2.0, 5.0, "good", "excellent")]:
            s = BusinessRules.evaluate_performance({"ctr": c, "roas": r})
            assert s["ctr"] == expected_c and s["roas"] == expected_r
        assert BusinessRules.evaluate_performance({}) == {}
    def test_sig(self):
        assert BusinessRules.is_significant(10, 100) and not BusinessRules.is_significant(1, 0)

class TestRecommendationEngine:
    def test_gen(self):
        assert len(RecommendationEngine.generate_recommendations({"roas": 0.5, "ctr": 0.1}, {"roas": "critical", "ctr": "poor"})) == 2
        assert RecommendationEngine.generate_recommendations({"roas": 6.0}, {"roas": "excellent"})[0]["type"] == "budget_increase"
    def test_fmt(self):
        assert "[HIGH] m" in RecommendationEngine._format_for_llm([{"severity": "high", "message": "m", "action": "a"}])

class TestLLMService:
    @patch('src.engine.analytics.llm_service.OpenAI')
    @patch('src.engine.analytics.llm_service.genai')
    @patch('src.engine.analytics.llm_service.os.getenv')
    def test_init(self, mock_getenv, mock_openai, mock_genai):
        # Case: Anthropic missing -> Gemini fallback
        mock_getenv.side_effect = lambda k, d=None: {'PRIMARY_LLM_PROVIDER': 'anthropic', 'ANTHROPIC_API_KEY': '', 'GOOGLE_API_KEY': 'gk'}.get(k, d)
        assert LLMService().primary_provider == 'gemini'
        # Case: OpenAI success path
        mock_getenv.side_effect = lambda k, d=None: {'PRIMARY_LLM_PROVIDER': 'openai', 'OPENAI_API_KEY': 'ok'}.get(k, d)
        assert LLMService().primary_provider == 'openai'
        # Case: All missing
        mock_getenv.side_effect = lambda k, d=None: {}.get(k, d)
        with pytest.raises(ValueError): LLMService()

    @patch('src.engine.analytics.llm_service.OpenAI')
    @patch('requests.post')
    def test_calls(self, mock_post, mock_openai):
        mock_post.return_value.json.return_value = {'content': [{'text': 'A'}]}
        with patch.dict('os.environ', {'ANTHROPIC_API_KEY': 'ak', 'PRIMARY_LLM_PROVIDER': 'anthropic'}):
            assert LLMService().generate_completion("p") == "A"
        mock_openai.return_value.chat.completions.create.return_value.choices[0].message.content = "O"
        with patch.dict('os.environ', {'OPENAI_API_KEY': 'ok', 'PRIMARY_LLM_PROVIDER': 'openai'}):
            assert LLMService()._call_openai("p", "s") == "O"

    def test_gemini_direct(self):
        with patch.dict('os.environ', {'GOOGLE_API_KEY': 'gk', 'PRIMARY_LLM_PROVIDER': 'gemini'}):
            s = LLMService(); s.gemini_client = MagicMock()
            s.gemini_client.generate_content.return_value.text = "G"
            assert s._call_gemini("p", "s") == "G"

    @patch.object(LLMService, '_call_openai')
    @patch.object(LLMService, '_call_gemini')
    def test_fallback(self, mock_gemini, mock_openai):
        mock_openai.side_effect = Exception("F"); mock_gemini.return_value = "FB"
        with patch.dict('os.environ', {'OPENAI_API_KEY': 'k', 'GOOGLE_API_KEY': 'g'}):
            s = LLMService(); s.gemini_client = MagicMock()
            assert s.generate_completion("p") == "FB"

class TestAnalyticsOrchestrator:
    @patch('src.engine.analytics.orchestrator.LLMService')
    @patch('src.engine.analytics.orchestrator.MetricsCalculator')
    def test_analyze_full(self, mock_metrics, mock_llm):
        orch = AnalyticsOrchestrator()
        # Empty inputs
        assert "error" in orch.analyze_campaigns(pl.DataFrame())
        assert "error" in orch.analyze_campaigns(pd.DataFrame())
        # Normalization paths
        mock_metrics.calculate_core_metrics.return_value = pl.DataFrame({"spend": [1], "roas": [1], "ctr": [1]})
        mock_llm.return_value.generate_completion.return_value = 'SUCCESS'
        # Polars with rename/collision
        df_pl = pl.DataFrame({"Spend": [1], "spend": [1], "cost": [None]})
        assert orch.analyze_campaigns(df_pl)["narrative"] == "SUCCESS..."
        # Pandas path
        df_pd = pd.DataFrame({"Campaign Name": [1], "Spend": [1]})
        assert orch.analyze_campaigns(df_pd)["narrative"] == "SUCCESS..."

    @patch('src.engine.analytics.orchestrator.LLMService')
    @patch('src.engine.analytics.orchestrator.MetricsCalculator')
    def test_failures(self, mock_metrics, mock_llm, sample_polars_df):
        mock_metrics.calculate_core_metrics.return_value = pl.DataFrame({"spend": [1], "roas": [1], "ctr": [1]})
        mock_metrics.calculate_aggregated_metrics.side_effect = Exception("E")
        mock_llm.return_value.generate_completion.side_effect = Exception("E")
        res = AnalyticsOrchestrator().analyze_campaigns(sample_polars_df)
        assert res["metrics"]["by_platform"] == [] and "could not" in res["narrative"].lower()

    @patch('src.engine.analytics.orchestrator.LLMService')
    def test_rag(self, mock_llm):
        orch = AnalyticsOrchestrator()
        # Mapping variants
        mock_llm.return_value.generate_completion.return_value = '{"key_takeaways": ["T1"]}'
        res = orch.generate_rag_summary({"metrics": {"overview": {}}})
        assert "Advanced analysis" in res["brief"] and "- T1" in res["detailed"]
        # Fallbacks
        mock_llm.return_value.generate_completion.return_value = 'INVALID'
        assert "available" in orch.generate_rag_summary({"metrics": {"overview": {}}})["brief"]
        mock_llm.return_value.generate_completion.side_effect = Exception("E")
        assert "service error" in orch.generate_rag_summary({}).get("brief").lower()

class TestMediaAnalyticsExpert:
    @patch('src.engine.analytics.auto_insights.AnalyticsOrchestrator')
    def test_expert(self, mock_orch):
        mock_orch.return_value.analyze_campaigns.return_value = {"metrics": {}, "status": {}, "recommendations": [], "narrative": "N", "execution_time": 1}
        assert MediaAnalyticsExpert().analyze_all(pl.DataFrame())["executive_summary"] == "N"
        mock_orch.return_value.generate_rag_summary.return_value = {"brief": "R"}
        assert MediaAnalyticsExpert().generate_executive_summary_with_rag({})["brief"] == "R"
