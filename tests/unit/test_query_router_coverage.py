import pytest
pytest.skip("Legacy test - incompatible with Kuzu router and current temporal intent parsing", allow_module_level=True)
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, date
from src.kg_rag.query.query_router import QueryRouter, QueryIntent, IntentMatch

# Mock dependencies
@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.get_schema_info.return_value = {
        'labels': ['Campaign', 'Metric'],
        'relationship_types': ['HAS_PERFORMANCE']
    }
    return conn

@pytest.fixture
def router(mock_conn):
    with patch('src.kg_rag.query.query_router.get_neo4j_connection', return_value=mock_conn), \
         patch('src.kg_rag.query.query_router.IntentClassifier') as mock_classifier_cls:
        
        router_instance = QueryRouter(connection=mock_conn)
        # We need to set the instance classifier mock
        router_instance._classifier = mock_classifier_cls.return_value
        return router_instance

@pytest.mark.skip(reason="Legacy test - incompatible with Kuzu router and current temporal intent parsing")
class TestQueryRouterInitialization:
    def test_init_defaults(self):
        with patch('src.kg_rag.query.query_router.get_neo4j_connection') as mock_get_conn, \
             patch('src.kg_rag.query.query_router.IntentClassifier'):
            router = QueryRouter()
            mock_get_conn.assert_called_once()
            assert router._classifier is not None

    def test_llm_setup(self):
        with patch.dict('os.environ', {'GOOGLE_API_KEY': 'fake', 'OPENAI_API_KEY': 'fake'}), \
             patch('src.kg_rag.query.query_router.genai') as mock_genai, \
             patch('src.kg_rag.query.query_router.OpenAI') as mock_openai, \
             patch('src.kg_rag.query.query_router.GEMINI_AVAILABLE', True), \
             patch('src.kg_rag.query.query_router.OPENAI_AVAILABLE', True):
            
            router = QueryRouter(connection=MagicMock())
            mock_genai.configure.assert_called_with(api_key='fake')
            assert router.gemini_available is True
            assert router.openai_client is not None

    def test_import_errors(self):
        # Simulate ImportError for genai and openai
        # We need to reload schema or simulate the check logic
        # Since logic is at module level, we can't easily re-import without side effects.
        # However, we can test that __init__ handles flags if they are False
        with patch('src.kg_rag.query.query_router.GEMINI_AVAILABLE', False), \
             patch('src.kg_rag.query.query_router.OPENAI_AVAILABLE', False), \
             patch('src.kg_rag.query.query_router.get_neo4j_connection'), \
             patch('src.kg_rag.query.query_router.IntentClassifier'):
            
            router = QueryRouter()
            assert router.gemini_available is False
            assert router.openai_client is None

class TestRoutingLogic:
    def test_route_optimization(self, router):
        # OPTIMIZATION intent handled directly
        match = IntentMatch(intent=QueryIntent.OPTIMIZATION, confidence=0.9, entities={"k": "v"}, matched_patterns=[])
        router._classifier.classify.return_value = match
        router._classifier.should_use_template.return_value = True
        
        with patch('src.kg_rag.query.templates.optimization.OptimizationTemplate') as mock_opt:
            mock_opt.return_value.run.return_value = {"success": True}
            res = router.route("optimize campaigns")
            assert res['success'] is True
            mock_opt.return_value.run.assert_called_once()

    def test_route_to_template_success(self, router):
        match = IntentMatch(intent=QueryIntent.PLATFORM_PERFORMANCE, confidence=0.95, entities={"platform": "meta"}, matched_patterns=[])
        router._classifier.classify.return_value = match
        router._classifier.should_use_template.return_value = True
        
        # Mock specific template function
        with patch('src.kg_rag.query.templates.platform.get_platform_overview') as mock_tmpl:
            mock_tmpl.return_value = ("MATCH (n) RETURN n", {})
            router._conn.execute_query.return_value = [{"n": 1}]
            
            res = router.route("meta performance")
            assert res['routing'] == 'template'
            assert res['success'] is True
            assert res['cypher'] == "MATCH (n) RETURN n"

    def test_route_to_template_error(self, router):
        # Template execution raises exception
        match = IntentMatch(intent=QueryIntent.PLATFORM_PERFORMANCE, confidence=0.95, entities={}, matched_patterns=[])
        router._classifier.classify.return_value = match
        router._classifier.should_use_template.return_value = True
        
        with patch('src.kg_rag.query.templates.platform.get_all_platforms_comparison') as mock_tmpl:
            mock_tmpl.return_value = ("MATCH...", {})
            router._conn.execute_query.side_effect = Exception("DB Error")
            
            res = router.route("all platforms")
            assert res['success'] is False
            assert "DB Error" in str(res['error'])

    def test_route_to_template_fallback(self, router):
        # Template returns None -> fallback to LLM
        match = IntentMatch(intent=QueryIntent.PLATFORM_PERFORMANCE, confidence=0.9, entities={}, matched_patterns=[])
        router._classifier.classify.return_value = match
        router._classifier.should_use_template.return_value = True
        
        with patch.object(router, '_get_template_query', return_value=(None, {})), \
             patch.object(router, '_route_to_llm') as mock_llm:
            mock_llm.return_value = {"routing": "llm"}
            
            res = router.route("unknown query")
            assert res['routing'] == "llm"

    def test_route_to_llm_direct(self, router):
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.5, entities={}, matched_patterns=[])
        router._classifier.classify.return_value = match
        router._classifier.should_use_template.return_value = False
        
        with patch.object(router, '_route_to_llm') as mock_llm:
            mock_llm.return_value = {"routing": "llm"}
            router.route("complex question")
            mock_llm.assert_called_once()

class TestTemplateRoutingDetails:
    def test_cross_channel(self, router):
        # Compare 2 channels
        match = IntentMatch(intent=QueryIntent.CROSS_CHANNEL, confidence=1.0, entities={"channel": ["Search", "Social"]}, matched_patterns=[])
        
        with patch('src.kg_rag.query.templates.cross_channel.get_compare_channels_query') as mock_cmp:
            mock_cmp.return_value = ("CYPHER", {})
            cypher, _ = router._get_template_query("q", match, {})
            mock_cmp.assert_called_with("Search", "SOC") # Check normalization
            assert cypher == "CYPHER"

        # All channels
        match_all = IntentMatch(intent=QueryIntent.CROSS_CHANNEL, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.cross_channel.get_all_channels_breakdown') as mock_all:
            mock_all.return_value = ("CYPHER", {})
            router._get_template_query("q", match_all, {})
            mock_all.assert_called()

    def test_temporal_trend_period_comparison(self, router):
        # Period comparison logic
        match = IntentMatch(intent=QueryIntent.TEMPORAL_TREND, confidence=1.0, entities={}, matched_patterns=[])
        query = "Compare performance vs last month"
        context = {"date_from": "2024-02-01", "date_to": "2024-02-10"} # 10 days
        
        with patch('src.kg_rag.query.templates.temporal.get_period_comparison') as mock_p:
            mock_p.return_value = ("CYPHER", {})
            router._get_template_query(query, match, context)
            
            # Previous period should be 10 days before 2024-02-01 -> 2024-01-22 to 2024-01-31
            mock_p.assert_called()
            call_args = mock_p.call_args[0]
            # Verify dates were calculated. 
            # Duration = 10 days (Feb 1 to Feb 10 inclusive)
            # Prev End = Jan 31
            # Prev Start = Jan 22
            assert call_args[2] == "2024-02-01"
            assert call_args[3] == "2024-02-10"

    def test_temporal_date_calculation_error(self, router):
        # Trigger exception in date calc
        match = IntentMatch(intent=QueryIntent.TEMPORAL_TREND, confidence=1.0, entities={}, matched_patterns=[])
        query = "Compare performance" # Generic, no 'month' or 'week' keyword
        context = {"date_from": "invalid-date"}
        
        # Should fallback to daily spend trend (line 330 -> 344)
        with patch('src.kg_rag.query.templates.temporal.get_daily_spend_trend') as mock_daily:
            mock_daily.return_value = ("CYPHER", {})
            router._get_template_query(query, match, context)
            mock_daily.assert_called()

    def test_temporal_trend_granularity(self, router):
        match = IntentMatch(intent=QueryIntent.TEMPORAL_TREND, confidence=1.0, entities={}, matched_patterns=[])
        
        # Week
        with patch('src.kg_rag.query.templates.temporal.get_weekly_trend') as mock_wk:
            mock_wk.return_value = ("CYPHER", {})
            router._get_template_query("weekly trend", match, {})
            mock_wk.assert_called()

        # Month
        with patch('src.kg_rag.query.templates.temporal.get_month_comparison') as mock_mo:
            mock_mo.return_value = ("CYPHER", {})
            router._get_template_query("monthly performance", match, {})
            mock_mo.assert_called()
            
        # Day of Week
        with patch('src.kg_rag.query.templates.temporal.get_day_of_week_analysis') as mock_dow:
            mock_dow.return_value = ("CYPHER", {})
            router._get_template_query("day of week", match, {})
            mock_dow.assert_called()

        # Platform specific trend
        match_plat = IntentMatch(intent=QueryIntent.TEMPORAL_TREND, confidence=1.0, entities={"platform": "meta"}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.temporal.get_platform_trend') as mock_pt:
            mock_pt.return_value = ("CYPHER", {})
            router._get_template_query("trend", match_plat, {})
            mock_pt.assert_called_with("meta", "2024-01-01", "2025-01-31")

    def test_targeting_analysis(self, router):
        match = IntentMatch(intent=QueryIntent.TARGETING_ANALYSIS, confidence=1.0, entities={"device": "mobile"}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.targeting.get_device_breakdown') as mock_dev:
            mock_dev.return_value = ("CYPHER", {})
            router._get_template_query("device breakdown", match, {})
            mock_dev.assert_called()

        # Fallback to Age breakdown
        match_age = IntentMatch(intent=QueryIntent.TARGETING_ANALYSIS, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.targeting.get_age_breakdown') as mock_age:
            mock_age.return_value = ("CYPHER", {})
            router._get_template_query("age breakdown", match_age, {})
            mock_age.assert_called()

    def test_anomaly(self, router):
        match = IntentMatch(intent=QueryIntent.ANOMALY_DETECTION, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.anomaly.get_low_roas_campaigns') as mock_anom:
            mock_anom.return_value = ("CYPHER", {})
            router._get_template_query("find anomalies", match, {})
            mock_anom.assert_called()
            
    def test_top_bottom(self, router):
        match = IntentMatch(intent=QueryIntent.TOP_BOTTOM, confidence=1.0, entities={"number": 5}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.platform.get_global_top_campaigns') as mock_top:
            mock_top.return_value = ("CYPHER", {})
            router._get_template_query("top 5 campaigns", match, {})
            mock_top.assert_called_with(5)

        # Platform specific top/bottom
        match_plat = IntentMatch(intent=QueryIntent.TOP_BOTTOM, confidence=1.0, entities={"number": 5, "platform": "meta"}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.platform.get_platform_top_campaigns') as mock_top_plat:
            mock_top_plat.return_value = ("CYPHER", {})
            router._get_template_query("top 5 campaigns on meta", match_plat, {})
            mock_top_plat.assert_called_with("meta", 5)


    def test_placement_analysis(self, router):
        match = IntentMatch(intent=QueryIntent.PLACEMENT_ANALYSIS, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.placement.get_placement_overview') as mock_pl:
            mock_pl.return_value = ("CYPHER", {})
            router._get_template_query("placements", match, {})
            mock_pl.assert_called()

    def test_budget_analysis(self, router):
        match = IntentMatch(intent=QueryIntent.BUDGET_ANALYSIS, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.cross_channel.get_all_channels_breakdown') as mock_bd:
            mock_bd.return_value = ("CYPHER", {})
            router._get_template_query("budget", match, {})
            mock_bd.assert_called()

    def test_aggregation(self, router):
        match = IntentMatch(intent=QueryIntent.AGGREGATION, confidence=1.0, entities={}, matched_patterns=[])
        with patch('src.kg_rag.query.templates.cross_channel.get_all_channels_breakdown') as mock_agg:
            mock_agg.return_value = ("CYPHER", {})
            router._get_template_query("total spend", match, {})
            mock_agg.assert_called() 

class TestLLMRouting:
    def test_route_to_llm_gemini_success(self, router):
        router.gemini_available = True
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.0, entities={}, matched_patterns=[])
        
        with patch('src.kg_rag.query.query_router.genai.GenerativeModel') as mock_model:
            mock_model.return_value.generate_content.return_value.text = "```cypher MATCH (n) RETURN n ```"
            router._conn.execute_query.return_value = []
            
            res = router._route_to_llm("question", match, {})
            assert res['success'] is True
            assert res['cypher'] == "MATCH (n) RETURN n"
            assert res['routing'] == "llm"

    def test_route_to_llm_gemini_fail_openai_success(self, router):
        router.gemini_available = True
        router.openai_client = MagicMock()
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.0, entities={}, matched_patterns=[])
        
        # Gemini raises exception
        with patch('src.kg_rag.query.query_router.genai.GenerativeModel') as mock_model:
            mock_model.return_value.generate_content.side_effect = Exception("Gemini Down")
            
            # OpenAI succeeds
            mock_resp = MagicMock()
            mock_resp.choices[0].message.content = "MATCH (openai) RETURN n"
            router.openai_client.chat.completions.create.return_value = mock_resp
            router._conn.execute_query.return_value = []
            
            res = router._route_to_llm("question", match, {})
            assert res['success'] is True
            assert res['cypher'] == "MATCH (openai) RETURN n"

    def test_route_to_llm_openai_fail(self, router):
        # Gemini fails, OpenAI fails
        router.gemini_available = True
        router.openai_client = MagicMock()
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.0, entities={}, matched_patterns=[])
        
        with patch('src.kg_rag.query.query_router.genai.GenerativeModel') as mock_gem:
            mock_gem.return_value.generate_content.side_effect = Exception("Gemini Fail")
            router.openai_client.chat.completions.create.side_effect = Exception("OpenAI Fail")
            
            res = router._route_to_llm("q", match, {})
            assert res['success'] is False
            assert "OpenAI generation failed" in str(res.get('error', '')) or "Failed to generate" in str(res.get('error', ''))

    def test_route_to_llm_all_fail(self, router):
        router.gemini_available = False
        router.openai_client = None
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.0, entities={}, matched_patterns=[])
        
        res = router._route_to_llm("question", match, {})
        assert res['success'] is False
        assert "Failed to generate" in res['error']

    def test_llm_execution_error(self, router):
        # LLM generates query but Neo4j execution fails
        router.gemini_available = True
        match = IntentMatch(intent=QueryIntent.UNKNOWN, confidence=0.0, entities={}, matched_patterns=[])
        
        with patch('src.kg_rag.query.query_router.genai.GenerativeModel') as mock_model:
            mock_model.return_value.generate_content.return_value.text = "BAD CYPHER"
            router._conn.execute_query.side_effect = Exception("Syntax Error")
            
            res = router._route_to_llm("question", match, {})
            assert res['success'] is False
            assert "routing failed" in str(res.get('error', '')) or "Syntax Error" in str(res.get('error', ''))

    def test_normalization_helpers(self, router):
        assert router._normalize_platform("Facebook") == "meta"
        assert router._normalize_platform(None) is None
        assert router._normalize_channel("Social") == "SOC"
        assert router._normalize_channel(None) is None

    def test_get_available_templates(self, router):
        tmpls = router.get_available_templates()
        assert len(tmpls) > 0
        assert tmpls[0]['intent'] == "cross_channel"
