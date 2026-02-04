
import pytest
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta, date
from unittest.mock import MagicMock, patch, AsyncMock
from src.engine.agents.reasoning_agent import ReasoningAgent
from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent, PatternDetector
from src.engine.agents.validated_reasoning_agent import ValidatedReasoningAgent
from src.platform.models.campaign import Campaign, NormalizedMetric, CampaignObjective, DateRange
from src.platform.models.platform import PlatformType, MetricType
from src.engine.agents.schemas import AgentOutput, PatternType, PriorityLevel

# --- ReasoningAgent Tests ---

class TestReasoningAgent:
    @pytest.fixture
    def mock_openai(self):
        with patch("src.engine.agents.reasoning_agent.AsyncOpenAI") as mock:
            client = mock.return_value
            client.chat.completions.create = AsyncMock()
            yield client

    @pytest.fixture
    def mock_anthropic(self):
        with patch("src.engine.agents.reasoning_agent.create_async_anthropic_client") as mock:
            client = mock.return_value
            client.messages.create = AsyncMock()
            yield client

    @pytest.mark.asyncio
    async def test_init_providers(self, mock_openai, mock_anthropic):
        # Default/OpenAI
        agent = ReasoningAgent(provider="openai")
        assert agent.provider == "openai"
        
        # Anthropic
        agent_ant = ReasoningAgent(provider="anthropic")
        assert agent_ant.provider == "anthropic"
        
        # Invalid
        with pytest.raises(ValueError, match="Unsupported provider"):
            ReasoningAgent(provider="invalid")

    @pytest.mark.asyncio
    async def test_analyze_campaign_openai(self, mock_openai):
        agent = ReasoningAgent(provider="openai")
        
        # Mock LLM Responses sequentially
        # 1. Single Channel Insights (one per channel, we have 1)
        # 2. Cross-Channel Analysis (list of dicts)
        # 3. Achievements (list of dicts)
        # 4. Recommendations (list of strings)
        mock_openai.chat.completions.create.side_effect = [
            MagicMock(choices=[MagicMock(message=MagicMock(content='{"strengths": ["Strong ROI"], "weaknesses": ["High CPA"], "opportunities": ["Targeting"]}') )]),
            MagicMock(choices=[MagicMock(message=MagicMock(content='[{"insight_type": "synergy", "title": "Cross-over", "description": "Good synergy", "affected_platforms": ["meta_ads"], "impact_score": 8.0}]') )]),
            MagicMock(choices=[MagicMock(message=MagicMock(content='[{"achievement_type": "performance", "title": "Winner", "description": "Winner winner", "metric_value": 5.0, "metric_name": "ROAS", "platform": "meta_ads", "impact_level": "high"}] * 5') )]),
            MagicMock(choices=[MagicMock(message=MagicMock(content='["Do More", "Scale Up", "Optimize", "Refresh", "Expand"]') )])
        ]
        
        # Mock Campaign
        campaign = Campaign(
            campaign_id="123",
            campaign_name="Test Campaign",
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 30)),
            normalized_metrics=[
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=1000.0, timestamp=datetime.now(), source_snapshot_id="s1", original_metric_name="spend"),
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.IMPRESSIONS, value=100000.0, timestamp=datetime.now(), source_snapshot_id="s1", original_metric_name="impr"),
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CLICKS, value=2000.0, timestamp=datetime.now(), source_snapshot_id="s1", original_metric_name="clicks"),
                NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CONVERSIONS, value=50.0, timestamp=datetime.now(), source_snapshot_id="s1", original_metric_name="conv"),
            ],
            objectives=[CampaignObjective.CONVERSION]
        )
        
        result = await agent.analyze_campaign(campaign)
        assert len(result.recommendations) > 0 # Recommendations populated from LLM mock
        assert mock_openai.chat.completions.create.call_count >= 1

    @pytest.mark.asyncio
    async def test_analyze_campaign_anthropic(self, mock_anthropic):
        agent = ReasoningAgent(provider="anthropic")
        
        mock_anthropic.messages.create.side_effect = [
            MagicMock(content=[MagicMock(text='{"strengths": ["ant"], "weaknesses": [], "opportunities": []}')]),
            MagicMock(content=[MagicMock(text='[]')]), # Cross
            MagicMock(content=[MagicMock(text='[]')]), # Achievement
            MagicMock(content=[MagicMock(text='[]')])  # Recs
        ]
        
        campaign = Campaign(
            campaign_id="456",
            campaign_name="Ant Campaign",
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 15)),
            normalized_metrics=[
                NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.SPEND, value=500, timestamp=datetime.now(), source_snapshot_id="s2", original_metric_name="cost")
            ],
            objectives=[CampaignObjective.AWARENESS]
        )
        
        await agent.analyze_campaign(campaign)
        assert mock_anthropic.messages.create.called

    def test_calculate_performance_score_edge_cases(self):
        agent = ReasoningAgent()
        perf = MagicMock()
        perf.total_impressions = 500000
        perf.ctr = 1.0
        perf.roas = 1.5
        perf.cpa = 50.0
        perf.total_spend = 1000.0
        
        # Consideration
        assert agent._calculate_performance_score(perf, [CampaignObjective.CONSIDERATION]) > 0
        
        # CPA logic
        score_cpa = agent._calculate_performance_score(perf, [CampaignObjective.CONVERSION])
        assert score_cpa > 0

    async def test_json_parse_error_handling(self, mock_openai):
        agent = ReasoningAgent()
        mock_openai.chat.completions.create.return_value.choices = [
            MagicMock(message=MagicMock(content='invalid json'))
        ]
        # Should return empty or default without crashing
        res = await agent._generate_channel_insights(MagicMock(), {}, [])
        assert res["strengths"] == []
        
        # Test _detect_achievements with invalid json
        res_ach = await agent._detect_achievements(MagicMock(), [])
        assert res_ach == []

    @pytest.mark.asyncio
    async def test_calculate_performance_score_all_branches(self):
        agent = ReasoningAgent()
        perf = MagicMock()
        perf.total_impressions = 500000
        perf.total_clicks = 5000
        perf.total_conversions = 100
        perf.total_spend = 1000.0
        perf.ctr = 0.01
        perf.cpc = 0.2
        perf.roas = 2.0
        perf.cpa = 10.0
        
        # Test each objective branch
        for obj in CampaignObjective:
            score = agent._calculate_performance_score(perf, [obj])
            assert 0 <= score <= 100

# --- EnhancedReasoningAgent & PatternDetector Tests ---

class TestEnhancedReasoningAgent:
    
    @pytest.fixture
    def sample_df(self):
        dates = pd.date_range(start="2024-01-01", periods=30)
        df = pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(100, 200, 30),
            'Impressions': np.random.randint(1000, 5000, 30),
            'Clicks': np.random.randint(50, 200, 30),
            'Conversions': np.random.randint(1, 10, 30),
            'Platform': ['Meta Ads'] * 30,
            'Target': ['Audience A'] * 30
        })
        df['CTR'] = df['Clicks'] / df['Impressions']
        df['CPC'] = df['Spend'] / df['Clicks']
        return df

    def test_pattern_detection_trends(self, sample_df):
        detector = PatternDetector()
        
        # 1. Improving Trend (Make it very strong and consistent)
        df_up = sample_df.copy()
        df_up['CTR'] = np.linspace(0.01, 1.0, 30) 
        df_up['CPC'] = np.linspace(10.0, 0.1, 30) # Improving (declining cost)
        res = detector._detect_trends(df_up)
        assert res['detected']
        assert res['direction'] == 'improving'
        
        # 2. Declining Trend
        df_down = sample_df.copy()
        df_down['CTR'] = np.linspace(1.0, 0.01, 30)
        df_down['CPC'] = np.linspace(0.1, 10.0, 30) # Declining (increasing cost)
        res_down = detector._detect_trends(df_down)
        assert res_down['detected']
        assert res_down['direction'] == 'declining'

    def test_pattern_detection_anomalies(self, sample_df):
        detector = PatternDetector()
        df_anomaly = sample_df.copy()
        df_anomaly.loc[15, 'Spend'] = 10000 # Massive spike
        res = detector._detect_anomalies(df_anomaly)
        assert res['detected']
        assert any(a['metric'] == 'Spend' for a in res['anomalies'])

    def test_pattern_detection_fatigue(self, sample_df):
        detector = PatternDetector()
        df_fatigue = sample_df.copy()
        df_fatigue['Frequency'] = 10.0
        df_fatigue['CTR'] = np.linspace(0.05, 0.01, 30) # Sharp decline
        res = detector._detect_creative_fatigue(df_fatigue)
        assert res['detected']
        assert res['severity'] == 'high'

    def test_pattern_detection_saturation(self, sample_df):
        detector = PatternDetector()
        df_sat = sample_df.copy()
        df_sat['Reach'] = np.linspace(1000, 500, 30) # Declining reach
        df_sat['Spend'] = 500 # Constant spend
        res = detector._detect_audience_saturation(df_sat)
        assert res['detected']

    def test_pattern_detection_seasonality(self, sample_df):
        detector = PatternDetector()
        df_season = sample_df.copy()
        # High conversions on Mondays (DayOfWeek 0)
        df_season['Conversions'] = [20 if d.dayofweek == 0 else 1 for d in df_season['Date']]
        res = detector._detect_seasonality(df_season)
        assert res['detected']
        assert res['best_day'] == 'Monday'

    def test_pattern_detection_dayparting(self, sample_df):
        detector = PatternDetector()
        df_dp = sample_df.copy()
        df_dp['Hour'] = [i % 24 for i in range(30)]
        df_dp['Conversions'] = [10 if h == 12 else 1 for h in df_dp['Hour']]
        res = detector._find_day_parting(df_dp)
        assert res['detected']

    def test_pattern_detection_budget_pacing(self, sample_df):
        detector = PatternDetector()
        # Accelerating spend (Very steep)
        df_pace = sample_df.copy()
        df_pace['Spend'] = [i**2 for i in range(30)] # Exponential increase
        res = detector._analyze_budget_pacing(df_pace)
        assert res['detected']
        assert res['status'] == 'accelerating'

    def test_pattern_detection_clusters(self, sample_df):
        detector = PatternDetector()
        # Need > 3 campaigns to have distinct top/bottom 3
        campaigns = [f"C{i}" for i in range(10)]
        df_cluster = pd.DataFrame({
            'Campaign': np.repeat(campaigns, 3),
            'Spend': [100]*30,
            'Conversions': [10]*3 + [1]*3 + [5]*24, # Mixed
            'ROAS': [10.0]*3 + [0.5]*3 + [2.0]*24, # Clear top/bottom
            'CTR': [0.1]*30
        })
        res = detector._identify_performance_clusters(df_cluster)
        assert res['detected']
        assert 'C0' in res['clusters']['high_performers']['campaigns']
        assert 'C1' in res['clusters']['low_performers']['campaigns']

    def test_full_analysis_with_mocks(self, sample_df):
        mock_rag = MagicMock()
        mock_rag.retrieve.return_value = [{"answer": "RAG Insight"}]
        mock_bench = MagicMock()
        mock_bench.get_contextual_benchmarks.return_value = {"CTR": {"status": "good", "actual": 0.05, "benchmark": 0.02}}
        
        # Need campaign_context for benchmarks to trigger
        mock_ctx = MagicMock()
        mock_ctx.business_model.value = "B2B"
        mock_ctx.industry_vertical = "SaaS"
        mock_ctx.geographic_focus = ["Global"]
        
        agent = EnhancedReasoningAgent(rag_retriever=mock_rag, benchmark_engine=mock_bench)
        result = agent.analyze(sample_df, campaign_context=mock_ctx)
        assert 'insights' in result
        assert agent.rag is not None
        assert result['benchmarks_applied'] is not None

    def test_pattern_detection_anomalies_types(self, sample_df):
        detector = PatternDetector()
        df = sample_df.copy()
        # Test different anomaly types
        df.loc[10, 'CTR'] = 0.5 # High CTR anomaly
        df.loc[20, 'CPC'] = 50.0 # High CPC anomaly
        res = detector._detect_anomalies(df)
        assert res['detected']
        assert len(res['anomalies']) >= 2

    def test_internal_insights_generation(self, sample_df):
        agent = EnhancedReasoningAgent()
        patterns = agent.pattern_detector.detect_all(sample_df)
        patterns['trends']['direction'] = 'declining' # Force declining
        patterns['creative_fatigue']['detected'] = True
        patterns['audience_saturation']['detected'] = True
        patterns['day_parting_opportunities']['detected'] = True
        
        benchmarks = {
            "benchmarks": {
                "ctr": {"good": 0.05},
                "cpc": {"good": 0.1}
            }
        }
        
        # Test _generate_insights with various inputs
        insights = agent._generate_insights(sample_df, {}, patterns, benchmarks)
        assert any("declining" in i for i in insights['pattern_insights'])
        assert 'benchmark_comparison' in insights

    def test_internal_recommendations_generation(self, sample_df):
        agent = EnhancedReasoningAgent()
        patterns = {
            'creative_fatigue': {'detected': True, 'evidence': {'recommendation': 'New Creative'}},
            'audience_saturation': {'detected': True, 'recommendation': 'New Audience'},
            'day_parting_opportunities': {'detected': True, 'recommendation': 'New Schedule'},
            'trends': {'detected': True, 'direction': 'declining'}
        }
        insights = {
            'benchmark_comparison': {
                'ctr': {'status': 'needs_work'},
                'cpc': {'status': 'needs_work'}
            }
        }
        
        # Test _generate_recommendations
        recs = agent._generate_recommendations(insights, patterns, True, None, None)
        assert len(recs) >= 5 # Fatigue, Saturation, DayPart, CTR, CPC

    def test_optimization_query_building(self, sample_df):
        agent = EnhancedReasoningAgent()
        patterns = agent.pattern_detector.detect_all(sample_df)
        insights = agent._generate_insights(sample_df, {}, patterns, None)
        query = agent._build_optimization_query(insights, patterns)
        assert "optimization" in query.lower()

# --- ValidatedReasoningAgent Tests ---

class TestValidatedReasoningAgent:
    
    def test_validated_analysis(self):
        agent = ValidatedReasoningAgent()
        
        # Create a df that triggers multiple patterns
        dates = pd.date_range(start="2024-01-01", periods=30)
        df = pd.DataFrame({
            'Date': dates,
            'Spend': np.linspace(100, 500, 30),
            'Impressions': [1000] * 30,
            'Clicks': [50] * 30,
            'Conversions': [5] * 30,
            'CTR': [0.05] * 30,
            'CPC': [2.0] * 30,
            'Platform': ['Google Search'] * 30,
            'Frequency': [8.0] * 30
        })
        
        output = agent.analyze(df)
        assert isinstance(output, AgentOutput)
        assert len(output.insights) > 0
        assert output.metadata.agent_name == "EnhancedReasoningAgent"

    def test_validation_error_fallback(self):
        agent = ValidatedReasoningAgent()
        # Mock _convert_to_schema to fail
        with patch.object(agent, '_convert_to_schema', side_effect=Exception("Validation Fail")):
            output = agent.analyze(pd.DataFrame())
            assert "Validation failed" in output.warnings[0]
            assert output.overall_confidence == 0.5

    def test_pattern_inference(self):
        agent = ValidatedReasoningAgent()
        assert agent._infer_pattern_type("The trend is improving") == PatternType.TREND
        assert agent._infer_pattern_type("detected an anomaly") == PatternType.ANOMALY
        assert agent._infer_pattern_type("creative fatigue is high") == PatternType.CREATIVE_FATIGUE
        assert agent._infer_pattern_type("audience saturation reached") == PatternType.AUDIENCE_SATURATION
        assert agent._infer_pattern_type("best time of day") == PatternType.DAY_PARTING
        assert agent._infer_pattern_type("budget pacing info") == PatternType.BUDGET_PACING
        assert agent._infer_pattern_type("random stuff") is None

    def test_extract_insights_benchmark(self):
        agent = ValidatedReasoningAgent()
        raw = {
            "insights": {
                "benchmark_comparison": {
                    "CTR": {"status": "needs_work", "actual": 0.01, "benchmark": 0.02},
                    "CPC": {"status": "good", "actual": 0.5, "benchmark": 0.8}
                }
            }
        }
        res = agent._extract_insights(raw)
        assert any("below benchmark" in i.text for i in res)

    def test_extract_recommendations_fields(self):
        agent = ValidatedReasoningAgent()
        raw = {
            "recommendations": [
                {
                    "recommendation": "Increase budget",
                    "issue": "High performing campaign",
                    "priority": "critical",
                    "expected_impact": "high",
                    "category": "budget"
                }
            ]
        }
        recs = agent._extract_recommendations(raw)
        assert recs[0].priority == PriorityLevel.CRITICAL
        assert recs[0].expected_impact == "high"

    def test_flatten_metrics_edge(self):
        agent = ValidatedReasoningAgent()
        assert agent._flatten_metrics(None) is None
        assert agent._flatten_metrics("not a dict") is None
        assert agent._flatten_metrics({"val": "string"}) is None # Should skip string
