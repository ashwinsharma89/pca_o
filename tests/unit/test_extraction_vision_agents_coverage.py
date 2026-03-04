
import pytest
import json
import base64
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, date

from src.engine.agents.extraction_agent import ExtractionAgent
from src.engine.agents.vision_agent import VisionAgent
from src.platform.models.platform import (
    PlatformType,
    PlatformSnapshot,
    ExtractedMetric,
    ExtractedChart,
    ExtractedTable,
    MetricType,
    ChartType,
    NormalizedMetric
)
from src.platform.models.campaign import Campaign, DateRange, CampaignObjective

# --- ExtractionAgent Tests ---

class TestExtractionAgent:
    @pytest.fixture
    def agent(self):
        return ExtractionAgent()

    @pytest.fixture
    def sample_snapshot(self):
        return PlatformSnapshot(
            snapshot_id="s1",
            platform=PlatformType.META_ADS,
            campaign_id="c1",
            file_path="/tmp/test.jpg",
            processing_status="completed",
            extracted_metrics=[
                ExtractedMetric(metric_name="Spend", metric_type=MetricType.SPEND, value=500.0, currency="USD"),
                ExtractedMetric(metric_name="Clicks", metric_type=MetricType.CLICKS, value=100.0),
                ExtractedMetric(metric_name="Imprs", metric_type=MetricType.IMPRESSIONS, value=10000.0),
                ExtractedMetric(metric_name="CTR", metric_type=MetricType.CTR, value=150.0)
            ],
            date_range="Jan 2024"
        )

    def test_normalize_campaign_data(self, agent, sample_snapshot):
        campaign = Campaign(
            campaign_id="c1",
            campaign_name="Test",
            objectives=[CampaignObjective.CONVERSION],
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 31)),
            snapshots=[
                sample_snapshot,
                PlatformSnapshot(snapshot_id="s2", platform=PlatformType.GOOGLE_ADS, campaign_id="c1", file_path="...", processing_status="pending")
            ]
        )
        
        updated_campaign = agent.normalize_campaign_data(campaign)
        # Should have metrics from s1, ignore s2
        assert len(updated_campaign.normalized_metrics) == 4
        # Branch 57-59: warnings logging
        assert any("WARNING: Unusual CTR" in log for log in updated_campaign.processing_logs)

    def test_normalize_campaign_data_no_warnings(self, agent):
        # Case with no validation warnings
        campaign = Campaign(
            campaign_id="c1", campaign_name="Test", objectives=[CampaignObjective.CONVERSION],
            date_range=DateRange(start=date(2024, 1, 1), end=date(2024, 1, 31)),
            snapshots=[PlatformSnapshot(
                snapshot_id="s1", platform=PlatformType.META_ADS, campaign_id="c1", file_path="...",
                processing_status="completed",
                extracted_metrics=[ExtractedMetric(metric_name="Spend", metric_type=MetricType.SPEND, value=100.0)]
            )]
        )
        updated = agent.normalize_campaign_data(campaign)
        assert len(updated.normalized_metrics) == 1
        assert len(updated.processing_logs) == 0

    def test_normalize_snapshot_unmapped_metric(self, agent):
        # Branch 73-74: skip unmapped metric
        snapshot = PlatformSnapshot(
            snapshot_id="s1", platform=PlatformType.META_ADS, campaign_id="c1", file_path="...",
            extracted_metrics=[ExtractedMetric(metric_name="Unknown", value=10)]
        )
        res = agent._normalize_snapshot_metrics(snapshot)
        assert len(res) == 0

    def test_validate_metrics_all_branches(self, agent):
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CTR, value=-1.0, source_snapshot_id="s1", original_metric_name="c"), # Neg CTR
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CTR, value=50.0, source_snapshot_id="s1", original_metric_name="c"), # Valid CTR
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.ROAS, value=-1.0, source_snapshot_id="s1", original_metric_name="r"), # Neg ROAS
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=-1.0, source_snapshot_id="s1", original_metric_name="s"), # Neg Spend
            # Duplicates with different values
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.CLICKS, value=10.0, source_snapshot_id="s1", original_metric_name="cl"),
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.CLICKS, value=20.0, source_snapshot_id="s2", original_metric_name="cl"),
        ]
        res = agent._validate_metrics(metrics)
        assert len(res["warnings"]) >= 2
        assert len(res["errors"]) >= 2

    def test_aggregate_metrics(self, agent):
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0, source_snapshot_id="s1", original_metric_name="s"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=200.0, source_snapshot_id="s2", original_metric_name="s"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CTR, value=2.0, source_snapshot_id="s1", original_metric_name="c"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CTR, value=4.0, source_snapshot_id="s2", original_metric_name="c")
        ]
        
        # Agg by platform
        agg_p = agent.aggregate_metrics(metrics, by="platform")
        assert agg_p["meta_ads"][MetricType.SPEND] == 300.0
        assert agg_p["meta_ads"][MetricType.CTR] == 3.0 # Average
        
        # Agg by type
        agg_t = agent.aggregate_metrics(metrics, by="metric_type")
        assert agg_t["spend"][MetricType.SPEND] == 300.0
        
        with pytest.raises(ValueError, match="Invalid aggregation key"):
            agent.aggregate_metrics(metrics, by="invalid")

    def test_calculate_derived_metrics(self, agent):
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.IMPRESSIONS, value=1000.0, source_snapshot_id="s1", original_metric_name="i"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CLICKS, value=50.0, source_snapshot_id="s1", original_metric_name="c"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0, source_snapshot_id="s1", original_metric_name="s", currency="USD"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CONVERSIONS, value=5.0, source_snapshot_id="s1", original_metric_name="conv")
        ]
        derived = agent.calculate_derived_metrics(metrics)
        # Expected: CTR (5%), CPC (2.0), CPA (20.0), ConvRate (10%)
        types = [d.metric_type for d in derived]
        assert all(t in types for t in [MetricType.CTR, MetricType.CPC, MetricType.CPA, MetricType.CONVERSION_RATE])
        
        # Branch 238->251 etc: skip if already present
        metrics.append(NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CTR, value=5.0, source_snapshot_id="s1", original_metric_name="ctr"))
        derived_v2 = agent.calculate_derived_metrics(metrics)
        assert not any(d.metric_type == MetricType.CTR for d in derived_v2) # Should skip CTR now

    def test_aggregate_metrics_averaging(self, agent):
        # 183->160: Ensure rate metrics are averaged
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CONVERSION_RATE, value=10.0, source_snapshot_id="s1", original_metric_name="cr"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.CONVERSION_RATE, value=20.0, source_snapshot_id="s2", original_metric_name="cr")
        ]
        agg = agent.aggregate_metrics(metrics, by="platform")
        assert agg["meta_ads"][MetricType.CONVERSION_RATE] == 15.0

    def test_get_platform_summary(self, agent):
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0, source_snapshot_id="s1", original_metric_name="s"),
            NormalizedMetric(platform=PlatformType.GOOGLE_ADS, metric_type=MetricType.SPEND, value=200.0, source_snapshot_id="s1", original_metric_name="s")
        ]
        summary = agent.get_platform_summary(metrics, PlatformType.META_ADS)
        assert summary == {"spend": 100.0}

    def test_validate_metrics_duplicates_same_value(self, agent):
        # Branch 112->109: duplicates but same value (no warning)
        metrics = [
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0, source_snapshot_id="s1", original_metric_name="s"),
            NormalizedMetric(platform=PlatformType.META_ADS, metric_type=MetricType.SPEND, value=100.0, source_snapshot_id="s2", original_metric_name="s")
        ]
        res = agent._validate_metrics(metrics)
        assert len(res["warnings"]) == 0

# --- VisionAgent Tests ---

class TestVisionAgent:
    @pytest.fixture
    def mock_openai(self):
        with patch("src.engine.agents.vision_agent.AsyncOpenAI") as mock:
            client = mock.return_value
            client.chat.completions.create = AsyncMock()
            yield client

    @pytest.fixture
    def mock_anthropic(self):
        with patch("src.engine.agents.vision_agent.create_async_anthropic_client") as mock:
            client = mock.return_value
            client.messages.create = AsyncMock()
            yield client

    @pytest.fixture
    def agent(self, mock_openai):
        return VisionAgent(provider="openai")

    @pytest.mark.asyncio
    async def test_init(self, mock_openai, mock_anthropic):
        # OpenAI
        v = VisionAgent(provider="openai")
        assert v.provider == "openai"
        
        # Anthropic
        v_ant = VisionAgent(provider="anthropic")
        assert v_ant.provider == "anthropic"
        
        # Invalid
        with pytest.raises(ValueError, match="Unsupported provider"):
            VisionAgent(provider="invalid")

    @pytest.mark.asyncio
    async def test_analyze_snapshot_detect_platform_branch(self, agent, mock_openai):
        # Branch 76->81: detected_platform is None
        with patch.object(agent, "_load_image", return_value="base64data"):
             mock_openai.chat.completions.create.return_value = MagicMock(choices=[MagicMock(message=MagicMock(content="meta_ads"))])
             snapshot = PlatformSnapshot(snapshot_id="s1", platform=PlatformType.META_ADS, campaign_id="c1", file_path="...")
             # Clear detected_platform
             snapshot.detected_platform = None
             
             # Mock the other internal calls to avoid massive side_effects
             agent._extract_metrics = AsyncMock(return_value=[])
             agent._extract_charts = AsyncMock(return_value=[])
             agent._extract_tables = AsyncMock(return_value=[])
             agent._extract_metadata = AsyncMock(return_value={})
             
             await agent.analyze_snapshot(snapshot)
             assert snapshot.detected_platform == PlatformType.META_ADS

    @pytest.mark.asyncio
    async def test_detect_platform_unknown(self, agent, mock_openai):
        # Branch 137-139: unknown platform string
        mock_openai.chat.completions.create.return_value = MagicMock(choices=[MagicMock(message=MagicMock(content="unknown_app"))])
        res = await agent._detect_platform("data")
        assert res is None

    @pytest.mark.asyncio
    async def test_json_parsing_errors(self, agent):
        # 326-328: _extract_tables error
        with patch.object(agent, "_call_vision_model", return_value="invalid"):
            res = await agent._extract_tables("data", PlatformType.META_ADS)
            assert res == []
            
            # 351-352: _extract_metadata error
            res_m = await agent._extract_metadata("data")
            assert res_m == {}

    @pytest.mark.asyncio
    async def test_provider_branch_anthropic(self, mock_anthropic):
        # 358-359: anthropic branch in _call_vision_model
        agent = VisionAgent(provider="anthropic")
        mock_anthropic.messages.create.return_value = MagicMock(content=[MagicMock(text="resp")])
        res = await agent._call_vision_model("data", "prompt")
        assert res == "resp"
        
        # Coverage for 50: init failure (mock settings to have no key or similar)
        with patch("src.engine.agents.vision_agent.create_async_anthropic_client", return_value=None):
            with pytest.raises(ValueError, match="Failed to initialize Anthropic"):
                VisionAgent(provider="anthropic")

    def test_map_metric_type_none_config(self, agent):
        # 423: PLATFORM_CONFIGS.get(platform) is None
        # We need an invalid platform type that bypasses enum if possible or just use a mock
        res = agent._map_metric_type("clicks", "invalid_platform")
        assert res is None

    @pytest.mark.asyncio
    async def test_vision_extraction_happy_paths(self, agent, mock_openai):
        # 147-212: _extract_metrics
        # 220-275: _extract_charts
        # 314-324: _extract_tables
        # 314-324: _extract_metadata
        mock_openai.chat.completions.create.side_effect = [
            MagicMock(choices=[MagicMock(message=MagicMock(content='[{"metric_name": "Cost", "value": 100}]'))]),
            MagicMock(choices=[MagicMock(message=MagicMock(content='[{"chart_type": "line_chart", "title": "T"}]'))]),
            MagicMock(choices=[MagicMock(message=MagicMock(content='[{"headers": ["A"], "rows": [["1"]], "confidence": 0.8}]'))]), # _extract_tables
            MagicMock(choices=[MagicMock(message=MagicMock(content='{"date_range": "2024"}'))])
        ]
        
        m = await agent._extract_metrics("img", PlatformType.GOOGLE_ADS)
        assert len(m) == 1
        
        c = await agent._extract_charts("img", PlatformType.GOOGLE_ADS)
        assert len(c) == 1

        t = await agent._extract_tables("img", PlatformType.GOOGLE_ADS)
        assert len(t) == 1
        
        meta = await agent._extract_metadata("img")
        assert meta["date_range"] == "2024"

    def test_load_image_real(self, agent):
        # 416-417: test real base64 encoding
        import os
        path = "/tmp/test_img.jpg"
        with open(path, "wb") as f:
            f.write(b"fake_jpeg_data")
        try:
            res = agent._load_image(path)
            assert res == base64.b64encode(b"fake_jpeg_data").decode("utf-8")
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_map_metric_type(self, agent):
        # Platform specific
        m1 = agent._map_metric_type("impr.", PlatformType.GOOGLE_ADS)
        assert m1 == MetricType.IMPRESSIONS
        
        # Direct enum match
        m2 = agent._map_metric_type("clicks", PlatformType.META_ADS)
        assert m2 == MetricType.CLICKS
        
        # Unknown
        m3 = agent._map_metric_type("whatever", PlatformType.META_ADS)
        assert m3 is None
