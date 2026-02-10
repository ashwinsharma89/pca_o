
import pytest
import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, mock_open, ANY
from pydantic import ValidationError
from src.engine.agents.schemas import (
    AgentOutput, AgentInsight, AgentRecommendation, DetectedPattern, 
    ConfidenceLevel, PriorityLevel, PatternType,
    filter_high_confidence_insights, filter_high_confidence_recommendations, filter_by_priority
)
from src.engine.agents.state import AgentState
from src.engine.agents.agent_memory import AgentMemory, MemoryEntry
from src.engine.agents.shared_context import SharedContext, get_shared_context, reset_shared_context

# --- Schemas Tests ---

class TestAgentSchemas:
    def test_agent_insight_validation(self):
        # Valid insight
        insight = AgentInsight(text="This is a valid insight text with sufficient length.", confidence=0.9)
        assert insight.confidence_level == ConfidenceLevel.HIGH
        
        # Auto-set confidence levels
        i_med = AgentInsight(text="Valid text string.", confidence=0.6)
        assert i_med.confidence_level == ConfidenceLevel.MEDIUM
        
        i_low = AgentInsight(text="Valid text string.", confidence=0.3)
        assert i_low.confidence_level == ConfidenceLevel.LOW

    def test_agent_recommendation_validation(self):
        rec = AgentRecommendation(
            action="Increase budget for high performing campaign",
            rationale="ROAS is above target and CPAs are stable.",
            priority=1,
            confidence=0.9
        )
        assert rec.priority == PriorityLevel.CRITICAL
        
        # Invalid priority raises error (handled by pydantic, but let's check behavior if passed as int)
        rec2 = AgentRecommendation(
            action="Action text here",
            rationale="Rational text here long enough",
            priority=5,
            confidence=0.8
        )
        assert rec2.priority == PriorityLevel.OPTIONAL
        
        with pytest.raises(ValueError):
            AgentRecommendation(
                action="Action", rationale="Rationale", priority=6, confidence=1
            )

        # Pass enum directly
        rec_enum = AgentRecommendation(
            action="Action text here",
            rationale="Rational text here long enough",
            priority=PriorityLevel.HIGH, # Passing enum directly
            confidence=0.8
        )
        assert rec_enum.priority == PriorityLevel.HIGH

    def test_agent_output_validation(self):
        # Must have at least one Item
        with pytest.raises(ValidationError) as excinfo:
            AgentOutput(
                metadata={"agent_name": "test"},
                overall_confidence=0.9,
                insights=[], recommendations=[], patterns=[]
            )
        assert "must contain at least one" in str(excinfo.value)
            
        # Overall confidence calculation
        param_meta = {"agent_name": "test"}
        out = AgentOutput(
            metadata=param_meta,
            overall_confidence=None, # Should trigger calculator
            insights=[AgentInsight(text="Insight text here", confidence=0.8)],
            recommendations=[AgentRecommendation(action="Action text", rationale="Rationale text must be longer than twenty chars", confidence=0.6, priority=3)]
        )
        # (0.8 + 0.6) / 2 = 0.7
        assert out.overall_confidence == 0.7
        
        # Test Default Confidence (if lists empty but bypass validator logic via direct call or mocking)
        # Actually simplest way is to use a valid object but force empty lists in `info.data` simulation
        # But since validated `after` prevents empty, we can just trust 97% coverage here is fine, OR
        # pass explicitly patterns which are not used in calculation
        out_pat = AgentOutput(
            metadata=param_meta,
            overall_confidence=None,
            patterns=[DetectedPattern(pattern_type=PatternType.TREND, description="Pattern desc", confidence=0.5)],
            insights=[], recommendations=[]
        )
        assert out_pat.overall_confidence == 0.5 # Default fallback

        # Test Vision Agent Output
        from src.engine.agents.schemas import VisionAgentOutput, VisionAgentMetric, MetricType
        with pytest.raises(ValidationError):
            VisionAgentOutput(
                platform="meta",
                metrics=[], # Empty metrics should fail
                overall_confidence=0.9
            )
            
        # Valid vision output
        v = VisionAgentOutput(
            platform="meta",
            metrics=[VisionAgentMetric(name="spend", value=100.0, metric_type=MetricType.CURRENCY, confidence=0.95)],
            overall_confidence=0.95
        )
        assert v.platform == "meta"

    def test_schema_filters(self):
        out = AgentOutput(
            metadata={"agent_name": "test"},
            overall_confidence=0.9,
            insights=[
                AgentInsight(text="High Confidence Insight", confidence=0.9),
                AgentInsight(text="Low Confidence Insight", confidence=0.4)
            ],
            recommendations=[
                AgentRecommendation(action="Action A with sufficient length", rationale="Rationale A must be longer than twenty chars for validation", confidence=0.9, priority=1),
                AgentRecommendation(action="Action B with sufficient length", rationale="Rationale B must be longer than twenty chars for validation", confidence=0.4, priority=4)
            ]
        )
        high = filter_high_confidence_insights(out, 0.8)
        assert len(high) == 1
        assert high[0].text == "High Confidence Insight"
        
        # Rec filters
        rec_filt = filter_high_confidence_recommendations(out, 0.8)
        assert len(rec_filt) == 1
        
        prio_filt = filter_by_priority(out, 2)
        assert len(prio_filt) == 1
        assert prio_filt[0].priority == PriorityLevel.CRITICAL


# --- State Tests ---

class TestAgentState:
    def test_state_TypedDict(self):
        # Just verifying structure availability
        state: AgentState = {
            "messages": [],
            "next": None,
            "campaign_data": {},
            "reports": [],
            "images": [],
            "errors": []
        }
        assert state['messages'] == []


# --- Agent Memory Tests ---

class TestAgentMemory:
    @pytest.fixture
    def memory_file(self):
        with patch('src.engine.agents.agent_memory.REDIS_AVAILABLE', False):
            mem = AgentMemory(user_id="u1")
            return mem

    @pytest.fixture
    def memory_redis(self):
        with patch('src.engine.agents.agent_memory.REDIS_AVAILABLE', True), \
             patch('src.engine.agents.agent_memory.redis.from_url') as mock_redis:
            mem = AgentMemory(user_id="u2", session_id="s2")
            mem._redis = mock_redis.return_value
            return mem

    def test_init_generates_session(self):
        with patch('src.engine.agents.agent_memory.REDIS_AVAILABLE', False):
            mem = AgentMemory(user_id="u1")
            assert mem.session_id is not None
            assert len(mem.session_id) == 16

    def test_init_redis_fail(self):
        with patch('src.engine.agents.agent_memory.REDIS_AVAILABLE', True), \
             patch('src.engine.agents.agent_memory.redis.from_url', side_effect=Exception("Conn fail")):
            mem = AgentMemory(user_id="u1")
            assert mem._redis is None

    def test_load_user_context_integration(self, memory_file):
        # Mock recall
        with patch.object(memory_file, 'recall') as mock_recall:
            mock_recall.side_effect = [
                # First call: preferences
                [MemoryEntry(id="1", user_id="u", session_id="s", memory_type="preference", content={"theme": "dark"})],
                # Second call: insights
                [MemoryEntry(id="2", user_id="u", session_id="s", memory_type="insight", content={"k": "v"})]
            ]
            memory_file._load_user_context()
            assert memory_file.context.user_preferences["theme"] == "dark"
            assert len(memory_file.context.analysis_context["recent_insights"]) == 1

    def test_remember_file_backend(self, memory_file):
        content = {"k": "v"}
        with patch("builtins.open", mock_open(read_data="[]")) as mock_file:
            # Need to mock exists to True for append logic, or False to start fresh
            with patch("pathlib.Path.exists", return_value=False):
                with patch("pathlib.Path.mkdir"):
                    entry = memory_file.remember("insight", content)
                    assert entry.content == content
                    # Verify write
                    mock_file.assert_called()

    def test_recall_file_backend(self, memory_file):
        fake_data = [{
            "id": "1", "user_id": "u1", "session_id": "s1", "memory_type": "insight",
            "content": {"a": 1}, "timestamp": datetime.utcnow().isoformat(),
            "ttl_hours": 24, "importance": 0.8
        }]
        
        with patch("builtins.open", mock_open(read_data=json.dumps(fake_data))):
            with patch("pathlib.Path.exists", return_value=True):
                res = memory_file.recall("insight")
                assert len(res) == 1
                assert res[0].content["a"] == 1

    def test_forget_file_backend(self, memory_file):
        fake_data = [
            {"id": "1", "memory_type": "insight", "timestamp": datetime.utcnow().isoformat(), "ttl_hours": 1},
            {"id": "2", "memory_type": "context", "timestamp": datetime.utcnow().isoformat(), "ttl_hours": 1}
        ]
        with patch("builtins.open", mock_open(read_data=json.dumps(fake_data))) as mock_file:
            with patch("pathlib.Path.exists", return_value=True):
                 # Forget by ID
                 memory_file.forget(memory_id="1")
                 
                 # Forget by Type
                 memory_file.forget(memory_type="context")
                 
                 # We assume writes happened. 
                 mock_file.return_value.write.assert_called()

    def test_redis_backend_operations(self, memory_redis):
        # Remember
        memory_redis.remember("test", {"a": 1})
        memory_redis._redis.setex.assert_called()
        
        # Recall
        # Mock scan/keys and get
        memory_redis._redis.keys.return_value = ["key1"]
        entry_json = json.dumps({
            "id": "1", "user_id": "u2", "session_id": "s2", "memory_type": "test",
            "content": {"a": 1}, "timestamp": datetime.utcnow().isoformat(),
            "ttl_hours": 24, "importance": 0.5
        })
        memory_redis._redis.get.return_value = entry_json
        
        items = memory_redis.recall("test")
        assert len(items) == 1
        
        # Not found case
        memory_redis._redis.get.return_value = None
        assert len(memory_redis.recall("test")) == 0
        
        # Forget by ID
        memory_redis.forget(memory_id="1")
        memory_redis._redis.delete.assert_called()
        
        # Forget by Type
        memory_redis.forget(memory_type="test")
        # Should call keys then delete loop
        assert memory_redis._redis.keys.called

    def test_context_management(self, memory_file):
        # Overflow messages
        for i in range(25):
            memory_file.add_message("user", f"msg {i}")
        assert len(memory_file.context.messages) == 20
        assert memory_file.context.messages[-1]["content"] == "msg 24"
        
        memory_file.set_campaign_context("Camp A", "Meta")
        assert memory_file.context.current_campaign == "Camp A"
        assert memory_file.context.current_platform == "Meta"
        
        # Update preferences
        with patch.object(memory_file, 'remember'):
            memory_file.update_preferences({"lang": "en"})
            assert memory_file.context.user_preferences["lang"] == "en"

    def test_save_session(self, memory_file):
        with patch.object(memory_file, 'remember') as mock_rem:
            memory_file.save_session()
            mock_rem.assert_called_with("context", ANY, importance=0.6, ttl_hours=24)

    def test_global_factory(self):
        # Ensure Redis doesn't try to connect
        with patch('src.engine.agents.agent_memory.REDIS_AVAILABLE', False):
            from src.engine.agents.agent_memory import get_agent_memory, _memories
            _memories.clear()
            m1 = get_agent_memory("u1")
            m2 = get_agent_memory("u1")
            assert m1 is m2
            m3 = get_agent_memory("u1", "s2")
            assert m1 is not m3

    def test_file_storage_init(self, memory_file):
        # Test loading when file does NOT exist
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.exists", return_value=False):
                res = memory_file.recall() # Should return empty list
                assert res == []
                
        # Test storing when file does NOT exist (creates new list)
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.exists", return_value=False):
                memory_file.remember("type", {})
                # It opens 'w' directly.
                mock_file.assert_called()

    def test_context_platform_none(self, memory_file):
        memory_file.set_campaign_context("Cam", None)
        assert memory_file.context.current_platform is None

    def test_summary_generation_extended(self, memory_file):
        # Reset context to ensure clean state
        memory_file.context.current_campaign = None
        memory_file.context.current_platform = None
        memory_file.context.user_preferences = {}
        memory_file.context.analysis_context = {}
        
        # Empty summary
        assert "New session" == memory_file.get_summary()
        
        # Partial summary pieces
        memory_file.set_campaign_context("C1", "Meta") # Both
        s = memory_file.get_summary()
        assert "Currently analyzing: C1" in s
        assert "Platform: Meta" in s
        
        memory_file.update_preferences({"k": "v"})
        assert "User preferences: k" in memory_file.get_summary()
        
        # Mock insights
        memory_file.context.analysis_context["recent_insights"] = ["i1"]
        assert "Recent insights: 1" in memory_file.get_summary()

    def test_memory_filters(self, memory_file):
        # Recall importance filter
        with patch.object(memory_file, '_load_all') as mock_load:
            mock_load.return_value = [
                MemoryEntry(id="1", user_id="u", session_id="s", memory_type="t", content={}, importance=0.1, timestamp=datetime.utcnow()),
                MemoryEntry(id="2", user_id="u", session_id="s", memory_type="t", content={}, importance=0.9, timestamp=datetime.utcnow())
            ]
            res = memory_file.recall(min_importance=0.5)
            assert len(res) == 1
            assert res[0].id == "2"


# --- Shared Context Tests ---

class TestSharedContext:
    def test_singleton(self):
        reset_shared_context()
        ctx1 = get_shared_context()
        ctx2 = get_shared_context()
        assert ctx1 is ctx2
        
    def test_data_operations(self):
        ctx = SharedContext()
        ctx.add_data("df_metrics", {"data": 1}, source="agent1")
        assert ctx.has_data("df_metrics")
        assert ctx.get_data("df_metrics") == {"data": 1}
        assert "df_metrics" in ctx.get_data_keys()
        
    def test_insight_operations(self):
        ctx = SharedContext()
        ctx.add_insight("agent1", "Growth detected")
        insights = ctx.get_insights()
        assert len(insights) == 1
        assert insights[0]['source'] == "agent1"
        
        # Filter source
        assert len(ctx.get_insights(source="agent2")) == 0
        
    def test_anomaly_operations(self):
        ctx = SharedContext()
        ctx.add_anomaly("agent1", "Spike", severity="critical")
        ctx.add_anomaly("agent1", "Noise", severity="low")
        
        crit = ctx.get_anomalies(min_severity="high")
        assert len(crit) == 1
        assert crit[0]['description'] == "Spike"
        
    def test_recommendation_operations(self):
        ctx = SharedContext()
        ctx.add_recommendation("agent1", "Fix it", priority="high")
        
        recs = ctx.get_recommendations(priority="high")
        assert len(recs) == 1
        assert ctx.get_recommendations(priority="low") == []

    def test_query_history_focus(self, caplog):
        ctx = SharedContext()
        import logging
        with caplog.at_level(logging.INFO):
            ctx.add_query("How is Meta performing?", {})
        assert "Meta campaigns" in ctx.get_current_focus()
        assert ctx.get_last_query() == "How is Meta performing?"
        assert ctx.get_last_result() == {}
        
        # Metric Focus
        ctx.add_query("Analyze CPA", {})
        assert "CPA analysis" in ctx.get_current_focus()

        # No focus match
        ctx.add_query("Hello world", {})
        # Should retain previous or change? Logic says `_current_focus` is only set if match found.
        # But `add_query` calls `_update_focus`.
        # If no match, `_current_focus` remains unchanged?
        # Let's check logic: _update_focus returns if match. If loop finishes, it does nothing?
        # Yes.
        assert "CPA analysis" in ctx.get_current_focus()

        # Insight object logging
        ctx.add_insight("agent", {"obj": "insight"}) # Not string

    def test_summary_export(self):
        ctx = SharedContext()
        ctx.add_data("k", "v")
        summary = ctx.get_summary()
        assert summary['data_cached'] == 1
        
        full_dict = ctx.to_dict()
        assert 'insights' in full_dict
        
    def test_clear(self):
        ctx = SharedContext()
        ctx.add_data("k", "v")
        ctx.clear()
        assert not ctx.has_data("k")
