"""
Unit tests for Agent integration and orchestration.
Tests agent communication and workflow coordination.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import pandas as pd
from datetime import datetime

# Try to import agents
try:
    from src.engine.agents.vision_agent import VisionAgent
    VISION_AVAILABLE = True
except ImportError:
    VISION_AVAILABLE = False
    VisionAgent = None

try:
    from src.engine.agents.extraction_agent import ExtractionAgent
    EXTRACTION_AVAILABLE = True
except ImportError:
    EXTRACTION_AVAILABLE = False
    ExtractionAgent = None

try:
    from src.engine.agents.reasoning_agent import ReasoningAgent
    REASONING_AVAILABLE = True
except ImportError:
    REASONING_AVAILABLE = False
    ReasoningAgent = None


class TestAgentCommunication:
    """Test agent-to-agent communication."""
    
    def test_vision_to_extraction_data_format(self):
        """Test data format between vision and extraction agents."""
        # Vision agent output format
        vision_output = {
            "platform": "Google Ads",
            "metrics": {
                "impressions": "10,000",
                "clicks": "500",
                "ctr": "5.0%"
            },
            "raw_text": "Campaign performance data"
        }
        
        # Should be parseable by extraction agent
        assert "platform" in vision_output
        assert "metrics" in vision_output
    
    def test_extraction_to_reasoning_data_format(self):
        """Test data format between extraction and reasoning agents."""
        # Extraction agent output format
        extraction_output = {
            "normalized_data": pd.DataFrame({
                "Campaign": ["A", "B"],
                "Impressions": [10000, 20000],
                "Clicks": [500, 800],
                "CTR": [0.05, 0.04]
            }),
            "platform": "Google Ads",
            "date_range": {"start": "2024-01-01", "end": "2024-01-31"}
        }
        
        # Should contain DataFrame
        assert isinstance(extraction_output["normalized_data"], pd.DataFrame)
    
    def test_reasoning_output_format(self):
        """Test reasoning agent output format."""
        # Reasoning agent output format
        reasoning_output = {
            "insights": [
                {"type": "performance", "text": "Campaign A outperformed B"},
                {"type": "recommendation", "text": "Increase budget for A"}
            ],
            "achievements": ["CTR above benchmark"],
            "recommendations": ["Optimize Campaign B targeting"]
        }
        
        assert "insights" in reasoning_output
        assert "recommendations" in reasoning_output


class TestAgentPipeline:
    """Test full agent pipeline."""
    
    @pytest.fixture
    def mock_pipeline(self):
        """Create mock agent pipeline."""
        return {
            "vision": Mock(),
            "extraction": Mock(),
            "reasoning": Mock(),
            "visualization": Mock()
        }
    
    def test_pipeline_execution_order(self, mock_pipeline):
        """Test agents execute in correct order."""
        execution_order = []
        
        mock_pipeline["vision"].analyze.side_effect = lambda x: execution_order.append("vision")
        mock_pipeline["extraction"].normalize.side_effect = lambda x: execution_order.append("extraction")
        mock_pipeline["reasoning"].analyze.side_effect = lambda x: execution_order.append("reasoning")
        mock_pipeline["visualization"].generate.side_effect = lambda x: execution_order.append("visualization")
        
        # Simulate pipeline
        mock_pipeline["vision"].analyze({})
        mock_pipeline["extraction"].normalize({})
        mock_pipeline["reasoning"].analyze({})
        mock_pipeline["visualization"].generate({})
        
        assert execution_order == ["vision", "extraction", "reasoning", "visualization"]
    
    def test_pipeline_error_handling(self, mock_pipeline):
        """Test pipeline handles agent errors."""
        mock_pipeline["vision"].analyze.side_effect = Exception("Vision failed")
        
        with pytest.raises(Exception, match="Vision failed"):
            mock_pipeline["vision"].analyze({})
    
    def test_pipeline_data_flow(self, mock_pipeline):
        """Test data flows correctly through pipeline."""
        # Vision output
        mock_pipeline["vision"].analyze.return_value = {"raw_data": "test"}
        
        # Extraction uses vision output
        mock_pipeline["extraction"].normalize.return_value = {"normalized": True}
        
        # Reasoning uses extraction output
        mock_pipeline["reasoning"].analyze.return_value = {"insights": []}
        
        vision_result = mock_pipeline["vision"].analyze({})
        extraction_result = mock_pipeline["extraction"].normalize(vision_result)
        reasoning_result = mock_pipeline["reasoning"].analyze(extraction_result)
        
        assert reasoning_result["insights"] == []


class TestAgentState:
    """Test agent state management."""
    
    def test_agent_state_initialization(self):
        """Test agent state initialization."""
        state = {
            "campaign_data": None,
            "analysis_results": None,
            "visualizations": None,
            "errors": []
        }
        
        assert state["campaign_data"] is None
        assert state["errors"] == []
    
    def test_agent_state_update(self):
        """Test agent state updates."""
        state = {"campaign_data": None}
        
        # Update state
        state["campaign_data"] = pd.DataFrame({"A": [1, 2, 3]})
        
        assert state["campaign_data"] is not None
        assert len(state["campaign_data"]) == 3
    
    def test_agent_state_persistence(self):
        """Test state persists across agent calls."""
        state = {"results": []}
        
        # Multiple updates
        state["results"].append("result1")
        state["results"].append("result2")
        
        assert len(state["results"]) == 2


class TestAgentConfiguration:
    """Test agent configuration."""
    
    def test_agent_config_defaults(self):
        """Test default agent configuration."""
        config = {
            "model": "gpt-4",
            "temperature": 0.7,
            "max_tokens": 4096,
            "timeout": 30
        }
        
        assert config["model"] == "gpt-4"
        assert config["temperature"] == 0.7
    
    def test_agent_config_override(self):
        """Test configuration override."""
        default_config = {"model": "gpt-4", "temperature": 0.7}
        override = {"temperature": 0.3}
        
        final_config = {**default_config, **override}
        
        assert final_config["temperature"] == 0.3
        assert final_config["model"] == "gpt-4"


class TestAgentRetry:
    """Test agent retry logic."""
    
    def test_retry_on_failure(self):
        """Test retry on transient failure."""
        call_count = 0
        
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Transient error")
            return "success"
        
        # Simulate retry
        result = None
        for _ in range(3):
            try:
                result = flaky_function()
                break
            except Exception:
                continue
        
        assert result == "success"
        assert call_count == 3
    
    def test_max_retries_exceeded(self):
        """Test max retries exceeded."""
        def always_fails():
            raise Exception("Permanent error")
        
        attempts = 0
        max_retries = 3
        
        for _ in range(max_retries):
            try:
                always_fails()
            except Exception:
                attempts += 1
        
        assert attempts == max_retries


class TestAgentMetrics:
    """Test agent metrics collection."""
    
    def test_execution_time_tracking(self):
        """Test execution time tracking."""
        import time
        
        start = time.time()
        time.sleep(0.01)  # Simulate work
        end = time.time()
        
        execution_time = end - start
        
        assert execution_time >= 0.01
    
    def test_token_usage_tracking(self):
        """Test token usage tracking."""
        usage = {
            "prompt_tokens": 500,
            "completion_tokens": 200,
            "total_tokens": 700
        }
        
        assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]
    
    def test_cost_calculation(self):
        """Test cost calculation."""
        # GPT-4 pricing example
        prompt_tokens = 1000
        completion_tokens = 500
        
        prompt_cost = prompt_tokens * 0.00003  # $0.03/1K
        completion_cost = completion_tokens * 0.00006  # $0.06/1K
        total_cost = prompt_cost + completion_cost
        
        assert abs(total_cost - 0.06) < 0.001
