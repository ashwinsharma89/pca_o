"""
Tests for LLM Router coverage (Phase 5.1).
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import os
from datetime import datetime

from src.core.config.llm_router import LLMRouter, TaskType, ModelPerformanceTracker

class TestLLMRouter:
    """Tests for LLM routing and calling logic."""
    
    def test_get_model_config(self):
        """Test configuration retrieval for valid/invalid types."""
        config = LLMRouter.get_model_config(TaskType.SQL_GENERATION)
        assert config['provider'] == 'openai'
        assert config['model'] == 'gpt-4'
        
        with pytest.raises(ValueError, match="Unknown task type"):
            # Using a mock for an enum that doesn't exist in MODEL_MAPPING if applicable,
            # but since TaskType is an Enum, we can just try to hack it or use a known one missing if any.
            # All TaskTypes are in the mapping. Let's pass a mock.
            LLMRouter.get_model_config(Mock())

    @patch('src.core.config.llm_router.create_anthropic_client')
    def test_get_client_anthropic(self, mock_create):
        """Test Anthropic client retrieval."""
        mock_client = Mock()
        mock_create.return_value = mock_client
        
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client, model, config = LLMRouter.get_client(TaskType.QUERY_INTERPRETATION)
            assert client == mock_client
            assert model == "claude-3-5-sonnet-20241022"
            mock_create.assert_called_once_with("test-key")

    @patch('openai.OpenAI')
    def test_get_client_openai(self, mock_openai):
        """Test OpenAI client retrieval."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            client, model, config = LLMRouter.get_client(TaskType.SQL_GENERATION)
            assert client == mock_client
            assert model == "gpt-4"

    @patch('google.generativeai.configure')
    @patch('google.generativeai.GenerativeModel')
    def test_get_client_google(self, mock_model, mock_conf):
        """Test Google client retrieval."""
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "test-key"}):
            client, model, config = LLMRouter.get_client(TaskType.EVALUATION)
            assert client.__name__ == 'google.generativeai'
            assert model == "gemini-2.0-flash-exp"
            mock_conf.assert_called_once_with(api_key="test-key")

    @patch('src.core.config.llm_router.LLMRouter.get_client')
    def test_call_llm_openai(self, mock_get_client):
        """Test calling OpenAI model."""
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value.choices[0].message.content = "SQL result"
        mock_get_client.return_value = (mock_client, "gpt-4", {"provider": "openai", "model": "gpt-4"})
        
        response = LLMRouter.call_llm(TaskType.SQL_GENERATION, "Write SQL")
        assert response == "SQL result"
        assert mock_client.chat.completions.create.called

    @patch('src.core.config.llm_router.LLMRouter.get_client')
    def test_call_llm_anthropic(self, mock_get_client):
        """Test calling Anthropic model."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value.content[0].text = "Claude result"
        mock_get_client.return_value = (mock_client, "claude-3", {"provider": "anthropic", "model": "claude-3"})
        
        response = LLMRouter.call_llm(TaskType.QUERY_INTERPRETATION, "Analyze this")
        assert response == "Claude result"
        assert mock_client.messages.create.called

    def test_get_cost_estimate(self):
        """Test cost calculation logic."""
        # GPT-4: $30/1M input, $60/1M output
        # 1M input + 1M output = $90
        cost = LLMRouter.get_cost_estimate(TaskType.SQL_GENERATION, 1000000, 1000000)
        assert cost == 90.0
        
        # Test unknown model default
        with patch.dict(LLMRouter.MODEL_MAPPING[TaskType.SQL_GENERATION], {"model": "unknown"}):
            assert LLMRouter.get_cost_estimate(TaskType.SQL_GENERATION, 1, 1) == 0.0

class TestModelPerformanceTracker:
    """Tests for performance tracking analytics."""
    
    def test_log_and_summary(self):
        tracker = ModelPerformanceTracker()
        
        # Log a successful call
        tracker.log_call(
            task_type=TaskType.SQL_GENERATION,
            response_time=1.5,
            input_tokens=100,
            output_tokens=50,
            success=True
        )
        
        # Log a failed call
        tracker.log_call(
            task_type=TaskType.SQL_GENERATION,
            response_time=0.5,
            input_tokens=10,
            output_tokens=0,
            success=False
        )
        
        summary = tracker.get_summary()
        stats = summary[TaskType.SQL_GENERATION.value]
        
        assert stats['total_calls'] == 2
        assert stats['success_rate'] == 0.5
        assert stats['avg_response_time'] == 1.0 # (1.5 + 0.5) / 2
        assert stats['total_cost'] > 0
