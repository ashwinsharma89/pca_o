"""
Unit tests for LLM Rate Limiting.
Tests rate-limited LLM client wrappers.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import time


class TestWithLLMRateLimit:
    """Tests for with_llm_rate_limit decorator."""
    
    def test_decorator_calls_function(self):
        """Test decorator calls wrapped function."""
        with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
            with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                from src.engine.intelligence.llm_utils import with_llm_rate_limit
                
                @with_llm_rate_limit("openai")
                def test_func():
                    return "result"
                
                result = test_func(user_id="test", tier="free")
                assert result == "result"
    
    def test_decorator_checks_rate_limit(self):
        """Test decorator checks rate limit."""
        mock_check = Mock()
        
        with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit", mock_check):
            with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                from src.engine.intelligence.llm_utils import with_llm_rate_limit
                
                @with_llm_rate_limit("openai")
                def test_func():
                    return "result"
                
                test_func(user_id="user123", tier="pro")
                
                mock_check.assert_called_once_with("user123", "openai", "pro")
    
    def test_decorator_tracks_call(self):
        """Test decorator tracks LLM call."""
        mock_track = Mock()
        
        with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
            with patch("src.engine.intelligence.llm_utils._track_llm_call", mock_track):
                from src.engine.intelligence.llm_utils import with_llm_rate_limit
                
                @with_llm_rate_limit("anthropic")
                def test_func():
                    return "result"
                
                test_func(user_id="user123", tier="free")
                
                assert mock_track.called
    
    def test_decorator_handles_exception(self):
        """Test decorator handles exceptions."""
        with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
            with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                from src.engine.intelligence.llm_utils import with_llm_rate_limit
                
                @with_llm_rate_limit("openai")
                def failing_func():
                    raise ValueError("Test error")
                
                with pytest.raises(ValueError):
                    failing_func(user_id="test", tier="free")


class TestTrackLLMCall:
    """Tests for _track_llm_call function."""
    
    def test_track_openai_call(self):
        """Test tracking OpenAI call."""
        mock_limiter = Mock()
        mock_result = Mock()
        mock_result.usage = Mock(total_tokens=100)
        mock_result.model = "gpt-4"
        
        with patch("src.engine.intelligence.llm_utils.get_llm_limiter", return_value=mock_limiter):
            from src.engine.intelligence.llm_utils import _track_llm_call
            
            _track_llm_call("user123", "openai", mock_result, 1.5)
            
            mock_limiter.track_llm_call.assert_called_once()
            call_args = mock_limiter.track_llm_call.call_args
            assert call_args.kwargs["user_id"] == "user123"
            assert call_args.kwargs["provider"] == "openai"
            assert call_args.kwargs["tokens"] == 100
    
    def test_track_anthropic_call(self):
        """Test tracking Anthropic call."""
        mock_limiter = Mock()
        mock_result = Mock()
        mock_result.usage = Mock(input_tokens=50, output_tokens=50)
        mock_result.model = "claude-3"
        
        with patch("src.engine.intelligence.llm_utils.get_llm_limiter", return_value=mock_limiter):
            from src.engine.intelligence.llm_utils import _track_llm_call
            
            _track_llm_call("user123", "anthropic", mock_result, 1.5)
            
            mock_limiter.track_llm_call.assert_called_once()
            call_args = mock_limiter.track_llm_call.call_args
            assert call_args.kwargs["tokens"] == 100
    
    def test_track_handles_missing_usage(self):
        """Test tracking handles missing usage attribute."""
        mock_limiter = Mock()
        mock_result = Mock(spec=[])  # No usage attribute
        
        with patch("src.engine.intelligence.llm_utils.get_llm_limiter", return_value=mock_limiter):
            from src.engine.intelligence.llm_utils import _track_llm_call
            
            # Should not raise
            _track_llm_call("user123", "openai", mock_result, 1.5)


class TestRateLimitedOpenAI:
    """Tests for RateLimitedOpenAI class."""
    
    def test_initialization(self):
        """Test RateLimitedOpenAI initialization."""
        with patch("src.engine.intelligence.llm_utils.OpenAI"):
            from src.engine.intelligence.llm_utils import RateLimitedOpenAI
            
            client = RateLimitedOpenAI(
                api_key="test-key",
                user_id="user123",
                tier="pro"
            )
            
            assert client.user_id == "user123"
            assert client.tier == "pro"
    
    def test_chat_completions_create(self):
        """Test chat completions with rate limiting."""
        mock_openai = Mock()
        mock_response = Mock()
        mock_openai.return_value.chat.completions.create.return_value = mock_response
        
        with patch("src.engine.intelligence.llm_utils.OpenAI", mock_openai):
            with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
                with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                    from src.engine.intelligence.llm_utils import RateLimitedOpenAI
                    
                    client = RateLimitedOpenAI(api_key="test")
                    result = client.chat_completions_create(
                        model="gpt-4",
                        messages=[{"role": "user", "content": "Hello"}]
                    )
                    
                    assert result == mock_response


class TestRateLimitedAnthropic:
    """Tests for RateLimitedAnthropic class."""
    
    def test_initialization(self):
        """Test RateLimitedAnthropic initialization."""
        with patch("src.engine.intelligence.llm_utils.Anthropic"):
            from src.engine.intelligence.llm_utils import RateLimitedAnthropic
            
            client = RateLimitedAnthropic(
                api_key="test-key",
                user_id="user456",
                tier="enterprise"
            )
            
            assert client.user_id == "user456"
            assert client.tier == "enterprise"
    
    def test_messages_create(self):
        """Test messages create with rate limiting."""
        mock_anthropic = Mock()
        mock_response = Mock()
        mock_anthropic.return_value.messages.create.return_value = mock_response
        
        with patch("src.engine.intelligence.llm_utils.Anthropic", mock_anthropic):
            with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
                with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                    from src.engine.intelligence.llm_utils import RateLimitedAnthropic
                    
                    client = RateLimitedAnthropic(api_key="test")
                    result = client.messages_create(
                        model="claude-3-opus",
                        messages=[{"role": "user", "content": "Hello"}]
                    )
                    
                    assert result == mock_response


class TestRateLimitedGemini:
    """Tests for RateLimitedGemini class."""
    
    def test_initialization(self):
        """Test RateLimitedGemini initialization."""
        with patch("src.engine.intelligence.llm_utils.genai"):
            from src.engine.intelligence.llm_utils import RateLimitedGemini
            
            client = RateLimitedGemini(
                api_key="test-key",
                user_id="user789",
                tier="free"
            )
            
            assert client.user_id == "user789"
            assert client.tier == "free"
    
    def test_generate_content(self):
        """Test generate content with rate limiting."""
        mock_genai = Mock()
        mock_model = Mock()
        mock_response = Mock()
        mock_model.generate_content.return_value = mock_response
        mock_genai.GenerativeModel.return_value = mock_model
        
        with patch("src.engine.intelligence.llm_utils.genai", mock_genai):
            with patch("src.engine.intelligence.llm_utils.check_llm_rate_limit"):
                with patch("src.engine.intelligence.llm_utils._track_llm_call"):
                    from src.engine.intelligence.llm_utils import RateLimitedGemini
                    
                    client = RateLimitedGemini()
                    result = client.generate_content(
                        model_name="gemini-pro",
                        prompt="Hello"
                    )
                    
                    assert result == mock_response


class TestGetLLMClient:
    """Tests for get_llm_client factory function."""
    
    def test_get_openai_client(self):
        """Test getting OpenAI client."""
        with patch("src.engine.intelligence.llm_utils.OpenAI"):
            from src.engine.intelligence.llm_utils import get_llm_client, RateLimitedOpenAI
            
            client = get_llm_client("openai", user_id="test")
            
            assert isinstance(client, RateLimitedOpenAI)
    
    def test_get_anthropic_client(self):
        """Test getting Anthropic client."""
        with patch("src.engine.intelligence.llm_utils.Anthropic"):
            from src.engine.intelligence.llm_utils import get_llm_client, RateLimitedAnthropic
            
            client = get_llm_client("anthropic", user_id="test")
            
            assert isinstance(client, RateLimitedAnthropic)
    
    def test_get_gemini_client(self):
        """Test getting Gemini client."""
        with patch("src.engine.intelligence.llm_utils.genai"):
            from src.engine.intelligence.llm_utils import get_llm_client, RateLimitedGemini
            
            client = get_llm_client("gemini", user_id="test")
            
            assert isinstance(client, RateLimitedGemini)
    
    def test_unsupported_provider(self):
        """Test unsupported provider raises error."""
        from src.engine.intelligence.llm_utils import get_llm_client
        
        with pytest.raises(ValueError, match="Unsupported provider"):
            get_llm_client("unsupported_provider")
    
    def test_case_insensitive_provider(self):
        """Test provider name is case insensitive."""
        with patch("src.engine.intelligence.llm_utils.OpenAI"):
            from src.engine.intelligence.llm_utils import get_llm_client, RateLimitedOpenAI
            
            client = get_llm_client("OPENAI", user_id="test")
            
            assert isinstance(client, RateLimitedOpenAI)
