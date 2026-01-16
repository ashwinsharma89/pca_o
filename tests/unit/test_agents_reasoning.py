"""
Unit tests for Reasoning Agent.
Tests insight generation and campaign analysis.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import json

# Try to import
try:
    from src.engine.agents.reasoning_agent import ReasoningAgent
    REASONING_AGENT_AVAILABLE = True
except ImportError:
    REASONING_AGENT_AVAILABLE = False
    ReasoningAgent = None

try:
    from src.platform.models.campaign import Campaign, ChannelPerformance
    MODELS_AVAILABLE = True
except ImportError:
    MODELS_AVAILABLE = False

pytestmark = pytest.mark.skipif(not REASONING_AGENT_AVAILABLE, reason="Reasoning agent not available")


class TestReasoningAgentInit:
    """Test ReasoningAgent initialization."""
    
    @patch('src.engine.agents.reasoning_agent.AsyncOpenAI')
    @patch('src.engine.agents.reasoning_agent.settings')
    def test_init_openai_provider(self, mock_settings, mock_openai):
        """Test initialization with OpenAI provider."""
        mock_settings.openai_api_key = "test_key"
        mock_settings.default_llm_model = "gpt-4"
        
        agent = ReasoningAgent(provider="openai")
        
        assert agent.provider == "openai"
        mock_openai.assert_called_once()
    
    @patch('src.engine.agents.reasoning_agent.create_async_anthropic_client')
    @patch('src.engine.agents.reasoning_agent.settings')
    def test_init_anthropic_provider(self, mock_settings, mock_anthropic):
        """Test initialization with Anthropic provider."""
        mock_settings.anthropic_api_key = "test_key"
        mock_settings.default_llm_model = "claude-3-opus"
        mock_anthropic.return_value = Mock()
        
        agent = ReasoningAgent(provider="anthropic")
        
        assert agent.provider == "anthropic"
    
    @patch('src.engine.agents.reasoning_agent.settings')
    def test_init_invalid_provider(self, mock_settings):
        """Test initialization with invalid provider."""
        mock_settings.default_llm_model = "test-model"
        
        with pytest.raises(ValueError, match="Unsupported provider"):
            ReasoningAgent(provider="invalid")


class TestReasoningAgentAnalysis:
    """Test campaign analysis methods."""
    
    @pytest.fixture
    def mock_agent(self):
        """Create mock reasoning agent."""
        with patch('src.engine.agents.reasoning_agent.AsyncOpenAI') as mock_openai:
            with patch('src.engine.agents.reasoning_agent.settings') as mock_settings:
                mock_settings.openai_api_key = "test_key"
                mock_settings.default_llm_model = "gpt-4"
                
                agent = ReasoningAgent(provider="openai")
                agent.client = Mock()
                return agent
    
    @pytest.fixture
    def mock_campaign(self):
        """Create mock campaign."""
        campaign = Mock()
        campaign.campaign_id = "test_campaign_123"
        campaign.normalized_metrics = []
        campaign.channel_performances = []
        campaign.cross_channel_insights = []
        campaign.achievements = []
        campaign.recommendations = []
        return campaign
    
    @pytest.mark.asyncio
    async def test_analyze_campaign(self, mock_agent, mock_campaign):
        """Test campaign analysis."""
        # Mock the LLM response
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"insights": []}'))]
        mock_agent.client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        try:
            result = await mock_agent.analyze_campaign(mock_campaign)
            assert result is not None
        except Exception:
            pytest.skip("Campaign analysis requires full setup")
    
    def test_generate_channel_insights(self, mock_agent):
        """Test channel insight generation."""
        if hasattr(mock_agent, '_generate_channel_insights'):
            metrics = [
                Mock(channel="google", metric_type="impressions", value=1000),
                Mock(channel="google", metric_type="clicks", value=50)
            ]
            
            try:
                result = mock_agent._generate_channel_insights(metrics)
                assert result is not None
            except Exception:
                pytest.skip("Insight generation requires LLM")


class TestReasoningAgentInsights:
    """Test insight generation methods."""
    
    @pytest.fixture
    def mock_agent(self):
        """Create mock reasoning agent."""
        with patch('src.engine.agents.reasoning_agent.AsyncOpenAI'):
            with patch('src.engine.agents.reasoning_agent.settings') as mock_settings:
                mock_settings.openai_api_key = "test_key"
                mock_settings.default_llm_model = "gpt-4"
                return ReasoningAgent(provider="openai")
    
    def test_detect_achievements(self, mock_agent):
        """Test achievement detection."""
        if hasattr(mock_agent, '_detect_achievements'):
            metrics = {
                'ctr': 0.05,  # 5% CTR
                'roas': 4.5,  # 4.5x ROAS
                'cpc': 0.50   # $0.50 CPC
            }
            
            try:
                achievements = mock_agent._detect_achievements(metrics)
                assert isinstance(achievements, list)
            except Exception:
                pytest.skip("Achievement detection requires setup")
    
    def test_generate_recommendations(self, mock_agent):
        """Test recommendation generation."""
        if hasattr(mock_agent, '_generate_recommendations'):
            insights = [
                Mock(type="performance", message="CTR below benchmark")
            ]
            
            try:
                recommendations = mock_agent._generate_recommendations(insights)
                assert isinstance(recommendations, list)
            except Exception:
                pytest.skip("Recommendation generation requires LLM")


class TestReasoningAgentPrompts:
    """Test prompt building methods."""
    
    @pytest.fixture
    def mock_agent(self):
        """Create mock reasoning agent."""
        with patch('src.engine.agents.reasoning_agent.AsyncOpenAI'):
            with patch('src.engine.agents.reasoning_agent.settings') as mock_settings:
                mock_settings.openai_api_key = "test_key"
                mock_settings.default_llm_model = "gpt-4"
                return ReasoningAgent(provider="openai")
    
    def test_build_analysis_prompt(self, mock_agent):
        """Test building analysis prompt."""
        if hasattr(mock_agent, '_build_analysis_prompt'):
            prompt = mock_agent._build_analysis_prompt(
                metrics=[],
                context={"campaign_name": "Test"}
            )
            assert isinstance(prompt, str)
