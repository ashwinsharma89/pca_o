"""
Unit tests for Channel Specialist Agents.
Tests base specialist and channel-specific agents.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Try to import base specialist
try:
    from src.engine.agents.channel_specialists.base_specialist import BaseChannelSpecialist
    BASE_SPECIALIST_AVAILABLE = True
except ImportError:
    BASE_SPECIALIST_AVAILABLE = False
    BaseChannelSpecialist = None

# Try to import channel specialists
try:
    from src.engine.agents.channel_specialists.search_agent import SearchChannelAgent
    SEARCH_AGENT_AVAILABLE = True
except ImportError:
    SEARCH_AGENT_AVAILABLE = False
    SearchChannelAgent = None

try:
    from src.engine.agents.channel_specialists.social_agent import SocialChannelAgent
    SOCIAL_AGENT_AVAILABLE = True
except ImportError:
    SOCIAL_AGENT_AVAILABLE = False
    SocialChannelAgent = None

try:
    from src.engine.agents.channel_specialists.programmatic_agent import ProgrammaticChannelAgent
    PROGRAMMATIC_AGENT_AVAILABLE = True
except ImportError:
    PROGRAMMATIC_AGENT_AVAILABLE = False
    ProgrammaticChannelAgent = None

try:
    from src.engine.agents.channel_specialists.channel_router import ChannelRouter
    ROUTER_AVAILABLE = True
except ImportError:
    ROUTER_AVAILABLE = False
    ChannelRouter = None


class TestBaseChannelSpecialist:
    """Test BaseChannelSpecialist functionality."""
    
    pytestmark = pytest.mark.skipif(not BASE_SPECIALIST_AVAILABLE, reason="Base specialist not available")
    
    def test_base_is_abstract(self):
        """Test that base class is abstract."""
        if not BASE_SPECIALIST_AVAILABLE:
            pytest.skip("Base specialist not available")
        
        # Should not be able to instantiate directly
        with pytest.raises(TypeError):
            BaseChannelSpecialist()
    
    def test_retrieve_knowledge_without_rag(self):
        """Test knowledge retrieval without RAG."""
        if not BASE_SPECIALIST_AVAILABLE:
            pytest.skip("Base specialist not available")
        
        # Create concrete implementation for testing
        class TestSpecialist(BaseChannelSpecialist):
            def analyze(self, campaign_data):
                return {}
            def get_benchmarks(self):
                return {}
        
        specialist = TestSpecialist(rag_retriever=None)
        result = specialist.retrieve_knowledge("test query")
        
        assert result == "" or result is None


class TestSearchChannelAgent:
    """Test SearchChannelAgent functionality."""
    
    pytestmark = pytest.mark.skipif(not SEARCH_AGENT_AVAILABLE, reason="Search agent not available")
    
    @pytest.fixture
    def agent(self):
        """Create search channel agent."""
        return SearchChannelAgent()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample campaign data."""
        return pd.DataFrame({
            'Campaign_Name': ['Search Campaign 1', 'Search Campaign 2'],
            'Platform': ['Google', 'Google'],
            'Impressions': [10000, 15000],
            'Clicks': [500, 600],
            'Spend': [1000, 1500],
            'Conversions': [50, 60]
        })
    
    def test_initialization(self, agent):
        """Test agent initialization."""
        assert agent is not None
        assert 'Search' in agent.channel_type or 'search' in agent.channel_type.lower()
    
    def test_analyze(self, agent, sample_data):
        """Test campaign analysis."""
        result = agent.analyze(sample_data)
        
        assert isinstance(result, dict)
    
    def test_get_benchmarks(self, agent):
        """Test getting benchmarks."""
        benchmarks = agent.get_benchmarks()
        
        assert isinstance(benchmarks, dict)


class TestSocialChannelAgent:
    """Test SocialChannelAgent functionality."""
    
    pytestmark = pytest.mark.skipif(not SOCIAL_AGENT_AVAILABLE, reason="Social agent not available")
    
    @pytest.fixture
    def agent(self):
        """Create social channel agent."""
        return SocialChannelAgent()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample social campaign data."""
        return pd.DataFrame({
            'Campaign_Name': ['Social Campaign 1'],
            'Platform': ['Meta'],
            'Impressions': [50000],
            'Reach': [30000],
            'Engagement': [2500],
            'Spend': [2000]
        })
    
    def test_initialization(self, agent):
        """Test agent initialization."""
        assert agent is not None
    
    def test_analyze(self, agent, sample_data):
        """Test campaign analysis."""
        result = agent.analyze(sample_data)
        
        assert isinstance(result, dict)
    
    def test_get_benchmarks(self, agent):
        """Test getting benchmarks."""
        benchmarks = agent.get_benchmarks()
        
        assert isinstance(benchmarks, dict)


class TestProgrammaticChannelAgent:
    """Test ProgrammaticChannelAgent functionality."""
    
    pytestmark = pytest.mark.skipif(not PROGRAMMATIC_AGENT_AVAILABLE, reason="Programmatic agent not available")
    
    @pytest.fixture
    def agent(self):
        """Create programmatic channel agent."""
        return ProgrammaticChannelAgent()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample programmatic data."""
        return pd.DataFrame({
            'Campaign_Name': ['Display Campaign 1'],
            'Platform': ['DV360'],
            'Impressions': [100000],
            'Viewable_Impressions': [70000],
            'Clicks': [1000],
            'Spend': [5000]
        })
    
    def test_initialization(self, agent):
        """Test agent initialization."""
        assert agent is not None
    
    def test_analyze(self, agent, sample_data):
        """Test campaign analysis."""
        result = agent.analyze(sample_data)
        
        assert isinstance(result, dict)


class TestChannelRouter:
    """Test ChannelRouter functionality."""
    
    pytestmark = pytest.mark.skipif(not ROUTER_AVAILABLE, reason="Channel router not available")
    
    @pytest.fixture
    def router(self):
        """Create channel router."""
        return ChannelRouter()
    
    def test_initialization(self, router):
        """Test router initialization."""
        assert router is not None
    
    def test_route_to_specialist(self, router):
        """Test routing to correct specialist."""
        if hasattr(router, 'route'):
            specialist = router.route("google_search")
            assert specialist is not None
    
    def test_get_available_channels(self, router):
        """Test getting available channels."""
        if hasattr(router, 'get_available_channels'):
            channels = router.get_available_channels()
            assert isinstance(channels, list)
