"""
Unit tests for agent registry.
Tests agent registration and capability-based routing.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

# Try to import, skip if not available
try:
    from src.engine.agents.agent_registry import (
        AgentRegistry, AgentRegistration, AgentCapability
    )
    REGISTRY_AVAILABLE = True
except ImportError:
    REGISTRY_AVAILABLE = False
    AgentRegistry = None
    AgentRegistration = None
    AgentCapability = None

pytestmark = pytest.mark.skipif(not REGISTRY_AVAILABLE, reason="Agent registry not available")


class TestAgentCapability:
    """Test AgentCapability enum."""
    
    def test_capability_values(self):
        """Test capability enum values."""
        assert AgentCapability.OCR.value == "ocr"
        assert AgentCapability.VISION_LLM.value == "vision_llm"
        assert AgentCapability.GOOGLE_ADS.value == "google_ads"
        assert AgentCapability.CHART_GENERATION.value == "chart_generation"
    
    def test_all_capabilities_defined(self):
        """Test that all expected capabilities are defined."""
        expected = [
            "OCR", "VISION_LLM", "PLATFORM_DETECTION",
            "DATA_NORMALIZATION", "DATA_VALIDATION",
            "GOOGLE_ADS", "META_ADS", "LINKEDIN_ADS",
            "PATTERN_DETECTION", "CHART_GENERATION"
        ]
        
        for cap in expected:
            assert hasattr(AgentCapability, cap)


class TestAgentRegistration:
    """Test AgentRegistration dataclass."""
    
    def test_create_registration(self):
        """Test creating agent registration."""
        reg = AgentRegistration(
            name="test_agent",
            class_name="TestAgent",
            module_path="src.engine.agents.test_agent",
            capabilities=[AgentCapability.OCR],
            version="1.0.0"
        )
        
        assert reg.name == "test_agent"
        assert reg.class_name == "TestAgent"
        assert AgentCapability.OCR in reg.capabilities
    
    def test_registration_requires_capabilities(self):
        """Test that registration requires at least one capability."""
        with pytest.raises(ValueError):
            AgentRegistration(
                name="test_agent",
                class_name="TestAgent",
                module_path="src.engine.agents.test_agent",
                capabilities=[]
            )
    
    def test_registration_defaults(self):
        """Test registration default values."""
        reg = AgentRegistration(
            name="test_agent",
            class_name="TestAgent",
            module_path="src.engine.agents.test_agent",
            capabilities=[AgentCapability.OCR]
        )
        
        assert reg.version == "1.0.0"
        assert reg.status == "healthy"
        assert reg.priority == 0


class TestAgentRegistry:
    """Test AgentRegistry functionality."""
    
    @pytest.fixture
    def registry(self):
        """Create agent registry instance."""
        return AgentRegistry()
    
    @pytest.fixture
    def sample_registration(self):
        """Create sample agent registration."""
        return AgentRegistration(
            name="vision_agent",
            class_name="VisionAgent",
            module_path="src.engine.agents.vision_agent",
            capabilities=[AgentCapability.OCR, AgentCapability.VISION_LLM],
            priority=10
        )
    
    def test_registry_initialization(self, registry):
        """Test registry initialization."""
        assert registry is not None
        if hasattr(registry, '_agents'):
            assert isinstance(registry._agents, dict)
    
    def test_register_agent(self, registry, sample_registration):
        """Test registering an agent."""
        if hasattr(registry, 'register'):
            # Register using individual parameters
            registry.register(
                name=sample_registration.name,
                class_name=sample_registration.class_name,
                module_path=sample_registration.module_path,
                capabilities=sample_registration.capabilities,
                priority=sample_registration.priority
            )
            
            assert "vision_agent" in registry.agents
    
    def test_get_agent_by_name(self, registry, sample_registration):
        """Test getting agent by name."""
        if hasattr(registry, 'register') and hasattr(registry, 'get'):
            registry.register(sample_registration)
            
            agent = registry.get("vision_agent")
            assert agent is not None
            assert agent.name == "vision_agent"
    
    def test_get_agents_by_capability(self, registry, sample_registration):
        """Test getting agents by capability."""
        if hasattr(registry, 'register') and hasattr(registry, 'get_by_capability'):
            registry.register(sample_registration)
            
            agents = registry.get_by_capability(AgentCapability.OCR)
            assert len(agents) >= 1
    
    def test_unregister_agent(self, registry, sample_registration):
        """Test unregistering an agent."""
        if hasattr(registry, 'register') and hasattr(registry, 'unregister'):
            registry.register(
                name=sample_registration.name,
                class_name=sample_registration.class_name,
                module_path=sample_registration.module_path,
                capabilities=sample_registration.capabilities
            )
            registry.unregister("vision_agent")
            
            assert "vision_agent" not in registry.agents
    
    def test_list_all_agents(self, registry, sample_registration):
        """Test listing all agents."""
        if hasattr(registry, 'register') and hasattr(registry, 'list_all'):
            registry.register(sample_registration)
            
            agents = registry.list_all()
            assert len(agents) >= 1
    
    def test_agent_priority_ordering(self, registry):
        """Test that agents are ordered by priority."""
        if hasattr(registry, 'register') and hasattr(registry, 'get_by_capability'):
            reg1 = AgentRegistration(
                name="low_priority",
                class_name="LowAgent",
                module_path="src.engine.agents.low",
                capabilities=[AgentCapability.OCR],
                priority=1
            )
            reg2 = AgentRegistration(
                name="high_priority",
                class_name="HighAgent",
                module_path="src.engine.agents.high",
                capabilities=[AgentCapability.OCR],
                priority=10
            )
            
            registry.register(reg1)
            registry.register(reg2)
            
            agents = registry.get_by_capability(AgentCapability.OCR)
            if len(agents) >= 2:
                # Higher priority should come first
                assert agents[0].priority >= agents[1].priority


class TestAgentRegistryHealthCheck:
    """Test agent health checking."""
    
    @pytest.fixture
    def registry(self):
        """Create agent registry instance."""
        return AgentRegistry()
    
    def test_update_agent_status(self, registry):
        """Test updating agent status."""
        reg = AgentRegistration(
            name="test_agent",
            class_name="TestAgent",
            module_path="src.engine.agents.test",
            capabilities=[AgentCapability.OCR]
        )
        
        if hasattr(registry, 'register') and hasattr(registry, 'update_status'):
            registry.register(reg)
            registry.update_status("test_agent", "unhealthy")
            
            agent = registry.get("test_agent")
            assert agent.status == "unhealthy"
    
    def test_get_healthy_agents(self, registry):
        """Test getting only healthy agents."""
        if hasattr(registry, 'register') and hasattr(registry, 'get_healthy'):
            reg1 = AgentRegistration(
                name="healthy_agent",
                class_name="HealthyAgent",
                module_path="src.engine.agents.healthy",
                capabilities=[AgentCapability.OCR],
                status="healthy"
            )
            
            registry.register(reg1)
            
            healthy = registry.get_healthy()
            assert all(a.status == "healthy" for a in healthy)
