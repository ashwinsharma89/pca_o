"""
Agent Registry for Dynamic Discovery and Routing.

Provides centralized registry for all agents with capability-based routing.
"""

from typing import Dict, Any, List, Optional, Type, Callable
from dataclasses import dataclass, field
from enum import Enum
import importlib
import inspect

from loguru import logger


class AgentCapability(Enum):
    """Agent capabilities for routing."""
    
    # Vision capabilities
    OCR = "ocr"
    VISION_LLM = "vision_llm"
    PLATFORM_DETECTION = "platform_detection"
    
    # Extraction capabilities
    DATA_NORMALIZATION = "data_normalization"
    DATA_VALIDATION = "data_validation"
    METRIC_CONVERSION = "metric_conversion"
    
    # Channel capabilities
    GOOGLE_ADS = "google_ads"
    BING_ADS = "bing_ads"
    DV360_SEARCH = "dv360_search"
    META_ADS = "meta_ads"
    LINKEDIN_ADS = "linkedin_ads"
    TIKTOK_ADS = "tiktok_ads"
    DV360_DISPLAY = "dv360_display"
    PROGRAMMATIC = "programmatic"
    
    # Analysis capabilities
    PATTERN_DETECTION = "pattern_detection"
    BENCHMARK_COMPARISON = "benchmark_comparison"
    RECOMMENDATION_GENERATION = "recommendation_generation"
    CROSS_CHANNEL_ANALYSIS = "cross_channel_analysis"
    
    # Context capabilities
    B2B_ANALYSIS = "b2b_analysis"
    B2C_ANALYSIS = "b2c_analysis"
    INDUSTRY_CONTEXT = "industry_context"
    
    # Output capabilities
    CHART_GENERATION = "chart_generation"
    REPORT_ASSEMBLY = "report_assembly"
    VISUALIZATION = "visualization"


@dataclass
class AgentRegistration:
    """Registration information for an agent."""
    
    name: str
    class_name: str
    module_path: str
    capabilities: List[AgentCapability]
    version: str = "1.0.0"
    status: str = "healthy"
    description: str = ""
    dependencies: List[str] = field(default_factory=list)
    priority: int = 0  # Higher priority agents are preferred
    
    def __post_init__(self):
        """Validate registration."""
        if not self.capabilities:
            raise ValueError(f"Agent {self.name} must have at least one capability")


class AgentRegistry:
    """
    Central registry for all agents.
    
    Provides dynamic discovery, capability-based routing, and health checking.
    """
    
    def __init__(self):
        """Initialize agent registry."""
        self.agents: Dict[str, AgentRegistration] = {}
        self.capability_map: Dict[AgentCapability, List[str]] = {}
        self.instances: Dict[str, Any] = {}
        logger.info("Initialized AgentRegistry")
    
    def register(
        self,
        name: str,
        class_name: str,
        module_path: str,
        capabilities: List[AgentCapability],
        version: str = "1.0.0",
        description: str = "",
        dependencies: Optional[List[str]] = None,
        priority: int = 0
    ):
        """
        Register an agent.
        
        Args:
            name: Unique agent name
            class_name: Agent class name
            module_path: Python module path
            capabilities: List of agent capabilities
            version: Agent version
            description: Agent description
            dependencies: List of required agent names
            priority: Agent priority (higher is preferred)
        """
        if name in self.agents:
            logger.warning(f"Agent {name} already registered, updating...")
        
        registration = AgentRegistration(
            name=name,
            class_name=class_name,
            module_path=module_path,
            capabilities=capabilities,
            version=version,
            description=description,
            dependencies=dependencies or [],
            priority=priority
        )
        
        self.agents[name] = registration
        
        # Update capability map
        for capability in capabilities:
            if capability not in self.capability_map:
                self.capability_map[capability] = []
            if name not in self.capability_map[capability]:
                self.capability_map[capability].append(name)
        
        logger.info(f"Registered agent: {name} with capabilities: {[c.value for c in capabilities]}")
    
    def unregister(self, name: str):
        """Unregister an agent."""
        if name not in self.agents:
            logger.warning(f"Agent {name} not registered")
            return
        
        registration = self.agents[name]
        
        # Remove from capability map
        for capability in registration.capabilities:
            if capability in self.capability_map:
                self.capability_map[capability].remove(name)
                if not self.capability_map[capability]:
                    del self.capability_map[capability]
        
        # Remove instance if exists
        if name in self.instances:
            del self.instances[name]
        
        del self.agents[name]
        logger.info(f"Unregistered agent: {name}")
    
    def get_agent(self, name: str, **init_kwargs) -> Optional[Any]:
        """
        Get agent instance by name.
        
        Args:
            name: Agent name
            **init_kwargs: Initialization arguments
        
        Returns:
            Agent instance or None
        """
        if name not in self.agents:
            logger.error(f"Agent {name} not registered")
            return None
        
        # Return cached instance if exists and no init_kwargs
        if name in self.instances and not init_kwargs:
            return self.instances[name]
        
        registration = self.agents[name]
        
        try:
            # Import module
            module = importlib.import_module(registration.module_path)
            
            # Get class
            agent_class = getattr(module, registration.class_name)
            
            # Create instance
            instance = agent_class(**init_kwargs)
            
            # Cache instance if no init_kwargs
            if not init_kwargs:
                self.instances[name] = instance
            
            logger.info(f"Created instance of {name}")
            return instance
            
        except Exception as e:
            logger.error(f"Failed to create agent {name}: {e}")
            self.agents[name].status = "unhealthy"
            return None
    
    def find_agents_by_capability(
        self,
        capability: AgentCapability,
        status: Optional[str] = "healthy"
    ) -> List[str]:
        """
        Find agents by capability.
        
        Args:
            capability: Required capability
            status: Optional status filter
        
        Returns:
            List of agent names
        """
        if capability not in self.capability_map:
            return []
        
        agent_names = self.capability_map[capability]
        
        if status:
            agent_names = [
                name for name in agent_names
                if self.agents[name].status == status
            ]
        
        # Sort by priority
        agent_names.sort(
            key=lambda name: self.agents[name].priority,
            reverse=True
        )
        
        return agent_names
    
    def route_to_agent(
        self,
        capability: AgentCapability,
        **init_kwargs
    ) -> Optional[Any]:
        """
        Route to best agent for capability.
        
        Args:
            capability: Required capability
            **init_kwargs: Initialization arguments
        
        Returns:
            Agent instance or None
        """
        agents = self.find_agents_by_capability(capability)
        
        if not agents:
            logger.error(f"No agents found for capability: {capability.value}")
            return None
        
        # Try agents in priority order
        for agent_name in agents:
            agent = self.get_agent(agent_name, **init_kwargs)
            if agent:
                logger.info(f"Routed to {agent_name} for capability {capability.value}")
                return agent
        
        logger.error(f"Failed to route to any agent for capability: {capability.value}")
        return None
    
    def get_all_registrations(self) -> Dict[str, Dict[str, Any]]:
        """Get all agent registrations."""
        return {
            name: {
                'name': reg.name,
                'class_name': reg.class_name,
                'module_path': reg.module_path,
                'capabilities': [c.value for c in reg.capabilities],
                'version': reg.version,
                'status': reg.status,
                'description': reg.description,
                'dependencies': reg.dependencies,
                'priority': reg.priority
            }
            for name, reg in self.agents.items()
        }
    
    def health_check(self, name: str) -> bool:
        """
        Check agent health.
        
        Args:
            name: Agent name
        
        Returns:
            True if healthy
        """
        if name not in self.agents:
            return False
        
        try:
            agent = self.get_agent(name)
            if agent is None:
                self.agents[name].status = "unhealthy"
                return False
            
            # Try to call a health check method if exists
            if hasattr(agent, 'health_check'):
                healthy = agent.health_check()
                self.agents[name].status = "healthy" if healthy else "unhealthy"
                return healthy
            
            self.agents[name].status = "healthy"
            return True
            
        except Exception as e:
            logger.error(f"Health check failed for {name}: {e}")
            self.agents[name].status = "unhealthy"
            return False
    
    def health_check_all(self) -> Dict[str, bool]:
        """Check health of all agents."""
        return {
            name: self.health_check(name)
            for name in self.agents.keys()
        }


# Global registry instance
_registry: Optional[AgentRegistry] = None


def get_agent_registry() -> AgentRegistry:
    """Get global agent registry instance."""
    global _registry
    if _registry is None:
        _registry = AgentRegistry()
        _register_default_agents()
    return _registry


def _register_default_agents():
    """Register all default agents."""
    registry = get_agent_registry()
    
    # Vision Agent
    registry.register(
        name="vision_agent",
        class_name="VisionAgent",
        module_path="src.engine.agents.vision_agent",
        capabilities=[
            AgentCapability.OCR,
            AgentCapability.VISION_LLM,
            AgentCapability.PLATFORM_DETECTION
        ],
        description="Processes dashboard screenshots using OCR and Vision LLM",
        priority=10
    )
    
    # Extraction Agent
    registry.register(
        name="extraction_agent",
        class_name="ExtractionAgent",
        module_path="src.engine.agents.extraction_agent",
        capabilities=[
            AgentCapability.DATA_NORMALIZATION,
            AgentCapability.DATA_VALIDATION,
            AgentCapability.METRIC_CONVERSION
        ],
        description="Normalizes and validates extracted data",
        dependencies=["vision_agent"],
        priority=10
    )
    
    # Search Specialist
    registry.register(
        name="search_specialist",
        class_name="SearchChannelAgent",
        module_path="src.engine.agents.channel_specialists.search_agent",
        capabilities=[
            AgentCapability.GOOGLE_ADS,
            AgentCapability.BING_ADS,
            AgentCapability.DV360_SEARCH
        ],
        description="Analyzes search campaign performance",
        priority=10
    )
    
    # Social Specialist
    registry.register(
        name="social_specialist",
        class_name="SocialChannelAgent",
        module_path="src.engine.agents.channel_specialists.social_agent",
        capabilities=[
            AgentCapability.META_ADS,
            AgentCapability.LINKEDIN_ADS,
            AgentCapability.TIKTOK_ADS
        ],
        description="Analyzes social media campaign performance",
        priority=10
    )
    
    # Programmatic Specialist
    registry.register(
        name="programmatic_specialist",
        class_name="ProgrammaticAgent",
        module_path="src.engine.agents.channel_specialists.programmatic_agent",
        capabilities=[
            AgentCapability.DV360_DISPLAY,
            AgentCapability.PROGRAMMATIC
        ],
        description="Analyzes programmatic display campaigns",
        priority=10
    )
    
    # Enhanced Reasoning Agent
    registry.register(
        name="enhanced_reasoning",
        class_name="EnhancedReasoningAgent",
        module_path="src.engine.agents.enhanced_reasoning_agent",
        capabilities=[
            AgentCapability.PATTERN_DETECTION,
            AgentCapability.BENCHMARK_COMPARISON,
            AgentCapability.RECOMMENDATION_GENERATION,
            AgentCapability.CROSS_CHANNEL_ANALYSIS
        ],
        description="Advanced pattern detection and recommendations",
        dependencies=["search_specialist", "social_specialist", "programmatic_specialist"],
        priority=10
    )
    
    # B2B Specialist
    registry.register(
        name="b2b_specialist",
        class_name="B2BSpecialistAgent",
        module_path="src.engine.agents.b2b_specialist_agent",
        capabilities=[
            AgentCapability.B2B_ANALYSIS,
            AgentCapability.INDUSTRY_CONTEXT
        ],
        description="Applies B2B-specific context and analysis",
        dependencies=["enhanced_reasoning"],
        priority=5
    )
    
    # Visualization Agent
    registry.register(
        name="visualization_agent",
        class_name="EnhancedVisualizationAgent",
        module_path="src.engine.agents.enhanced_visualization_agent",
        capabilities=[
            AgentCapability.CHART_GENERATION,
            AgentCapability.VISUALIZATION
        ],
        description="Generates charts and visualizations",
        priority=10
    )
    
    # Report Agent
    registry.register(
        name="report_agent",
        class_name="ReportAgent",
        module_path="src.engine.agents.report_agent",
        capabilities=[
            AgentCapability.REPORT_ASSEMBLY
        ],
        description="Assembles final PowerPoint reports",
        dependencies=["visualization_agent"],
        priority=10
    )
    
    logger.info("Registered all default agents")
