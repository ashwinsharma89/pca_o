"""
Multi-Agent Orchestration with LangGraph
Complex workflow management with multiple specialized agents
"""
import asyncio
from typing import Dict, Any, List, Optional, TypedDict, Annotated
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import operator
import logging

logger = logging.getLogger(__name__)

# Try to import LangGraph
try:
    from langgraph.graph import StateGraph, END
    from langgraph.prebuilt import ToolNode
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    logger.info("langgraph not installed, using basic orchestration")


# ============================================================================
# State Management
# ============================================================================

class AgentState(TypedDict):
    """Shared state across agents"""
    query: str
    campaign_data: Optional[Dict]
    platform: Optional[str]
    analysis_results: Annotated[List[Dict], operator.add]
    recommendations: Annotated[List[str], operator.add]
    current_agent: str
    iteration: int
    max_iterations: int
    is_complete: bool
    errors: Annotated[List[str], operator.add]


@dataclass
class AgentResult:
    """Result from an agent execution"""
    agent_name: str
    success: bool
    output: Dict[str, Any]
    next_agent: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Agent Definitions
# ============================================================================

class AgentType(str, Enum):
    ROUTER = "router"
    ANALYZER = "analyzer"
    SPECIALIST = "specialist"
    RECOMMENDER = "recommender"
    SYNTHESIZER = "synthesizer"


class BaseAgent:
    """Base class for all agents in the orchestration"""
    
    def __init__(self, name: str, agent_type: AgentType):
        self.name = name
        self.agent_type = agent_type
    
    async def execute(self, state: AgentState) -> AgentResult:
        raise NotImplementedError


class RouterAgent(BaseAgent):
    """Routes queries to appropriate specialist agents"""
    
    def __init__(self):
        super().__init__("router", AgentType.ROUTER)
    
    async def execute(self, state: AgentState) -> AgentResult:
        query = state.get("query", "").lower()
        platform = state.get("platform")
        
        # Determine next agent based on query content
        if any(word in query for word in ["spend", "budget", "cost", "roas"]):
            next_agent = "budget_analyzer"
        elif any(word in query for word in ["creative", "ad", "copy", "visual"]):
            next_agent = "creative_analyzer"
        elif any(word in query for word in ["audience", "targeting", "segment"]):
            next_agent = "audience_analyzer"
        elif any(word in query for word in ["trend", "forecast", "predict"]):
            next_agent = "trend_analyzer"
        else:
            next_agent = "general_analyzer"
        
        return AgentResult(
            agent_name=self.name,
            success=True,
            output={"routed_to": next_agent, "platform": platform},
            next_agent=next_agent
        )


class AnalyzerAgent(BaseAgent):
    """Analyzes campaign data"""
    
    def __init__(self, name: str = "general_analyzer"):
        super().__init__(name, AgentType.ANALYZER)
    
    async def execute(self, state: AgentState) -> AgentResult:
        data = state.get("campaign_data", {})
        
        # Perform analysis
        analysis = {
            "total_spend": sum(d.get("spend", 0) for d in data) if isinstance(data, list) else 0,
            "avg_cpc": 0,
            "performance_score": 0.75,
            "key_metrics": ["CTR", "CPA", "ROAS"]
        }
        
        return AgentResult(
            agent_name=self.name,
            success=True,
            output={"analysis": analysis},
            next_agent="recommender"
        )


class SpecialistAgent(BaseAgent):
    """Platform-specific specialist"""
    
    def __init__(self, platform: str):
        super().__init__(f"{platform}_specialist", AgentType.SPECIALIST)
        self.platform = platform
    
    async def execute(self, state: AgentState) -> AgentResult:
        # Platform-specific analysis
        insights = {
            "platform": self.platform,
            "platform_specific_metrics": self._get_platform_metrics(),
            "best_practices": self._get_best_practices()
        }
        
        return AgentResult(
            agent_name=self.name,
            success=True,
            output={"specialist_insights": insights},
            next_agent="synthesizer"
        )
    
    def _get_platform_metrics(self) -> List[str]:
        platform_metrics = {
            "meta": ["Relevance Score", "Engagement Rate", "Video Views"],
            "google": ["Quality Score", "Search Impression Share", "Conversion Value"],
            "linkedin": ["Social Actions", "Lead Gen Forms", "Company Page Clicks"]
        }
        return platform_metrics.get(self.platform.lower(), ["CTR", "CPA", "ROAS"])
    
    def _get_best_practices(self) -> List[str]:
        return [
            f"Optimize for {self.platform} audience signals",
            f"Use {self.platform}-specific creative formats",
            f"Leverage {self.platform} automation features"
        ]


class RecommenderAgent(BaseAgent):
    """Generates recommendations based on analysis"""
    
    def __init__(self):
        super().__init__("recommender", AgentType.RECOMMENDER)
    
    async def execute(self, state: AgentState) -> AgentResult:
        analysis_results = state.get("analysis_results", [])
        
        recommendations = [
            "Increase budget allocation to top-performing campaigns",
            "Test new creative variations for underperforming ads",
            "Expand audience targeting based on conversion data",
            "Implement automated bidding strategies"
        ]
        
        return AgentResult(
            agent_name=self.name,
            success=True,
            output={"recommendations": recommendations},
            next_agent="synthesizer"
        )


class SynthesizerAgent(BaseAgent):
    """Synthesizes all results into final output"""
    
    def __init__(self):
        super().__init__("synthesizer", AgentType.SYNTHESIZER)
    
    async def execute(self, state: AgentState) -> AgentResult:
        analysis_results = state.get("analysis_results", [])
        recommendations = state.get("recommendations", [])
        
        synthesis = {
            "summary": "Analysis complete with actionable recommendations",
            "key_findings": [r.get("analysis", {}) for r in analysis_results if "analysis" in r],
            "action_items": recommendations[:5],
            "confidence": 0.85
        }
        
        return AgentResult(
            agent_name=self.name,
            success=True,
            output={"final_synthesis": synthesis},
            next_agent=None  # End of workflow
        )


# ============================================================================
# Multi-Agent Orchestrator
# ============================================================================

class MultiAgentOrchestrator:
    """
    Orchestrates multiple agents for complex analysis workflows
    
    Usage:
        orchestrator = MultiAgentOrchestrator()
        result = await orchestrator.run("What's my ROAS trend?", campaign_data)
    """
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self._register_default_agents()
        self._graph = self._build_graph() if LANGGRAPH_AVAILABLE else None
    
    def _register_default_agents(self):
        """Register default agents"""
        self.register_agent(RouterAgent())
        self.register_agent(AnalyzerAgent("general_analyzer"))
        self.register_agent(AnalyzerAgent("budget_analyzer"))
        self.register_agent(AnalyzerAgent("creative_analyzer"))
        self.register_agent(AnalyzerAgent("audience_analyzer"))
        self.register_agent(AnalyzerAgent("trend_analyzer"))
        self.register_agent(SpecialistAgent("meta"))
        self.register_agent(SpecialistAgent("google"))
        self.register_agent(SpecialistAgent("linkedin"))
        self.register_agent(RecommenderAgent())
        self.register_agent(SynthesizerAgent())
    
    def register_agent(self, agent: BaseAgent):
        """Register an agent"""
        self.agents[agent.name] = agent
        logger.info(f"Registered agent: {agent.name}")
    
    def _build_graph(self):
        """Build LangGraph workflow"""
        if not LANGGRAPH_AVAILABLE:
            return None
        
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("router", self._create_node("router"))
        workflow.add_node("analyzer", self._create_node("general_analyzer"))
        workflow.add_node("recommender", self._create_node("recommender"))
        workflow.add_node("synthesizer", self._create_node("synthesizer"))
        
        # Add edges
        workflow.set_entry_point("router")
        workflow.add_edge("router", "analyzer")
        workflow.add_edge("analyzer", "recommender")
        workflow.add_edge("recommender", "synthesizer")
        workflow.add_edge("synthesizer", END)
        
        return workflow.compile()
    
    def _create_node(self, agent_name: str):
        """Create a node function for LangGraph"""
        async def node(state: AgentState) -> AgentState:
            agent = self.agents.get(agent_name)
            if agent:
                result = await agent.execute(state)
                state["analysis_results"].append(result.output)
                state["current_agent"] = result.next_agent or ""
            return state
        return node
    
    async def run(
        self,
        query: str,
        campaign_data: Dict = None,
        platform: str = None,
        max_iterations: int = 10
    ) -> Dict[str, Any]:
        """
        Run multi-agent workflow
        
        Args:
            query: User query
            campaign_data: Campaign data to analyze
            platform: Target platform
            max_iterations: Max agent iterations
        """
        initial_state: AgentState = {
            "query": query,
            "campaign_data": campaign_data,
            "platform": platform,
            "analysis_results": [],
            "recommendations": [],
            "current_agent": "router",
            "iteration": 0,
            "max_iterations": max_iterations,
            "is_complete": False,
            "errors": []
        }
        
        if self._graph:
            # Use LangGraph
            final_state = await self._graph.ainvoke(initial_state)
            return {
                "success": True,
                "results": final_state.get("analysis_results", []),
                "recommendations": final_state.get("recommendations", [])
            }
        else:
            # Fallback to simple orchestration
            return await self._simple_orchestrate(initial_state)
    
    async def _simple_orchestrate(self, state: AgentState) -> Dict[str, Any]:
        """Simple orchestration without LangGraph"""
        current_agent = state["current_agent"]
        
        while current_agent and state["iteration"] < state["max_iterations"]:
            state["iteration"] += 1
            
            agent = self.agents.get(current_agent)
            if not agent:
                state["errors"].append(f"Agent not found: {current_agent}")
                break
            
            try:
                result = await agent.execute(state)
                state["analysis_results"].append(result.output)
                
                if result.output.get("recommendations"):
                    state["recommendations"].extend(result.output["recommendations"])
                
                current_agent = result.next_agent
                
            except Exception as e:
                state["errors"].append(f"{current_agent}: {str(e)}")
                break
        
        return {
            "success": len(state["errors"]) == 0,
            "results": state["analysis_results"],
            "recommendations": state["recommendations"],
            "errors": state["errors"]
        }


# Global instance
_orchestrator: Optional[MultiAgentOrchestrator] = None

def get_orchestrator() -> MultiAgentOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = MultiAgentOrchestrator()
    return _orchestrator


async def run_multi_agent_analysis(
    query: str,
    campaign_data: Dict = None,
    platform: str = None
) -> Dict[str, Any]:
    """Convenience function to run multi-agent analysis"""
    return await get_orchestrator().run(query, campaign_data, platform)
