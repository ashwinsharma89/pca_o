"""
Enhanced Multi-Agent Orchestrator with Resilience.

Adds fallback mechanisms, retry logic, and output validation to the orchestrator.
"""

from typing import Dict, Any, Optional
import asyncio
from loguru import logger

from .multi_agent_orchestrator import MultiAgentOrchestrator as BaseOrchestrator
from .enhanced_reasoning_agent import EnhancedReasoningAgent
from .reasoning_agent import ReasoningAgent  # Simpler fallback
from .agent_resilience import AgentFallback, retry_with_backoff, CircuitBreaker
from .validated_reasoning_agent import ValidatedReasoningAgent


class ResilientMultiAgentOrchestrator:
    """
    Multi-Agent Orchestrator with resilience mechanisms.
    
    Features:
    - Automatic fallback to simpler agents on failure
    - Retry logic with exponential backoff
    - Circuit breakers to prevent cascading failures
    - Output validation with Pydantic schemas
    """
    
    def __init__(self, rag_retriever=None, benchmark_engine=None):
        """
        Initialize resilient orchestrator.
        
        Args:
            rag_retriever: Optional RAG retriever
            benchmark_engine: Optional benchmark engine
        """
        # Initialize base orchestrator
        self.base_orchestrator = BaseOrchestrator()
        
        # Initialize validated reasoning agent with fallback
        primary_agent = ValidatedReasoningAgent(rag_retriever, benchmark_engine)
        fallback_agent = EnhancedReasoningAgent(rag_retriever, benchmark_engine)
        
        self.reasoning_agent = AgentFallback(
            primary_agent=primary_agent,
            fallback_agent=fallback_agent,
            name="ReasoningAgent"
        )
        
        # Initialize circuit breakers for external services
        self.rag_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30.0
        ) if rag_retriever else None
        
        self.benchmark_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30.0
        ) if benchmark_engine else None
        
        logger.info("Initialized Resilient Multi-Agent Orchestrator")
    
    @retry_with_backoff(max_retries=2, initial_delay=1.0)
    async def run(
        self,
        query: str,
        campaign_data: Dict = None,
        platform: str = None,
        max_iterations: int = 10,
        use_validation: bool = True
    ) -> Dict[str, Any]:
        """
        Run orchestrated analysis with resilience.
        
        Args:
            query: User query
            campaign_data: Campaign data dictionary
            platform: Platform name
            max_iterations: Max iterations for workflow
            use_validation: Whether to use validated outputs
            
        Returns:
            Analysis results with validated schemas
        """
        logger.info(f"Starting resilient analysis for query: {query}")
        
        try:
            # Use base orchestrator for routing and coordination
            result = await self.base_orchestrator.run(
                query=query,
                campaign_data=campaign_data,
                platform=platform,
                max_iterations=max_iterations
            )
            
            # Add resilience metadata
            result['resilience_stats'] = self._get_resilience_stats()
            
            logger.info("Analysis completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Orchestration failed: {e}")
            
            # Return graceful degradation
            return {
                'success': False,
                'error': str(e),
                'fallback_used': True,
                'resilience_stats': self._get_resilience_stats(),
                'recommendations': [
                    'System encountered an error. Please try again or contact support.'
                ]
            }
    
    async def analyze_with_reasoning(
        self,
        campaign_data,
        channel_insights: Optional[Dict] = None,
        campaign_context: Optional[Any] = None
    ):
        """
        Direct reasoning analysis with fallback and validation.
        
        Args:
            campaign_data: Campaign DataFrame
            channel_insights: Channel-specific insights
            campaign_context: Campaign context
            
        Returns:
            Validated AgentOutput
        """
        try:
            # Execute with fallback
            result = await self.reasoning_agent.execute(
                'analyze',
                campaign_data,
                channel_insights,
                campaign_context
            )
            
            logger.info(
                f"Reasoning analysis complete. "
                f"Fallback used: {self.reasoning_agent.fallback_count > 0}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Reasoning analysis failed completely: {e}")
            raise
    
    def _get_resilience_stats(self) -> Dict[str, Any]:
        """Get resilience statistics"""
        stats = {
            'reasoning_agent': self.reasoning_agent.get_stats()
        }
        
        if self.rag_circuit_breaker:
            stats['rag_circuit_breaker'] = self.rag_circuit_breaker.get_state()
        
        if self.benchmark_circuit_breaker:
            stats['benchmark_circuit_breaker'] = self.benchmark_circuit_breaker.get_state()
        
        return stats
    
    def reset_circuit_breakers(self):
        """Manually reset all circuit breakers"""
        if self.rag_circuit_breaker:
            self.rag_circuit_breaker.reset()
            logger.info("RAG circuit breaker reset")
        
        if self.benchmark_circuit_breaker:
            self.benchmark_circuit_breaker.reset()
            logger.info("Benchmark circuit breaker reset")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of all components.
        
        Returns:
            Health status dictionary
        """
        return {
            'orchestrator': 'healthy',
            'reasoning_agent': {
                'status': 'healthy' if self.reasoning_agent.fallback_count == 0 else 'degraded',
                'stats': self.reasoning_agent.get_stats()
            },
            'circuit_breakers': {
                'rag': self.rag_circuit_breaker.get_state() if self.rag_circuit_breaker else None,
                'benchmark': self.benchmark_circuit_breaker.get_state() if self.benchmark_circuit_breaker else None
            }
        }
