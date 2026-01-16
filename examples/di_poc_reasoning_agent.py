"""
Proof of Concept: Enhanced Reasoning Agent with Dependency Injection

This demonstrates how to refactor an existing agent to use:
- Protocol-based dependencies (IRetriever, IBenchmarkEngine)
- Base class inheritance (BaseAgent)
- Constructor injection
- Type hints for better IDE support

Benefits:
- Easy to test with mocks
- Clear dependencies
- No tight coupling to concrete implementations
- Can swap implementations without changing agent code
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
from loguru import logger

# Import protocols instead of concrete classes
from src.interfaces.protocols import IRetriever, IBenchmarkEngine
from src.interfaces.base_classes import BaseAgent


class EnhancedReasoningAgentDI(BaseAgent):
    """
    Enhanced Reasoning Agent with Dependency Injection.
    
    This is a refactored version that uses protocols for dependencies,
    making it easier to test and maintain.
    """
    
    def __init__(
        self,
        retriever: Optional[IRetriever] = None,
        benchmark_engine: Optional[IBenchmarkEngine] = None,
        name: str = "EnhancedReasoningAgent"
    ):
        """
        Initialize with injected dependencies.
        
        Args:
            retriever: RAG retriever (protocol interface)
            benchmark_engine: Benchmark data provider (protocol interface)
            name: Agent name for logging
        """
        # Call base class constructor
        super().__init__(name=name)
        
        # Store protocol-based dependencies
        self.retriever = retriever
        self.benchmark_engine = benchmark_engine
        
        logger.info(
            f"Initialized {self.name} with "
            f"retriever={retriever is not None}, "
            f"benchmarks={benchmark_engine is not None}"
        )
    
    def analyze(
        self,
        campaign_data: pd.DataFrame,
        channel_insights: Optional[Dict[str, Any]] = None,
        campaign_context: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Perform comprehensive analysis.
        
        Args:
            campaign_data: Campaign performance data
            channel_insights: Optional channel-specific insights
            campaign_context: Optional business context
            
        Returns:
            Analysis results with insights and recommendations
        """
        start_time = datetime.utcnow()
        
        try:
            # Detect platform and objective
            platform = self._detect_platform(campaign_data)
            objective = self._detect_objective(campaign_data)
            
            # Get benchmarks using protocol interface
            benchmarks = None
            if self.benchmark_engine:
                benchmarks = self.benchmark_engine.get_benchmarks(
                    platform=platform,
                    objective=objective
                )
            
            # Detect patterns (simplified for POC)
            patterns = self._detect_patterns(campaign_data)
            
            # Generate insights
            insights = self._generate_insights(
                campaign_data,
                patterns,
                benchmarks
            )
            
            # Get optimization strategies using protocol interface
            optimization_context = None
            if self.retriever and patterns:
                query = self._build_optimization_query(insights, patterns)
                optimization_context = self.retriever.retrieve(
                    query=query,
                    top_k=3,
                    filters={'platform': platform, 'objective': objective}
                )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                insights,
                patterns,
                benchmarks,
                optimization_context
            )
            
            # Track execution time
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._track_execution(execution_time)
            
            return {
                'insights': insights,
                'patterns': patterns,
                'benchmarks_applied': benchmarks is not None,
                'optimization_context': optimization_context,
                'recommendations': recommendations,
                'analysis_timestamp': datetime.now().isoformat(),
                'execution_time_seconds': execution_time
            }
            
        except Exception as e:
            self._handle_error(e, {
                'platform': platform,
                'data_shape': campaign_data.shape
            })
    
    def _detect_platform(self, data: pd.DataFrame) -> str:
        """Detect platform from campaign data."""
        if 'Platform' in data.columns and len(data) > 0:
            return str(data['Platform'].iloc[0]).lower()
        return 'unknown'
    
    def _detect_objective(self, data: pd.DataFrame) -> str:
        """Detect campaign objective from data."""
        # Simplified logic for POC
        return 'conversions'
    
    def _detect_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect patterns in campaign data (simplified for POC)."""
        return {
            'trends': {'detected': False},
            'anomalies': {'detected': False},
            'seasonality': {'detected': False}
        }
    
    def _generate_insights(
        self,
        data: pd.DataFrame,
        patterns: Dict[str, Any],
        benchmarks: Optional[Dict[str, float]]
    ) -> Dict[str, Any]:
        """Generate insights from data and patterns."""
        insights = {
            'pattern_insights': [],
            'performance_summary': {},
            'benchmark_comparison': {}
        }
        
        # Add benchmark comparison if available
        if benchmarks:
            for metric, benchmark_value in benchmarks.items():
                if metric in data.columns:
                    actual_value = data[metric].mean()
                    insights['benchmark_comparison'][metric] = {
                        'actual': actual_value,
                        'benchmark': benchmark_value,
                        'status': 'good' if actual_value >= benchmark_value else 'needs_work'
                    }
        
        return insights
    
    def _build_optimization_query(
        self,
        insights: Dict[str, Any],
        patterns: Dict[str, Any]
    ) -> str:
        """Build query for RAG retrieval."""
        return "optimization strategies for campaign performance"
    
    def _generate_recommendations(
        self,
        insights: Dict[str, Any],
        patterns: Dict[str, Any],
        benchmarks: Optional[Dict[str, float]],
        optimization_context: Optional[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Generate recommendations based on benchmark comparison
        if insights.get('benchmark_comparison'):
            for metric, comparison in insights['benchmark_comparison'].items():
                if comparison['status'] == 'needs_work':
                    recommendations.append({
                        'issue': f"{metric} below benchmark",
                        'recommendation': f"Improve {metric} performance",
                        'priority': 'high',
                        'expected_impact': 'medium'
                    })
        
        return recommendations


# ============================================================================
# Example Usage
# ============================================================================

def example_usage():
    """Demonstrate how to use the DI-enabled agent."""
    
    # Example 1: Create agent with mock dependencies for testing
    from unittest.mock import Mock
    
    mock_retriever = Mock(spec=IRetriever)
    mock_retriever.retrieve.return_value = [
        {'content': 'Strategy 1', 'score': 0.9},
        {'content': 'Strategy 2', 'score': 0.8}
    ]
    
    mock_benchmarks = Mock(spec=IBenchmarkEngine)
    mock_benchmarks.get_benchmarks.return_value = {
        'CTR': 0.05,
        'CPC': 2.0,
        'Conversions': 100
    }
    
    # Create agent with mocked dependencies
    agent = EnhancedReasoningAgentDI(
        retriever=mock_retriever,
        benchmark_engine=mock_benchmarks
    )
    
    # Create test data
    test_data = pd.DataFrame({
        'Platform': ['meta'] * 10,
        'CTR': [0.04] * 10,
        'CPC': [2.5] * 10,
        'Conversions': [80] * 10
    })
    
    # Run analysis
    result = agent.analyze(test_data)
    
    print("Analysis Result:")
    print(f"- Benchmarks applied: {result['benchmarks_applied']}")
    print(f"- Recommendations: {len(result['recommendations'])}")
    print(f"- Execution time: {result['execution_time_seconds']:.3f}s")
    
    # Example 2: Create agent without dependencies (graceful degradation)
    minimal_agent = EnhancedReasoningAgentDI()
    result2 = minimal_agent.analyze(test_data)
    print(f"\nMinimal agent (no dependencies):")
    print(f"- Benchmarks applied: {result2['benchmarks_applied']}")
    print(f"- Still works: {result2 is not None}")


if __name__ == "__main__":
    example_usage()
