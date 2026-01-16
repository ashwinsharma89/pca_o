"""
Comprehensive examples demonstrating the new DI system.

Shows how to:
1. Get agents from the container
2. Use agents with injected dependencies
3. Test with mocked dependencies
4. Create custom configurations
"""

import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock

# Import DI container
from src.di.containers import (
    get_container,
    init_container,
    get_reasoning_agent,
    get_orchestrator,
    get_knowledge_base
)

# Import protocols for mocking
from src.interfaces.protocols import IRetriever, IBenchmarkEngine


# ============================================================================
# Example 1: Basic Usage - Get Agents from Container
# ============================================================================

def example_1_basic_usage():
    """Get agents from the DI container."""
    print("=" * 60)
    print("Example 1: Basic Usage")
    print("=" * 60)
    
    # Initialize container
    container = init_container()
    
    # Get agents (all dependencies injected automatically)
    reasoning_agent = container.agents.enhanced_reasoning_agent()
    orchestrator = container.agents.orchestrator()
    knowledge_base = container.knowledge.knowledge_base()
    
    print(f"✅ Reasoning Agent: {reasoning_agent}")
    print(f"✅ Orchestrator: {orchestrator}")
    print(f"✅ Knowledge Base: {knowledge_base}")
    print()


# ============================================================================
# Example 2: Using Agents with Real Data
# ============================================================================

def example_2_real_usage():
    """Use agents with real campaign data."""
    print("=" * 60)
    print("Example 2: Real Usage with Campaign Data")
    print("=" * 60)
    
    # Get reasoning agent from container
    agent = get_reasoning_agent()
    
    if not agent:
        print("⚠️  Agent not available (dependencies missing)")
        return
    
    # Create sample campaign data
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    data = pd.DataFrame({
        'Date': dates,
        'Platform': ['meta'] * 30,
        'Spend': np.random.uniform(1000, 2000, 30),
        'Impressions': np.random.uniform(10000, 20000, 30),
        'Clicks': np.random.uniform(500, 1000, 30),
        'Conversions': np.random.uniform(50, 100, 30),
        'CTR': np.random.uniform(0.04, 0.06, 30),
        'CPC': np.random.uniform(1.5, 2.5, 30),
        'Campaign': ['Campaign_A'] * 30
    })
    
    # Run analysis
    print("Running analysis...")
    try:
        result = agent.analyze(
            campaign_data=data,
            channel_insights=None,
            campaign_context=None
        )
        
        print(f"✅ Analysis complete!")
        print(f"   Insights: {len(result.get('insights', {}).get('pattern_insights', []))}")
        print(f"   Recommendations: {len(result.get('recommendations', []))}")
        print(f"   Benchmarks applied: {result.get('benchmarks_applied', False)}")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
    
    print()


# ============================================================================
# Example 3: Testing with Mocked Dependencies
# ============================================================================

def example_3_testing_with_mocks():
    """Demonstrate testing with mocked dependencies."""
    print("=" * 60)
    print("Example 3: Testing with Mocked Dependencies")
    print("=" * 60)
    
    # Create mocks
    mock_retriever = Mock(spec=IRetriever)
    mock_retriever.retrieve.return_value = [
        {'content': 'Optimization strategy 1', 'score': 0.9},
        {'content': 'Optimization strategy 2', 'score': 0.8},
        {'content': 'Optimization strategy 3', 'score': 0.7}
    ]
    
    mock_benchmarks = Mock(spec=IBenchmarkEngine)
    mock_benchmarks.get_benchmarks.return_value = {
        'CTR': 0.05,
        'CPC': 2.0,
        'Conversions': 100,
        'ROAS': 3.0
    }
    
    # Import the DI-enabled agent POC
    from examples.di_poc_reasoning_agent import EnhancedReasoningAgentDI
    
    # Create agent with mocked dependencies
    agent = EnhancedReasoningAgentDI(
        retriever=mock_retriever,
        benchmarks=mock_benchmarks,
        name="TestAgent"
    )
    
    # Create test data
    test_data = pd.DataFrame({
        'Platform': ['meta'] * 10,
        'CTR': [0.03] * 10,  # Below benchmark
        'CPC': [2.5] * 10,   # Above benchmark
        'Conversions': [80] * 10  # Below benchmark
    })
    
    # Run analysis
    result = agent.analyze(test_data)
    
    # Verify mocks were called
    print(f"✅ Mock retriever called: {mock_retriever.retrieve.called}")
    print(f"✅ Mock benchmarks called: {mock_benchmarks.get_benchmarks.called}")
    print(f"✅ Recommendations generated: {len(result['recommendations'])}")
    
    # Check that recommendations were generated for below-benchmark metrics
    for rec in result['recommendations']:
        print(f"   - {rec['issue']}: {rec['recommendation']}")
    
    print()


# ============================================================================
# Example 4: Custom Configuration
# ============================================================================

def example_4_custom_configuration():
    """Create custom container configuration."""
    print("=" * 60)
    print("Example 4: Custom Configuration")
    print("=" * 60)
    
    from src.di.containers import ApplicationContainer
    
    # Create custom container
    custom_container = ApplicationContainer()
    
    # Override configuration
    custom_container.config.use_anthropic.override(True)
    custom_container.config.anthropic_api_key.override("custom-key")
    
    print("✅ Custom container created with overrides")
    print(f"   Use Anthropic: {custom_container.config.use_anthropic()}")
    print()


# ============================================================================
# Example 5: Accessing Different Agent Types
# ============================================================================

def example_5_different_agents():
    """Access different types of agents from container."""
    print("=" * 60)
    print("Example 5: Different Agent Types")
    print("=" * 60)
    
    container = get_container()
    
    # Get different agent types
    agents = {
        'Reasoning': container.agents.reasoning_agent(),
        'Enhanced Reasoning': container.agents.enhanced_reasoning_agent(),
        'Validated Reasoning': container.agents.validated_reasoning_agent(),
        'Search Specialist': container.agents.search_specialist(),
        'Social Specialist': container.agents.social_specialist(),
        'Vision Agent': container.agents.vision_agent(),
        'Orchestrator': container.agents.orchestrator()
    }
    
    print("Available agents:")
    for name, agent in agents.items():
        status = "✅ Available" if agent else "⚠️  Not available"
        print(f"   {name}: {status}")
    
    print()


# ============================================================================
# Example 6: Knowledge Base Access
# ============================================================================

def example_6_knowledge_base():
    """Access knowledge base components."""
    print("=" * 60)
    print("Example 6: Knowledge Base Components")
    print("=" * 60)
    
    container = get_container()
    
    # Get knowledge components
    retriever = container.knowledge.hybrid_retriever()
    benchmarks = container.knowledge.benchmark_engine()
    kb = container.knowledge.knowledge_base()
    
    print("Knowledge base components:")
    print(f"   Retriever: {'✅ Available' if retriever else '⚠️  Not available'}")
    print(f"   Benchmarks: {'✅ Available' if benchmarks else '⚠️  Not available'}")
    print(f"   Knowledge Base: {'✅ Available' if kb else '⚠️  Not available'}")
    print()


# ============================================================================
# Example 7: Repository Access
# ============================================================================

def example_7_repositories():
    """Access repositories from container."""
    print("=" * 60)
    print("Example 7: Repository Access")
    print("=" * 60)
    
    container = get_container()
    
    # Get repositories
    try:
        campaign_repo = container.repositories.campaign_repository()
        analysis_repo = container.repositories.analysis_repository()
        
        print("✅ Repositories available:")
        print(f"   Campaign Repository: {campaign_repo}")
        print(f"   Analysis Repository: {analysis_repo}")
    except Exception as e:
        print(f"⚠️  Repositories not available: {e}")
    
    print()


# ============================================================================
# Example 8: Service Access
# ============================================================================

def example_8_services():
    """Access services from container."""
    print("=" * 60)
    print("Example 8: Service Access")
    print("=" * 60)
    
    container = get_container()
    
    # Get services
    analytics = container.services.analytics_expert()
    user_service = container.services.user_service()
    
    print("Services:")
    print(f"   Analytics Expert: {'✅ Available' if analytics else '⚠️  Not available'}")
    print(f"   User Service: {'✅ Available' if user_service else '⚠️  Not available'}")
    print()


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 10 + "Dependency Injection Examples" + " " * 18 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        example_1_basic_usage()
        example_2_real_usage()
        example_3_testing_with_mocks()
        example_4_custom_configuration()
        example_5_different_agents()
        example_6_knowledge_base()
        example_7_repositories()
        example_8_services()
        
        print("=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
