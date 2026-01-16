"""
Simple test of the new DI system.
"""

import pandas as pd
import numpy as np

# Import DI functions
from src.di.containers import (
    get_container,
    init_container,
    get_reasoning_agent,
    get_validated_reasoning_agent,
    get_hybrid_retriever,
    get_benchmark_engine
)


def test_container_initialization():
    """Test that container initializes correctly."""
    print("=" * 60)
    print("Test 1: Container Initialization")
    print("=" * 60)
    
    container = init_container()
    print(f"✅ Container initialized: {container}")
    print(f"   Knowledge container: {container.knowledge}")
    print(f"   Agents container: {container.agents}")
    print()


def test_knowledge_components():
    """Test knowledge base components."""
    print("=" * 60)
    print("Test 2: Knowledge Base Components")
    print("=" * 60)
    
    retriever = get_hybrid_retriever()
    benchmarks = get_benchmark_engine()
    
    print(f"Retriever: {'✅ Available' if retriever else '⚠️  Not available'}")
    print(f"Benchmarks: {'✅ Available' if benchmarks else '⚠️  Not available'}")
    print()


def test_agent_creation():
    """Test agent creation from container."""
    print("=" * 60)
    print("Test 3: Agent Creation")
    print("=" * 60)
    
    reasoning_agent = get_reasoning_agent()
    validated_agent = get_validated_reasoning_agent()
    
    print(f"Reasoning Agent: {'✅ Available' if reasoning_agent else '⚠️  Not available'}")
    print(f"Validated Agent: {'✅ Available' if validated_agent else '⚠️  Not available'}")
    print()


def test_agent_usage():
    """Test using an agent with real data."""
    print("=" * 60)
    print("Test 4: Agent Usage with Data")
    print("=" * 60)
    
    agent = get_reasoning_agent()
    
    if not agent:
        print("⚠️  Agent not available")
        return
    
    # Create test data
    dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
    data = pd.DataFrame({
        'Date': dates,
        'Platform': ['meta'] * 10,
        'Spend': np.random.uniform(1000, 2000, 10),
        'Impressions': np.random.uniform(10000, 20000, 10),
        'Clicks': np.random.uniform(500, 1000, 10),
        'Conversions': np.random.uniform(50, 100, 10),
        'CTR': np.random.uniform(0.04, 0.06, 10),
        'CPC': np.random.uniform(1.5, 2.5, 10),
        'Campaign': ['Campaign_A'] * 10
    })
    
    print("Running analysis...")
    try:
        result = agent.analyze(data)
        print(f"✅ Analysis complete!")
        print(f"   Result keys: {list(result.keys())}")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
    
    print()


def test_singleton_behavior():
    """Test that singletons return same instance."""
    print("=" * 60)
    print("Test 5: Singleton Behavior")
    print("=" * 60)
    
    retriever1 = get_hybrid_retriever()
    retriever2 = get_hybrid_retriever()
    
    if retriever1 and retriever2:
        same = retriever1 is retriever2
        print(f"Singleton check: {'✅ Same instance' if same else '❌ Different instances'}")
    else:
        print("⚠️  Retrievers not available")
    
    print()


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "DI System Tests" + " " * 27 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        test_container_initialization()
        test_knowledge_components()
        test_agent_creation()
        test_agent_usage()
        test_singleton_behavior()
        
        print("=" * 60)
        print("✅ All tests completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
