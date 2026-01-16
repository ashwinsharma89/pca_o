"""
Production deployment script for Phase 2 integration.

This script validates the installation and provides a simple test.
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime

def test_imports():
    """Test that all required modules can be imported"""
    print("🔍 Testing imports...")
    
    try:
        from src.engine.agents.schemas import (
            AgentOutput,
            AgentInsight,
            AgentRecommendation,
            filter_high_confidence_insights
        )
        print("  ✅ Schemas imported successfully")
    except ImportError as e:
        print(f"  ❌ Failed to import schemas: {e}")
        return False
    
    try:
        from src.engine.agents.agent_resilience import (
            AgentFallback,
            retry_with_backoff,
            CircuitBreaker
        )
        print("  ✅ Resilience mechanisms imported successfully")
    except ImportError as e:
        print(f"  ❌ Failed to import resilience: {e}")
        return False
    
    try:
        from src.engine.agents.validated_reasoning_agent import ValidatedReasoningAgent
        print("  ✅ Validated reasoning agent imported successfully")
    except ImportError as e:
        print(f"  ❌ Failed to import validated agent: {e}")
        return False
    
    return True


def test_basic_functionality():
    """Test basic functionality"""
    print("\n🧪 Testing basic functionality...")
    
    try:
        from src.engine.agents.agent_resilience import AgentFallback
        from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
        from src.engine.agents.reasoning_agent import ReasoningAgent
        
        # Create sample data
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = pd.DataFrame({
            'Date': dates,
            'Spend': np.random.uniform(1000, 2000, 10),
            'Impressions': np.random.uniform(10000, 20000, 10),
            'Clicks': np.random.uniform(500, 1000, 10),
            'Conversions': np.random.uniform(50, 100, 10),
            'CTR': np.random.uniform(0.04, 0.06, 10),
            'CPC': np.random.uniform(1.5, 2.5, 10),
            'Campaign': ['Test_Campaign'] * 10
        })
        
        # Setup agents
        primary = EnhancedReasoningAgent()
        fallback = ReasoningAgent()
        agent = AgentFallback(primary, fallback, name="TestAgent")
        
        # Execute
        result = agent.execute('analyze', data)
        
        print("  ✅ Basic agent execution successful")
        print(f"     Insights: {len(result.get('insights', {}).get('pattern_insights', []))}")
        print(f"     Recommendations: {len(result.get('recommendations', []))}")
        
        # Check stats
        stats = agent.get_stats()
        print(f"  ✅ Statistics tracking working")
        print(f"     Executions: {stats['total_executions']}")
        print(f"     Fallback rate: {stats['fallback_rate_percent']}%")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Basic functionality test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_validation():
    """Test Pydantic validation"""
    print("\n✅ Testing Pydantic validation...")
    
    try:
        from src.engine.agents.schemas import AgentOutput, AgentInsight, AgentRecommendation, AgentMetadata
        
        # Create validated output
        output = AgentOutput(
            insights=[
                AgentInsight(
                    text="Test insight with sufficient length",
                    confidence=0.85
                )
            ],
            recommendations=[
                AgentRecommendation(
                    action="Test recommendation action",
                    rationale="Test rationale with sufficient length",
                    priority=2,
                    confidence=0.75
                )
            ],
            metadata=AgentMetadata(
                agent_name="TestAgent",
                data_points_analyzed=10
            ),
            overall_confidence=0.8
        )
        
        print("  ✅ Pydantic validation working")
        print(f"     Insights: {len(output.insights)}")
        print(f"     Recommendations: {len(output.recommendations)}")
        print(f"     Overall confidence: {output.overall_confidence:.2%}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Validation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all deployment tests"""
    print("=" * 60)
    print("Phase 2 Integration - Production Deployment Test")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Run tests
    tests = [
        ("Import Test", test_imports),
        ("Basic Functionality", test_basic_functionality),
        ("Validation Test", test_validation),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print()
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Phase 2 is production-ready.")
        print("\nNext steps:")
        print("1. Monitor fallback rates in production")
        print("2. Set up alerting for high fallback rates (>20%)")
        print("3. Review confidence scores regularly")
        print("4. Add monitoring dashboards")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please review errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
