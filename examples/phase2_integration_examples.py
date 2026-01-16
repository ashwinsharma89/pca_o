"""
Simple example demonstrating Phase 2 integration usage.

This example shows how to use the validated agents and resilience mechanisms
in a production environment.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio

# Import the new validated components
from src.agents.schemas import (
    AgentOutput,
    filter_high_confidence_insights,
    filter_high_confidence_recommendations,
    filter_by_priority
)
from src.agents.agent_resilience import AgentFallback, retry_with_backoff
from src.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
from src.agents.reasoning_agent import ReasoningAgent


def create_sample_data():
    """Create sample campaign data for testing"""
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    
    return pd.DataFrame({
        'Date': dates,
        'Spend': np.random.uniform(1000, 2000, 30),
        'Impressions': np.random.uniform(10000, 20000, 30),
        'Clicks': np.random.uniform(500, 1000, 30),
        'Conversions': np.random.uniform(50, 100, 30),
        'CTR': np.random.uniform(0.04, 0.06, 30),
        'CPC': np.random.uniform(1.5, 2.5, 30),
        'ROAS': np.random.uniform(3.0, 6.0, 30),
        'Campaign': ['Campaign_A'] * 30,
        'Platform': ['Google Ads'] * 30
    })


def example_1_basic_agent_with_fallback():
    """
    Example 1: Basic agent usage with fallback mechanism
    """
    print("=" * 60)
    print("Example 1: Agent with Fallback")
    print("=" * 60)
    
    # Create sample data
    data = create_sample_data()
    
    # Setup agents with fallback
    primary_agent = EnhancedReasoningAgent()
    fallback_agent = ReasoningAgent()
    
    resilient_agent = AgentFallback(
        primary_agent=primary_agent,
        fallback_agent=fallback_agent,
        name="ReasoningAgent"
    )
    
    # Analyze
    print("\n📊 Running analysis...")
    result = resilient_agent.execute('analyze', data)
    
    # Check results
    print(f"\n✅ Analysis complete!")
    print(f"   Insights: {len(result.get('insights', {}).get('pattern_insights', []))}")
    print(f"   Recommendations: {len(result.get('recommendations', []))}")
    
    # Check fallback stats
    stats = resilient_agent.get_stats()
    print(f"\n📈 Resilience Stats:")
    print(f"   Total executions: {stats['total_executions']}")
    print(f"   Fallback count: {stats['fallback_count']}")
    print(f"   Fallback rate: {stats['fallback_rate_percent']}%")
    
    return result


def example_2_with_retry():
    """
    Example 2: Using retry decorator for resilience
    """
    print("\n" + "=" * 60)
    print("Example 2: Retry with Exponential Backoff")
    print("=" * 60)
    
    call_count = 0
    
    @retry_with_backoff(max_retries=3, initial_delay=0.1)
    def potentially_failing_operation():
        nonlocal call_count
        call_count += 1
        
        print(f"\n🔄 Attempt {call_count}")
        
        # Simulate failure on first 2 attempts
        if call_count < 3:
            raise ValueError(f"Simulated failure #{call_count}")
        
        return {"status": "success", "data": "Operation completed"}
    
    try:
        result = potentially_failing_operation()
        print(f"\n✅ Success after {call_count} attempts!")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"\n❌ Failed after {call_count} attempts: {e}")


def example_3_confidence_filtering():
    """
    Example 3: Using Pydantic schemas for confidence filtering
    """
    print("\n" + "=" * 60)
    print("Example 3: Confidence-Based Filtering")
    print("=" * 60)
    
    # Import schemas
    from src.agents.schemas import AgentOutput, AgentInsight, AgentRecommendation, AgentMetadata
    
    # Create sample validated output
    output = AgentOutput(
        insights=[
            AgentInsight(text="High confidence insight about CTR improvement", confidence=0.92),
            AgentInsight(text="Medium confidence insight about spend patterns", confidence=0.65),
            AgentInsight(text="Low confidence insight about seasonality", confidence=0.42),
        ],
        recommendations=[
            AgentRecommendation(
                action="Increase budget by 20%",
                rationale="CTR is high and CPA is below target",
                priority=1,
                confidence=0.88
            ),
            AgentRecommendation(
                action="Test new creative variations",
                rationale="Current creative showing signs of fatigue",
                priority=2,
                confidence=0.75
            ),
            AgentRecommendation(
                action="Expand audience targeting",
                rationale="Current audience may be saturating",
                priority=3,
                confidence=0.55
            ),
        ],
        metadata=AgentMetadata(
            agent_name="EnhancedReasoningAgent",
            data_points_analyzed=30
        ),
        overall_confidence=0.73
    )
    
    print(f"\n📊 Total Output:")
    print(f"   Insights: {len(output.insights)}")
    print(f"   Recommendations: {len(output.recommendations)}")
    print(f"   Overall Confidence: {output.overall_confidence:.2%}")
    
    # Filter high-confidence insights
    high_conf_insights = filter_high_confidence_insights(output, threshold=0.8)
    print(f"\n🎯 High Confidence Insights (≥80%):")
    print(f"   Count: {len(high_conf_insights)}")
    for insight in high_conf_insights:
        print(f"   • {insight.text} ({insight.confidence:.0%})")
    
    # Filter high-confidence recommendations
    high_conf_recs = filter_high_confidence_recommendations(output, threshold=0.7)
    print(f"\n🎯 High Confidence Recommendations (≥70%):")
    print(f"   Count: {len(high_conf_recs)}")
    for rec in high_conf_recs:
        print(f"   • [{rec.priority}] {rec.action} ({rec.confidence:.0%})")
    
    # Filter by priority
    critical_recs = filter_by_priority(output, max_priority=2)
    print(f"\n⚡ Critical Recommendations (Priority 1-2):")
    print(f"   Count: {len(critical_recs)}")
    for rec in critical_recs:
        print(f"   • [{rec.priority}] {rec.action}")


def example_4_health_monitoring():
    """
    Example 4: Health monitoring and statistics
    """
    print("\n" + "=" * 60)
    print("Example 4: Health Monitoring")
    print("=" * 60)
    
    # Create agent with fallback
    primary = EnhancedReasoningAgent()
    fallback = ReasoningAgent()
    agent = AgentFallback(primary, fallback, name="ReasoningAgent")
    
    # Run multiple analyses
    data = create_sample_data()
    
    print("\n🔄 Running 5 analyses...")
    for i in range(5):
        try:
            agent.execute('analyze', data)
            print(f"   Analysis {i+1}: ✅")
        except Exception as e:
            print(f"   Analysis {i+1}: ❌ {e}")
    
    # Get health stats
    stats = agent.get_stats()
    
    print(f"\n📊 Health Statistics:")
    print(f"   Agent: {stats['agent_name']}")
    print(f"   Total Executions: {stats['total_executions']}")
    print(f"   Fallback Count: {stats['fallback_count']}")
    print(f"   Fallback Rate: {stats['fallback_rate_percent']}%")
    print(f"   Has Fallback: {stats['has_fallback']}")
    
    # Determine health status
    if stats['fallback_rate_percent'] == 0:
        status = "🟢 HEALTHY"
    elif stats['fallback_rate_percent'] < 20:
        status = "🟡 DEGRADED"
    else:
        status = "🔴 UNHEALTHY"
    
    print(f"\n   Status: {status}")


def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("Phase 2 Integration - Usage Examples")
    print("=" * 60)
    
    # Run examples
    example_1_basic_agent_with_fallback()
    example_2_with_retry()
    example_3_confidence_filtering()
    example_4_health_monitoring()
    
    print("\n" + "=" * 60)
    print("✅ All examples completed successfully!")
    print("=" * 60)
    print("\nKey Takeaways:")
    print("1. ✅ Fallback mechanisms prevent complete failures")
    print("2. ✅ Retry logic handles transient errors")
    print("3. ✅ Confidence filtering enables quality control")
    print("4. ✅ Health monitoring tracks system performance")
    print("\n")


if __name__ == "__main__":
    main()
