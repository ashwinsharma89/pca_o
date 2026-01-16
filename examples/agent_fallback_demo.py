"""
Demonstration: How Agent Failure Cascade is Fixed

This example shows how the AgentFallback mechanism prevents
one agent failure from bringing down the entire workflow.
"""

from src.agents.agent_resilience import AgentFallback
from src.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
from src.agents.reasoning_agent import ReasoningAgent
import pandas as pd
import numpy as np

# ============================================================================
# BEFORE: Agent Failures Cascade ❌
# ============================================================================

def old_workflow_without_fallback(data):
    """
    OLD WAY: If EnhancedReasoningAgent fails, entire workflow fails
    """
    agent = EnhancedReasoningAgent()
    
    try:
        result = agent.analyze(data)
        return result
    except Exception as e:
        # PROBLEM: Workflow fails completely
        print(f"❌ WORKFLOW FAILED: {e}")
        raise  # Entire system crashes


# ============================================================================
# AFTER: Agent Failures Handled Gracefully ✅
# ============================================================================

def new_workflow_with_fallback(data):
    """
    NEW WAY: If primary agent fails, automatically use fallback agent
    """
    # Setup primary and fallback agents
    primary_agent = EnhancedReasoningAgent()
    fallback_agent = ReasoningAgent()
    
    # Wrap with AgentFallback
    resilient_agent = AgentFallback(
        primary_agent=primary_agent,
        fallback_agent=fallback_agent,
        name="ReasoningAgent"
    )
    
    # Execute - will automatically fallback if primary fails
    result = resilient_agent.execute('analyze', data)
    
    # Check if fallback was used
    stats = resilient_agent.get_stats()
    if stats['fallback_count'] > 0:
        print(f"⚠️  Primary agent failed, used fallback successfully")
        print(f"   Fallback rate: {stats['fallback_rate_percent']}%")
    else:
        print(f"✅ Primary agent succeeded")
    
    return result


# ============================================================================
# Example Usage
# ============================================================================

def main():
    """Demonstrate the difference"""
    
    # Create sample data
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    data = pd.DataFrame({
        'Date': dates,
        'Spend': np.random.uniform(1000, 2000, 30),
        'Impressions': np.random.uniform(10000, 20000, 30),
        'Clicks': np.random.uniform(500, 1000, 30),
        'Conversions': np.random.uniform(50, 100, 30),
        'CTR': np.random.uniform(0.04, 0.06, 30),
        'CPC': np.random.uniform(1.5, 2.5, 30),
        'Campaign': ['Campaign_A'] * 30
    })
    
    print("=" * 60)
    print("Agent Failure Cascade - FIXED ✅")
    print("=" * 60)
    print()
    
    # Use new workflow with fallback
    print("Running analysis with fallback protection...")
    result = new_workflow_with_fallback(data)
    
    print()
    print("✅ Workflow completed successfully!")
    print(f"   Insights: {len(result.get('insights', {}).get('pattern_insights', []))}")
    print(f"   Recommendations: {len(result.get('recommendations', []))}")
    print()
    
    print("=" * 60)
    print("Benefits of Fallback Mechanism:")
    print("=" * 60)
    print("1. ✅ Primary agent failure doesn't crash system")
    print("2. ✅ Automatically switches to simpler fallback agent")
    print("3. ✅ Tracks fallback statistics for monitoring")
    print("4. ✅ Graceful degradation instead of complete failure")
    print("5. ✅ User still gets results (maybe less sophisticated)")
    print()


if __name__ == "__main__":
    main()
