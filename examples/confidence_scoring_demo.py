"""
Demonstration: Confidence Scoring in Agent Outputs

This example shows how ALL agent outputs now include confidence scores
that allow filtering of low-quality insights.
"""

from src.agents.validated_reasoning_agent import ValidatedReasoningAgent
from src.agents.schemas import AgentOutput, ConfidenceLevel
import pandas as pd
import numpy as np

# ============================================================================
# BEFORE: No Confidence Scores ❌
# ============================================================================

def old_output_without_confidence():
    """
    OLD WAY: No way to know if insights are reliable
    """
    return {
        'insights': ['CTR is trending up'],
        'recommendations': ['Increase budget'],
        # ❌ No confidence score - can't filter low-quality insights
    }


# ============================================================================
# AFTER: All Outputs Have Confidence Scores ✅
# ============================================================================

def new_output_with_confidence(data):
    """
    NEW WAY: Every output includes confidence scores
    """
    agent = ValidatedReasoningAgent()
    
    # Get validated output with confidence scores
    result: AgentOutput = agent.analyze(
        campaign_data=data,
        return_validated=True  # Returns Pydantic schema with confidence
    )
    
    return result


# ============================================================================
# Filtering Low-Quality Insights
# ============================================================================

def filter_by_confidence(agent_output: AgentOutput, min_confidence: float = 0.7):
    """
    Filter insights and recommendations by confidence threshold
    """
    print(f"\n{'='*60}")
    print(f"Filtering Results (min confidence: {min_confidence})")
    print(f"{'='*60}\n")
    
    # Overall confidence
    print(f"Overall Confidence: {agent_output.overall_confidence:.2f}")
    print()
    
    # Filter high-confidence insights
    high_confidence_insights = [
        insight for insight in agent_output.insights
        if insight.confidence >= min_confidence
    ]
    
    print(f"High-Confidence Insights ({len(high_confidence_insights)}/{len(agent_output.insights)}):")
    for insight in high_confidence_insights:
        print(f"  ✅ [{insight.confidence:.2f}] {insight.text}")
        if insight.pattern_type:
            print(f"     Type: {insight.pattern_type}")
    print()
    
    # Filter high-confidence recommendations
    high_confidence_recs = [
        rec for rec in agent_output.recommendations
        if rec.confidence >= min_confidence
    ]
    
    print(f"High-Confidence Recommendations ({len(high_confidence_recs)}/{len(agent_output.recommendations)}):")
    for rec in high_confidence_recs:
        print(f"  ✅ [{rec.confidence:.2f}] {rec.action}")
        print(f"     Priority: {rec.priority}, Rationale: {rec.rationale[:50]}...")
    print()
    
    return high_confidence_insights, high_confidence_recs


# ============================================================================
# Confidence Level Classification
# ============================================================================

def show_confidence_levels(agent_output: AgentOutput):
    """
    Show how confidence scores are automatically classified
    """
    print(f"\n{'='*60}")
    print("Confidence Level Classification")
    print(f"{'='*60}\n")
    
    # Group insights by confidence level
    by_level = {}
    for insight in agent_output.insights:
        level = insight.confidence_level
        if level not in by_level:
            by_level[level] = []
        by_level[level].append(insight)
    
    # Display by level
    level_order = [ConfidenceLevel.HIGH, ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW]
    
    for level in level_order:
        insights = by_level.get(level, [])
        if insights:
            emoji = "🟢" if level == ConfidenceLevel.HIGH else \
                    "🟡" if level == ConfidenceLevel.MEDIUM else "🔴"
            print(f"{emoji} {level} ({len(insights)} insights):")
            for insight in insights:
                print(f"   [{insight.confidence:.2f}] {insight.text[:60]}...")
            print()


# ============================================================================
# Example Usage
# ============================================================================

def main():
    """Demonstrate confidence scoring"""
    
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
    print("Confidence Scoring - FIXED ✅")
    print("=" * 60)
    print()
    
    # Get output with confidence scores
    print("Running analysis with confidence scoring...")
    result = new_output_with_confidence(data)
    
    print(f"\n✅ Analysis complete!")
    print(f"   Overall Confidence: {result.overall_confidence:.2f}")
    print(f"   Total Insights: {len(result.insights)}")
    print(f"   Total Recommendations: {len(result.recommendations)}")
    
    # Show confidence level distribution
    show_confidence_levels(result)
    
    # Filter by confidence threshold
    high_conf_insights, high_conf_recs = filter_by_confidence(result, min_confidence=0.7)
    
    print("=" * 60)
    print("Benefits of Confidence Scoring:")
    print("=" * 60)
    print("1. ✅ Filter low-quality insights automatically")
    print("2. ✅ Prioritize high-confidence recommendations")
    print("3. ✅ Track confidence trends over time")
    print("4. ✅ Alert when confidence drops below threshold")
    print("5. ✅ Make data-driven decisions with reliability metrics")
    print()


if __name__ == "__main__":
    main()
