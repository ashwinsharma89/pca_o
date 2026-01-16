"""
Enhanced Reasoning Agent Adapter with Pydantic Validation.

Wraps the existing EnhancedReasoningAgent to return validated AgentOutput schemas
while maintaining backward compatibility.
"""

from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
from loguru import logger

from .enhanced_reasoning_agent import EnhancedReasoningAgent as BaseAgent
from .schemas import (
    AgentOutput,
    AgentInsight,
    AgentRecommendation,
    AgentMetadata,
    DetectedPattern,
    ConfidenceLevel,
    PriorityLevel,
    PatternType
)
from .agent_resilience import retry_with_backoff


class ValidatedReasoningAgent:
    """
    Enhanced Reasoning Agent with Pydantic validation.
    
    Wraps the base EnhancedReasoningAgent to return validated AgentOutput schemas.
    """
    
    def __init__(self, rag_retriever=None, benchmark_engine=None):
        """Initialize with base agent"""
        self.base_agent = BaseAgent(rag_retriever, benchmark_engine)
        logger.info("Initialized Validated Reasoning Agent with Pydantic schemas")
    
    def analyze(
        self,
        campaign_data: pd.DataFrame,
        channel_insights: Optional[Dict[str, Any]] = None,
        campaign_context: Optional[Any] = None,
        return_validated: bool = True
    ) -> AgentOutput:
        """
        Perform comprehensive analysis with validated output.
        
        Args:
            campaign_data: Campaign performance data
            channel_insights: Channel-specific insights from specialists
            campaign_context: Business context
            return_validated: If True, return AgentOutput schema; if False, return dict
            
        Returns:
            AgentOutput: Validated agent output with insights and recommendations
        """
        start_time = datetime.utcnow()
        
        # Call base agent (sync)
        raw_output = self.base_agent.analyze(
            campaign_data,
            channel_insights,
            campaign_context
        )
        
        if not return_validated:
            return raw_output
        
        # Convert to validated schema
        try:
            validated_output = self._convert_to_schema(
                raw_output,
                campaign_data,
                start_time
            )
            
            logger.info(
                f"Analysis complete: {len(validated_output.insights)} insights, "
                f"{len(validated_output.recommendations)} recommendations, "
                f"confidence: {validated_output.overall_confidence:.2f}"
            )
            
            return validated_output
            
        except Exception as e:
            logger.error(f"Failed to validate output: {e}")
            # Return with minimal validation
            return AgentOutput(
                insights=[],
                recommendations=[],
                metadata=AgentMetadata(
                    agent_name="EnhancedReasoningAgent",
                    agent_version="2.0",
                    data_points_analyzed=len(campaign_data)
                ),
                overall_confidence=0.5,
                warnings=[f"Validation failed: {str(e)}"]
            )
    
    def _convert_to_schema(
        self,
        raw_output: Dict[str, Any],
        campaign_data: pd.DataFrame,
        start_time: datetime
    ) -> AgentOutput:
        """Convert raw output to validated AgentOutput schema"""
        
        # Extract pattern insights
        insights = self._extract_insights(raw_output)
        
        # Extract recommendations
        recommendations = self._extract_recommendations(raw_output)
        
        # Extract detected patterns
        patterns = self._extract_patterns(raw_output.get('patterns', {}))
        
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Create metadata
        metadata = AgentMetadata(
            agent_name="EnhancedReasoningAgent",
            agent_version="2.0",
            execution_time_ms=execution_time,
            data_points_analyzed=len(campaign_data),
            platform=self._detect_platform(campaign_data)
        )
        
        # Calculate overall confidence
        all_confidences = []
        all_confidences.extend([i.confidence for i in insights])
        all_confidences.extend([r.confidence for r in recommendations])
        
        overall_confidence = (
            sum(all_confidences) / len(all_confidences)
            if all_confidences else 0.7
        )
        
        return AgentOutput(
            insights=insights,
            recommendations=recommendations,
            patterns=patterns if patterns else None,
            metadata=metadata,
            overall_confidence=overall_confidence
        )
    
    def _extract_insights(self, raw_output: Dict[str, Any]) -> List[AgentInsight]:
        """Extract and validate insights from raw output"""
        insights = []
        
        # Extract pattern insights
        pattern_insights = raw_output.get('insights', {}).get('pattern_insights', [])
        for insight_text in pattern_insights:
            if isinstance(insight_text, str) and len(insight_text) >= 10:
                # Determine confidence based on pattern detection
                confidence = self._calculate_insight_confidence(insight_text, raw_output)
                
                insights.append(AgentInsight(
                    text=insight_text,
                    confidence=confidence,
                    pattern_type=self._infer_pattern_type(insight_text)
                ))
        
        # Extract performance summary insights
        perf_summary = raw_output.get('insights', {}).get('performance_summary', {})
        if perf_summary:
            summary_text = self._format_performance_summary(perf_summary)
            if summary_text:
                insights.append(AgentInsight(
                    text=summary_text,
                    confidence=0.9,  # High confidence for calculated metrics
                    supporting_data=perf_summary
                ))
        
        # Extract benchmark comparison insights
        benchmark_comp = raw_output.get('insights', {}).get('benchmark_comparison')
        if benchmark_comp:
            for metric, comparison in benchmark_comp.items():
                if comparison.get('status') == 'needs_work':
                    insights.append(AgentInsight(
                        text=f"{metric.upper()} is below benchmark: {comparison.get('actual', 0):.2%} vs {comparison.get('benchmark', 0):.2%}",
                        confidence=0.85,
                        supporting_data=comparison
                    ))
        
        return insights
    
    def _extract_recommendations(self, raw_output: Dict[str, Any]) -> List[AgentRecommendation]:
        """Extract and validate recommendations from raw output"""
        recommendations = []
        
        raw_recs = raw_output.get('recommendations', [])
        for rec in raw_recs:
            if isinstance(rec, dict):
                # Map priority string to enum
                priority_map = {
                    'critical': PriorityLevel.CRITICAL,
                    'high': PriorityLevel.HIGH,
                    'medium': PriorityLevel.MEDIUM,
                    'low': PriorityLevel.LOW,
                    'optional': PriorityLevel.OPTIONAL
                }
                
                priority = priority_map.get(rec.get('priority', 'medium').lower(), PriorityLevel.MEDIUM)
                
                # Calculate confidence based on evidence
                confidence = self._calculate_recommendation_confidence(rec, raw_output)
                
                recommendations.append(AgentRecommendation(
                    action=rec.get('recommendation', 'Take action'),
                    rationale=rec.get('issue', 'Performance optimization needed'),
                    priority=priority,
                    confidence=confidence,
                    expected_impact=rec.get('expected_impact'),
                    category=rec.get('category')
                ))
        
        return recommendations
    
    def _extract_patterns(self, patterns_dict: Dict[str, Any]) -> List[DetectedPattern]:
        """Extract detected patterns"""
        detected_patterns = []
        
        pattern_type_map = {
            'trends': PatternType.TREND,
            'anomalies': PatternType.ANOMALY,
            'seasonality': PatternType.SEASONALITY,
            'creative_fatigue': PatternType.CREATIVE_FATIGUE,
            'audience_saturation': PatternType.AUDIENCE_SATURATION,
            'day_parting_opportunities': PatternType.DAY_PARTING,
            'budget_pacing': PatternType.BUDGET_PACING,
            'performance_clusters': PatternType.PERFORMANCE_CLUSTER
        }
        
        for pattern_key, pattern_type in pattern_type_map.items():
            pattern_data = patterns_dict.get(pattern_key, {})
            
            if pattern_data.get('detected'):
                description = pattern_data.get('description', '') or pattern_data.get('recommendation', '')
                
                if description:
                    # Flatten nested metrics to simple Dict[str, float]
                    metrics = self._flatten_metrics(
                        pattern_data.get('evidence') or pattern_data.get('metrics')
                    )
                    
                    detected_patterns.append(DetectedPattern(
                        pattern_type=pattern_type,
                        description=description,
                        confidence=self._calculate_pattern_confidence(pattern_data),
                        severity=pattern_data.get('severity'),
                        metrics=metrics
                    ))
        
        return detected_patterns
    
    def _flatten_metrics(self, metrics: Any) -> Optional[Dict[str, float]]:
        """Flatten nested metrics dictionaries to simple key-value pairs"""
        if not metrics:
            return None
        
        if not isinstance(metrics, dict):
            return None
        
        flattened = {}
        
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                # Already a number, use as-is
                flattened[key] = float(value)
            elif isinstance(value, dict):
                # Nested dict - extract numeric values
                for nested_key, nested_value in value.items():
                    if isinstance(nested_value, (int, float)):
                        flattened[f"{key}_{nested_key}"] = float(nested_value)
        
        return flattened if flattened else None
    
    def _calculate_insight_confidence(self, insight_text: str, raw_output: Dict) -> float:
        """Calculate confidence score for an insight"""
        # Base confidence
        confidence = 0.7
        
        # Increase if backed by strong patterns
        patterns = raw_output.get('patterns', {})
        if any(p.get('detected') and p.get('severity') == 'high' for p in patterns.values() if isinstance(p, dict)):
            confidence += 0.15
        
        # Increase if backed by benchmarks
        if raw_output.get('benchmarks_applied'):
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _calculate_recommendation_confidence(self, rec: Dict, raw_output: Dict) -> float:
        """Calculate confidence score for a recommendation"""
        # Base confidence by priority
        priority_confidence = {
            'critical': 0.9,
            'high': 0.85,
            'medium': 0.75,
            'low': 0.65,
            'optional': 0.6
        }
        
        confidence = priority_confidence.get(rec.get('priority', 'medium').lower(), 0.7)
        
        # Adjust based on expected impact
        if rec.get('expected_impact') == 'high':
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def _calculate_pattern_confidence(self, pattern_data: Dict) -> float:
        """Calculate confidence for detected pattern"""
        # Check for statistical significance indicators
        if 'r_squared' in pattern_data.get('metrics', {}):
            r_squared = pattern_data['metrics']['r_squared']
            return min(r_squared + 0.2, 1.0)
        
        # Check severity
        severity_confidence = {
            'high': 0.9,
            'medium': 0.75,
            'low': 0.6
        }
        
        return severity_confidence.get(pattern_data.get('severity'), 0.7)
    
    def _format_performance_summary(self, summary: Dict) -> str:
        """Format performance summary as insight text"""
        parts = []
        
        if 'total_spend' in summary:
            parts.append(f"Total spend: ${summary['total_spend']:,.2f}")
        
        if 'total_conversions' in summary:
            parts.append(f"{summary['total_conversions']:,} conversions")
        
        if 'overall_ctr' in summary:
            parts.append(f"CTR: {summary['overall_ctr']:.2%}")
        
        if parts:
            return "Campaign performance: " + ", ".join(parts)
        
        return ""
    
    def _infer_pattern_type(self, insight_text: str) -> Optional[PatternType]:
        """Infer pattern type from insight text"""
        text_lower = insight_text.lower()
        
        if 'trend' in text_lower or 'improving' in text_lower or 'declining' in text_lower:
            return PatternType.TREND
        elif 'anomaly' in text_lower or 'spike' in text_lower or 'unusual' in text_lower:
            return PatternType.ANOMALY
        elif 'creative' in text_lower and 'fatigue' in text_lower:
            return PatternType.CREATIVE_FATIGUE
        elif 'audience' in text_lower and 'saturation' in text_lower:
            return PatternType.AUDIENCE_SATURATION
        elif 'day' in text_lower or 'time' in text_lower or 'hour' in text_lower:
            return PatternType.DAY_PARTING
        elif 'budget' in text_lower or 'pacing' in text_lower:
            return PatternType.BUDGET_PACING
        
        return None
    
    def _detect_platform(self, data: pd.DataFrame) -> Optional[str]:
        """Detect platform from data"""
        if 'Platform' in data.columns and len(data) > 0:
            return str(data['Platform'].iloc[0])
        return None
