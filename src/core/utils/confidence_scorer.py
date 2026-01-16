"""
Confidence Scoring System for RAG Recommendations.
Assigns confidence scores to recommendations based on multiple factors.
"""
import re
from typing import Dict, List, Any
from loguru import logger


class ConfidenceScorer:
    """Calculate confidence scores for recommendations."""
    
    def __init__(self):
        self.min_data_points = 14  # Minimum days of data for high confidence
        self.min_sources = 2  # Minimum supporting sources
    
    def score_recommendation(
        self,
        recommendation: str,
        metrics: Dict[str, Any],
        rag_context: List[Dict[str, Any]],
        data_quality: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive confidence score for a recommendation.
        
        Args:
            recommendation: The recommendation text
            metrics: Campaign metrics
            rag_context: Retrieved knowledge chunks
            data_quality: Data quality assessment
            
        Returns:
            Dictionary with confidence score and breakdown
        """
        
        # Factor 1: Evidence Strength (40%)
        evidence_score = self._score_evidence_strength(recommendation, rag_context)
        
        # Factor 2: Data Quality (30%)
        data_score = self._score_data_quality(data_quality)
        
        # Factor 3: Consistency (20%)
        consistency_score = self._score_consistency(recommendation, rag_context, metrics)
        
        # Factor 4: Specificity (10%)
        specificity_score = self._score_specificity(recommendation)
        
        # Weighted confidence score
        confidence = (
            evidence_score * 0.40 +
            data_score * 0.30 +
            consistency_score * 0.20 +
            specificity_score * 0.10
        )
        
        # Determine confidence level
        if confidence >= 0.80:
            level = "HIGH"
            interpretation = "Strong evidence and data support this recommendation"
        elif confidence >= 0.60:
            level = "MEDIUM"
            interpretation = "Moderate confidence - consider additional validation"
        else:
            level = "LOW"
            interpretation = "Limited evidence - proceed with caution and monitor closely"
        
        return {
            'confidence_score': round(confidence, 2),
            'confidence_level': level,
            'interpretation': interpretation,
            'factors': {
                'evidence_strength': round(evidence_score, 2),
                'data_quality': round(data_score, 2),
                'consistency': round(consistency_score, 2),
                'specificity': round(specificity_score, 2)
            },
            'warnings': self._generate_warnings(confidence, evidence_score, data_score, consistency_score)
        }
    
    def _score_evidence_strength(self, recommendation: str, rag_context: List[Dict]) -> float:
        """
        Score based on how many knowledge sources support this recommendation.
        
        Returns: 0.0 to 1.0
        """
        if not rag_context:
            return 0.3  # Low confidence without knowledge base
        
        # Count supporting sources
        supporting_sources = 0
        rec_lower = recommendation.lower()
        
        # Key recommendation patterns
        patterns = {
            'increase': ['scale', 'increase', 'expand', 'grow', 'boost'],
            'decrease': ['reduce', 'pause', 'decrease', 'cut', 'lower'],
            'optimize': ['optimize', 'improve', 'refine', 'adjust', 'test']
        }
        
        # Identify recommendation type
        rec_type = None
        for rtype, keywords in patterns.items():
            if any(kw in rec_lower for kw in keywords):
                rec_type = rtype
                break
        
        # Check if knowledge sources support this type of action
        for chunk in rag_context:
            content = chunk.get('content', '').lower()
            
            if rec_type == 'increase':
                if any(kw in content for kw in ['scale', 'high roas', 'strong performance', 'top performer']):
                    supporting_sources += 1
            elif rec_type == 'decrease':
                if any(kw in content for kw in ['underperform', 'inefficient', 'high cpa', 'low roas']):
                    supporting_sources += 1
            elif rec_type == 'optimize':
                if any(kw in content for kw in ['optimize', 'improve', 'best practice', 'benchmark']):
                    supporting_sources += 1
        
        # Score based on number of supporting sources
        if supporting_sources >= 3:
            return 1.0
        elif supporting_sources == 2:
            return 0.8
        elif supporting_sources == 1:
            return 0.6
        else:
            return 0.4
    
    def _score_data_quality(self, data_quality: Dict[str, Any]) -> float:
        """
        Score based on data completeness and reliability.
        
        Returns: 0.0 to 1.0
        """
        score = 0.0
        
        # Check data recency
        days_of_data = data_quality.get('days_of_data', 0)
        if days_of_data >= 30:
            score += 0.4
        elif days_of_data >= 14:
            score += 0.3
        elif days_of_data >= 7:
            score += 0.2
        else:
            score += 0.1
        
        # Check data completeness
        completeness = data_quality.get('completeness', 0.0)
        score += completeness * 0.3
        
        # Check metric availability
        has_conversions = data_quality.get('has_conversions', False)
        has_revenue = data_quality.get('has_revenue', False)
        
        if has_conversions and has_revenue:
            score += 0.3
        elif has_conversions:
            score += 0.2
        else:
            score += 0.1
        
        return min(score, 1.0)
    
    def _score_consistency(self, recommendation: str, rag_context: List[Dict], metrics: Dict) -> float:
        """
        Score based on consistency with best practices and current performance.
        
        Returns: 0.0 to 1.0
        """
        score = 0.5  # Start neutral
        
        rec_lower = recommendation.lower()
        
        # Check for contradictions
        contradictions = 0
        
        # Example: Recommending to increase budget on underperforming channel
        if 'increase' in rec_lower or 'scale' in rec_lower:
            # Check if metrics support scaling
            platform_metrics = metrics.get('by_platform', {})
            for platform, data in platform_metrics.items():
                if platform.lower() in rec_lower:
                    roas = data.get('ROAS', data.get('roas', 0))
                    cpa = data.get('CPA', data.get('cpa', 0))
                    
                    # If recommending scale but performance is poor
                    if roas > 0 and roas < 2.0:
                        contradictions += 1
                    if cpa > 0 and cpa > 50:
                        contradictions += 1
        
        # Check against knowledge base
        for chunk in rag_context:
            content = chunk.get('content', '').lower()
            
            # Look for contradicting advice
            if 'increase' in rec_lower and 'reduce' in content:
                contradictions += 0.5
            elif 'reduce' in rec_lower and 'scale' in content:
                contradictions += 0.5
        
        # Adjust score based on contradictions
        score -= (contradictions * 0.2)
        
        return max(0.0, min(score, 1.0))
    
    def _score_specificity(self, recommendation: str) -> float:
        """
        Score based on how specific and actionable the recommendation is.
        
        Returns: 0.0 to 1.0
        """
        score = 0.0
        
        # Check for specific numbers
        if re.search(r'\d+\s*(?:%|percent|K|M|dollars?)', recommendation):
            score += 0.3
        
        # Check for specific platform/campaign names
        if any(platform in recommendation.upper() for platform in ['DIS', 'SOC', 'SEARCH', 'DISPLAY', 'SOCIAL']):
            score += 0.2
        
        # Check for specific actions
        action_words = ['increase', 'decrease', 'pause', 'test', 'optimize', 'adjust', 'expand', 'reduce']
        if any(word in recommendation.lower() for word in action_words):
            score += 0.2
        
        # Check for timeline
        if any(word in recommendation.lower() for word in ['immediately', 'week', 'month', 'day', 'within']):
            score += 0.2
        
        # Check for expected impact
        if 'expect' in recommendation.lower() or 'impact' in recommendation.lower():
            score += 0.1
        
        return min(score, 1.0)
    
    def _generate_warnings(self, confidence: float, evidence: float, data: float, consistency: float) -> List[str]:
        """Generate warnings based on low scores."""
        warnings = []
        
        if confidence < 0.6:
            warnings.append("Overall confidence is low - consider gathering more data or validating with additional sources")
        
        if evidence < 0.5:
            warnings.append("Limited supporting evidence from knowledge base - recommendation may not align with best practices")
        
        if data < 0.5:
            warnings.append("Data quality concerns - insufficient historical data or missing key metrics")
        
        if consistency < 0.5:
            warnings.append("Potential inconsistency detected - recommendation may contradict current performance or best practices")
        
        return warnings


def assess_data_quality(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Assess the quality and completeness of campaign data.
    
    Args:
        metrics: Campaign metrics dictionary
        
    Returns:
        Data quality assessment
    """
    overview = metrics.get('overview', {})
    platform_metrics = metrics.get('by_platform', {})
    
    # Estimate days of data (if date_range available)
    date_range = metrics.get('date_range', '')
    days_of_data = 30  # Default assumption
    
    if 'to' in date_range.lower():
        # Try to parse date range
        try:
            parts = date_range.split('to')
            if len(parts) == 2:
                # Simplified - assume 30 days if range exists
                days_of_data = 30
        except:
            pass
    
    # Check metric completeness
    available_metrics = []
    if overview.get('total_spend', 0) > 0:
        available_metrics.append('spend')
    if overview.get('total_impressions', 0) > 0:
        available_metrics.append('impressions')
    if overview.get('total_clicks', 0) > 0:
        available_metrics.append('clicks')
    if overview.get('total_conversions', 0) > 0:
        available_metrics.append('conversions')
    if overview.get('total_revenue', 0) > 0:
        available_metrics.append('revenue')
    
    completeness = len(available_metrics) / 5.0  # 5 key metrics
    
    return {
        'days_of_data': days_of_data,
        'completeness': completeness,
        'has_conversions': 'conversions' in available_metrics,
        'has_revenue': 'revenue' in available_metrics,
        'platform_count': len(platform_metrics),
        'available_metrics': available_metrics
    }
