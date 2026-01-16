"""
Early Performance Indicators (EPIs) Module
Predict campaign success from first 24-48 hours of data
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from scipy.stats import pearsonr
from loguru import logger


class EarlyPerformanceIndicators:
    """
    Analyze early campaign metrics to predict final success
    """
    
    def __init__(self):
        self.thresholds = {
            'high_success': {
                'early_ctr': 2.5,  # %
                'early_conv_rate': 3.0,  # %
                'early_cpa': 50,  # $
                'engagement_velocity': 0.05,  # % per hour
                'success_probability': 85  # %
            },
            'medium_success': {
                'early_ctr': 1.5,
                'early_conv_rate': 2.0,
                'early_cpa': 75,
                'engagement_velocity': 0.03,
                'success_probability': 60
            },
            'low_success': {
                'early_ctr': 0.8,
                'early_conv_rate': 1.0,
                'early_cpa': 100,
                'engagement_velocity': 0.01,
                'success_probability': 30
            }
        }
    
    def analyze_early_metrics(
        self,
        campaign_id: str,
        early_data: pd.DataFrame,
        hours_elapsed: int = 24
    ) -> Dict:
        """
        Analyze first 24-48 hours to predict success
        
        Args:
            campaign_id: Campaign identifier
            early_data: DataFrame with hourly metrics
            hours_elapsed: Hours since campaign start
            
        Returns:
            Dict with predictions and recommendations
        """
        logger.info(f"Analyzing early metrics for campaign {campaign_id} ({hours_elapsed}h elapsed)")
        
        # Calculate early metrics
        early_metrics = self._calculate_early_metrics(early_data, hours_elapsed)
        
        # Predict success probability
        success_prediction = self._predict_success(early_metrics)
        
        # Identify warning signals
        warnings = self._identify_warnings(early_metrics)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            early_metrics,
            success_prediction,
            warnings
        )
        
        return {
            'campaign_id': campaign_id,
            'hours_elapsed': hours_elapsed,
            'early_metrics': early_metrics,
            'success_prediction': success_prediction,
            'warnings': warnings,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_early_metrics(
        self,
        data: pd.DataFrame,
        hours: int
    ) -> Dict:
        """Calculate key early performance metrics"""
        
        # Filter to early period
        early_data = data[data['hours_since_start'] <= hours]
        
        if len(early_data) == 0:
            logger.warning("No early data available")
            return {}
        
        # Basic metrics
        total_impressions = early_data['impressions'].sum()
        total_clicks = early_data['clicks'].sum()
        total_conversions = early_data['conversions'].sum()
        total_spend = early_data['spend'].sum()
        total_revenue = early_data['revenue'].sum()
        
        # Calculate rates
        early_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        early_conv_rate = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0
        early_cpa = (total_spend / total_conversions) if total_conversions > 0 else float('inf')
        early_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        
        # Engagement velocity (rate of change)
        engagement_velocity = self._calculate_engagement_velocity(early_data)
        
        # Audience quality score
        audience_quality = self._calculate_audience_quality(early_data)
        
        # Cost efficiency trend
        cost_trend = self._calculate_cost_trend(early_data)
        
        return {
            'early_ctr': round(early_ctr, 2),
            'early_conv_rate': round(early_conv_rate, 2),
            'early_cpa': round(early_cpa, 2),
            'early_roas': round(early_roas, 2),
            'engagement_velocity': round(engagement_velocity, 4),
            'audience_quality_score': round(audience_quality, 2),
            'cost_efficiency_trend': cost_trend,
            'total_impressions': int(total_impressions),
            'total_clicks': int(total_clicks),
            'total_conversions': int(total_conversions),
            'total_spend': round(total_spend, 2)
        }
    
    def _calculate_engagement_velocity(self, data: pd.DataFrame) -> float:
        """
        Calculate rate of engagement increase
        Higher velocity = stronger momentum
        """
        if len(data) < 2:
            return 0.0
        
        # Calculate hourly engagement rate
        data = data.sort_values('hours_since_start')
        data['engagement_rate'] = (
            (data['clicks'] + data['conversions']) / data['impressions'] * 100
        )
        
        # Calculate slope (velocity)
        hours = data['hours_since_start'].values
        engagement = data['engagement_rate'].values
        
        if len(hours) > 1:
            velocity = np.polyfit(hours, engagement, 1)[0]
            return velocity
        
        return 0.0
    
    def _calculate_audience_quality(self, data: pd.DataFrame) -> float:
        """
        Score audience quality based on early responders
        High quality = high conversion rate, low CPA
        """
        if len(data) == 0:
            return 0.0
        
        # Factors for quality score
        conv_rate = data['conversions'].sum() / data['clicks'].sum() if data['clicks'].sum() > 0 else 0
        cpa = data['spend'].sum() / data['conversions'].sum() if data['conversions'].sum() > 0 else float('inf')
        
        # Normalize to 0-100 scale
        conv_rate_score = min(conv_rate * 20, 50)  # Max 50 points
        cpa_score = max(50 - (cpa / 10), 0)  # Max 50 points, decreases with higher CPA
        
        quality_score = conv_rate_score + cpa_score
        
        return min(quality_score, 100)
    
    def _calculate_cost_trend(self, data: pd.DataFrame) -> str:
        """
        Analyze if costs are improving or worsening
        """
        if len(data) < 3:
            return 'insufficient_data'
        
        data = data.sort_values('hours_since_start')
        data['cpa'] = data['spend'] / data['conversions']
        data['cpa'] = data['cpa'].replace([np.inf, -np.inf], np.nan)
        
        # Calculate trend
        recent_cpa = data['cpa'].tail(3).mean()
        early_cpa = data['cpa'].head(3).mean()
        
        if pd.isna(recent_cpa) or pd.isna(early_cpa):
            return 'insufficient_data'
        
        change = (recent_cpa - early_cpa) / early_cpa if early_cpa > 0 else 0
        
        if change < -0.1:
            return 'improving'
        elif change > 0.1:
            return 'worsening'
        else:
            return 'stable'
    
    def _predict_success(self, metrics: Dict) -> Dict:
        """
        Predict campaign success probability based on early metrics
        """
        if not metrics:
            return {
                'probability': 0,
                'confidence': 'low',
                'category': 'unknown'
            }
        
        # Score against thresholds
        scores = []
        
        for category, thresholds in self.thresholds.items():
            score = 0
            
            # CTR check
            if metrics['early_ctr'] >= thresholds['early_ctr']:
                score += 1
            
            # Conversion rate check
            if metrics['early_conv_rate'] >= thresholds['early_conv_rate']:
                score += 1
            
            # CPA check
            if metrics['early_cpa'] <= thresholds['early_cpa']:
                score += 1
            
            # Engagement velocity check
            if metrics['engagement_velocity'] >= thresholds['engagement_velocity']:
                score += 1
            
            scores.append((category, score, thresholds['success_probability']))
        
        # Find best matching category
        best_match = max(scores, key=lambda x: x[1])
        category = best_match[0]
        base_probability = best_match[2]
        
        # Adjust based on audience quality
        quality_adjustment = (metrics['audience_quality_score'] - 50) / 5
        
        # Adjust based on cost trend
        trend_adjustment = {
            'improving': 5,
            'stable': 0,
            'worsening': -10,
            'insufficient_data': 0
        }[metrics['cost_efficiency_trend']]
        
        final_probability = base_probability + quality_adjustment + trend_adjustment
        final_probability = max(0, min(100, final_probability))
        
        # Determine confidence
        if metrics['total_conversions'] >= 50:
            confidence = 'high'
        elif metrics['total_conversions'] >= 20:
            confidence = 'medium'
        else:
            confidence = 'low'
        
        return {
            'probability': round(final_probability, 1),
            'confidence': confidence,
            'category': category,
            'factors': {
                'base_probability': base_probability,
                'quality_adjustment': round(quality_adjustment, 1),
                'trend_adjustment': trend_adjustment
            }
        }
    
    def _identify_warnings(self, metrics: Dict) -> List[Dict]:
        """
        Identify warning signals in early performance
        """
        warnings = []
        
        if not metrics:
            return warnings
        
        # Low CTR warning
        if metrics['early_ctr'] < 0.8:
            warnings.append({
                'severity': 'high',
                'type': 'low_ctr',
                'message': f"CTR is {metrics['early_ctr']}%, below 0.8% threshold",
                'impact': 'Campaign may not reach target impressions efficiently'
            })
        
        # Low conversion rate warning
        if metrics['early_conv_rate'] < 1.0:
            warnings.append({
                'severity': 'high',
                'type': 'low_conversion_rate',
                'message': f"Conversion rate is {metrics['early_conv_rate']}%, below 1% threshold",
                'impact': 'Campaign unlikely to meet conversion goals'
            })
        
        # High CPA warning
        if metrics['early_cpa'] > 100:
            warnings.append({
                'severity': 'medium',
                'type': 'high_cpa',
                'message': f"CPA is ${metrics['early_cpa']}, above $100 threshold",
                'impact': 'Campaign may not be cost-efficient'
            })
        
        # Negative engagement velocity
        if metrics['engagement_velocity'] < 0:
            warnings.append({
                'severity': 'high',
                'type': 'declining_engagement',
                'message': f"Engagement velocity is negative ({metrics['engagement_velocity']})",
                'impact': 'Performance is declining - possible ad fatigue'
            })
        
        # Worsening cost trend
        if metrics['cost_efficiency_trend'] == 'worsening':
            warnings.append({
                'severity': 'medium',
                'type': 'worsening_costs',
                'message': "CPA is increasing over time",
                'impact': 'Campaign efficiency is declining'
            })
        
        # Low audience quality
        if metrics['audience_quality_score'] < 30:
            warnings.append({
                'severity': 'high',
                'type': 'low_audience_quality',
                'message': f"Audience quality score is {metrics['audience_quality_score']}/100",
                'impact': 'Targeting may need adjustment'
            })
        
        return warnings
    
    def _generate_recommendations(
        self,
        metrics: Dict,
        prediction: Dict,
        warnings: List[Dict]
    ) -> List[Dict]:
        """
        Generate actionable recommendations based on early performance
        """
        recommendations = []
        
        if not metrics:
            return recommendations
        
        # Success probability based recommendations
        if prediction['probability'] < 40:
            recommendations.append({
                'priority': 'high',
                'action': 'pause_and_review',
                'message': 'Campaign has low success probability - consider pausing for review',
                'expected_impact': 'Prevent wasted spend on underperforming campaign'
            })
        elif prediction['probability'] > 80:
            recommendations.append({
                'priority': 'high',
                'action': 'scale_up',
                'message': 'Campaign showing strong early performance - consider increasing budget',
                'expected_impact': 'Capture more conversions while performance is strong'
            })
        
        # Warning-based recommendations
        for warning in warnings:
            if warning['type'] == 'low_ctr':
                recommendations.append({
                    'priority': 'high',
                    'action': 'refresh_creative',
                    'message': 'Test new ad creatives to improve CTR',
                    'expected_impact': 'Increase click-through rate by 50-100%'
                })
            
            elif warning['type'] == 'low_conversion_rate':
                recommendations.append({
                    'priority': 'high',
                    'action': 'optimize_landing_page',
                    'message': 'Review and optimize landing page experience',
                    'expected_impact': 'Improve conversion rate by 30-50%'
                })
            
            elif warning['type'] == 'high_cpa':
                recommendations.append({
                    'priority': 'medium',
                    'action': 'adjust_targeting',
                    'message': 'Narrow targeting to higher-intent audiences',
                    'expected_impact': 'Reduce CPA by 20-30%'
                })
            
            elif warning['type'] == 'declining_engagement':
                recommendations.append({
                    'priority': 'high',
                    'action': 'rotate_creative',
                    'message': 'Rotate to fresh creative to combat ad fatigue',
                    'expected_impact': 'Restore engagement to initial levels'
                })
            
            elif warning['type'] == 'low_audience_quality':
                recommendations.append({
                    'priority': 'high',
                    'action': 'refine_targeting',
                    'message': 'Refine audience targeting based on early responder profiles',
                    'expected_impact': 'Improve audience quality score by 40-60%'
                })
        
        # Positive reinforcement
        if metrics['cost_efficiency_trend'] == 'improving':
            recommendations.append({
                'priority': 'low',
                'action': 'maintain_course',
                'message': 'Campaign is optimizing well - maintain current settings',
                'expected_impact': 'Continue positive trajectory'
            })
        
        return recommendations
    
    def calculate_epi_correlations(
        self,
        historical_campaigns: pd.DataFrame
    ) -> Dict:
        """
        Analyze which early metrics correlate most with final success
        
        Args:
            historical_campaigns: DataFrame with early and final metrics
            
        Returns:
            Dict with correlation coefficients
        """
        logger.info("Calculating EPI correlations from historical data")
        
        correlations = {}
        
        early_metrics = [
            'early_ctr', 'early_conv_rate', 'early_cpa',
            'engagement_velocity', 'audience_quality_score'
        ]
        
        for metric in early_metrics:
            if metric in historical_campaigns.columns:
                corr, p_value = pearsonr(
                    historical_campaigns[metric],
                    historical_campaigns['final_roas']
                )
                
                correlations[metric] = {
                    'correlation': round(corr, 3),
                    'p_value': round(p_value, 4),
                    'significance': 'significant' if p_value < 0.05 else 'not_significant'
                }
        
        # Sort by absolute correlation
        sorted_correlations = dict(
            sorted(
                correlations.items(),
                key=lambda x: abs(x[1]['correlation']),
                reverse=True
            )
        )
        
        logger.info(f"Calculated correlations for {len(correlations)} metrics")
        
        return sorted_correlations


if __name__ == "__main__":
    # Example usage
    epi = EarlyPerformanceIndicators()
    
    # Simulate early campaign data
    early_data = pd.DataFrame({
        'hours_since_start': range(24),
        'impressions': np.random.randint(10000, 50000, 24),
        'clicks': np.random.randint(100, 500, 24),
        'conversions': np.random.randint(5, 30, 24),
        'spend': np.random.uniform(500, 2000, 24),
        'revenue': np.random.uniform(2000, 8000, 24)
    })
    
    # Analyze
    result = epi.analyze_early_metrics('CAMP_001', early_data, hours_elapsed=24)
    
    print("\n=== Early Performance Analysis ===")
    print(f"Success Probability: {result['success_prediction']['probability']}%")
    print(f"Confidence: {result['success_prediction']['confidence']}")
    print(f"\nWarnings: {len(result['warnings'])}")
    for warning in result['warnings']:
        print(f"  - [{warning['severity']}] {warning['message']}")
    print(f"\nRecommendations: {len(result['recommendations'])}")
    for rec in result['recommendations']:
        print(f"  - [{rec['priority']}] {rec['message']}")
