"""
Enhanced Reasoning Agent with Pattern Recognition
Advanced pattern detection for trends, anomalies, and optimization opportunities
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy import stats
from loguru import logger


class EnhancedReasoningAgent:
    """Enhanced reasoning with pattern recognition and contextual analysis"""
    
    def __init__(self, rag_retriever=None, benchmark_engine=None):
        """
        Initialize Enhanced Reasoning Agent
        
        Args:
            rag_retriever: Optional RAG retriever for knowledge base access
            benchmark_engine: Optional Dynamic Benchmark Engine
        """
        self.rag = rag_retriever
        self.benchmarks = benchmark_engine
        self.pattern_detector = PatternDetector()
        logger.info("Initialized Enhanced Reasoning Agent with Pattern Recognition")
    
    def analyze(self, campaign_data: pd.DataFrame,
                channel_insights: Optional[Dict[str, Any]] = None,
                campaign_context: Optional[Any] = None) -> Dict[str, Any]:
        """
        Perform comprehensive analysis with pattern recognition
        
        Args:
            campaign_data: Campaign performance data
            channel_insights: Channel-specific insights from specialists
            campaign_context: Business context (CampaignContext object)
            
        Returns:
            Dictionary with insights, patterns, benchmarks, and recommendations
        """
        logger.info("Starting enhanced reasoning analysis")
        
        # Detect patterns
        patterns = self.pattern_detector.detect_all(campaign_data)
        
        # Get contextual benchmarks if available
        benchmarks = None
        if self.benchmarks and campaign_context:
            try:
                # Detect platform from data
                platform = self._detect_platform(campaign_data)
                objective = self._detect_objective(campaign_data)
                
                benchmarks = self.benchmarks.get_contextual_benchmarks(
                    channel=platform,
                    business_model=campaign_context.business_model.value,
                    industry=campaign_context.industry_vertical,
                    objective=objective,
                    region=campaign_context.geographic_focus[0] if campaign_context.geographic_focus else None
                )
            except Exception as e:
                logger.warning(f"Could not retrieve contextual benchmarks: {e}")
        
        # Generate insights with full context
        insights = self._generate_insights(
            campaign_data,
            channel_insights,
            patterns,
            benchmarks
        )
        
        # Retrieve optimization strategies from RAG
        optimization_context = None
        if self.rag and patterns:
            try:
                query = self._build_optimization_query(insights, patterns)
                optimization_context = self.rag.retrieve(
                    query=query,
                    filters={'category': 'optimization', 'priority': 1},
                    top_k=5
                )
            except Exception as e:
                logger.warning(f"Could not retrieve optimization context: {e}")
        
        # Generate actionable recommendations
        recommendations = self._generate_recommendations(
            insights,
            patterns,
            benchmarks,
            optimization_context,
            campaign_context
        )
        
        return {
            'insights': insights,
            'patterns': patterns,
            'benchmarks_applied': benchmarks,
            'optimization_context': optimization_context,
            'recommendations': recommendations,
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def _detect_platform(self, data: pd.DataFrame) -> str:
        """Detect platform from campaign data"""
        if 'Platform' in data.columns:
            platform = data['Platform'].iloc[0].lower() if len(data) > 0 else ''
            if 'linkedin' in platform:
                return 'linkedin'
            elif 'meta' in platform or 'facebook' in platform or 'instagram' in platform:
                return 'meta'
            elif 'dv360' in platform or 'display' in platform:
                return 'dv360'
        return 'google_search'
    
    def _detect_objective(self, data: pd.DataFrame) -> Optional[str]:
        """Detect campaign objective from data"""
        if 'Objective' in data.columns:
            return data['Objective'].iloc[0] if len(data) > 0 else None
        return None
    
    def _generate_insights(self, data: pd.DataFrame,
                          channel_insights: Optional[Dict],
                          patterns: Dict[str, Any],
                          benchmarks: Optional[Dict]) -> Dict[str, Any]:
        """Generate comprehensive insights"""
        insights = {
            'performance_summary': self._summarize_performance(data, benchmarks),
            'pattern_insights': self._interpret_patterns(patterns),
            'channel_insights': channel_insights or {},
            'benchmark_comparison': self._compare_to_benchmarks(data, benchmarks) if benchmarks else None
        }
        
        return insights
    
    def _summarize_performance(self, data: pd.DataFrame,
                               benchmarks: Optional[Dict]) -> Dict[str, Any]:
        """Summarize overall performance"""
        summary = {}
        
        # Calculate key metrics
        if 'Spend' in data.columns:
            summary['total_spend'] = float(data['Spend'].sum())
        if 'Impressions' in data.columns:
            summary['total_impressions'] = int(data['Impressions'].sum())
        if 'Clicks' in data.columns:
            summary['total_clicks'] = int(data['Clicks'].sum())
        if 'Conversions' in data.columns:
            summary['total_conversions'] = int(data['Conversions'].sum())
        
        # Calculate rates
        if 'Impressions' in data.columns and 'Clicks' in data.columns:
            total_impr = data['Impressions'].sum()
            total_clicks = data['Clicks'].sum()
            summary['overall_ctr'] = float(total_clicks / total_impr) if total_impr > 0 else 0
        
        if 'Clicks' in data.columns and 'Conversions' in data.columns:
            total_clicks = data['Clicks'].sum()
            total_conv = data['Conversions'].sum()
            summary['overall_conversion_rate'] = float(total_conv / total_clicks) if total_clicks > 0 else 0
        
        return summary
    
    def _interpret_patterns(self, patterns: Dict[str, Any]) -> List[str]:
        """Interpret detected patterns into insights"""
        insights = []
        
        # Trend insights
        if patterns.get('trends', {}).get('detected'):
            trend_data = patterns['trends']
            if trend_data.get('direction') == 'improving':
                insights.append(f"📈 Performance is improving: {trend_data.get('description', '')}")
            elif trend_data.get('direction') == 'declining':
                insights.append(f"📉 Performance is declining: {trend_data.get('description', '')}")
        
        # Anomaly insights
        if patterns.get('anomalies', {}).get('detected'):
            anomaly_data = patterns['anomalies']
            insights.append(f"⚠️ Anomaly detected: {anomaly_data.get('description', '')}")
        
        # Creative fatigue
        if patterns.get('creative_fatigue', {}).get('detected'):
            fatigue_data = patterns['creative_fatigue']
            insights.append(f"🎨 Creative fatigue detected: {fatigue_data.get('evidence', {}).get('recommendation', '')}")
        
        # Audience saturation
        if patterns.get('audience_saturation', {}).get('detected'):
            saturation_data = patterns['audience_saturation']
            insights.append(f"👥 Audience saturation detected: {saturation_data.get('recommendation', '')}")
        
        # Day parting opportunities
        if patterns.get('day_parting_opportunities', {}).get('detected'):
            daypart_data = patterns['day_parting_opportunities']
            insights.append(f"⏰ Day parting opportunity: {daypart_data.get('recommendation', '')}")
        
        return insights
    
    def _compare_to_benchmarks(self, data: pd.DataFrame,
                               benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """Compare performance to benchmarks"""
        if not benchmarks or 'benchmarks' not in benchmarks:
            return {}
        
        comparison = {}
        benchmark_ranges = benchmarks['benchmarks']
        
        # CTR comparison
        if 'CTR' in data.columns and 'ctr' in benchmark_ranges:
            actual_ctr = data['CTR'].mean()
            ctr_benchmarks = benchmark_ranges['ctr']
            comparison['ctr'] = {
                'actual': actual_ctr,
                'benchmark': ctr_benchmarks.get('good', 0),
                'status': 'good' if actual_ctr >= ctr_benchmarks.get('good', 0) else 'needs_work'
            }
        
        # CPC comparison
        if 'CPC' in data.columns and 'cpc' in benchmark_ranges:
            actual_cpc = data['CPC'].mean()
            cpc_benchmarks = benchmark_ranges['cpc']
            comparison['cpc'] = {
                'actual': actual_cpc,
                'benchmark': cpc_benchmarks.get('good', 0),
                'status': 'good' if actual_cpc <= cpc_benchmarks.get('good', 999) else 'needs_work'
            }
        
        return comparison
    
    def _build_optimization_query(self, insights: Dict[str, Any],
                                  patterns: Dict[str, Any]) -> str:
        """Build RAG query for optimization strategies"""
        query_parts = ["optimization strategies for"]
        
        # Add pattern-specific queries
        if patterns.get('creative_fatigue', {}).get('detected'):
            query_parts.append("creative fatigue")
        if patterns.get('audience_saturation', {}).get('detected'):
            query_parts.append("audience saturation")
        if patterns.get('trends', {}).get('direction') == 'declining':
            query_parts.append("declining performance")
        
        return " ".join(query_parts)
    
    def _generate_recommendations(self, insights: Dict[str, Any],
                                 patterns: Dict[str, Any],
                                 benchmarks: Optional[Dict],
                                 optimization_context: Optional[Any],
                                 campaign_context: Optional[Any]) -> List[Dict[str, str]]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Pattern-based recommendations
        if patterns.get('creative_fatigue', {}).get('detected'):
            fatigue = patterns['creative_fatigue']
            recommendations.append({
                'priority': 'high',
                'category': 'Creative',
                'issue': 'Creative fatigue detected',
                'recommendation': fatigue.get('evidence', {}).get('recommendation', 'Refresh creative assets'),
                'expected_impact': 'high'
            })
        
        if patterns.get('audience_saturation', {}).get('detected'):
            saturation = patterns['audience_saturation']
            recommendations.append({
                'priority': 'high',
                'category': 'Audience',
                'issue': 'Audience saturation detected',
                'recommendation': saturation.get('recommendation', 'Expand audience targeting'),
                'expected_impact': 'medium'
            })
        
        if patterns.get('day_parting_opportunities', {}).get('detected'):
            daypart = patterns['day_parting_opportunities']
            recommendations.append({
                'priority': 'medium',
                'category': 'Scheduling',
                'issue': 'Suboptimal time distribution',
                'recommendation': daypart.get('recommendation', 'Adjust day parting strategy'),
                'expected_impact': 'medium'
            })
        
        # Benchmark-based recommendations
        if benchmarks and insights.get('benchmark_comparison'):
            comparison = insights['benchmark_comparison']
            
            if comparison.get('ctr', {}).get('status') == 'needs_work':
                recommendations.append({
                    'priority': 'medium',
                    'category': 'CTR Optimization',
                    'issue': 'CTR below benchmark',
                    'recommendation': 'Improve ad copy and creative relevance',
                    'expected_impact': 'medium'
                })
            
            if comparison.get('cpc', {}).get('status') == 'needs_work':
                recommendations.append({
                    'priority': 'medium',
                    'category': 'Cost Efficiency',
                    'issue': 'CPC above benchmark',
                    'recommendation': 'Optimize targeting and improve Quality Score',
                    'expected_impact': 'high'
                })
        
        # Trend-based recommendations
        if patterns.get('trends', {}).get('direction') == 'declining':
            recommendations.append({
                'priority': 'high',
                'category': 'Performance',
                'issue': 'Declining performance trend',
                'recommendation': 'Conduct comprehensive campaign audit and implement corrective actions',
                'expected_impact': 'high'
            })
        
        return recommendations


class PatternDetector:
    """Detect performance patterns and anomalies"""
    
    def detect_all(self, campaign_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect all patterns in campaign data
        
        Args:
            campaign_data: Campaign performance DataFrame
            
        Returns:
            Dictionary of detected patterns
        """
        return {
            'trends': self._detect_trends(campaign_data),
            'anomalies': self._detect_anomalies(campaign_data),
            'seasonality': self._detect_seasonality(campaign_data),
            'creative_fatigue': self._detect_creative_fatigue(campaign_data),
            'audience_saturation': self._detect_audience_saturation(campaign_data),
            'day_parting_opportunities': self._find_day_parting(campaign_data),
            'budget_pacing': self._analyze_budget_pacing(campaign_data),
            'performance_clusters': self._identify_performance_clusters(campaign_data),
            'conversion_patterns': self._analyze_conversion_patterns(campaign_data)
        }
    
    def _detect_trends(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect performance trends over time"""
        if 'Date' not in data.columns or len(data) < 7:
            return {'detected': False, 'reason': 'Insufficient time-series data'}
        
        try:
            # Sort by date
            data_sorted = data.sort_values('Date')
            
            # Calculate trend for key metrics
            trends = {}
            
            if 'CTR' in data.columns:
                ctr_values = data_sorted['CTR'].values
                if len(ctr_values) > 2:
                    slope, _, r_value, _, _ = stats.linregress(range(len(ctr_values)), ctr_values)
                    trends['ctr'] = {
                        'slope': float(slope),
                        'r_squared': float(r_value ** 2),
                        'direction': 'improving' if slope > 0 else 'declining'
                    }
            
            if 'CPC' in data.columns:
                cpc_values = data_sorted['CPC'].values
                if len(cpc_values) > 2:
                    slope, _, r_value, _, _ = stats.linregress(range(len(cpc_values)), cpc_values)
                    trends['cpc'] = {
                        'slope': float(slope),
                        'r_squared': float(r_value ** 2),
                        'direction': 'improving' if slope < 0 else 'declining'  # Lower CPC is better
                    }
            
            # Determine overall trend
            if trends:
                improving_count = sum(1 for t in trends.values() if t['direction'] == 'improving')
                declining_count = sum(1 for t in trends.values() if t['direction'] == 'declining')
                
                if improving_count > declining_count:
                    overall_direction = 'improving'
                    description = f"{improving_count} metrics improving"
                elif declining_count > improving_count:
                    overall_direction = 'declining'
                    description = f"{declining_count} metrics declining"
                else:
                    overall_direction = 'stable'
                    description = "Performance is stable"
                
                return {
                    'detected': True,
                    'direction': overall_direction,
                    'description': description,
                    'metrics': trends
                }
        
        except Exception as e:
            logger.warning(f"Error detecting trends: {e}")
        
        return {'detected': False}
    
    def _detect_anomalies(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect anomalies in performance metrics"""
        anomalies = []
        
        try:
            # Check for anomalies in key metrics
            for metric in ['CTR', 'CPC', 'Conversions', 'Spend']:
                if metric in data.columns:
                    values = data[metric].dropna()
                    if len(values) > 5:
                        # Calculate z-scores
                        z_scores = np.abs(stats.zscore(values))
                        
                        # Find outliers (z-score > 3)
                        outliers = np.where(z_scores > 3)[0]
                        
                        if len(outliers) > 0:
                            anomalies.append({
                                'metric': metric,
                                'count': len(outliers),
                                'severity': 'high' if len(outliers) > len(values) * 0.1 else 'medium'
                            })
            
            if anomalies:
                return {
                    'detected': True,
                    'anomalies': anomalies,
                    'description': f"Found {len(anomalies)} metrics with anomalies"
                }
        
        except Exception as e:
            logger.warning(f"Error detecting anomalies: {e}")
        
        return {'detected': False}
    
    def _detect_seasonality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect seasonal patterns"""
        if 'Date' not in data.columns or len(data) < 14:
            return {'detected': False, 'reason': 'Insufficient data for seasonality detection'}
        
        try:
            data_sorted = data.sort_values('Date')
            
            # Check for day-of-week patterns
            if 'Conversions' in data.columns:
                data_sorted['DayOfWeek'] = pd.to_datetime(data_sorted['Date']).dt.dayofweek
                dow_performance = data_sorted.groupby('DayOfWeek')['Conversions'].mean()
                
                # Check if there's significant variation
                if dow_performance.std() / dow_performance.mean() > 0.3:  # 30% coefficient of variation
                    best_day = dow_performance.idxmax()
                    worst_day = dow_performance.idxmin()
                    
                    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    return {
                        'detected': True,
                        'type': 'day_of_week',
                        'best_day': day_names[best_day],
                        'worst_day': day_names[worst_day],
                        'variation': float(dow_performance.std() / dow_performance.mean())
                    }
        
        except Exception as e:
            logger.warning(f"Error detecting seasonality: {e}")
        
        return {'detected': False}
    
    def _detect_creative_fatigue(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect when creative performance is declining"""
        try:
            # Check for frequency and CTR decline
            frequency_threshold = 7
            ctr_decline_threshold = -0.15  # 15% decline
            
            # Calculate average frequency if available
            avg_frequency = None
            if 'Frequency' in data.columns:
                avg_frequency = data['Frequency'].mean()
            
            # Calculate CTR trend
            ctr_trend = None
            if 'CTR' in data.columns and 'Date' in data.columns and len(data) >= 7:
                data_sorted = data.sort_values('Date')
                ctr_values = data_sorted['CTR'].values
                
                if len(ctr_values) > 2:
                    # Compare first half vs second half
                    mid_point = len(ctr_values) // 2
                    first_half_avg = np.mean(ctr_values[:mid_point])
                    second_half_avg = np.mean(ctr_values[mid_point:])
                    
                    if first_half_avg > 0:
                        ctr_trend = (second_half_avg - first_half_avg) / first_half_avg
            
            # Detect fatigue
            if avg_frequency and avg_frequency > frequency_threshold:
                if ctr_trend and ctr_trend < ctr_decline_threshold:
                    return {
                        'detected': True,
                        'severity': 'high',
                        'evidence': {
                            'frequency': float(avg_frequency),
                            'ctr_decline': float(ctr_trend),
                            'recommendation': 'Refresh creative within 48 hours - CTR declining significantly'
                        }
                    }
                elif ctr_trend and ctr_trend < -0.05:  # 5% decline
                    return {
                        'detected': True,
                        'severity': 'medium',
                        'evidence': {
                            'frequency': float(avg_frequency),
                            'ctr_decline': float(ctr_trend),
                            'recommendation': 'Consider creative refresh - early signs of fatigue'
                        }
                    }
        
        except Exception as e:
            logger.warning(f"Error detecting creative fatigue: {e}")
        
        return {'detected': False}
    
    def _detect_audience_saturation(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect audience saturation"""
        try:
            # Check for declining reach with stable or increasing spend
            if 'Reach' in data.columns and 'Spend' in data.columns and 'Date' in data.columns:
                if len(data) >= 7:
                    data_sorted = data.sort_values('Date')
                    
                    # Calculate trends
                    reach_values = data_sorted['Reach'].values
                    spend_values = data_sorted['Spend'].values
                    
                    if len(reach_values) > 2 and len(spend_values) > 2:
                        reach_slope, _, _, _, _ = stats.linregress(range(len(reach_values)), reach_values)
                        spend_slope, _, _, _, _ = stats.linregress(range(len(spend_values)), spend_values)
                        
                        # Saturation: declining reach with stable/increasing spend
                        if reach_slope < 0 and spend_slope >= 0:
                            return {
                                'detected': True,
                                'severity': 'high',
                                'evidence': {
                                    'reach_trend': 'declining',
                                    'spend_trend': 'stable/increasing'
                                },
                                'recommendation': 'Expand audience targeting or test new audience segments'
                            }
            
            # Alternative: Check frequency increase
            if 'Frequency' in data.columns and 'Date' in data.columns:
                if len(data) >= 7:
                    data_sorted = data.sort_values('Date')
                    freq_values = data_sorted['Frequency'].values
                    
                    if len(freq_values) > 2:
                        freq_slope, _, _, _, _ = stats.linregress(range(len(freq_values)), freq_values)
                        avg_freq = np.mean(freq_values)
                        
                        # High frequency and increasing
                        if avg_freq > 5 and freq_slope > 0:
                            return {
                                'detected': True,
                                'severity': 'medium',
                                'evidence': {
                                    'average_frequency': float(avg_freq),
                                    'frequency_trend': 'increasing'
                                },
                                'recommendation': 'Audience showing signs of saturation - consider expansion'
                            }
        
        except Exception as e:
            logger.warning(f"Error detecting audience saturation: {e}")
        
        return {'detected': False}
    
    def _find_day_parting(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Find day parting opportunities"""
        try:
            if 'Hour' in data.columns and 'Conversions' in data.columns:
                # Group by hour
                hourly_performance = data.groupby('Hour').agg({
                    'Conversions': 'sum',
                    'Spend': 'sum',
                    'Impressions': 'sum'
                })
                
                if len(hourly_performance) >= 12:  # At least half-day data
                    # Calculate conversion rate by hour
                    hourly_performance['Conv_Rate'] = (
                        hourly_performance['Conversions'] / hourly_performance['Impressions']
                    )
                    
                    # Find best and worst performing hours
                    best_hours = hourly_performance.nlargest(3, 'Conv_Rate').index.tolist()
                    worst_hours = hourly_performance.nsmallest(3, 'Conv_Rate').index.tolist()
                    
                    # Check if there's significant variation
                    if hourly_performance['Conv_Rate'].std() / hourly_performance['Conv_Rate'].mean() > 0.3:
                        return {
                            'detected': True,
                            'best_hours': [int(h) for h in best_hours],
                            'worst_hours': [int(h) for h in worst_hours],
                            'recommendation': f"Increase bids during hours {best_hours}, decrease during {worst_hours}"
                        }
            
            # Alternative: Day of week analysis
            if 'Date' in data.columns and 'Conversions' in data.columns:
                data['DayOfWeek'] = pd.to_datetime(data['Date']).dt.dayofweek
                daily_performance = data.groupby('DayOfWeek').agg({
                    'Conversions': 'sum',
                    'Spend': 'sum'
                })
                
                if len(daily_performance) >= 5:
                    daily_performance['CPA'] = daily_performance['Spend'] / daily_performance['Conversions']
                    
                    best_days = daily_performance.nsmallest(2, 'CPA').index.tolist()
                    worst_days = daily_performance.nlargest(2, 'CPA').index.tolist()
                    
                    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    return {
                        'detected': True,
                        'type': 'day_of_week',
                        'best_days': [day_names[d] for d in best_days],
                        'worst_days': [day_names[d] for d in worst_days],
                        'recommendation': f"Focus budget on {[day_names[d] for d in best_days]}"
                    }
        
        except Exception as e:
            logger.warning(f"Error finding day parting opportunities: {e}")
        
        return {'detected': False}
    
    def _analyze_budget_pacing(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze budget pacing and spending patterns
        
        Detects if budget is being spent too quickly or too slowly
        """
        try:
            if 'Date' not in data.columns or 'Spend' not in data.columns:
                return {'detected': False, 'reason': 'Missing required columns'}
            
            if len(data) < 7:
                return {'detected': False, 'reason': 'Insufficient data'}
            
            data_sorted = data.sort_values('Date')
            
            # Calculate daily spend
            daily_spend = data_sorted.groupby('Date')['Spend'].sum()
            
            # Calculate expected vs actual pacing
            total_days = len(daily_spend)
            days_elapsed = total_days  # Assuming we're analyzing complete period
            
            total_spend = daily_spend.sum()
            avg_daily_spend = total_spend / total_days
            
            # Calculate spend velocity (trend)
            spend_values = daily_spend.values
            if len(spend_values) > 2:
                slope, _, r_value, _, _ = stats.linregress(range(len(spend_values)), spend_values)
                
                # Determine pacing status
                if slope > avg_daily_spend * 0.1:  # Accelerating spend
                    severity = 'high' if slope > avg_daily_spend * 0.2 else 'medium'
                    return {
                        'detected': True,
                        'status': 'accelerating',
                        'severity': severity,
                        'evidence': {
                            'daily_increase': float(slope),
                            'avg_daily_spend': float(avg_daily_spend),
                            'acceleration_rate': float(slope / avg_daily_spend)
                        },
                        'recommendation': 'Budget pacing ahead of schedule - review daily caps',
                        'expected_impact': 'Budget may exhaust early, missing end-of-period opportunities'
                    }
                elif slope < -avg_daily_spend * 0.1:  # Decelerating spend
                    severity = 'high' if slope < -avg_daily_spend * 0.2 else 'medium'
                    return {
                        'detected': True,
                        'status': 'decelerating',
                        'severity': severity,
                        'evidence': {
                            'daily_decrease': float(slope),
                            'avg_daily_spend': float(avg_daily_spend),
                            'deceleration_rate': float(slope / avg_daily_spend)
                        },
                        'recommendation': 'Budget pacing behind schedule - increase bids or expand targeting',
                        'expected_impact': 'Underutilized budget, missing potential conversions'
                    }
                else:
                    return {
                        'detected': True,
                        'status': 'optimal',
                        'severity': 'low',
                        'evidence': {
                            'avg_daily_spend': float(avg_daily_spend),
                            'consistency': 'high'
                        },
                        'recommendation': 'Budget pacing is optimal',
                        'expected_impact': 'Maintaining current trajectory'
                    }
        
        except Exception as e:
            logger.warning(f"Error analyzing budget pacing: {e}")
        
        return {'detected': False}
    
    def _identify_performance_clusters(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Identify clusters of high/low performing campaigns or segments
        
        Groups campaigns by performance characteristics
        """
        try:
            if 'Campaign' not in data.columns or len(data) < 5:
                return {'detected': False, 'reason': 'Insufficient campaign data'}
            
            # Identify available aggregation columns
            agg_dict = {}
            
            # Find spend column
            spend_col = next((c for c in data.columns if c.lower() in ['spend', 'total spent', 'total_spent']), None)
            if spend_col:
                agg_dict[spend_col] = 'sum'
            
            # Find conversions column
            conv_col = next((c for c in data.columns if c.lower() in ['conversions', 'site visit', 'site_visit']), None)
            if conv_col:
                agg_dict[conv_col] = 'sum'
            
            # CTR and ROAS (calculated or pre-existing)
            if 'CTR' in data.columns:
                agg_dict['CTR'] = 'mean'
            if 'ROAS' in data.columns:
                agg_dict['ROAS'] = 'mean'
                
            if not agg_dict:
                return {'detected': False, 'reason': 'No required metrics found for clustering'}
                
            # Calculate performance metrics by campaign
            campaign_metrics = data.groupby('Campaign').agg(agg_dict)
            
            # Rename for consistency within this method
            if spend_col: campaign_metrics = campaign_metrics.rename(columns={spend_col: 'Spend'})
            if conv_col: campaign_metrics = campaign_metrics.rename(columns={conv_col: 'Conversions'})
            
            if len(campaign_metrics) < 3:
                return {'detected': False, 'reason': 'Too few campaigns'}
            
            # Calculate CPA
            campaign_metrics['CPA'] = campaign_metrics['Spend'] / campaign_metrics['Conversions']
            campaign_metrics['CPA'] = campaign_metrics['CPA'].replace([np.inf, -np.inf], np.nan)
            
            # Identify top and bottom performers
            if 'ROAS' in campaign_metrics.columns:
                top_performers = campaign_metrics.nlargest(3, 'ROAS').index.tolist()
                bottom_performers = campaign_metrics.nsmallest(3, 'ROAS').index.tolist()
                
                top_avg_roas = campaign_metrics.loc[top_performers, 'ROAS'].mean()
                bottom_avg_roas = campaign_metrics.loc[bottom_performers, 'ROAS'].mean()
                
                performance_gap = top_avg_roas - bottom_avg_roas
                
                if performance_gap > 1.0:  # Significant gap
                    return {
                        'detected': True,
                        'clusters': {
                            'high_performers': {
                                'campaigns': top_performers,
                                'avg_roas': float(top_avg_roas),
                                'count': len(top_performers)
                            },
                            'low_performers': {
                                'campaigns': bottom_performers,
                                'avg_roas': float(bottom_avg_roas),
                                'count': len(bottom_performers)
                            }
                        },
                        'performance_gap': float(performance_gap),
                        'recommendation': f'Shift budget from low performers ({bottom_performers}) to high performers ({top_performers})',
                        'expected_impact': f'Potential ROAS improvement: +{performance_gap:.1f}x'
                    }
        
        except Exception as e:
            logger.warning(f"Error identifying performance clusters: {e}")
        
        return {'detected': False}
    
    def _analyze_conversion_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze conversion patterns and identify optimization opportunities
        
        Looks for patterns in conversion rates, timing, and segments
        """
        try:
            if 'Conversions' not in data.columns or len(data) < 10:
                return {'detected': False, 'reason': 'Insufficient conversion data'}
            
            patterns = {}
            
            # 1. Device conversion patterns
            if 'Device' in data.columns:
                device_conv = data.groupby('Device').agg({
                    'Conversions': 'sum',
                    'Spend': 'sum',
                    'Clicks': 'sum' if 'Clicks' in data.columns else lambda x: 0
                })
                
                if len(device_conv) > 1:
                    device_conv['CPA'] = device_conv['Spend'] / device_conv['Conversions']
                    device_conv['Conv_Rate'] = device_conv['Conversions'] / device_conv['Clicks']
                    
                    best_device = device_conv['CPA'].idxmin()
                    worst_device = device_conv['CPA'].idxmax()
                    
                    patterns['device'] = {
                        'best_device': best_device,
                        'worst_device': worst_device,
                        'best_cpa': float(device_conv.loc[best_device, 'CPA']),
                        'worst_cpa': float(device_conv.loc[worst_device, 'CPA']),
                        'recommendation': f'Prioritize {best_device} - {(device_conv.loc[worst_device, "CPA"] / device_conv.loc[best_device, "CPA"] - 1) * 100:.0f}% lower CPA'
                    }
            
            # 2. Time-based conversion patterns
            if 'Date' in data.columns:
                data_sorted = data.sort_values('Date')
                data_sorted['DayOfWeek'] = pd.to_datetime(data_sorted['Date']).dt.dayofweek
                
                dow_conv = data_sorted.groupby('DayOfWeek')['Conversions'].sum()
                
                if len(dow_conv) >= 5:
                    best_day = dow_conv.idxmax()
                    worst_day = dow_conv.idxmin()
                    
                    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    patterns['timing'] = {
                        'best_day': day_names[best_day],
                        'worst_day': day_names[worst_day],
                        'best_day_conversions': int(dow_conv[best_day]),
                        'worst_day_conversions': int(dow_conv[worst_day]),
                        'recommendation': f'Increase budget on {day_names[best_day]}'
                    }
            
            # 3. Funnel stage patterns
            if 'Funnel_Stage' in data.columns:
                funnel_conv = data.groupby('Funnel_Stage').agg({
                    'Conversions': 'sum',
                    'Spend': 'sum'
                })
                
                if len(funnel_conv) > 1:
                    funnel_conv['CPA'] = funnel_conv['Spend'] / funnel_conv['Conversions']
                    
                    best_stage = funnel_conv['CPA'].idxmin()
                    
                    patterns['funnel'] = {
                        'best_stage': best_stage,
                        'best_cpa': float(funnel_conv.loc[best_stage, 'CPA']),
                        'recommendation': f'Focus on {best_stage} stage campaigns'
                    }
            
            if patterns:
                return {
                    'detected': True,
                    'patterns': patterns,
                    'summary': f'Found {len(patterns)} conversion pattern opportunities'
                }
        
        except Exception as e:
            logger.warning(f"Error analyzing conversion patterns: {e}")
        
        return {'detected': False}
