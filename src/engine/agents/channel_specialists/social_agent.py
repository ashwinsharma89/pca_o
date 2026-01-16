"""
Social Channel Specialist Agent
Specializes in Meta, LinkedIn, Snapchat, TikTok analysis
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from loguru import logger

from .base_specialist import BaseChannelSpecialist


class SocialBenchmarks:
    """Industry benchmarks for social media advertising"""
    
    BENCHMARKS = {
        'meta': {
            'ctr': 0.009,  # 0.9% CTR
            'cpc': 0.97,  # $0.97 CPC
            'cpm': 7.19,  # $7.19 CPM
            'engagement_rate': 0.06,  # 6% engagement
            'frequency': 2.5,  # Optimal frequency
            'conversion_rate': 0.025  # 2.5% conversion rate
        },
        'linkedin': {
            'ctr': 0.0045,  # 0.45% CTR (B2B)
            'cpc': 5.26,  # $5.26 CPC
            'cpm': 33.80,  # $33.80 CPM
            'engagement_rate': 0.04,  # 4% engagement
            'conversion_rate': 0.02  # 2% conversion rate
        },
        'snapchat': {
            'ctr': 0.005,  # 0.5% CTR
            'cpc': 2.95,  # $2.95 CPC
            'cpm': 2.95,  # $2.95 CPM
            'engagement_rate': 0.08,  # 8% engagement
            'frequency': 3.0
        },
        'tiktok': {
            'ctr': 0.016,  # 1.6% CTR
            'cpc': 1.00,  # $1.00 CPC
            'cpm': 10.00,  # $10.00 CPM
            'engagement_rate': 0.15,  # 15% engagement
            'frequency': 2.0
        }
    }
    
    @classmethod
    def get_benchmark(cls, platform: str, metric: str) -> float:
        """Get benchmark value for a platform and metric"""
        platform_lower = platform.lower()
        if 'meta' in platform_lower or 'facebook' in platform_lower or 'instagram' in platform_lower:
            platform_key = 'meta'
        elif 'linkedin' in platform_lower:
            platform_key = 'linkedin'
        elif 'snapchat' in platform_lower or 'snap' in platform_lower:
            platform_key = 'snapchat'
        elif 'tiktok' in platform_lower:
            platform_key = 'tiktok'
        else:
            platform_key = 'meta'  # Default
        
        return cls.BENCHMARKS.get(platform_key, {}).get(metric.lower(), 0)


class SocialChannelAgent(BaseChannelSpecialist):
    """Specializes in Meta, LinkedIn, Snapchat, TikTok analysis"""
    
    def __init__(self, rag_retriever=None):
        super().__init__(rag_retriever)
        self.benchmarks = SocialBenchmarks()
    
    def analyze(self, campaign_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform social-specific analysis
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Dictionary with social-specific insights
        """
        logger.info("Starting social channel analysis")
        
        platform = self.detect_platform(campaign_data)
        
        insights = {
            'platform': platform,
            'creative_fatigue': self._detect_creative_fatigue(campaign_data),
            'audience_saturation': self._analyze_frequency(campaign_data),
            'engagement_metrics': self._analyze_engagement(campaign_data),
            'algorithm_performance': self._analyze_delivery(campaign_data),
            'creative_performance': self._analyze_creative_performance(campaign_data),
            'audience_insights': self._analyze_audience(campaign_data)
        }
        
        # Social-specific RAG retrieval
        context = self.retrieve_knowledge(
            query=f"social media {platform} creative performance optimization",
            filters={'category': 'social'}
        )
        
        # Generate recommendations
        insights['recommendations'] = self._generate_recommendations(insights, context)
        insights['overall_health'] = self._calculate_overall_health(insights)
        
        logger.info(f"Social analysis complete. Platform: {platform}, Health: {insights['overall_health']}")
        return insights
    
    def get_benchmarks(self) -> Dict[str, float]:
        """Get social-specific benchmarks"""
        return SocialBenchmarks.BENCHMARKS
    
    def _detect_creative_fatigue(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect creative fatigue patterns
        
        Args:
            data: Campaign data
            
        Returns:
            Creative fatigue analysis
        """
        analysis = {
            'metric': 'Creative Fatigue',
            'status': 'unknown',
            'findings': []
        }
        
        # Check for date and CTR columns
        date_cols = [col for col in data.columns if 'date' in col.lower()]
        
        if not date_cols or 'CTR' not in data.columns:
            analysis['status'] = 'unavailable'
            return analysis
        
        date_col = date_cols[0]
        
        # Sort by date and calculate CTR trend
        if pd.api.types.is_datetime64_any_dtype(data[date_col]):
            sorted_data = data.sort_values(date_col)
        else:
            try:
                data[date_col] = pd.to_datetime(data[date_col])
                sorted_data = data.sort_values(date_col)
            except:
                analysis['status'] = 'unavailable'
                return analysis
        
        # Calculate CTR decline
        if len(sorted_data) >= 7:
            first_week_ctr = sorted_data.head(7)['CTR'].mean()
            last_week_ctr = sorted_data.tail(7)['CTR'].mean()
            
            if first_week_ctr > 0:
                decline_pct = ((last_week_ctr - first_week_ctr) / first_week_ctr) * 100
                analysis['ctr_decline_pct'] = round(decline_pct, 1)
                
                if decline_pct < -30:
                    analysis['status'] = 'severe'
                    analysis['findings'].append(f"🚨 Severe creative fatigue detected: {abs(decline_pct):.1f}% CTR decline")
                    analysis['recommendation'] = "Urgent: Refresh creative immediately. Test new formats, messaging, and visuals"
                elif decline_pct < -15:
                    analysis['status'] = 'moderate'
                    analysis['findings'].append(f"⚠️ Moderate creative fatigue: {abs(decline_pct):.1f}% CTR decline")
                    analysis['recommendation'] = "Plan creative refresh within 1-2 weeks"
                else:
                    analysis['status'] = 'healthy'
                    analysis['findings'].append(f"✅ Creative performance stable (CTR change: {decline_pct:+.1f}%)")
        
        # Check frequency if available
        freq_cols = [col for col in data.columns if 'frequency' in col.lower()]
        if freq_cols:
            avg_freq = data[freq_cols[0]].mean()
            analysis['average_frequency'] = round(avg_freq, 2)
            
            if avg_freq > 4:
                analysis['findings'].append(f"⚠️ High frequency ({avg_freq:.1f}) may contribute to fatigue")
        
        return analysis
    
    def _analyze_frequency(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze audience saturation through frequency metrics
        
        Args:
            data: Campaign data
            
        Returns:
            Frequency/saturation analysis
        """
        analysis = {
            'metric': 'Audience Saturation',
            'status': 'unknown',
            'findings': []
        }
        
        # Look for frequency column
        freq_cols = [col for col in data.columns if 'frequency' in col.lower()]
        
        if not freq_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        freq_col = freq_cols[0]
        avg_frequency = data[freq_col].mean()
        max_frequency = data[freq_col].max()
        
        analysis['average_frequency'] = round(avg_frequency, 2)
        analysis['max_frequency'] = round(max_frequency, 2)
        
        # Optimal frequency is typically 2-3 for social
        optimal_freq = 2.5
        
        if avg_frequency > 5:
            analysis['status'] = 'saturated'
            analysis['findings'].append(f"🚨 Audience saturation detected (freq: {avg_frequency:.1f})")
            analysis['recommendation'] = "Expand audience targeting or refresh creative to reduce fatigue"
        elif avg_frequency > 3.5:
            analysis['status'] = 'approaching_saturation'
            analysis['findings'].append(f"⚠️ Frequency approaching saturation ({avg_frequency:.1f})")
            analysis['recommendation'] = "Monitor closely and prepare audience expansion"
        elif avg_frequency < 1.5:
            analysis['status'] = 'underexposed'
            analysis['findings'].append(f"ℹ️ Low frequency ({avg_frequency:.1f}) - audience may be underexposed")
            analysis['recommendation'] = "Consider increasing budget or narrowing targeting"
        else:
            analysis['status'] = 'optimal'
            analysis['findings'].append(f"✅ Frequency in optimal range ({avg_frequency:.1f})")
        
        # Check for frequency distribution
        high_freq_campaigns = data[data[freq_col] > 5]
        if len(high_freq_campaigns) > 0:
            analysis['high_frequency_campaigns'] = len(high_freq_campaigns)
            analysis['findings'].append(f"{len(high_freq_campaigns)} campaigns with frequency > 5 need attention")
        
        return analysis
    
    def _analyze_engagement(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze engagement metrics (likes, comments, shares)
        
        Args:
            data: Campaign data
            
        Returns:
            Engagement analysis
        """
        analysis = {
            'metric': 'Engagement',
            'status': 'unknown',
            'findings': []
        }
        
        # Look for engagement columns
        engagement_cols = []
        for col in data.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['like', 'comment', 'share', 'reaction', 'engagement']):
                engagement_cols.append(col)
        
        if not engagement_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        # Calculate engagement rate
        if 'Impressions' in data.columns:
            total_impressions = data['Impressions'].sum()
            total_engagements = sum(data[col].sum() for col in engagement_cols if col in data.columns)
            
            if total_impressions > 0:
                engagement_rate = total_engagements / total_impressions
                analysis['engagement_rate'] = round(engagement_rate * 100, 2)
                
                platform = self.detect_platform(data)
                benchmark = self.benchmarks.get_benchmark(platform, 'engagement_rate') * 100
                analysis['benchmark'] = benchmark
                
                analysis['status'] = self._calculate_metric_health(
                    engagement_rate * 100, 
                    benchmark, 
                    higher_is_better=True
                )
                
                if engagement_rate * 100 < benchmark * 0.5:
                    analysis['findings'].append(f"⚠️ Low engagement rate ({engagement_rate*100:.2f}%) - creative may not resonate")
                    analysis['recommendation'] = "Test more engaging creative formats (video, carousel, UGC)"
                else:
                    analysis['findings'].append(f"✅ Engagement rate: {engagement_rate*100:.2f}%")
        
        # Analyze engagement types
        engagement_breakdown = {}
        for col in engagement_cols:
            if col in data.columns:
                engagement_breakdown[col] = int(data[col].sum())
        
        if engagement_breakdown:
            analysis['engagement_breakdown'] = engagement_breakdown
            
            # Identify dominant engagement type
            if engagement_breakdown:
                dominant_type = max(engagement_breakdown, key=engagement_breakdown.get)
                analysis['findings'].append(f"Primary engagement: {dominant_type}")
        
        return analysis
    
    def _analyze_delivery(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze algorithm delivery and pacing
        
        Args:
            data: Campaign data
            
        Returns:
            Delivery analysis
        """
        analysis = {
            'metric': 'Algorithm Performance',
            'status': 'unknown',
            'findings': []
        }
        
        # Check for delivery/pacing indicators
        if 'Impressions' not in data.columns or 'Spend' not in data.columns:
            analysis['status'] = 'unavailable'
            return analysis
        
        # Calculate CPM
        total_spend = data['Spend'].sum()
        total_impressions = data['Impressions'].sum()
        
        if total_impressions > 0:
            cpm = (total_spend / total_impressions) * 1000
            analysis['cpm'] = round(cpm, 2)
            
            platform = self.detect_platform(data)
            benchmark_cpm = self.benchmarks.get_benchmark(platform, 'cpm')
            analysis['benchmark_cpm'] = benchmark_cpm
            
            if cpm > benchmark_cpm * 1.5:
                analysis['status'] = 'poor'
                analysis['findings'].append(f"⚠️ High CPM (${cpm:.2f}) indicates poor algorithm performance")
                analysis['recommendation'] = "Review targeting, creative quality, and bid strategy"
            elif cpm < benchmark_cpm * 0.7:
                analysis['status'] = 'excellent'
                analysis['findings'].append(f"✅ Excellent CPM (${cpm:.2f}) - algorithm performing well")
            else:
                analysis['status'] = 'good'
                analysis['findings'].append(f"CPM: ${cpm:.2f} (benchmark: ${benchmark_cpm:.2f})")
        
        # Check for delivery consistency
        if 'Date' in data.columns or any('date' in col.lower() for col in data.columns):
            date_col = 'Date' if 'Date' in data.columns else [col for col in data.columns if 'date' in col.lower()][0]
            
            daily_impressions = data.groupby(date_col)['Impressions'].sum()
            if len(daily_impressions) > 1:
                cv = daily_impressions.std() / daily_impressions.mean() if daily_impressions.mean() > 0 else 0
                
                if cv > 0.5:
                    analysis['findings'].append("⚠️ Inconsistent delivery - check budget pacing")
                else:
                    analysis['findings'].append("✅ Consistent delivery pattern")
        
        return analysis
    
    def _analyze_creative_performance(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze creative asset performance
        
        Args:
            data: Campaign data
            
        Returns:
            Creative performance analysis
        """
        analysis = {
            'metric': 'Creative Performance',
            'findings': []
        }
        
        # Look for creative identifiers
        creative_cols = [col for col in data.columns if any(term in col.lower() for term in ['creative', 'ad', 'asset'])]
        
        if not creative_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        creative_col = creative_cols[0]
        total_creatives = data[creative_col].nunique()
        analysis['total_creatives'] = total_creatives
        
        # Performance distribution
        if 'CTR' in data.columns and 'Spend' in data.columns:
            creative_perf = data.groupby(creative_col).agg({
                'CTR': 'mean',
                'Spend': 'sum',
                'Impressions': 'sum'
            }).sort_values('CTR', ascending=False)
            
            # Top performers
            top_20_pct = max(1, int(len(creative_perf) * 0.2))
            top_performers = creative_perf.head(top_20_pct)
            bottom_performers = creative_perf.tail(top_20_pct)
            
            top_avg_ctr = top_performers['CTR'].mean()
            bottom_avg_ctr = bottom_performers['CTR'].mean()
            
            if top_avg_ctr > 0:
                performance_gap = ((top_avg_ctr - bottom_avg_ctr) / top_avg_ctr) * 100
                analysis['performance_gap'] = round(performance_gap, 1)
                
                if performance_gap > 50:
                    analysis['findings'].append(f"⚠️ Large performance gap ({performance_gap:.1f}%) between top and bottom creatives")
                    analysis['recommendation'] = "Pause bottom 20% performers and scale top performers"
            
            # Spend concentration
            top_spend_pct = (top_performers['Spend'].sum() / creative_perf['Spend'].sum()) * 100
            analysis['top_20_spend_concentration'] = round(top_spend_pct, 1)
            
            if top_spend_pct < 50:
                analysis['findings'].append(f"ℹ️ Only {top_spend_pct:.1f}% of spend on top performers - opportunity to optimize")
        
        return analysis
    
    def _analyze_audience(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze audience targeting and performance
        
        Args:
            data: Campaign data
            
        Returns:
            Audience analysis
        """
        analysis = {
            'metric': 'Audience Performance',
            'findings': []
        }
        
        # Look for audience columns
        audience_cols = [col for col in data.columns if 'audience' in col.lower() or 'segment' in col.lower()]
        
        if not audience_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        audience_col = audience_cols[0]
        total_audiences = data[audience_col].nunique()
        analysis['total_audiences'] = total_audiences
        
        # Performance by audience
        if 'Conversions' in data.columns and 'Spend' in data.columns:
            audience_perf = data.groupby(audience_col).agg({
                'Conversions': 'sum',
                'Spend': 'sum'
            })
            
            audience_perf['CPA'] = audience_perf['Spend'] / audience_perf['Conversions'].replace(0, np.nan)
            audience_perf = audience_perf.sort_values('CPA')
            
            best_audience = audience_perf.index[0] if len(audience_perf) > 0 else None
            worst_audience = audience_perf.index[-1] if len(audience_perf) > 0 else None
            
            if best_audience and worst_audience:
                analysis['best_performing_audience'] = str(best_audience)
                analysis['findings'].append(f"✅ Top audience: {best_audience}")
                
                # Check for zero-conversion audiences
                zero_conv_audiences = audience_perf[audience_perf['Conversions'] == 0]
                if len(zero_conv_audiences) > 0:
                    analysis['findings'].append(f"⚠️ {len(zero_conv_audiences)} audiences with 0 conversions")
                    analysis['recommendation'] = "Pause non-converting audiences and reallocate budget"
        
        return analysis
    
    def _calculate_overall_health(self, insights: Dict[str, Any]) -> str:
        """Calculate overall social campaign health"""
        health_scores = []
        
        for key, value in insights.items():
            if isinstance(value, dict) and 'status' in value:
                status = value['status']
                if status == 'excellent' or status == 'optimal' or status == 'healthy':
                    health_scores.append(4)
                elif status == 'good':
                    health_scores.append(3)
                elif status == 'average' or status == 'moderate' or status == 'approaching_saturation':
                    health_scores.append(2)
                elif status == 'poor' or status == 'severe' or status == 'saturated':
                    health_scores.append(1)
        
        if not health_scores:
            return 'unknown'
        
        avg_score = sum(health_scores) / len(health_scores)
        
        if avg_score >= 3.5:
            return 'excellent'
        elif avg_score >= 2.5:
            return 'good'
        elif avg_score >= 1.5:
            return 'needs_attention'
        else:
            return 'critical'
