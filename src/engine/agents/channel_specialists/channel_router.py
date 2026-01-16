"""
Channel Router
Routes campaign data to appropriate channel specialist based on platform detection
"""

from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger

from .search_agent import SearchChannelAgent
from .social_agent import SocialChannelAgent
from .programmatic_agent import ProgrammaticAgent
from .base_specialist import BaseChannelSpecialist


class ChannelRouter:
    """Routes campaigns to appropriate channel specialist agents"""
    
    # Platform to channel type mapping
    PLATFORM_MAPPING = {
        'google ads': 'search',
        'google': 'search',
        'bing': 'search',
        'bing ads': 'search',
        'microsoft ads': 'search',
        'dv360 search': 'search',
        'sa360': 'search',
        
        'meta': 'social',
        'facebook': 'social',
        'instagram': 'social',
        'linkedin': 'social',
        'snapchat': 'social',
        'snap': 'social',
        'tiktok': 'social',
        'twitter': 'social',
        'x': 'social',
        'pinterest': 'social',
        
        'dv360': 'programmatic',
        'dbm': 'programmatic',
        'cm360': 'programmatic',
        'dcm': 'programmatic',
        'the trade desk': 'programmatic',
        'ttd': 'programmatic',
        'amazon dsp': 'programmatic',
        'xandr': 'programmatic',
        'yahoo dsp': 'programmatic'
    }
    
    def __init__(self, rag_retriever=None):
        """
        Initialize channel router with specialist agents
        
        Args:
            rag_retriever: RAG retriever for knowledge base access
        """
        self.rag = rag_retriever
        
        # Initialize specialist agents
        self.specialists = {
            'search': SearchChannelAgent(rag_retriever),
            'social': SocialChannelAgent(rag_retriever),
            'programmatic': ProgrammaticAgent(rag_retriever)
        }
        
        logger.info("Channel Router initialized with 3 specialist agents")
    
    def detect_channel_type(self, campaign_data: pd.DataFrame) -> str:
        """
        Detect the channel type from campaign data
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Channel type: 'search', 'social', 'programmatic', or 'unknown'
        """
        # First, try to detect platform
        platform = self._detect_platform(campaign_data)
        
        if platform:
            platform_lower = platform.lower()
            
            # Check direct mapping
            if platform_lower in self.PLATFORM_MAPPING:
                channel_type = self.PLATFORM_MAPPING[platform_lower]
                logger.info(f"Detected channel type: {channel_type} (platform: {platform})")
                return channel_type
            
            # Check partial matches
            for platform_key, channel_type in self.PLATFORM_MAPPING.items():
                if platform_key in platform_lower or platform_lower in platform_key:
                    logger.info(f"Detected channel type: {channel_type} (platform: {platform})")
                    return channel_type
        
        # Fallback: Analyze column names and metrics
        channel_type = self._infer_from_metrics(campaign_data)
        logger.info(f"Inferred channel type from metrics: {channel_type}")
        return channel_type
    
    def _detect_platform(self, data: pd.DataFrame) -> Optional[str]:
        """
        Detect platform from column names or data
        
        Args:
            data: Campaign data
            
        Returns:
            Platform name or None
        """
        columns = [col.lower() for col in data.columns]
        
        # Check for explicit platform column
        platform_cols = [col for col in data.columns if 'platform' in col.lower() or 'source' in col.lower()]
        if platform_cols:
            # Get most common platform value
            platform_values = data[platform_cols[0]].value_counts()
            if len(platform_values) > 0:
                return str(platform_values.index[0])
        
        # Check column name patterns
        if any('google' in col or 'adwords' in col for col in columns):
            return 'Google Ads'
        elif any('meta' in col or 'facebook' in col or 'instagram' in col for col in columns):
            return 'Meta'
        elif any('linkedin' in col for col in columns):
            return 'LinkedIn'
        elif any('dv360' in col or 'dbm' in col for col in columns):
            return 'DV360'
        elif any('cm360' in col or 'dcm' in col for col in columns):
            return 'CM360'
        elif any('snapchat' in col or 'snap' in col for col in columns):
            return 'Snapchat'
        elif any('tiktok' in col for col in columns):
            return 'TikTok'
        elif any('bing' in col or 'microsoft' in col for col in columns):
            return 'Bing Ads'
        
        return None
    
    def _infer_from_metrics(self, data: pd.DataFrame) -> str:
        """
        Infer channel type from available metrics
        
        Args:
            data: Campaign data
            
        Returns:
            Channel type: 'search', 'social', 'programmatic', or 'unknown'
        """
        columns = [col.lower() for col in data.columns]
        
        # Search indicators
        search_indicators = [
            'quality score', 'quality_score',
            'impression share', 'impression_share',
            'search term', 'search_term',
            'keyword', 'match type', 'match_type'
        ]
        search_score = sum(1 for indicator in search_indicators if any(indicator in col for col in columns))
        
        # Social indicators
        social_indicators = [
            'frequency',
            'engagement', 'like', 'comment', 'share', 'reaction',
            'video completion', 'video_completion',
            'post', 'creative'
        ]
        social_score = sum(1 for indicator in social_indicators if any(indicator in col for col in columns))
        
        # Programmatic indicators
        programmatic_indicators = [
            'viewability', 'viewable',
            'brand safety', 'brand_safety',
            'placement', 'site', 'domain',
            'exchange', 'ssp',
            'invalid traffic', 'ivt'
        ]
        programmatic_score = sum(1 for indicator in programmatic_indicators if any(indicator in col for col in columns))
        
        # Determine channel type based on scores
        scores = {
            'search': search_score,
            'social': social_score,
            'programmatic': programmatic_score
        }
        
        if max(scores.values()) == 0:
            return 'unknown'
        
        return max(scores, key=scores.get)
    
    def route_and_analyze(self, campaign_data: pd.DataFrame, 
                          channel_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Route campaign data to appropriate specialist and perform analysis
        
        Args:
            campaign_data: Campaign performance data
            channel_type: Optional explicit channel type (overrides detection)
            
        Returns:
            Analysis results from specialist agent
        """
        # Detect channel type if not provided
        if channel_type is None:
            channel_type = self.detect_channel_type(campaign_data)
        
        # Get appropriate specialist
        specialist = self.get_specialist(channel_type)
        
        if specialist is None:
            logger.warning(f"No specialist available for channel type: {channel_type}")
            return {
                'channel_type': channel_type,
                'status': 'no_specialist',
                'message': f'No specialist agent available for {channel_type} channel'
            }
        
        # Perform analysis
        logger.info(f"Routing to {channel_type} specialist for analysis")
        try:
            # Validate data before analysis
            if campaign_data.empty:
                logger.warning("Empty campaign data provided")
                return {
                    'channel_type': channel_type,
                    'status': 'error',
                    'error': 'Empty dataset',
                    'message': 'No data available for analysis'
                }
            
            # Make a copy to avoid modifying original
            campaign_data = campaign_data.copy()
            
            # Clean data: Check for extremely long string values that shouldn't be there
            for col in campaign_data.columns:
                if campaign_data[col].dtype == 'object':  # String column
                    # Check if any value is suspiciously long (>500 chars)
                    max_length = campaign_data[col].astype(str).str.len().max()
                    if max_length > 500:
                        logger.warning(f"Column '{col}' has extremely long values (max: {max_length} chars). Replacing with 'Mixed'.")
                        # Replace long strings with a placeholder
                        campaign_data[col] = campaign_data[col].apply(
                            lambda x: 'Mixed' if isinstance(x, str) and len(x) > 500 else x
                        )
            
            # Force numeric conversion for common metric columns
            numeric_cols = ['Spend', 'Cost', 'Revenue', 'Conversions', 'Clicks', 'Impressions', 
                          'CTR', 'CPC', 'CPA', 'ROAS', 'ROI', 'Quality_Score', 'Avg_CPC',
                          'Impression_Share', 'Conversion_Rate', 'Bounce_Rate']
            
            for col in numeric_cols:
                if col in campaign_data.columns:
                    # Try to convert to numeric, coercing errors to NaN
                    campaign_data[col] = pd.to_numeric(campaign_data[col], errors='coerce')
                    logger.debug(f"Converted {col} to numeric")
            
            # Log data info for debugging
            logger.debug(f"Campaign data shape: {campaign_data.shape}")
            logger.debug(f"Campaign data columns: {campaign_data.columns.tolist()}")
            logger.debug(f"Campaign data dtypes: {campaign_data.dtypes.to_dict()}")
            
            analysis = specialist.analyze(campaign_data)
            analysis['channel_type'] = channel_type
            analysis['specialist_used'] = specialist.__class__.__name__
            return analysis
        except (ValueError, TypeError) as e:
            error_msg = str(e)
            logger.error(f"Data error in {channel_type} specialist analysis: {error_msg[:200]}")
            import traceback
            logger.debug(traceback.format_exc())
            
            # Return a basic analysis instead of error
            return self._generate_fallback_analysis(campaign_data, channel_type, error_msg)
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error in {channel_type} specialist analysis: {error_msg[:200]}")
            import traceback
            logger.debug(traceback.format_exc())
            
            # Return a basic analysis instead of error
            return self._generate_fallback_analysis(campaign_data, channel_type, error_msg)
    
    def get_specialist(self, channel_type: str) -> Optional[BaseChannelSpecialist]:
        """
        Get specialist agent for a channel type
        
        Args:
            channel_type: Channel type ('search', 'social', 'programmatic')
            
        Returns:
            Specialist agent or None
        """
        return self.specialists.get(channel_type.lower())
    
    def analyze_multi_channel(self, campaigns_by_channel: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyze multiple channels and provide cross-channel insights
        
        Args:
            campaigns_by_channel: Dictionary mapping channel types to campaign data
            
        Returns:
            Combined analysis with cross-channel insights
        """
        results = {}
        
        # Analyze each channel
        for channel_type, campaign_data in campaigns_by_channel.items():
            logger.info(f"Analyzing {channel_type} channel")
            results[channel_type] = self.route_and_analyze(campaign_data, channel_type)
        
        # Generate cross-channel insights
        cross_channel_insights = self._generate_cross_channel_insights(results)
        
        return {
            'channel_analyses': results,
            'cross_channel_insights': cross_channel_insights,
            'channels_analyzed': list(results.keys()),
            'total_channels': len(results)
        }
    
    def _generate_cross_channel_insights(self, channel_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights across multiple channels
        
        Args:
            channel_results: Analysis results from each channel
            
        Returns:
            Cross-channel insights
        """
        insights = {
            'overall_health': 'unknown',
            'best_performing_channel': None,
            'needs_attention': [],
            'recommendations': []
        }
        
        # Calculate overall health
        health_scores = []
        for channel, result in channel_results.items():
            if isinstance(result, dict) and 'overall_health' in result:
                health = result['overall_health']
                if health == 'excellent':
                    health_scores.append(4)
                elif health == 'good':
                    health_scores.append(3)
                elif health in ['average', 'needs_attention', 'needs_optimization']:
                    health_scores.append(2)
                else:
                    health_scores.append(1)
        
        if health_scores:
            avg_health = sum(health_scores) / len(health_scores)
            if avg_health >= 3.5:
                insights['overall_health'] = 'excellent'
            elif avg_health >= 2.5:
                insights['overall_health'] = 'good'
            elif avg_health >= 1.5:
                insights['overall_health'] = 'needs_improvement'
            else:
                insights['overall_health'] = 'critical'
        
        # Identify channels needing attention
        for channel, result in channel_results.items():
            if isinstance(result, dict) and 'overall_health' in result:
                health = result['overall_health']
                if health in ['poor', 'critical', 'needs_improvement', 'critical_issues']:
                    insights['needs_attention'].append(channel)
        
        # Generate cross-channel recommendations
        if len(insights['needs_attention']) > 0:
            insights['recommendations'].append({
                'priority': 'high',
                'area': 'multi_channel',
                'recommendation': f"Focus optimization efforts on {', '.join(insights['needs_attention'])} channel(s)",
                'impact': 'high'
            })
        
        return insights
    
    def _generate_fallback_analysis(self, campaign_data: pd.DataFrame, channel_type: str, error_msg: str) -> Dict[str, Any]:
        """
        Generate a basic fallback analysis when specialist fails.
        
        Args:
            campaign_data: Campaign data
            channel_type: Type of channel
            error_msg: Error message from failed analysis
            
        Returns:
            Basic analysis dictionary
        """
        logger.info(f"Generating fallback analysis for {channel_type} channel")
        
        # Try to extract basic metrics safely
        metrics = {}
        
        # Safe metric extraction
        def safe_sum(col):
            if col in campaign_data.columns:
                try:
                    return pd.to_numeric(campaign_data[col], errors='coerce').sum()
                except:
                    return 0
            return 0
        
        def safe_mean(col):
            if col in campaign_data.columns:
                try:
                    return pd.to_numeric(campaign_data[col], errors='coerce').mean()
                except:
                    return 0
            return 0
        
        metrics['total_spend'] = safe_sum('Spend') or safe_sum('Cost')
        metrics['total_clicks'] = safe_sum('Clicks')
        metrics['total_impressions'] = safe_sum('Impressions')
        metrics['total_conversions'] = safe_sum('Conversions')
        metrics['avg_ctr'] = safe_mean('CTR')
        metrics['avg_cpc'] = safe_mean('CPC') or safe_mean('Avg_CPC')
        metrics['avg_roas'] = safe_mean('ROAS')
        
        # Determine platform
        platform = 'Unknown'
        if 'Platform' in campaign_data.columns:
            try:
                platform = campaign_data['Platform'].mode().iloc[0] if len(campaign_data['Platform'].mode()) > 0 else 'Unknown'
            except:
                platform = 'Unknown'
        
        # Determine health based on available metrics
        health = 'average'
        if metrics['avg_roas'] > 3:
            health = 'good'
        elif metrics['avg_roas'] > 4:
            health = 'excellent'
        elif metrics['avg_roas'] < 2 and metrics['avg_roas'] > 0:
            health = 'needs_improvement'
        
        return {
            'channel_type': channel_type,
            'platform': platform,
            'overall_health': health,
            'status': 'fallback',
            'message': 'Basic analysis generated due to data format issues',
            'metrics': metrics,
            'basic_metrics': {
                'metric': 'Overview',
                'status': health,
                'findings': [
                    f"Total spend: ${metrics['total_spend']:,.2f}" if metrics['total_spend'] > 0 else "Spend data unavailable",
                    f"Total conversions: {metrics['total_conversions']:,.0f}" if metrics['total_conversions'] > 0 else "Conversion data unavailable",
                    f"Average ROAS: {metrics['avg_roas']:.2f}x" if metrics['avg_roas'] > 0 else "ROAS data unavailable"
                ],
                'recommendation': 'Review data format for more detailed analysis'
            },
            'recommendations': [
                {
                    'area': 'data_quality',
                    'priority': 'medium',
                    'recommendation': 'Ensure numeric columns contain only numbers for detailed analysis',
                    'expected_impact': 'Better insights'
                }
            ],
            'specialist_used': 'FallbackAnalyzer',
            'data_issue': error_msg[:200] if len(error_msg) > 200 else error_msg
        }
    
    def get_available_specialists(self) -> Dict[str, str]:
        """
        Get list of available specialist agents
        
        Returns:
            Dictionary of channel types and specialist names
        """
        return {
            channel: specialist.__class__.__name__
            for channel, specialist in self.specialists.items()
        }
