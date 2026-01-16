"""
Visualization Generation Agent for creating charts and infographics.
"""
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Any, Optional
from pathlib import Path
import json

from loguru import logger

from src.platform.models.campaign import Campaign, ChannelPerformance, Achievement
from src.platform.models.platform import PlatformType, MetricType, PLATFORM_CONFIGS
from src.core.config.settings import settings


class VisualizationAgent:
    """Agent for generating charts, graphs, and infographics."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize Visualization Agent.
        
        Args:
            output_dir: Directory to save generated visualizations
        """
        self.output_dir = output_dir or settings.report_dir / "visualizations"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized VisualizationAgent with output_dir: {self.output_dir}")
    
    def generate_all_visualizations(
        self,
        campaign: Campaign,
        channel_performances: List[ChannelPerformance]
    ) -> List[Dict[str, Any]]:
        """
        Generate all visualizations for the campaign.
        
        Args:
            campaign: Campaign data
            channel_performances: Channel performance data
            
        Returns:
            List of visualization metadata
        """
        logger.info(f"Generating visualizations for campaign {campaign.campaign_id}")
        
        visualizations = []
        campaign_id = campaign.campaign_id
        
        # 1. Cross-channel performance comparison
        viz = self.create_channel_comparison_chart(channel_performances, campaign_id)
        if viz:
            visualizations.append(viz)
        
        # 2. Spend distribution pie chart
        viz = self.create_spend_distribution_chart(channel_performances, campaign_id)
        if viz:
            visualizations.append(viz)
        
        # 3. ROAS comparison bar chart
        viz = self.create_roas_comparison_chart(channel_performances, campaign_id)
        if viz:
            visualizations.append(viz)
        
        # 4. Efficiency scatter plot (CPA vs Conversions)
        viz = self.create_efficiency_scatter(channel_performances, campaign_id)
        if viz:
            visualizations.append(viz)
        
        # 5. Performance score radar chart
        viz = self.create_performance_radar(channel_performances, campaign_id)
        if viz:
            visualizations.append(viz)
        
        # 6. Funnel visualization
        viz = self.create_funnel_chart(campaign, campaign_id)
        if viz:
            visualizations.append(viz)
        
        logger.info(f"Generated {len(visualizations)} visualizations")
        return visualizations
    
    def create_channel_comparison_chart(
        self,
        channel_performances: List[ChannelPerformance],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create grouped bar chart comparing key metrics across channels."""
        if not channel_performances:
            return None
        
        platforms = [cp.platform_name for cp in channel_performances]
        impressions = [cp.total_impressions or 0 for cp in channel_performances]
        clicks = [cp.total_clicks or 0 for cp in channel_performances]
        conversions = [cp.total_conversions or 0 for cp in channel_performances]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Impressions',
            x=platforms,
            y=impressions,
            marker_color='lightblue'
        ))
        
        fig.add_trace(go.Bar(
            name='Clicks',
            x=platforms,
            y=clicks,
            marker_color='royalblue'
        ))
        
        fig.add_trace(go.Bar(
            name='Conversions',
            x=platforms,
            y=conversions,
            marker_color='darkblue'
        ))
        
        fig.update_layout(
            title='Cross-Channel Performance Comparison',
            xaxis_title='Platform',
            yaxis_title='Count',
            barmode='group',
            template='plotly_white',
            height=500,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_channel_comparison.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "bar_chart",
            "title": "Cross-Channel Performance Comparison",
            "filepath": str(filepath),
            "description": "Comparison of impressions, clicks, and conversions across all channels"
        }
    
    def create_spend_distribution_chart(
        self,
        channel_performances: List[ChannelPerformance],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create pie chart showing spend distribution."""
        platforms = []
        spends = []
        colors = []
        
        for cp in channel_performances:
            if cp.total_spend and cp.total_spend > 0:
                platforms.append(cp.platform_name)
                spends.append(cp.total_spend)
                config = PLATFORM_CONFIGS.get(cp.platform)
                colors.append(config.color if config else '#999999')
        
        if not spends:
            return None
        
        fig = go.Figure(data=[go.Pie(
            labels=platforms,
            values=spends,
            marker=dict(colors=colors),
            textinfo='label+percent',
            textposition='inside'
        )])
        
        fig.update_layout(
            title='Budget Distribution by Channel',
            template='plotly_white',
            height=500,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_spend_distribution.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "pie_chart",
            "title": "Budget Distribution by Channel",
            "filepath": str(filepath),
            "description": "Percentage of total budget allocated to each channel"
        }
    
    def create_roas_comparison_chart(
        self,
        channel_performances: List[ChannelPerformance],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create bar chart comparing ROAS across channels."""
        platforms = []
        roas_values = []
        colors = []
        
        for cp in channel_performances:
            if cp.roas is not None:
                platforms.append(cp.platform_name)
                roas_values.append(cp.roas)
                config = PLATFORM_CONFIGS.get(cp.platform)
                colors.append(config.color if config else '#999999')
        
        if not roas_values:
            return None
        
        fig = go.Figure(data=[go.Bar(
            x=platforms,
            y=roas_values,
            marker_color=colors,
            text=[f'{v:.2f}x' for v in roas_values],
            textposition='outside'
        )])
        
        # Add target line at 3.0x
        fig.add_hline(y=3.0, line_dash="dash", line_color="red",
                     annotation_text="Target ROAS (3.0x)")
        
        fig.update_layout(
            title='Return on Ad Spend (ROAS) by Channel',
            xaxis_title='Platform',
            yaxis_title='ROAS',
            template='plotly_white',
            height=500,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_roas_comparison.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "bar_chart",
            "title": "Return on Ad Spend (ROAS) by Channel",
            "filepath": str(filepath),
            "description": "ROAS comparison across channels with target benchmark"
        }
    
    def create_efficiency_scatter(
        self,
        channel_performances: List[ChannelPerformance],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create scatter plot showing CPA vs Conversions."""
        platforms = []
        cpas = []
        conversions = []
        colors = []
        
        for cp in channel_performances:
            if cp.cpa and cp.total_conversions:
                platforms.append(cp.platform_name)
                cpas.append(cp.cpa)
                conversions.append(cp.total_conversions)
                config = PLATFORM_CONFIGS.get(cp.platform)
                colors.append(config.color if config else '#999999')
        
        if not cpas:
            return None
        
        fig = go.Figure(data=[go.Scatter(
            x=conversions,
            y=cpas,
            mode='markers+text',
            marker=dict(size=15, color=colors),
            text=platforms,
            textposition='top center',
            textfont=dict(size=10)
        )])
        
        fig.update_layout(
            title='Channel Efficiency: CPA vs Conversions',
            xaxis_title='Total Conversions',
            yaxis_title='Cost Per Acquisition ($)',
            template='plotly_white',
            height=500,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_efficiency_scatter.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "scatter_plot",
            "title": "Channel Efficiency: CPA vs Conversions",
            "filepath": str(filepath),
            "description": "Efficiency analysis showing cost per acquisition relative to conversion volume"
        }
    
    def create_performance_radar(
        self,
        channel_performances: List[ChannelPerformance],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create radar chart showing performance scores."""
        if not channel_performances:
            return None
        
        categories = ['Performance Score', 'Efficiency', 'Volume', 'Engagement', 'ROI']
        
        fig = go.Figure()
        
        for cp in channel_performances[:5]:  # Limit to top 5 channels
            # Normalize metrics to 0-100 scale
            scores = [
                cp.performance_score or 0,
                min((cp.roas or 0) / 5 * 100, 100),  # ROAS normalized
                min((cp.total_conversions or 0) / 1000 * 100, 100),  # Conversions normalized
                min((cp.ctr or 0) / 5 * 100, 100),  # CTR normalized
                min((cp.roas or 0) / 5 * 100, 100)  # ROI (same as ROAS)
            ]
            
            fig.add_trace(go.Scatterpolar(
                r=scores,
                theta=categories,
                fill='toself',
                name=cp.platform_name
            ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title='Multi-Dimensional Performance Analysis',
            template='plotly_white',
            height=600,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_performance_radar.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "radar_chart",
            "title": "Multi-Dimensional Performance Analysis",
            "filepath": str(filepath),
            "description": "Radar chart showing performance across multiple dimensions"
        }
    
    def create_funnel_chart(
        self,
        campaign: Campaign,
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create funnel visualization showing conversion path."""
        # Aggregate metrics across all platforms
        total_impressions = 0
        total_clicks = 0
        total_conversions = 0
        
        for metric in campaign.normalized_metrics:
            if metric.metric_type == MetricType.IMPRESSIONS:
                total_impressions += metric.value
            elif metric.metric_type == MetricType.CLICKS:
                total_clicks += metric.value
            elif metric.metric_type == MetricType.CONVERSIONS:
                total_conversions += metric.value
        
        if not (total_impressions and total_clicks and total_conversions):
            return None
        
        fig = go.Figure(go.Funnel(
            y=['Impressions', 'Clicks', 'Conversions'],
            x=[total_impressions, total_clicks, total_conversions],
            textinfo='value+percent initial',
            marker=dict(color=['lightblue', 'royalblue', 'darkblue'])
        ))
        
        fig.update_layout(
            title='Campaign Conversion Funnel',
            template='plotly_white',
            height=500,
            font=dict(size=12)
        )
        
        filepath = self.output_dir / f"{campaign_id}_funnel.png"
        fig.write_image(str(filepath))
        
        return {
            "type": "funnel_chart",
            "title": "Campaign Conversion Funnel",
            "filepath": str(filepath),
            "description": "Conversion funnel showing drop-off at each stage"
        }
    
    def create_achievement_infographic(
        self,
        achievements: List[Achievement],
        campaign_id: str
    ) -> Optional[Dict[str, Any]]:
        """Create infographic highlighting top achievements."""
        # This would create a custom infographic
        # For now, return metadata for manual creation
        return {
            "type": "infographic",
            "title": "Key Campaign Achievements",
            "description": "Visual summary of top 5 campaign achievements",
            "data": [a.model_dump() for a in achievements[:5]]
        }
