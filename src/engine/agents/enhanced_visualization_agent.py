"""
Enhanced Visualization Agent
Integrates Smart Visualization Engine, Marketing Rules, and Chart Generators
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from pathlib import Path
from loguru import logger

from .smart_visualization_engine import SmartVisualizationEngine, VisualizationType
from .marketing_visualization_rules import MarketingVisualizationRules, MarketingColorSchemes
from .chart_generators import SmartChartGenerator


class EnhancedVisualizationAgent:
    """Enhanced visualization agent with intelligent chart selection"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize Enhanced Visualization Agent
        
        Args:
            output_dir: Optional directory to save visualizations
        """
        self.viz_engine = SmartVisualizationEngine()
        self.rules = MarketingVisualizationRules()
        self.chart_gen = SmartChartGenerator()
        self.output_dir = output_dir
        
        logger.info("Initialized Enhanced Visualization Agent with all 3 layers")
    
    def create_visualizations_for_insights(self,
                                          insights: List[Dict],
                                          campaign_data: Optional[pd.DataFrame] = None,
                                          context: Optional[Dict] = None) -> List[Dict]:
        """
        Automatically create appropriate visualizations for each insight
        
        Args:
            insights: List of insight objects from reasoning agent
            campaign_data: Raw campaign performance data
            context: Campaign context (B2B/B2C, industry, etc.)
        
        Returns:
            List of {
                'insight_id': str,
                'chart': plotly.Figure,
                'chart_type': str,
                'title': str,
                'description': str,
                'category': str
            }
        """
        
        logger.info(f"Creating visualizations for {len(insights)} insights")
        visualizations = []
        
        for idx, insight in enumerate(insights):
            try:
                # Determine insight category
                insight_category = self._categorize_insight(insight)
                
                # Get visualization rules for this category
                viz_rules = self.rules.get_visualization_for_insight(
                    insight_category,
                    insight.get('data', {})
                )
                
                # Generate the chart
                chart = self._generate_chart(
                    chart_type=viz_rules['chart_type'],
                    data=insight.get('data', {}),
                    styling=viz_rules.get('styling', {}),
                    benchmarks=insight.get('benchmarks'),
                    title=insight.get('title', f'Insight {idx + 1}')
                )
                
                visualizations.append({
                    'insight_id': insight.get('id', f'insight_{idx}'),
                    'chart': chart,
                    'chart_type': viz_rules['chart_type'].value,
                    'title': insight.get('title', f'Insight {idx + 1}'),
                    'description': self._generate_chart_description(insight, viz_rules),
                    'category': insight_category
                })
                
                logger.debug(f"Created {viz_rules['chart_type'].value} for insight: {insight.get('title')}")
                
            except Exception as e:
                logger.error(f"Error creating visualization for insight {idx}: {e}")
                continue
        
        logger.info(f"Successfully created {len(visualizations)} visualizations")
        return visualizations
    
    def create_chart_for_category(self,
                                  category: str,
                                  data: Any,
                                  title: str = "",
                                  benchmarks: Optional[Dict] = None,
                                  context: Optional[Dict] = None) -> Dict:
        """
        Create a chart for a specific marketing category
        
        Args:
            category: Marketing insight category
            data: Data for the chart
            title: Chart title
            benchmarks: Optional benchmark values
            context: Optional context for adjustments
        
        Returns:
            Dictionary with chart and metadata
        """
        
        # Get visualization rules
        viz_rules = self.rules.get_visualization_for_insight(category, data)
        
        # Generate chart
        chart = self._generate_chart(
            chart_type=viz_rules['chart_type'],
            data=data,
            styling=viz_rules.get('styling', {}),
            benchmarks=benchmarks,
            title=title
        )
        
        return {
            'chart': chart,
            'chart_type': viz_rules['chart_type'].value,
            'title': title,
            'category': category,
            'config': viz_rules
        }
    
    def _categorize_insight(self, insight: Dict) -> str:
        """
        Categorize insight to apply correct visualization rules
        
        Args:
            insight: Insight dictionary
        
        Returns:
            Marketing insight category
        """
        
        # Check if category is explicitly provided
        if 'category' in insight:
            return insight['category']
        
        # Use keywords to categorize
        text = f"{insight.get('title', '')} {insight.get('description', '')}".lower()
        
        # Channel comparison
        if any(word in text for word in ['channel', 'platform', 'compare', 'comparison']):
            return 'channel_comparison'
        
        # Performance trend
        elif any(word in text for word in ['trend', 'over time', 'daily', 'weekly', 'monthly', 'time series']):
            return 'performance_trend'
        
        # Budget distribution
        elif any(word in text for word in ['budget', 'allocation', 'distribution', 'spend']):
            return 'budget_distribution'
        
        # Audience performance
        elif any(word in text for word in ['audience', 'segment', 'demographic']):
            return 'audience_performance'
        
        # Creative fatigue
        elif any(word in text for word in ['creative', 'fatigue', 'decay', 'ad performance']):
            return 'creative_decay'
        
        # Attribution flow
        elif any(word in text for word in ['attribution', 'journey', 'path', 'touchpoint']):
            return 'attribution_flow'
        
        # Conversion funnel
        elif any(word in text for word in ['funnel', 'conversion', 'stage', 'drop-off']):
            return 'conversion_funnel'
        
        # Quality score
        elif any(word in text for word in ['quality score', 'components', 'qs']):
            return 'quality_score_components'
        
        # Day parting
        elif any(word in text for word in ['hour', 'day parting', 'time of day', 'hourly']):
            return 'hourly_performance'
        
        # Device breakdown
        elif any(word in text for word in ['device', 'mobile', 'desktop', 'tablet']):
            return 'device_breakdown'
        
        # Geographic performance
        elif any(word in text for word in ['geo', 'geographic', 'region', 'location']):
            return 'geo_performance'
        
        # Keyword efficiency
        elif any(word in text for word in ['keyword', 'search term', 'query']):
            return 'keyword_efficiency'
        
        # Frequency analysis
        elif any(word in text for word in ['frequency', 'distribution', 'histogram']):
            return 'frequency_analysis'
        
        # Benchmark comparison
        elif any(word in text for word in ['benchmark', 'vs', 'compared to', 'industry']):
            return 'benchmark_comparison'
        
        # Campaign health
        elif any(word in text for word in ['health', 'score', 'overall']):
            return 'campaign_health'
        
        # Default
        else:
            logger.warning(f"Could not categorize insight, using default: {text[:50]}")
            return 'channel_comparison'
    
    def _generate_chart(self,
                       chart_type: VisualizationType,
                       data: Any,
                       styling: Dict,
                       benchmarks: Optional[Dict] = None,
                       title: str = "") -> Any:
        """
        Route to appropriate chart generator
        
        Args:
            chart_type: Type of visualization
            data: Data for the chart
            styling: Styling configuration
            benchmarks: Optional benchmark values
            title: Chart title
        
        Returns:
            Plotly figure object
        """
        
        chart_type_str = chart_type.value if hasattr(chart_type, 'value') else str(chart_type)
        
        try:
            # Channel comparison / Grouped bar
            if chart_type in [VisualizationType.GROUPED_BAR, VisualizationType.BAR_CHART]:
                return self.chart_gen.create_channel_comparison_chart(
                    data=data,
                    metrics=styling.get('metrics', ['spend', 'conversions', 'roas']),
                    benchmarks=benchmarks
                )
            
            # Performance trend / Multi-line
            elif chart_type in [VisualizationType.MULTI_LINE, VisualizationType.LINE_CHART]:
                return self.chart_gen.create_performance_trend_chart(
                    data=data,
                    metrics=styling.get('metrics', ['ctr', 'cpc']),
                    show_anomalies=styling.get('highlight_anomalies', True)
                )
            
            # Attribution flow / Sankey
            elif chart_type == VisualizationType.SANKEY:
                return self.chart_gen.create_attribution_sankey(data)
            
            # Performance gauge
            elif chart_type == VisualizationType.GAUGE:
                return self.chart_gen.create_performance_gauge(
                    actual=data.get('value', data.get('actual', 0)),
                    target=data.get('target', 100),
                    metric_name=data.get('metric_name', title or 'Performance'),
                    benchmarks=benchmarks
                )
            
            # Heatmap (hourly or general)
            elif chart_type == VisualizationType.HEATMAP:
                if 'hour' in str(data).lower() or styling.get('x_axis') == 'hour_of_day':
                    return self.chart_gen.create_hourly_heatmap(data.get('values', data))
                else:
                    # Generic heatmap - use hourly as fallback
                    return self.chart_gen.create_hourly_heatmap(data.get('values', data))
            
            # Scatter / Bubble (keyword efficiency)
            elif chart_type in [VisualizationType.SCATTER_PLOT, VisualizationType.BUBBLE_CHART]:
                return self.chart_gen.create_keyword_opportunity_scatter(
                    keyword_data=data.get('keywords', data if isinstance(data, list) else []),
                    benchmarks=benchmarks
                )
            
            # Treemap (budget allocation)
            elif chart_type == VisualizationType.TREEMAP:
                return self.chart_gen.create_budget_treemap(data)
            
            # Funnel (conversion stages)
            elif chart_type == VisualizationType.FUNNEL:
                return self.chart_gen.create_conversion_funnel(
                    funnel_data=data,
                    show_percentages=styling.get('show_drop_off_rate', True)
                )
            
            # Donut chart (device breakdown)
            elif chart_type == VisualizationType.DONUT_CHART:
                return self.chart_gen.create_device_donut(data)
            
            # Histogram (frequency analysis)
            elif chart_type == VisualizationType.HISTOGRAM:
                return self.chart_gen.create_frequency_histogram(
                    frequency_data=data.get('values', data if isinstance(data, list) else []),
                    optimal_range=styling.get('highlight_optimal_range')
                )
            
            # Bullet chart (benchmark comparison)
            elif chart_type == VisualizationType.BULLET_CHART:
                # Use gauge as fallback for bullet chart
                return self.chart_gen.create_performance_gauge(
                    actual=data.get('actual', 0),
                    target=data.get('target', 100),
                    metric_name=title or 'Performance',
                    benchmarks=benchmarks
                )
            
            # Default fallback - channel comparison
            else:
                logger.warning(f"No specific generator for {chart_type_str}, using default")
                return self.chart_gen.create_channel_comparison_chart(
                    data=data,
                    metrics=['spend']
                )
        
        except Exception as e:
            logger.error(f"Error generating {chart_type_str} chart: {e}")
            # Return a simple fallback chart
            return self.chart_gen.create_channel_comparison_chart(
                data=data if isinstance(data, dict) else {},
                metrics=['spend']
            )
    
    def _generate_chart_description(self, insight: Dict, viz_rules: Dict) -> str:
        """
        Generate natural language description of what the chart shows
        
        Args:
            insight: Insight dictionary
            viz_rules: Visualization rules
        
        Returns:
            Chart description string
        """
        
        chart_type = viz_rules['chart_type'].value.replace('_', ' ').title()
        description = insight.get('description', 'performance data')
        
        return f"This {chart_type} visualization shows {description}"
    
    def get_color_for_channel(self, channel: str) -> str:
        """
        Get brand color for a specific channel
        
        Args:
            channel: Channel name
        
        Returns:
            Hex color code
        """
        return MarketingColorSchemes.get_channel_color(channel)
    
    def get_performance_color(self,
                            value: float,
                            benchmark: float,
                            higher_is_better: bool = True) -> str:
        """
        Get color based on performance vs benchmark
        
        Args:
            value: Actual value
            benchmark: Benchmark value
            higher_is_better: Whether higher values are better
        
        Returns:
            Hex color code
        """
        return MarketingColorSchemes.get_performance_color(
            value, benchmark, higher_is_better
        )
    
    def create_dashboard_visualizations(self,
                                       campaign_data: pd.DataFrame,
                                       context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create a complete set of dashboard visualizations
        
        Args:
            campaign_data: Campaign performance DataFrame
            context: Optional campaign context
        
        Returns:
            Dictionary of visualizations by category
        """
        
        logger.info("Creating complete dashboard visualizations")
        
        dashboard = {}
        
        # Channel comparison
        if 'Channel' in campaign_data.columns or 'Platform' in campaign_data.columns:
            channel_col = 'Channel' if 'Channel' in campaign_data.columns else 'Platform'
            channel_data = {}
            
            for channel in campaign_data[channel_col].unique():
                channel_df = campaign_data[campaign_data[channel_col] == channel]
                channel_data[channel] = {
                    'spend': channel_df['Spend'].sum() if 'Spend' in channel_df.columns else 0,
                    'conversions': channel_df['Conversions'].sum() if 'Conversions' in channel_df.columns else 0,
                    'roas': channel_df['ROAS'].mean() if 'ROAS' in channel_df.columns else 0
                }
            
            dashboard['channel_comparison'] = self.create_chart_for_category(
                'channel_comparison',
                channel_data,
                title="Channel Performance Comparison"
            )
        
        # Performance trend
        if 'Date' in campaign_data.columns:
            trend_data = {
                'dates': campaign_data['Date'].unique().tolist(),
                'metrics': {}
            }
            
            if 'CTR' in campaign_data.columns:
                trend_data['metrics']['ctr'] = campaign_data.groupby('Date')['CTR'].mean().tolist()
            if 'CPC' in campaign_data.columns:
                trend_data['metrics']['cpc'] = campaign_data.groupby('Date')['CPC'].mean().tolist()
            
            if trend_data['metrics']:
                dashboard['performance_trend'] = self.create_chart_for_category(
                    'performance_trend',
                    trend_data,
                    title="Performance Trends Over Time"
                )
        
        # Device breakdown
        if 'Device' in campaign_data.columns:
            device_data = {
                'devices': campaign_data['Device'].unique().tolist(),
                'values': campaign_data.groupby('Device')['Conversions'].sum().tolist() if 'Conversions' in campaign_data.columns else []
            }
            
            if device_data['values']:
                dashboard['device_breakdown'] = self.create_chart_for_category(
                    'device_breakdown',
                    device_data,
                    title="Conversions by Device"
                )
        
        logger.info(f"Created {len(dashboard)} dashboard visualizations")
        return dashboard
    
    def create_executive_dashboard(self,
                                   insights: List[Dict],
                                   campaign_data: pd.DataFrame,
                                   context: Optional[Dict] = None) -> List[Dict]:
        """
        Create executive dashboard: High-level, visual, actionable
        
        Features:
        - Fewer charts (5-7 max)
        - More gauges, donut charts, simple bars
        - Focus on top insights only
        - Big numbers, clear trends
        - Action-oriented
        
        Args:
            insights: List of insights from reasoning agent
            campaign_data: Campaign performance DataFrame
            context: Optional campaign context
        
        Returns:
            List of executive-friendly visualizations
        """
        
        logger.info("Creating executive dashboard (high-level, actionable)")
        
        executive_viz = []
        
        # 1. Overall Performance Gauge (Campaign Health)
        if 'ROAS' in campaign_data.columns:
            avg_roas = campaign_data['ROAS'].mean()
            target_roas = context.get('target_roas', 2.5) if context else 2.5
            
            executive_viz.append({
                'title': 'Overall Campaign Performance',
                'chart': self.chart_gen.create_performance_gauge(
                    actual=avg_roas,
                    target=target_roas,
                    metric_name='Campaign ROAS',
                    benchmarks={'poor': 1.5, 'good': 2.5, 'excellent': 4.0}
                ),
                'chart_type': 'gauge',
                'priority': 1,
                'description': f'Overall ROAS: {avg_roas:.2f} (Target: {target_roas:.2f})'
            })
        
        # 2. Top 3-5 Channels Comparison (Simplified)
        if 'Channel' in campaign_data.columns or 'Platform' in campaign_data.columns:
            channel_col = 'Channel' if 'Channel' in campaign_data.columns else 'Platform'
            
            # Get top 5 channels by spend
            top_channels = campaign_data.groupby(channel_col)['Spend'].sum().nlargest(5).index.tolist()
            
            channel_data = {}
            for channel in top_channels:
                channel_df = campaign_data[campaign_data[channel_col] == channel]
                channel_data[channel] = {
                    'spend': channel_df['Spend'].sum() if 'Spend' in channel_df.columns else 0,
                    'conversions': channel_df['Conversions'].sum() if 'Conversions' in channel_df.columns else 0,
                    'roas': channel_df['ROAS'].mean() if 'ROAS' in channel_df.columns else 0
                }
            
            executive_viz.append({
                'title': 'Top 5 Channels Performance',
                'chart': self.chart_gen.create_channel_comparison_chart(
                    data=channel_data,
                    metrics=['spend', 'conversions', 'roas'],
                    benchmarks={'roas': target_roas} if 'target_roas' in locals() else None
                ),
                'chart_type': 'grouped_bar',
                'priority': 2,
                'description': f'Performance comparison of top {len(channel_data)} channels'
            })
        
        # 3. Budget Allocation (Treemap)
        if 'Channel' in campaign_data.columns:
            # Build hierarchical budget data
            labels = []
            parents = []
            values = []
            performance = []
            
            for channel in campaign_data['Channel'].unique():
                channel_df = campaign_data[campaign_data['Channel'] == channel]
                spend = channel_df['Spend'].sum()
                roas = channel_df['ROAS'].mean()
                
                labels.append(channel)
                parents.append('')
                values.append(spend)
                performance.append(roas)
            
            executive_viz.append({
                'title': 'Budget Allocation & Efficiency',
                'chart': self.chart_gen.create_budget_treemap({
                    'labels': labels,
                    'parents': parents,
                    'values': values,
                    'performance': performance
                }),
                'chart_type': 'treemap',
                'priority': 3,
                'description': 'Budget distribution sized by spend, colored by ROAS'
            })
        
        # 4. Key Trend (Last 30 Days - Simplified)
        if 'Date' in campaign_data.columns:
            # Get last 30 days
            recent_data = campaign_data.sort_values('Date').tail(min(30 * len(campaign_data['Channel'].unique()), len(campaign_data)))
            
            trend_data = {
                'dates': recent_data.groupby('Date').first().index.astype(str).tolist(),
                'metrics': {
                    'roas': recent_data.groupby('Date')['ROAS'].mean().tolist() if 'ROAS' in recent_data.columns else []
                }
            }
            
            if trend_data['metrics']['roas']:
                executive_viz.append({
                    'title': 'ROAS Trend (Last 30 Days)',
                    'chart': self.chart_gen.create_performance_trend_chart(
                        data=trend_data,
                        metrics=['roas'],
                        show_anomalies=False  # Simplified for executives
                    ),
                    'chart_type': 'line',
                    'priority': 4,
                    'description': 'ROAS performance trend over the last 30 days'
                })
        
        # 5. Device Performance (Donut - Simple)
        if 'Device' in campaign_data.columns and 'Conversions' in campaign_data.columns:
            device_data = {
                'devices': campaign_data['Device'].unique().tolist(),
                'values': campaign_data.groupby('Device')['Conversions'].sum().tolist()
            }
            
            executive_viz.append({
                'title': 'Conversions by Device',
                'chart': self.chart_gen.create_device_donut(device_data),
                'chart_type': 'donut',
                'priority': 5,
                'description': 'Device distribution of conversions'
            })
        
        # 6. Top Insight/Recommendation (if available)
        if insights:
            # Get highest priority insight
            top_insight = max(insights, key=lambda x: x.get('priority', 0))
            
            if top_insight.get('data'):
                try:
                    insight_viz = self.create_visualizations_for_insights(
                        [top_insight],
                        campaign_data,
                        context
                    )
                    
                    if insight_viz:
                        executive_viz.append({
                            'title': f"Key Insight: {top_insight.get('title', 'Top Recommendation')}",
                            'chart': insight_viz[0]['chart'],
                            'chart_type': insight_viz[0]['chart_type'],
                            'priority': 6,
                            'description': top_insight.get('description', '')
                        })
                except Exception as e:
                    logger.warning(f"Could not create top insight visualization: {e}")
        
        # Sort by priority and limit to 7
        executive_viz = sorted(executive_viz, key=lambda x: x['priority'])[:7]
        
        logger.info(f"Created {len(executive_viz)} executive dashboard visualizations")
        return executive_viz
    
    def create_analyst_dashboard(self,
                                insights: List[Dict],
                                campaign_data: pd.DataFrame,
                                context: Optional[Dict] = None) -> List[Dict]:
        """
        Create analyst dashboard: Detailed, comprehensive, exploratory
        
        Features:
        - More charts (15-20)
        - Scatter plots, heatmaps, detailed breakdowns
        - All insights visualized
        - Granular data
        - Deep-dive analysis
        
        Args:
            insights: List of insights from reasoning agent
            campaign_data: Campaign performance DataFrame
            context: Optional campaign context
        
        Returns:
            List of detailed analyst visualizations
        """
        
        logger.info("Creating analyst dashboard (detailed, comprehensive)")
        
        analyst_viz = []
        
        # 1. All Insights Visualized
        if insights:
            insight_viz = self.create_visualizations_for_insights(
                insights,
                campaign_data,
                context
            )
            analyst_viz.extend([{
                **viz,
                'priority': idx + 1,
                'section': 'insights'
            } for idx, viz in enumerate(insight_viz)])
        
        # 2. Detailed Channel Comparison (All Channels, All Metrics)
        if 'Channel' in campaign_data.columns:
            channel_data = {}
            for channel in campaign_data['Channel'].unique():
                channel_df = campaign_data[campaign_data['Channel'] == channel]
                channel_data[channel] = {
                    'spend': channel_df['Spend'].sum() if 'Spend' in channel_df.columns else 0,
                    'impressions': channel_df['Impressions'].sum() if 'Impressions' in channel_df.columns else 0,
                    'clicks': channel_df['Clicks'].sum() if 'Clicks' in channel_df.columns else 0,
                    'conversions': channel_df['Conversions'].sum() if 'Conversions' in channel_df.columns else 0,
                    'ctr': channel_df['CTR'].mean() if 'CTR' in channel_df.columns else 0,
                    'cpc': channel_df['CPC'].mean() if 'CPC' in channel_df.columns else 0,
                    'roas': channel_df['ROAS'].mean() if 'ROAS' in channel_df.columns else 0
                }
            
            analyst_viz.append({
                'title': 'Comprehensive Channel Analysis',
                'chart': self.chart_gen.create_channel_comparison_chart(
                    data=channel_data,
                    metrics=['spend', 'conversions', 'ctr', 'roas']
                ),
                'chart_type': 'grouped_bar',
                'priority': 100,
                'section': 'channel_analysis'
            })
        
        # 3. Detailed Performance Trends (Multiple Metrics)
        if 'Date' in campaign_data.columns:
            trend_data = {
                'dates': campaign_data.groupby('Date').first().index.astype(str).tolist(),
                'metrics': {}
            }
            
            for metric in ['CTR', 'CPC', 'ROAS', 'Conversions']:
                if metric in campaign_data.columns:
                    trend_data['metrics'][metric.lower()] = campaign_data.groupby('Date')[metric].mean().tolist()
            
            if trend_data['metrics']:
                analyst_viz.append({
                    'title': 'Detailed Performance Trends',
                    'chart': self.chart_gen.create_performance_trend_chart(
                        data=trend_data,
                        metrics=list(trend_data['metrics'].keys()),
                        show_anomalies=True  # Show anomalies for analysts
                    ),
                    'chart_type': 'multi_line',
                    'priority': 101,
                    'section': 'trend_analysis'
                })
        
        # 4. Device Breakdown (Detailed)
        if 'Device' in campaign_data.columns:
            device_data = {
                'devices': campaign_data['Device'].unique().tolist(),
                'values': campaign_data.groupby('Device')['Conversions'].sum().tolist() if 'Conversions' in campaign_data.columns else []
            }
            
            analyst_viz.append({
                'title': 'Device Performance Breakdown',
                'chart': self.chart_gen.create_device_donut(device_data),
                'chart_type': 'donut',
                'priority': 102,
                'section': 'device_analysis'
            })
        
        # 5. Hourly Performance Heatmap (if hour data available)
        if 'Hour' in campaign_data.columns and 'Day' in campaign_data.columns:
            # Create 7x24 heatmap
            import numpy as np
            heatmap_data = np.zeros((7, 24))
            
            for day in range(7):
                for hour in range(24):
                    mask = (campaign_data['Day'] == day) & (campaign_data['Hour'] == hour)
                    if mask.any():
                        heatmap_data[day, hour] = campaign_data[mask]['CTR'].mean() if 'CTR' in campaign_data.columns else 0
            
            analyst_viz.append({
                'title': 'Hourly Performance Heatmap',
                'chart': self.chart_gen.create_hourly_heatmap(heatmap_data),
                'chart_type': 'heatmap',
                'priority': 103,
                'section': 'time_analysis'
            })
        
        # 6. Frequency Distribution (if frequency data available)
        if 'Frequency' in campaign_data.columns:
            analyst_viz.append({
                'title': 'Frequency Distribution Analysis',
                'chart': self.chart_gen.create_frequency_histogram(
                    frequency_data=campaign_data['Frequency'].tolist(),
                    optimal_range=(3, 7)
                ),
                'chart_type': 'histogram',
                'priority': 104,
                'section': 'frequency_analysis'
            })
        
        # 7. Budget Allocation Treemap (Detailed)
        if 'Channel' in campaign_data.columns and 'Campaign' in campaign_data.columns:
            # Hierarchical budget data
            labels = []
            parents = []
            values = []
            performance = []
            
            for channel in campaign_data['Channel'].unique():
                channel_df = campaign_data[campaign_data['Channel'] == channel]
                channel_spend = channel_df['Spend'].sum()
                channel_roas = channel_df['ROAS'].mean()
                
                labels.append(channel)
                parents.append('')
                values.append(channel_spend)
                performance.append(channel_roas)
                
                # Add campaigns under each channel
                for campaign in channel_df['Campaign'].unique()[:5]:  # Top 5 campaigns per channel
                    campaign_df = channel_df[channel_df['Campaign'] == campaign]
                    labels.append(f"{channel}-{campaign}")
                    parents.append(channel)
                    values.append(campaign_df['Spend'].sum())
                    performance.append(campaign_df['ROAS'].mean())
            
            analyst_viz.append({
                'title': 'Detailed Budget Allocation (Channel > Campaign)',
                'chart': self.chart_gen.create_budget_treemap({
                    'labels': labels,
                    'parents': parents,
                    'values': values,
                    'performance': performance
                }),
                'chart_type': 'treemap',
                'priority': 105,
                'section': 'budget_analysis'
            })
        
        # 8. Conversion Funnel (if funnel data available)
        if all(col in campaign_data.columns for col in ['Impressions', 'Clicks', 'Conversions']):
            funnel_data = {
                'stages': ['Impressions', 'Clicks', 'Conversions'],
                'values': [
                    campaign_data['Impressions'].sum(),
                    campaign_data['Clicks'].sum(),
                    campaign_data['Conversions'].sum()
                ]
            }
            
            analyst_viz.append({
                'title': 'Conversion Funnel Analysis',
                'chart': self.chart_gen.create_conversion_funnel(
                    funnel_data=funnel_data,
                    show_percentages=True
                ),
                'chart_type': 'funnel',
                'priority': 106,
                'section': 'conversion_analysis'
            })
        
        # Sort by priority
        analyst_viz = sorted(analyst_viz, key=lambda x: x['priority'])
        
        logger.info(f"Created {len(analyst_viz)} analyst dashboard visualizations")
        return analyst_viz
