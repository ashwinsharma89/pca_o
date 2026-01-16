"""
Smart Chart Generator for PCA Agent
Automatically generates relevant visualizations based on data and query context
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Dict, Any, Optional
from loguru import logger


class SmartChartGenerator:
    """Generates context-aware visualizations for campaign data."""
    
    # Color schemes
    COLORS = {
        'primary': ['#667eea', '#764ba2', '#f093fb', '#4facfe'],
        'performance': ['#11998e', '#38ef7d'],
        'warning': ['#f2994a', '#f2c94c'],
        'danger': ['#eb3349', '#f45c43'],
        'cool': ['#4facfe', '#00f2fe'],
    }
    
    def __init__(self):
        """Initialize chart generator."""
        self.template = 'plotly_dark'
    
    def generate_overview_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Generate overview charts when data is first loaded.
        
        Args:
            df: Campaign data DataFrame
            
        Returns:
            List of chart dictionaries with 'title', 'fig', 'description'
        """
        charts = []
        
        try:
            # Detect available columns
            cols = self._detect_columns(df)
            
            # 1. Spend vs Conversions scatter
            if cols['spend'] and cols['conversions']:
                chart = self._create_spend_vs_conversions(df, cols)
                if chart:
                    charts.append(chart)
            
            # 2. KPI Trends over time
            if cols['date']:
                chart = self._create_kpi_trends(df, cols)
                if chart:
                    charts.append(chart)
            
            # 3. Channel Performance comparison
            if cols['channel'] or cols['platform']:
                chart = self._create_channel_performance(df, cols)
                if chart:
                    charts.append(chart)
            
            # 4. Funnel Analysis
            if self._has_funnel_metrics(cols):
                chart = self._create_funnel_chart(df, cols)
                if chart:
                    charts.append(chart)
            
            # 5. ROAS/CPA Distribution
            if cols['roas'] or cols['cpa']:
                chart = self._create_efficiency_distribution(df, cols)
                if chart:
                    charts.append(chart)
            
            # 6. Top/Bottom Performers
            if cols['campaign']:
                chart = self._create_top_bottom_campaigns(df, cols)
                if chart:
                    charts.append(chart)
        
        except Exception as e:
            logger.error(f"Error generating overview charts: {e}")
        
        return charts
    
    def generate_query_charts(
        self, 
        query: str, 
        results_df: pd.DataFrame,
        original_df: Optional[pd.DataFrame] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate charts based on natural language query and results.
        
        Args:
            query: User's natural language query
            results_df: Query results DataFrame
            original_df: Original full dataset (for context)
            
        Returns:
            List of chart dictionaries
        """
        charts = []
        
        try:
            query_lower = query.lower()
            
            # Detect query intent
            is_comparison = any(word in query_lower for word in ['compare', 'vs', 'versus', 'last', 'previous'])
            is_trend = any(word in query_lower for word in ['trend', 'over time', 'week', 'month', 'day'])
            is_performance = any(word in query_lower for word in ['performance', 'kpi', 'metrics', 'roas', 'ctr', 'cpa'])
            is_channel = any(word in query_lower for word in ['channel', 'platform', 'source'])
            is_funnel = any(word in query_lower for word in ['funnel', 'conversion', 'stage'])
            
            cols = self._detect_columns(results_df)
            
            # Generate appropriate charts based on intent
            if is_comparison and cols['date']:
                chart = self._create_comparison_chart(results_df, cols, query)
                if chart:
                    charts.append(chart)
            
            if is_trend and cols['date']:
                chart = self._create_trend_analysis(results_df, cols)
                if chart:
                    charts.append(chart)
            
            if is_performance:
                chart = self._create_performance_metrics(results_df, cols)
                if chart:
                    charts.append(chart)
            
            if is_channel and (cols['channel'] or cols['platform']):
                chart = self._create_channel_breakdown(results_df, cols)
                if chart:
                    charts.append(chart)
            
            if is_funnel and self._has_funnel_metrics(cols):
                chart = self._create_funnel_chart(results_df, cols)
                if chart:
                    charts.append(chart)
            
            # Default: if results have numeric columns, create a summary chart
            if not charts and len(results_df) > 0:
                chart = self._create_default_visualization(results_df, cols)
                if chart:
                    charts.append(chart)
        
        except Exception as e:
            logger.error(f"Error generating query charts: {e}")
        
        return charts
    
    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """Detect relevant columns in DataFrame."""
        cols_lower = {col.lower(): col for col in df.columns}
        
        return {
            'date': self._find_col(cols_lower, ['date', 'day', 'week', 'month', 'time']),
            'campaign': self._find_col(cols_lower, ['campaign', 'campaign_name', 'campaign name']),
            'channel': self._find_col(cols_lower, ['channel', 'source']),
            'platform': self._find_col(cols_lower, ['platform', 'network']),
            'spend': self._find_col(cols_lower, ['spend', 'cost', 'investment', 'total spent', 'total_spent']),
            'impressions': self._find_col(cols_lower, ['impressions', 'impr', 'views']),
            'clicks': self._find_col(cols_lower, ['clicks', 'click']),
            'conversions': self._find_col(cols_lower, ['conversions', 'conv', 'leads', 'site visit', 'site_visit']),
            'revenue': self._find_col(cols_lower, ['revenue', 'sales']),
            'ctr': self._find_col(cols_lower, ['ctr', 'click_through_rate', 'clickrate']),
            'cpc': self._find_col(cols_lower, ['cpc', 'cost_per_click']),
            'cpa': self._find_col(cols_lower, ['cpa', 'cost_per_acquisition', 'cost_per_conversion']),
            'roas': self._find_col(cols_lower, ['roas', 'return_on_ad_spend']),
            'cpm': self._find_col(cols_lower, ['cpm', 'cost_per_mille']),
        }
    
    def _find_col(self, cols_dict: Dict[str, str], candidates: List[str]) -> Optional[str]:
        """Find first matching column from candidates."""
        for candidate in candidates:
            if candidate in cols_dict:
                return cols_dict[candidate]
        return None
    
    def _has_funnel_metrics(self, cols: Dict[str, Optional[str]]) -> bool:
        """Check if data has funnel metrics."""
        return bool(cols['impressions'] and cols['clicks'] and cols['conversions'])
    
    def _create_spend_vs_conversions(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create spend vs conversions scatter plot."""
        try:
            color_col = cols['channel'] or cols['platform']
            
            fig = px.scatter(
                df,
                x=cols['spend'],
                y=cols['conversions'],
                color=color_col,
                size=cols['spend'],
                hover_data=[cols['campaign']] if cols['campaign'] else None,
                title='Spend vs Conversions by Channel',
                template=self.template,
                color_discrete_sequence=self.COLORS['primary']
            )
            
            fig.update_layout(
                xaxis_title='Spend ($)',
                yaxis_title='Conversions',
                height=500
            )
            
            return {
                'title': '💰 Spend vs Conversions',
                'fig': fig,
                'description': 'Relationship between ad spend and conversions across channels'
            }
        except Exception as e:
            logger.warning(f"Failed to create spend vs conversions chart: {e}")
            return None
    
    def _create_kpi_trends(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create multi-line KPI trends over time."""
        try:
            df_sorted = df.sort_values(cols['date'])
            
            # Determine which KPIs to plot
            kpis_to_plot = []
            if cols['ctr']: kpis_to_plot.append(('CTR (%)', cols['ctr']))
            if cols['roas']: kpis_to_plot.append(('ROAS', cols['roas']))
            if cols['cpa']: kpis_to_plot.append(('CPA ($)', cols['cpa']))
            
            if not kpis_to_plot:
                # Fallback: plot spend and conversions
                if cols['spend']: kpis_to_plot.append(('Spend ($)', cols['spend']))
                if cols['conversions']: kpis_to_plot.append(('Conversions', cols['conversions']))
            
            if not kpis_to_plot:
                return None
            
            fig = make_subplots(
                rows=len(kpis_to_plot), cols=1,
                subplot_titles=[kpi[0] for kpi in kpis_to_plot],
                vertical_spacing=0.1
            )
            
            for idx, (kpi_name, col_name) in enumerate(kpis_to_plot, 1):
                fig.add_trace(
                    go.Scatter(
                        x=df_sorted[cols['date']],
                        y=df_sorted[col_name],
                        mode='lines+markers',
                        name=kpi_name,
                        line=dict(color=self.COLORS['primary'][idx % len(self.COLORS['primary'])], width=3),
                        marker=dict(size=6)
                    ),
                    row=idx, col=1
                )
            
            fig.update_layout(
                height=200 * len(kpis_to_plot),
                template=self.template,
                showlegend=False,
                title_text='KPI Trends Over Time'
            )
            
            return {
                'title': '📈 KPI Trends',
                'fig': fig,
                'description': 'Performance metrics trending over time'
            }
        except Exception as e:
            logger.warning(f"Failed to create KPI trends chart: {e}")
            return None
    
    def _create_channel_performance(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create channel performance comparison."""
        try:
            channel_col = cols['channel'] or cols['platform']
            
            # Aggregate by channel
            agg_dict = {}
            if cols['spend']: agg_dict[cols['spend']] = 'sum'
            if cols['conversions']: agg_dict[cols['conversions']] = 'sum'
            if cols['revenue']: agg_dict[cols['revenue']] = 'sum'
            
            if not agg_dict:
                return None
            
            df_agg = df.groupby(channel_col).agg(agg_dict).reset_index()
            
            # Create grouped bar chart
            fig = go.Figure()
            
            colors = self.COLORS['primary']
            for idx, col_name in enumerate(agg_dict.keys()):
                fig.add_trace(go.Bar(
                    x=df_agg[channel_col],
                    y=df_agg[col_name],
                    name=str(col_name).replace('_', ' ').title(),
                    marker_color=colors[idx % len(colors)]
                ))
            
            fig.update_layout(
                title='Channel Performance Comparison',
                xaxis_title='Channel',
                yaxis_title='Value',
                barmode='group',
                template=self.template,
                height=500
            )
            
            return {
                'title': '🎯 Channel Performance',
                'fig': fig,
                'description': 'Performance metrics by channel/platform'
            }
        except Exception as e:
            logger.warning(f"Failed to create channel performance chart: {e}")
            return None
    
    def _create_funnel_chart(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create conversion funnel visualization."""
        try:
            # Aggregate funnel metrics
            total_impressions = float(df[cols['impressions']].sum()) if cols['impressions'] else 0
            total_clicks = float(df[cols['clicks']].sum()) if cols['clicks'] else 0
            total_conversions = float(df[cols['conversions']].sum()) if cols['conversions'] else 0
            
            fig = go.Figure(go.Funnel(
                y=['Impressions', 'Clicks', 'Conversions'],
                x=[total_impressions, total_clicks, total_conversions],
                textinfo='value+percent initial',
                marker=dict(color=self.COLORS['performance'])
            ))
            
            fig.update_layout(
                title='Conversion Funnel',
                template=self.template,
                height=500
            )
            
            return {
                'title': '🔄 Conversion Funnel',
                'fig': fig,
                'description': 'User journey from impressions to conversions'
            }
        except Exception as e:
            logger.warning(f"Failed to create funnel chart: {e}")
            return None
    
    def _create_efficiency_distribution(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create distribution of efficiency metrics."""
        try:
            metric_col = cols['roas'] if cols['roas'] else cols['cpa']
            metric_name = 'ROAS' if cols['roas'] else 'CPA'
            
            fig = px.histogram(
                df,
                x=metric_col,
                nbins=30,
                title=f'{metric_name} Distribution',
                template=self.template,
                color_discrete_sequence=self.COLORS['cool']
            )
            
            fig.update_layout(
                xaxis_title=metric_name,
                yaxis_title='Frequency',
                height=400
            )
            
            return {
                'title': f'📊 {metric_name} Distribution',
                'fig': fig,
                'description': f'Distribution of {metric_name} across campaigns'
            }
        except Exception as e:
            logger.warning(f"Failed to create efficiency distribution chart: {e}")
            return None
    
    def _create_top_bottom_campaigns(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create top and bottom performers chart."""
        try:
            # Sort by conversions or spend
            sort_col = cols['conversions'] if cols['conversions'] else cols['spend']
            df_sorted = df.sort_values(sort_col, ascending=False)
            
            # Get top 5 and bottom 5
            top_5 = df_sorted.head(5)
            bottom_5 = df_sorted.tail(5)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                y=top_5[cols['campaign']].astype(str),
                x=top_5[sort_col],
                name='Top 5',
                orientation='h',
                marker_color=self.COLORS['performance'][0]
            ))
            
            fig.add_trace(go.Bar(
                y=bottom_5[cols['campaign']].astype(str),
                x=bottom_5[sort_col],
                name='Bottom 5',
                orientation='h',
                marker_color=self.COLORS['danger'][0]
            ))
            
            fig.update_layout(
                title='Top & Bottom Performing Campaigns',
                xaxis_title=str(sort_col).replace('_', ' ').title(),
                template=self.template,
                height=500,
                barmode='group'
            )
            
            return {
                'title': '🏆 Top & Bottom Performers',
                'fig': fig,
                'description': 'Best and worst performing campaigns'
            }
        except Exception as e:
            logger.warning(f"Failed to create top/bottom campaigns chart: {e}")
            return None
    
    def _create_comparison_chart(self, df: pd.DataFrame, cols: Dict, query: str) -> Optional[Dict[str, Any]]:
        """Create comparison chart for time period comparisons."""
        try:
            df_sorted = df.sort_values(cols['date'])
            
            fig = go.Figure()
            
            metric_cols = [c for c in [cols['spend'], cols['conversions'], cols['ctr'], cols['roas']] if c]
            
            for metric_col in metric_cols[:3]:  # Limit to 3 metrics
                fig.add_trace(go.Scatter(
                    x=df_sorted[cols['date']],
                    y=df_sorted[metric_col],
                    mode='lines+markers',
                    name=str(metric_col).replace('_', ' ').title(),
                    line=dict(width=3)
                ))
            
            fig.update_layout(
                title='Period Comparison',
                xaxis_title='Date',
                yaxis_title='Value',
                template=self.template,
                height=500
            )
            
            return {
                'title': '📊 Period Comparison',
                'fig': fig,
                'description': 'Comparison of metrics across time periods'
            }
        except Exception as e:
            logger.warning(f"Failed to create comparison chart: {e}")
            return None
    
    def _create_trend_analysis(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create trend analysis chart."""
        return self._create_kpi_trends(df, cols)
    
    def _create_performance_metrics(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create performance metrics overview."""
        try:
            metrics = []
            values = []
            
            if cols['ctr'] and cols['ctr'] in df.columns:
                metrics.append('CTR')
                values.append(float(df[cols['ctr']].mean()))
            if cols['roas'] and cols['roas'] in df.columns:
                metrics.append('ROAS')
                values.append(float(df[cols['roas']].mean()))
            if cols['cpa'] and cols['cpa'] in df.columns:
                metrics.append('CPA')
                values.append(float(df[cols['cpa']].mean()))
            
            if not metrics:
                return None
            
            fig = go.Figure(data=go.Bar(
                x=metrics,
                y=values,
                marker_color=self.COLORS['primary']
            ))
            
            fig.update_layout(
                title='Average Performance Metrics',
                yaxis_title='Average Value',
                template=self.template,
                height=400
            )
            
            return {
                'title': '📈 Performance Metrics',
                'fig': fig,
                'description': 'Average performance across key metrics'
            }
        except Exception as e:
            logger.warning(f"Failed to create performance metrics chart: {e}")
            return None
    
    def _create_channel_breakdown(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create channel breakdown pie chart."""
        try:
            channel_col = cols['channel'] or cols['platform']
            value_col = cols['spend'] if cols['spend'] else cols['conversions']
            
            df_agg = df.groupby(channel_col)[value_col].sum().reset_index()
            
            fig = px.pie(
                df_agg,
                values=value_col,
                names=channel_col,
                title=f'{str(value_col).replace("_", " ").title()} by Channel',
                template=self.template,
                color_discrete_sequence=self.COLORS['primary']
            )
            
            fig.update_layout(height=500)
            
            return {
                'title': '🎯 Channel Breakdown',
                'fig': fig,
                'description': f'Distribution of {value_col} across channels'
            }
        except Exception as e:
            logger.warning(f"Failed to create channel breakdown chart: {e}")
            return None
    
    def _create_default_visualization(self, df: pd.DataFrame, cols: Dict) -> Optional[Dict[str, Any]]:
        """Create a default visualization when query intent is unclear."""
        try:
            # Find numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            
            if len(numeric_cols) >= 2:
                # Create scatter plot of first two numeric columns
                fig = px.scatter(
                    df,
                    x=numeric_cols[0],
                    y=numeric_cols[1],
                    title=f'{numeric_cols[1]} vs {numeric_cols[0]}',
                    template=self.template,
                    color_discrete_sequence=self.COLORS['primary']
                )
            elif len(numeric_cols) == 1:
                # Create bar chart
                fig = px.bar(
                    df,
                    y=numeric_cols[0],
                    title=f'{numeric_cols[0]} Distribution',
                    template=self.template,
                    color_discrete_sequence=self.COLORS['primary']
                )
            else:
                # No numeric data
                return None
            
            fig.update_layout(height=400)
            
            return {
                'title': '📊 Query Results Visualization',
                'fig': fig,
                'description': 'Visual representation of query results'
            }
        except Exception as e:
            logger.warning(f"Failed to create default visualization: {e}")
            return None
