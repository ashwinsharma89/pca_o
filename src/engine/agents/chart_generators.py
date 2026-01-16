"""
Smart Chart Generators
Publication-ready charts with intelligent defaults for digital marketing
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger


class SmartChartGenerator:
    """Generate publication-ready charts with intelligent defaults"""
    
    def __init__(self, brand_colors: Optional[Dict] = None):
        """
        Initialize chart generator
        
        Args:
            brand_colors: Optional custom brand color palette
        """
        self.brand_colors = brand_colors or self._default_brand_colors()
        self.benchmark_color = "#FFA500"  # Orange for benchmarks
        self.anomaly_color = "#D50000"    # Red for anomalies
        logger.info("Initialized Smart Chart Generator")
    
    def _default_brand_colors(self) -> Dict:
        """Default color palette optimized for data viz"""
        return {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e',
            'success': '#2ca02c',
            'warning': '#ffbb28',
            'danger': '#d62728',
            'neutral': '#7f7f7f',
            'channels': {
                'google': '#4285F4',
                'google_search': '#4285F4',
                'google_ads': '#4285F4',
                'meta': '#0668E1',
                'facebook': '#0668E1',
                'instagram': '#E4405F',
                'linkedin': '#0A66C2',
                'snapchat': '#FFFC00',
                'tiktok': '#000000',
                'twitter': '#1DA1F2',
                'pinterest': '#E60023',
                'youtube': '#FF0000',
                'dv360': '#34A853',
                'display': '#34A853'
            },
            'performance': {
                'excellent': '#00C853',
                'good': '#64DD17',
                'average': '#FFD600',
                'poor': '#FF6D00',
                'critical': '#D50000'
            }
        }
    
    def create_channel_comparison_chart(self,
                                       data: Dict[str, Dict],
                                       metrics: List[str],
                                       benchmarks: Optional[Dict] = None) -> go.Figure:
        """
        Smart channel comparison with automatic metric selection
        
        Args:
            data: {
                'google_ads': {'spend': 10000, 'conversions': 200, 'cpa': 50, 'roas': 3.2},
                'linkedin': {'spend': 8000, 'conversions': 150, 'cpa': 53.33, 'roas': 2.8},
                ...
            }
            metrics: ['spend', 'conversions', 'cpa', 'roas']
            benchmarks: Optional benchmark values for each metric
        
        Returns:
            Plotly figure object
        """
        
        channels = list(data.keys())
        num_metrics = min(len(metrics), 4)  # Max 4 subplots
        
        # Create subplots
        rows = 2 if num_metrics > 2 else 1
        cols = 2 if num_metrics > 1 else 1
        
        subplot_titles = [metric.upper().replace('_', ' ') for metric in metrics[:num_metrics]]
        
        fig = make_subplots(
            rows=rows,
            cols=cols,
            subplot_titles=subplot_titles,
            specs=[[{'type': 'bar'} for _ in range(cols)] for _ in range(rows)]
        )
        
        positions = [(1, 1), (1, 2), (2, 1), (2, 2)]
        
        for idx, metric in enumerate(metrics[:num_metrics]):
            row, col = positions[idx]
            
            values = [data[ch].get(metric, 0) for ch in channels]
            colors = [self.brand_colors['channels'].get(ch.lower(), self.brand_colors['primary']) 
                     for ch in channels]
            
            # Add main bars
            fig.add_trace(
                go.Bar(
                    x=channels,
                    y=values,
                    name=metric.upper(),
                    marker_color=colors,
                    text=[f"{v:,.2f}" if v < 100 else f"{v:,.0f}" for v in values],
                    textposition='outside',
                    showlegend=False,
                    hovertemplate='<b>%{x}</b><br>' +
                                 f'{metric.upper()}: %{{y:,.2f}}<br>' +
                                 '<extra></extra>'
                ),
                row=row, col=col
            )
            
            # Add benchmark line if available
            if benchmarks and metric in benchmarks:
                fig.add_hline(
                    y=benchmarks[metric],
                    line_dash="dash",
                    line_color=self.benchmark_color,
                    line_width=2,
                    annotation_text=f"Benchmark: {benchmarks[metric]:,.2f}",
                    annotation_position="right",
                    row=row, col=col
                )
        
        fig.update_layout(
            title_text="Channel Performance Comparison",
            height=600,
            showlegend=False,
            font=dict(size=12),
            hovermode='closest'
        )
        
        return fig
    
    def create_performance_trend_chart(self,
                                      data: Dict,
                                      metrics: List[str],
                                      show_forecast: bool = False,
                                      show_anomalies: bool = True) -> go.Figure:
        """
        Smart multi-line trend chart with anomaly highlighting
        
        Args:
            data: {
                'dates': ['2024-01-01', '2024-01-02', ...],
                'metrics': {
                    'ctr': [0.02, 0.025, 0.022, ...],
                    'cpc': [2.5, 2.3, 2.4, ...],
                    ...
                }
            }
            metrics: List of metrics to display
            show_forecast: Whether to show forecast (future feature)
            show_anomalies: Whether to highlight anomalies
        
        Returns:
            Plotly figure object
        """
        
        fig = go.Figure()
        
        dates = data['dates']
        
        for metric in metrics:
            values = data['metrics'][metric]
            
            # Calculate moving average
            ma_window = min(7, len(values) // 3)  # Adaptive window
            if len(values) >= ma_window:
                ma = np.convolve(values, np.ones(ma_window)/ma_window, mode='valid')
                ma_dates = dates[ma_window-1:]
            
            # Add main line
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                mode='lines+markers',
                name=metric.upper().replace('_', ' '),
                line=dict(width=2),
                marker=dict(size=4),
                hovertemplate='<b>%{x}</b><br>' +
                             f'{metric.upper()}: %{{y:.4f}}<br>' +
                             '<extra></extra>'
            ))
            
            # Add moving average
            if len(values) >= ma_window:
                fig.add_trace(go.Scatter(
                    x=ma_dates,
                    y=ma,
                    mode='lines',
                    name=f'{metric.upper()} ({ma_window}-day MA)',
                    line=dict(width=1, dash='dash'),
                    opacity=0.6,
                    showlegend=True
                ))
            
            # Highlight anomalies (values > 2 std from mean)
            if show_anomalies and len(values) > 10:
                mean = np.mean(values)
                std = np.std(values)
                anomalies_idx = [i for i, v in enumerate(values) 
                               if abs(v - mean) > 2 * std]
                
                if anomalies_idx:
                    fig.add_trace(go.Scatter(
                        x=[dates[i] for i in anomalies_idx],
                        y=[values[i] for i in anomalies_idx],
                        mode='markers',
                        name=f'{metric} Anomalies',
                        marker=dict(
                            size=12,
                            symbol='x',
                            color=self.anomaly_color,
                            line=dict(width=2)
                        ),
                        showlegend=False,
                        hovertemplate='<b>Anomaly</b><br>' +
                                     '%{x}<br>' +
                                     f'{metric.upper()}: %{{y:.4f}}<br>' +
                                     '<extra></extra>'
                    ))
        
        fig.update_layout(
            title="Performance Trends Over Time",
            xaxis_title="Date",
            yaxis_title="Value",
            height=500,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
    
    def create_attribution_sankey(self, touchpoint_data: Dict) -> go.Figure:
        """
        Create customer journey Sankey diagram
        
        Args:
            touchpoint_data: {
                'paths': [
                    {'path': ['Google', 'LinkedIn', 'Conversion'], 'count': 150},
                    {'path': ['Meta', 'Google', 'Conversion'], 'count': 120},
                    ...
                ]
            }
        
        Returns:
            Plotly figure object
        """
        
        # Build nodes and links
        all_nodes = set()
        link_dict = {}
        
        for path_data in touchpoint_data['paths']:
            path = path_data['path']
            count = path_data['count']
            
            for i in range(len(path) - 1):
                source = path[i]
                target = path[i + 1]
                all_nodes.add(source)
                all_nodes.add(target)
                
                key = (source, target)
                link_dict[key] = link_dict.get(key, 0) + count
        
        # Create node list and mapping
        node_list = list(all_nodes)
        node_map = {node: idx for idx, node in enumerate(node_list)}
        
        # Prepare Sankey data
        source_indices = [node_map[source] for source, _ in link_dict.keys()]
        target_indices = [node_map[target] for _, target in link_dict.keys()]
        values = list(link_dict.values())
        
        # Assign colors based on channel
        node_colors = [self.brand_colors['channels'].get(node.lower(), self.brand_colors['neutral']) 
                      for node in node_list]
        
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=node_list,
                color=node_colors,
                hovertemplate='<b>%{label}</b><br>' +
                             'Total: %{value}<br>' +
                             '<extra></extra>'
            ),
            link=dict(
                source=source_indices,
                target=target_indices,
                value=values,
                color='rgba(0,0,0,0.2)',
                hovertemplate='%{source.label} → %{target.label}<br>' +
                             'Count: %{value}<br>' +
                             '<extra></extra>'
            )
        )])
        
        fig.update_layout(
            title="Customer Journey Attribution Flow",
            height=600,
            font_size=12
        )
        
        return fig
    
    def create_performance_gauge(self,
                                actual: float,
                                target: float,
                                metric_name: str,
                                benchmarks: Optional[Dict] = None) -> go.Figure:
        """
        Create gauge chart for single metric performance
        
        Args:
            actual: Current value (e.g., 0.025 for 2.5% CTR)
            target: Target value
            metric_name: Display name
            benchmarks: {'poor': X, 'good': Y, 'excellent': Z}
        
        Returns:
            Plotly figure object
        """
        
        # Auto-set ranges if benchmarks provided
        if benchmarks:
            ranges = [
                benchmarks.get('poor', target * 0.5),
                benchmarks.get('good', target),
                benchmarks.get('excellent', target * 1.5)
            ]
        else:
            ranges = [target * 0.7, target, target * 1.3]
        
        # Ensure ranges are properly ordered
        ranges = sorted(ranges)
        max_range = max(ranges[-1], actual * 1.2)
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=actual,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': metric_name, 'font': {'size': 20}},
            delta={
                'reference': target,
                'relative': True,
                'valueformat': '.1%',
                'increasing': {'color': self.brand_colors['performance']['good']},
                'decreasing': {'color': self.brand_colors['performance']['poor']}
            },
            number={'valueformat': '.2f'},
            gauge={
                'axis': {'range': [None, max_range]},
                'bar': {'color': "darkblue", 'thickness': 0.75},
                'steps': [
                    {'range': [0, ranges[0]], 'color': "#ffcccc"},
                    {'range': [ranges[0], ranges[1]], 'color': "#ffffcc"},
                    {'range': [ranges[1], max_range], 'color': "#ccffcc"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': target
                }
            }
        ))
        
        fig.update_layout(height=300)
        
        return fig
    
    def create_hourly_heatmap(self, data: np.ndarray) -> go.Figure:
        """
        Day/hour heatmap for dayparting analysis
        
        Args:
            data: 2D array [7 days x 24 hours] with performance values
        
        Returns:
            Plotly figure object
        """
        
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        hours = [f"{h:02d}:00" for h in range(24)]
        
        # Ensure data is the right shape
        if isinstance(data, list):
            data = np.array(data)
        
        fig = go.Figure(data=go.Heatmap(
            z=data,
            x=hours,
            y=days,
            colorscale='RdYlGn',
            hoverongaps=False,
            text=[[f"{val:.2%}" for val in row] for row in data],
            texttemplate="%{text}",
            textfont={"size": 8},
            hovertemplate='<b>%{y}</b> at %{x}<br>' +
                         'Conversion Rate: %{z:.2%}<br>' +
                         '<extra></extra>'
        ))
        
        fig.update_layout(
            title="Conversion Rate by Day and Hour",
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            height=400,
            xaxis={'side': 'bottom'}
        )
        
        return fig
    
    def create_keyword_opportunity_scatter(self,
                                          keyword_data: List[Dict],
                                          benchmarks: Optional[Dict] = None) -> go.Figure:
        """
        Bubble chart for keyword opportunity identification
        
        Args:
            keyword_data: List of {
                'keyword': str,
                'impressions': int,
                'conversion_rate': float,
                'spend': float,
                'quality_score': int
            }
            benchmarks: Optional benchmark values
        
        Returns:
            Plotly figure object
        """
        
        keywords = [kw['keyword'] for kw in keyword_data]
        impressions = [kw['impressions'] for kw in keyword_data]
        conv_rates = [kw['conversion_rate'] for kw in keyword_data]
        spends = [kw['spend'] for kw in keyword_data]
        quality_scores = [kw.get('quality_score', 5) for kw in keyword_data]
        
        # Normalize bubble sizes
        max_spend = max(spends) if spends else 1
        bubble_sizes = [max(10, min(60, (s / max_spend) * 60)) for s in spends]
        
        fig = go.Figure(data=go.Scatter(
            x=impressions,
            y=conv_rates,
            mode='markers',
            marker=dict(
                size=bubble_sizes,
                color=quality_scores,
                colorscale='RdYlGn',
                showscale=True,
                colorbar=dict(title="Quality<br>Score"),
                line=dict(width=0.5, color='white'),
                cmin=1,
                cmax=10
            ),
            text=keywords,
            customdata=spends,
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Impressions: %{x:,.0f}<br>' +
                'Conv Rate: %{y:.2%}<br>' +
                'Spend: $%{customdata:,.2f}<br>' +
                '<extra></extra>'
        ))
        
        # Add quadrant lines (median splits)
        if len(impressions) > 4:
            median_impressions = np.median(impressions)
            median_conv_rate = np.median(conv_rates)
            
            fig.add_vline(x=median_impressions, line_dash="dash", line_color="gray", opacity=0.5)
            fig.add_hline(y=median_conv_rate, line_dash="dash", line_color="gray", opacity=0.5)
            
            # Add annotations for quadrants
            max_impr = max(impressions)
            max_cvr = max(conv_rates)
            
            fig.add_annotation(
                x=median_impressions + (max_impr - median_impressions) * 0.5,
                y=median_conv_rate + (max_cvr - median_conv_rate) * 0.5,
                text="Stars ⭐<br>(High volume, High CVR)",
                showarrow=False,
                font=dict(size=10, color="green"),
                bgcolor="rgba(255,255,255,0.8)"
            )
            
            fig.add_annotation(
                x=median_impressions * 0.5,
                y=median_conv_rate + (max_cvr - median_conv_rate) * 0.5,
                text="Niche Winners 🎯<br>(Low volume, High CVR)",
                showarrow=False,
                font=dict(size=10, color="blue"),
                bgcolor="rgba(255,255,255,0.8)"
            )
            
            fig.add_annotation(
                x=median_impressions + (max_impr - median_impressions) * 0.5,
                y=median_conv_rate * 0.5,
                text="Opportunities 💡<br>(High volume, Low CVR)",
                showarrow=False,
                font=dict(size=10, color="orange"),
                bgcolor="rgba(255,255,255,0.8)"
            )
            
            fig.add_annotation(
                x=median_impressions * 0.5,
                y=median_conv_rate * 0.5,
                text="Underperformers ⚠️<br>(Low volume, Low CVR)",
                showarrow=False,
                font=dict(size=10, color="red"),
                bgcolor="rgba(255,255,255,0.8)"
            )
        
        fig.update_layout(
            title="Keyword Performance Matrix",
            xaxis_title="Impressions (Volume)",
            yaxis_title="Conversion Rate",
            xaxis_type="log" if max(impressions) / min(impressions) > 100 else "linear",
            height=600,
            hovermode='closest'
        )
        
        return fig
    
    def create_budget_treemap(self, budget_data: Dict) -> go.Figure:
        """
        Hierarchical treemap for budget allocation
        
        Args:
            budget_data: {
                'labels': ['Google', 'Google-Campaign1', 'Google-Campaign2', 'LinkedIn', ...],
                'parents': ['', 'Google', 'Google', '', ...],
                'values': [50000, 30000, 20000, 30000, ...],  # Spend
                'performance': [2.5, 3.0, 2.0, 1.8, ...]  # ROAS
            }
        
        Returns:
            Plotly figure object
        """
        
        fig = go.Figure(go.Treemap(
            labels=budget_data['labels'],
            parents=budget_data['parents'],
            values=budget_data['values'],
            marker=dict(
                colors=budget_data['performance'],
                colorscale='RdYlGn',
                cmid=2.0,  # Neutral ROAS
                colorbar=dict(title="ROAS"),
                line=dict(width=2, color='white')
            ),
            text=[f"${v:,.0f}<br>ROAS: {p:.2f}" 
                  for v, p in zip(budget_data['values'], budget_data['performance'])],
            textposition='middle center',
            textfont=dict(size=12),
            hovertemplate=
                '<b>%{label}</b><br>' +
                'Spend: $%{value:,.0f}<br>' +
                'ROAS: %{color:.2f}<br>' +
                '<extra></extra>'
        ))
        
        fig.update_layout(
            title="Budget Allocation by Channel & Campaign<br><sub>(sized by spend, colored by ROAS)</sub>",
            height=600
        )
        
        return fig
    
    def create_conversion_funnel(self,
                                funnel_data: Dict,
                                show_percentages: bool = True) -> go.Figure:
        """
        Create conversion funnel chart
        
        Args:
            funnel_data: {
                'stages': ['Impressions', 'Clicks', 'Leads', 'Conversions'],
                'values': [100000, 5000, 500, 100]
            }
            show_percentages: Whether to show drop-off percentages
        
        Returns:
            Plotly figure object
        """
        
        stages = funnel_data['stages']
        values = funnel_data['values']
        
        # Calculate percentages
        percentages = [100]
        for i in range(1, len(values)):
            pct = (values[i] / values[0]) * 100 if values[0] > 0 else 0
            percentages.append(pct)
        
        # Calculate drop-off rates
        drop_offs = []
        for i in range(1, len(values)):
            drop = ((values[i-1] - values[i]) / values[i-1]) * 100 if values[i-1] > 0 else 0
            drop_offs.append(drop)
        
        fig = go.Figure(go.Funnel(
            y=stages,
            x=values,
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(
                color=['#4285F4', '#34A853', '#FBBC04', '#EA4335'][:len(stages)]
            ),
            hovertemplate='<b>%{y}</b><br>' +
                         'Count: %{x:,.0f}<br>' +
                         'Percentage: %{percentInitial:.1%}<br>' +
                         '<extra></extra>'
        ))
        
        # Add drop-off annotations
        if show_percentages and len(drop_offs) > 0:
            for i, drop in enumerate(drop_offs):
                fig.add_annotation(
                    x=values[i+1],
                    y=i + 0.5,
                    text=f"↓ {drop:.1f}%",
                    showarrow=False,
                    font=dict(size=10, color="red"),
                    xshift=50
                )
        
        fig.update_layout(
            title="Conversion Funnel Analysis",
            height=500
        )
        
        return fig
    
    def create_frequency_histogram(self,
                                   frequency_data: List[float],
                                   optimal_range: Optional[Tuple[float, float]] = None) -> go.Figure:
        """
        Create frequency distribution histogram
        
        Args:
            frequency_data: List of frequency values
            optimal_range: Optional tuple of (min, max) for optimal frequency
        
        Returns:
            Plotly figure object
        """
        
        fig = go.Figure(data=[go.Histogram(
            x=frequency_data,
            nbinsx=20,
            marker_color=self.brand_colors['primary'],
            opacity=0.7,
            hovertemplate='Frequency: %{x:.1f}<br>' +
                         'Count: %{y}<br>' +
                         '<extra></extra>'
        )])
        
        # Add optimal range if provided
        if optimal_range:
            fig.add_vrect(
                x0=optimal_range[0],
                x1=optimal_range[1],
                fillcolor="green",
                opacity=0.2,
                layer="below",
                line_width=0,
                annotation_text="Optimal Range",
                annotation_position="top left"
            )
        
        # Add mean and median lines
        mean_freq = np.mean(frequency_data)
        median_freq = np.median(frequency_data)
        
        fig.add_vline(
            x=mean_freq,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Mean: {mean_freq:.1f}",
            annotation_position="top"
        )
        
        fig.add_vline(
            x=median_freq,
            line_dash="dot",
            line_color="blue",
            annotation_text=f"Median: {median_freq:.1f}",
            annotation_position="bottom"
        )
        
        fig.update_layout(
            title="Frequency Distribution Analysis",
            xaxis_title="Frequency",
            yaxis_title="Count",
            height=400,
            showlegend=False
        )
        
        return fig
    
    def create_device_donut(self, device_data: Dict) -> go.Figure:
        """
        Create device breakdown donut chart
        
        Args:
            device_data: {
                'devices': ['Desktop', 'Mobile', 'Tablet'],
                'values': [5000, 8000, 2000]
            }
        
        Returns:
            Plotly figure object
        """
        
        devices = device_data['devices']
        values = device_data['values']
        
        # Device-specific colors
        device_colors = {
            'desktop': '#5E35B1',
            'mobile': '#00897B',
            'tablet': '#FB8C00'
        }
        
        colors = [device_colors.get(d.lower(), self.brand_colors['neutral']) for d in devices]
        
        fig = go.Figure(data=[go.Pie(
            labels=devices,
            values=values,
            hole=0.4,
            marker=dict(colors=colors, line=dict(color='white', width=2)),
            textinfo='label+percent',
            textposition='outside',
            hovertemplate='<b>%{label}</b><br>' +
                         'Conversions: %{value:,.0f}<br>' +
                         'Percentage: %{percent}<br>' +
                         '<extra></extra>'
        )])
        
        # Add center annotation
        total = sum(values)
        fig.add_annotation(
            text=f"Total<br>{total:,.0f}",
            x=0.5,
            y=0.5,
            font_size=16,
            showarrow=False
        )
        
        fig.update_layout(
            title="Conversions by Device Type",
            height=500,
            showlegend=True
        )
        
        return fig
