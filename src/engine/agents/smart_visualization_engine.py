"""
Smart Visualization Engine
Intelligently selects the best visualization type based on data characteristics,
insight type, audience, and context.
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from loguru import logger


class VisualizationType(Enum):
    """All available visualization types"""
    # Comparison charts
    BAR_CHART = "bar"
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"
    HORIZONTAL_BAR = "horizontal_bar"
    
    # Trend charts
    LINE_CHART = "line"
    AREA_CHART = "area"
    MULTI_LINE = "multi_line"
    STACKED_AREA = "stacked_area"
    
    # Part-to-whole
    PIE_CHART = "pie"
    DONUT_CHART = "donut"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    
    # Distribution
    HISTOGRAM = "histogram"
    BOX_PLOT = "box"
    VIOLIN_PLOT = "violin"
    
    # Relationship
    SCATTER_PLOT = "scatter"
    BUBBLE_CHART = "bubble"
    HEATMAP = "heatmap"
    
    # Flow/Journey
    SANKEY = "sankey"
    FUNNEL = "funnel"
    
    # Performance
    GAUGE = "gauge"
    BULLET_CHART = "bullet"
    WATERFALL = "waterfall"
    
    # Special
    SPARKLINE = "sparkline"
    SMALL_MULTIPLE = "small_multiple"
    KPI_CARD = "kpi_card"


class InsightType(Enum):
    """Types of insights to visualize"""
    COMPARISON = "comparison"
    TREND = "trend"
    COMPOSITION = "composition"
    DISTRIBUTION = "distribution"
    RELATIONSHIP = "relationship"
    PERFORMANCE = "performance"
    JOURNEY = "journey"
    RANKING = "ranking"


class SmartVisualizationEngine:
    """
    Intelligent visualization selection based on:
    1. Data characteristics (cardinality, distribution, relationship)
    2. Insight type (comparison, trend, composition, distribution)
    3. Audience (executive vs analyst)
    4. Context (B2B vs B2C, channel type)
    """
    
    def __init__(self):
        """Initialize the visualization engine"""
        self.decision_tree = self._build_decision_tree()
        logger.info("Initialized Smart Visualization Engine")
    
    def select_visualization(self, 
                           data: Any,
                           insight_type: str,
                           audience: str = "analyst",
                           context: Optional[Dict] = None) -> VisualizationType:
        """
        Intelligently select the best visualization
        
        Args:
            data: The data to visualize (DataFrame, dict, or list)
            insight_type: 'comparison', 'trend', 'composition', 'distribution', 
                         'relationship', 'performance', 'journey', 'ranking'
            audience: 'executive' or 'analyst'
            context: Additional context (B2B/B2C, channel, etc.)
        
        Returns:
            Optimal visualization type
        """
        
        # Analyze data characteristics
        data_profile = self._profile_data(data)
        
        logger.info(f"Selecting visualization for {insight_type} insight")
        logger.debug(f"Data profile: {data_profile}")
        
        # Apply decision logic
        viz_type = self._apply_decision_tree(
            insight_type=insight_type,
            data_profile=data_profile,
            audience=audience,
            context=context or {}
        )
        
        logger.info(f"Selected visualization: {viz_type.value}")
        return viz_type
    
    def create_visualization(self,
                           data: Any,
                           viz_type: VisualizationType,
                           title: str = "",
                           **kwargs) -> go.Figure:
        """
        Create the actual visualization
        
        Args:
            data: Data to visualize
            viz_type: Type of visualization to create
            title: Chart title
            **kwargs: Additional parameters for the chart
            
        Returns:
            Plotly figure object
        """
        
        # Map visualization types to creation methods
        viz_creators = {
            VisualizationType.BAR_CHART: self._create_bar_chart,
            VisualizationType.GROUPED_BAR: self._create_grouped_bar,
            VisualizationType.HORIZONTAL_BAR: self._create_horizontal_bar,
            VisualizationType.LINE_CHART: self._create_line_chart,
            VisualizationType.MULTI_LINE: self._create_multi_line,
            VisualizationType.AREA_CHART: self._create_area_chart,
            VisualizationType.DONUT_CHART: self._create_donut_chart,
            VisualizationType.TREEMAP: self._create_treemap,
            VisualizationType.HEATMAP: self._create_heatmap,
            VisualizationType.SCATTER_PLOT: self._create_scatter,
            VisualizationType.GAUGE: self._create_gauge,
            VisualizationType.WATERFALL: self._create_waterfall,
            VisualizationType.FUNNEL: self._create_funnel,
            VisualizationType.KPI_CARD: self._create_kpi_card,
        }
        
        creator = viz_creators.get(viz_type, self._create_bar_chart)
        fig = creator(data, title, **kwargs)
        
        return fig
    
    def _profile_data(self, data: Any) -> Dict:
        """Analyze data characteristics"""
        
        profile = {
            'cardinality': 0,
            'has_time_series': False,
            'has_categories': False,
            'num_metrics': 1,
            'has_hierarchy': False,
            'data_type': 'standard',
            'num_dimensions': 1,
            'has_nulls': False,
            'value_range': (0, 0)
        }
        
        # Handle DataFrame
        if isinstance(data, pd.DataFrame):
            profile['cardinality'] = len(data)
            profile['num_metrics'] = len([col for col in data.columns if data[col].dtype in ['int64', 'float64']])
            profile['has_time_series'] = any('date' in col.lower() or 'time' in col.lower() for col in data.columns)
            profile['has_categories'] = any(data[col].dtype == 'object' for col in data.columns)
            profile['has_nulls'] = data.isnull().any().any()
            
            # Get value range for numeric columns
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                profile['value_range'] = (
                    float(data[numeric_cols].min().min()),
                    float(data[numeric_cols].max().max())
                )
        
        # Handle dictionary
        elif isinstance(data, dict):
            if 'values' in data:
                profile['cardinality'] = len(data['values'])
            elif 'categories' in data:
                profile['cardinality'] = len(data['categories'])
            
            profile['has_time_series'] = self._detect_time_series(data)
            profile['has_categories'] = self._detect_categories(data)
            profile['num_metrics'] = self._count_metrics(data)
            profile['has_hierarchy'] = self._detect_hierarchy(data)
            profile['data_type'] = self._classify_data_type(data)
        
        # Handle list
        elif isinstance(data, list):
            profile['cardinality'] = len(data)
            if data and isinstance(data[0], dict):
                profile['num_metrics'] = len(data[0])
        
        return profile
    
    def _apply_decision_tree(self, 
                            insight_type: str,
                            data_profile: Dict,
                            audience: str,
                            context: Dict) -> VisualizationType:
        """Apply decision rules to select visualization"""
        
        # COMPARISON INSIGHTS
        if insight_type == "comparison":
            return self._select_comparison_viz(data_profile, audience)
        
        # TREND INSIGHTS
        elif insight_type == "trend":
            return self._select_trend_viz(data_profile, audience)
        
        # COMPOSITION INSIGHTS (part-to-whole)
        elif insight_type == "composition":
            return self._select_composition_viz(data_profile, audience)
        
        # DISTRIBUTION INSIGHTS
        elif insight_type == "distribution":
            return self._select_distribution_viz(data_profile, audience)
        
        # RELATIONSHIP INSIGHTS (correlation)
        elif insight_type == "relationship":
            return self._select_relationship_viz(data_profile, audience)
        
        # PERFORMANCE INSIGHTS (vs target/benchmark)
        elif insight_type == "performance":
            return self._select_performance_viz(data_profile, audience, context)
        
        # JOURNEY INSIGHTS (attribution, funnel)
        elif insight_type == "journey":
            return self._select_journey_viz(data_profile, audience)
        
        # RANKING INSIGHTS
        elif insight_type == "ranking":
            return self._select_ranking_viz(data_profile, audience)
        
        # Default fallback
        return VisualizationType.BAR_CHART
    
    def _select_comparison_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for comparing values"""
        
        cardinality = data_profile['cardinality']
        num_metrics = data_profile['num_metrics']
        
        # Single metric, few categories (2-10) → Simple bar chart
        if num_metrics == 1 and 2 <= cardinality <= 10:
            return VisualizationType.BAR_CHART
        
        # Single metric, many categories (>10) → Horizontal bar
        elif num_metrics == 1 and cardinality > 10:
            if audience == "executive":
                return VisualizationType.BAR_CHART  # Show top 10
            else:
                return VisualizationType.HORIZONTAL_BAR
        
        # Multiple metrics, few categories → Grouped bar
        elif num_metrics > 1 and cardinality <= 7:
            return VisualizationType.GROUPED_BAR
        
        # Multiple metrics, many categories → Heatmap
        elif num_metrics > 1 and cardinality > 7:
            return VisualizationType.HEATMAP
        
        # Hierarchical data → Treemap
        elif data_profile['has_hierarchy']:
            return VisualizationType.TREEMAP
        
        return VisualizationType.BAR_CHART
    
    def _select_trend_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for showing trends over time"""
        
        num_metrics = data_profile['num_metrics']
        
        # Single metric over time → Line chart
        if num_metrics == 1:
            return VisualizationType.LINE_CHART
        
        # Multiple metrics over time (2-4) → Multi-line chart
        elif 2 <= num_metrics <= 4:
            return VisualizationType.MULTI_LINE
        
        # Many metrics → Small multiples
        elif num_metrics > 4:
            if audience == "executive":
                return VisualizationType.MULTI_LINE  # Top 4 only
            else:
                return VisualizationType.SMALL_MULTIPLE
        
        return VisualizationType.LINE_CHART
    
    def _select_composition_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for part-to-whole relationships"""
        
        cardinality = data_profile['cardinality']
        
        # Few categories (2-5) → Donut chart
        if 2 <= cardinality <= 5:
            return VisualizationType.DONUT_CHART
        
        # Medium categories (6-10) → Treemap
        elif 6 <= cardinality <= 10:
            return VisualizationType.TREEMAP
        
        # Many categories or hierarchical → Sunburst
        elif cardinality > 10 or data_profile['has_hierarchy']:
            if audience == "executive":
                return VisualizationType.DONUT_CHART  # Top 5 + Other
            else:
                return VisualizationType.SUNBURST
        
        return VisualizationType.TREEMAP
    
    def _select_distribution_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for showing distribution"""
        
        cardinality = data_profile['cardinality']
        
        # Large dataset → Histogram
        if cardinality > 50:
            return VisualizationType.HISTOGRAM
        
        # Medium dataset → Box plot
        elif cardinality > 20:
            return VisualizationType.BOX_PLOT
        
        # Small dataset → Violin plot (shows more detail)
        else:
            if audience == "executive":
                return VisualizationType.BOX_PLOT
            else:
                return VisualizationType.VIOLIN_PLOT
    
    def _select_relationship_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for showing relationships"""
        
        num_metrics = data_profile['num_metrics']
        
        # Two variables → Scatter plot
        if num_metrics == 2:
            return VisualizationType.SCATTER_PLOT
        
        # Three variables → Bubble chart
        elif num_metrics == 3:
            return VisualizationType.BUBBLE_CHART
        
        # Many variables → Heatmap (correlation matrix)
        elif num_metrics > 3:
            return VisualizationType.HEATMAP
        
        return VisualizationType.SCATTER_PLOT
    
    def _select_performance_viz(self, data_profile: Dict, audience: str, context: Dict) -> VisualizationType:
        """Select best chart for performance vs benchmark/target"""
        
        num_metrics = data_profile['num_metrics']
        
        # Single metric vs target → Gauge or bullet chart
        if num_metrics == 1:
            if audience == "executive":
                return VisualizationType.GAUGE
            else:
                return VisualizationType.BULLET_CHART
        
        # Multiple metrics vs benchmarks → Grouped bar
        elif num_metrics <= 5:
            return VisualizationType.GROUPED_BAR
        
        # Variance analysis → Waterfall chart
        elif context.get('show_variance'):
            return VisualizationType.WATERFALL
        
        return VisualizationType.BULLET_CHART
    
    def _select_journey_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for customer journey/attribution"""
        
        data_type = data_profile['data_type']
        
        # Multi-touch attribution → Sankey diagram
        if data_type == 'flow':
            return VisualizationType.SANKEY
        
        # Conversion funnel → Funnel chart
        elif data_type == 'funnel':
            return VisualizationType.FUNNEL
        
        return VisualizationType.FUNNEL
    
    def _select_ranking_viz(self, data_profile: Dict, audience: str) -> VisualizationType:
        """Select best chart for ranking/leaderboard"""
        
        cardinality = data_profile['cardinality']
        
        # Few items → Horizontal bar (easier to read labels)
        if cardinality <= 15:
            return VisualizationType.HORIZONTAL_BAR
        
        # Many items → Show top 10 in horizontal bar
        else:
            return VisualizationType.HORIZONTAL_BAR
    
    # Helper methods for data profiling
    def _detect_time_series(self, data: Dict) -> bool:
        """Check if data has time component"""
        time_keywords = ['date', 'timestamp', 'time', 'period', 'month', 'year', 'day']
        return any(keyword in str(data).lower() for keyword in time_keywords)
    
    def _detect_categories(self, data: Dict) -> bool:
        """Check if data has categorical dimensions"""
        return 'categories' in data or 'labels' in data or 'names' in data
    
    def _count_metrics(self, data: Dict) -> int:
        """Count number of metrics being visualized"""
        if 'metrics' in data:
            return len(data['metrics'])
        elif 'values' in data and isinstance(data['values'], list):
            if data['values'] and isinstance(data['values'][0], dict):
                return len(data['values'][0])
        return 1
    
    def _detect_hierarchy(self, data: Dict) -> bool:
        """Check if data has hierarchical structure"""
        hierarchy_keywords = ['hierarchy', 'parent', 'children', 'level']
        return any(keyword in data for keyword in hierarchy_keywords)
    
    def _classify_data_type(self, data: Dict) -> str:
        """Classify the type of data"""
        data_str = str(data).lower()
        
        if ('source' in data and 'target' in data) or 'flow' in data_str:
            return 'flow'
        elif 'funnel' in data_str or 'stage' in data:
            return 'funnel'
        elif 'hierarchy' in data_str or 'parent' in data:
            return 'hierarchical'
        
        return 'standard'
    
    def _build_decision_tree(self) -> Dict:
        """Build the decision tree for visualization selection"""
        return {
            'comparison': {
                'few_categories': VisualizationType.BAR_CHART,
                'many_categories': VisualizationType.HORIZONTAL_BAR,
                'multiple_metrics': VisualizationType.GROUPED_BAR,
                'hierarchical': VisualizationType.TREEMAP
            },
            'trend': {
                'single_metric': VisualizationType.LINE_CHART,
                'multiple_metrics': VisualizationType.MULTI_LINE,
                'cumulative': VisualizationType.AREA_CHART
            },
            'composition': {
                'few_parts': VisualizationType.DONUT_CHART,
                'many_parts': VisualizationType.TREEMAP,
                'hierarchical': VisualizationType.SUNBURST
            },
            'performance': {
                'single_kpi': VisualizationType.GAUGE,
                'multiple_kpis': VisualizationType.BULLET_CHART,
                'variance': VisualizationType.WATERFALL
            }
        }
    
    # Visualization creation methods
    def _create_bar_chart(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a bar chart"""
        if isinstance(data, pd.DataFrame):
            x_col = kwargs.get('x', data.columns[0])
            y_col = kwargs.get('y', data.columns[1])
            
            fig = px.bar(data, x=x_col, y=y_col, title=title)
        else:
            fig = go.Figure(data=[go.Bar(
                x=data.get('categories', []),
                y=data.get('values', [])
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_grouped_bar(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a grouped bar chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.bar(data, x=data.columns[0], y=data.columns[1:], 
                        title=title, barmode='group')
        else:
            fig = go.Figure()
            fig.update_layout(title=title, barmode='group')
        
        return fig
    
    def _create_horizontal_bar(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a horizontal bar chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.bar(data, y=data.columns[0], x=data.columns[1],
                        title=title, orientation='h')
        else:
            fig = go.Figure(data=[go.Bar(
                y=data.get('categories', []),
                x=data.get('values', []),
                orientation='h'
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_line_chart(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a line chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.line(data, x=data.columns[0], y=data.columns[1], title=title)
        else:
            fig = go.Figure(data=[go.Scatter(
                x=data.get('x', []),
                y=data.get('y', []),
                mode='lines'
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_multi_line(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a multi-line chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.line(data, x=data.columns[0], y=data.columns[1:], title=title)
        else:
            fig = go.Figure()
            fig.update_layout(title=title)
        
        return fig
    
    def _create_area_chart(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create an area chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.area(data, x=data.columns[0], y=data.columns[1], title=title)
        else:
            fig = go.Figure(data=[go.Scatter(
                x=data.get('x', []),
                y=data.get('y', []),
                fill='tozeroy'
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_donut_chart(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a donut chart"""
        if isinstance(data, pd.DataFrame):
            fig = px.pie(data, names=data.columns[0], values=data.columns[1],
                        title=title, hole=0.4)
        else:
            fig = go.Figure(data=[go.Pie(
                labels=data.get('labels', []),
                values=data.get('values', []),
                hole=0.4
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_treemap(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a treemap"""
        if isinstance(data, pd.DataFrame):
            fig = px.treemap(data, path=[data.columns[0]], values=data.columns[1],
                           title=title)
        else:
            fig = go.Figure()
            fig.update_layout(title=title)
        
        return fig
    
    def _create_heatmap(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a heatmap"""
        if isinstance(data, pd.DataFrame):
            fig = px.imshow(data.select_dtypes(include=[np.number]),
                          title=title, aspect='auto')
        else:
            fig = go.Figure()
            fig.update_layout(title=title)
        
        return fig
    
    def _create_scatter(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a scatter plot"""
        if isinstance(data, pd.DataFrame):
            fig = px.scatter(data, x=data.columns[0], y=data.columns[1], title=title)
        else:
            fig = go.Figure(data=[go.Scatter(
                x=data.get('x', []),
                y=data.get('y', []),
                mode='markers'
            )])
            fig.update_layout(title=title)
        
        return fig
    
    def _create_gauge(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a gauge chart"""
        value = data.get('value', 0) if isinstance(data, dict) else 0
        target = data.get('target', 100) if isinstance(data, dict) else 100
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title},
            delta={'reference': target},
            gauge={
                'axis': {'range': [None, target * 1.2]},
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': target
                }
            }
        ))
        
        return fig
    
    def _create_waterfall(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a waterfall chart"""
        if isinstance(data, pd.DataFrame):
            fig = go.Figure(go.Waterfall(
                x=data.iloc[:, 0],
                y=data.iloc[:, 1],
                text=data.iloc[:, 1]
            ))
        else:
            fig = go.Figure()
        
        fig.update_layout(title=title)
        return fig
    
    def _create_funnel(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a funnel chart"""
        if isinstance(data, pd.DataFrame):
            fig = go.Figure(go.Funnel(
                y=data.iloc[:, 0],
                x=data.iloc[:, 1]
            ))
        else:
            fig = go.Figure(go.Funnel(
                y=data.get('stages', []),
                x=data.get('values', [])
            ))
        
        fig.update_layout(title=title)
        return fig
    
    def _create_kpi_card(self, data: Any, title: str, **kwargs) -> go.Figure:
        """Create a KPI card (indicator)"""
        value = data.get('value', 0) if isinstance(data, dict) else 0
        delta = data.get('delta', 0) if isinstance(data, dict) else 0
        
        fig = go.Figure(go.Indicator(
            mode="number+delta",
            value=value,
            delta={'reference': value - delta, 'relative': True},
            title={'text': title}
        ))
        
        return fig
