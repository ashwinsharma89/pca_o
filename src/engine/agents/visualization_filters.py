"""
Intelligent Filter System for Smart Visualizations
Provides smart filtering capabilities with automatic suggestions and impact analysis
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger


class FilterType(Enum):
    """Types of filters available"""
    # Temporal filters
    DATE_RANGE = "date_range"
    DATE_PRESET = "date_preset"  # Last 7 days, Last 30 days, etc.
    DATE_COMPARISON = "date_comparison"  # Compare periods
    
    # Dimensional filters
    CHANNEL = "channel"
    CAMPAIGN = "campaign"
    AD_GROUP = "ad_group"
    CREATIVE = "creative"
    DEVICE = "device"
    GEOGRAPHY = "geography"
    AUDIENCE = "audience"
    
    # Performance filters
    METRIC_THRESHOLD = "metric_threshold"  # CTR > 2%, Spend > $1000
    PERFORMANCE_TIER = "performance_tier"  # Top/Middle/Bottom performers
    BENCHMARK_RELATIVE = "benchmark_relative"  # Above/Below benchmark
    
    # Business context filters
    BUSINESS_MODEL = "business_model"  # B2B/B2C
    FUNNEL_STAGE = "funnel_stage"  # Awareness/Consideration/Conversion
    OBJECTIVE = "objective"  # Brand/Performance
    
    # Advanced filters
    STATISTICAL = "statistical"  # Statistically significant only
    ANOMALY = "anomaly"  # Show only anomalies
    CUSTOM = "custom"  # User-defined filter logic


class FilterCondition(Enum):
    """Filter condition operators"""
    EQUALS = "=="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class SmartFilterEngine:
    """
    Intelligent filter engine that:
    1. Suggests relevant filters based on data characteristics
    2. Applies filters efficiently
    3. Maintains filter state across visualizations
    4. Provides filter recommendations
    """
    
    def __init__(self):
        """Initialize the smart filter engine"""
        self.active_filters = {}
        self.filter_history = []
        self.filter_suggestions = {}
        logger.info("Initialized Smart Filter Engine")
    
    def suggest_filters_for_data(self, 
                                 data: pd.DataFrame,
                                 context: Optional[Dict] = None) -> List[Dict]:
        """
        Analyze data and suggest relevant filters
        
        Args:
            data: Campaign data DataFrame
            context: Campaign context (business model, objective, etc.)
        
        Returns:
            List of suggested filter configurations
        """
        
        if context is None:
            context = {}
        
        suggestions = []
        
        logger.info(f"Analyzing data for filter suggestions: {len(data)} rows, {len(data.columns)} columns")
        
        # 1. DATE FILTERS (always relevant)
        date_col = self._get_date_column(data)
        if date_col:
            data[date_col] = pd.to_datetime(data[date_col], errors='coerce')
            date_range = (data[date_col].min(), data[date_col].max())
            days_span = (date_range[1] - date_range[0]).days
            
            suggestions.append({
                'type': FilterType.DATE_PRESET,
                'label': 'Time Period',
                'options': self._generate_date_presets(days_span),
                'default': 'last_30_days',
                'priority': 'high',
                'reasoning': 'Time-based analysis is fundamental for trend detection'
            })
            
            # Suggest comparison if enough data
            if days_span >= 60:
                suggestions.append({
                    'type': FilterType.DATE_COMPARISON,
                    'label': 'Compare Periods',
                    'options': ['week_over_week', 'month_over_month', 'year_over_year'],
                    'default': None,
                    'priority': 'medium',
                    'reasoning': 'Sufficient historical data for period comparison'
                })
        
        # 2. CHANNEL FILTERS
        channel_col = self._get_column(data, ['channel', 'platform', 'source'])
        if channel_col:
            channels = data[channel_col].dropna().unique().tolist()
            
            if len(channels) > 1:
                suggestions.append({
                    'type': FilterType.CHANNEL,
                    'label': 'Channel',
                    'column': channel_col,
                    'options': channels,
                    'default': 'all',
                    'multi_select': True,
                    'priority': 'high',
                    'reasoning': f'Multiple channels detected ({len(channels)}). Filter to focus analysis.'
                })
        
        # 3. PERFORMANCE TIER FILTERS
        conv_col = self._get_column(data, ['conversions', 'conv', 'leads'])
        revenue_col = self._get_column(data, ['revenue', 'sales'])
        
        if conv_col or revenue_col:
            metric = conv_col if conv_col else revenue_col
            suggestions.append({
                'type': FilterType.PERFORMANCE_TIER,
                'label': 'Performance Tier',
                'metric': metric,
                'options': [
                    {'value': 'top', 'label': 'Top Performers (Top 20%)', 'icon': '⭐'},
                    {'value': 'middle', 'label': 'Middle Performers (21-80%)', 'icon': '➡️'},
                    {'value': 'bottom', 'label': 'Bottom Performers (Bottom 20%)', 'icon': '⚠️'}
                ],
                'default': 'all',
                'priority': 'medium',
                'reasoning': 'Identify and analyze performance patterns by tier'
            })
        
        # 4. DEVICE FILTERS
        device_col = self._get_column(data, ['device', 'device_type'])
        if device_col:
            devices = data[device_col].dropna().unique().tolist()
            if len(devices) > 1:
                suggestions.append({
                    'type': FilterType.DEVICE,
                    'label': 'Device Type',
                    'column': device_col,
                    'options': devices,
                    'default': 'all',
                    'multi_select': True,
                    'priority': 'low',
                    'reasoning': 'Device performance often varies significantly'
                })
        
        # 5. METRIC THRESHOLD FILTERS
        numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
        key_metrics = ['ctr', 'cpc', 'cpa', 'roas', 'conversion_rate', 'spend']
        available_metrics = [col for col in key_metrics if col.lower() in [c.lower() for c in numeric_columns]]
        
        if available_metrics:
            suggestions.append({
                'type': FilterType.METRIC_THRESHOLD,
                'label': 'Metric Filters',
                'metrics': available_metrics,
                'default': None,
                'priority': 'medium',
                'reasoning': 'Filter by performance thresholds to focus on specific segments'
            })
        
        # 6. BENCHMARK RELATIVE FILTERS
        if context.get('benchmarks'):
            suggestions.append({
                'type': FilterType.BENCHMARK_RELATIVE,
                'label': 'Performance vs Benchmark',
                'options': [
                    {'value': 'above', 'label': 'Above Benchmark', 'icon': '📈'},
                    {'value': 'below', 'label': 'Below Benchmark', 'icon': '📉'},
                    {'value': 'at', 'label': 'At Benchmark (±10%)', 'icon': '➡️'}
                ],
                'default': 'all',
                'priority': 'high',
                'reasoning': 'Benchmarks available - filter by relative performance'
            })
        
        # 7. STATISTICAL SIGNIFICANCE FILTER (if A/B test data)
        if context.get('has_ab_test') or 'variant' in data.columns or 'test_group' in data.columns:
            suggestions.append({
                'type': FilterType.STATISTICAL,
                'label': 'Statistical Significance',
                'options': [
                    {'value': 'significant', 'label': 'Statistically Significant Only (p<0.05)'},
                    {'value': 'all', 'label': 'All Results'}
                ],
                'default': 'all',
                'priority': 'high',
                'reasoning': 'A/B test detected - filter for significant results'
            })
        
        # 8. ANOMALY FILTER
        if self._has_sufficient_data_for_anomaly_detection(data):
            suggestions.append({
                'type': FilterType.ANOMALY,
                'label': 'Show Anomalies',
                'options': [
                    {'value': 'anomalies_only', 'label': 'Anomalies Only'},
                    {'value': 'normal_only', 'label': 'Normal Data Only'},
                    {'value': 'all', 'label': 'All Data'}
                ],
                'default': 'all',
                'priority': 'low',
                'reasoning': 'Sufficient historical data for anomaly detection'
            })
        
        # 9. BUSINESS CONTEXT FILTERS
        if context.get('business_model') == 'B2B':
            funnel_col = self._get_column(data, ['funnel_stage', 'funnel', 'stage'])
            if funnel_col:
                suggestions.append({
                    'type': FilterType.FUNNEL_STAGE,
                    'label': 'Funnel Stage',
                    'column': funnel_col,
                    'options': ['awareness', 'consideration', 'decision', 'retention'],
                    'default': 'all',
                    'multi_select': True,
                    'priority': 'medium',
                    'reasoning': 'B2B context - analyze by funnel stage'
                })
        
        # 10. CAMPAIGN FILTER
        campaign_col = self._get_column(data, ['campaign', 'campaign_name', 'campaign_id'])
        if campaign_col:
            campaigns = data[campaign_col].dropna().unique().tolist()
            if len(campaigns) > 1 and len(campaigns) <= 50:  # Don't suggest if too many
                suggestions.append({
                    'type': FilterType.CAMPAIGN,
                    'label': 'Campaign',
                    'column': campaign_col,
                    'options': campaigns,
                    'default': 'all',
                    'multi_select': True,
                    'priority': 'medium',
                    'reasoning': f'{len(campaigns)} campaigns detected'
                })
        
        # Sort by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        suggestions.sort(key=lambda x: priority_order.get(x['priority'], 3))
        
        logger.info(f"Generated {len(suggestions)} filter suggestions")
        return suggestions
    
    def apply_filters(self, data: pd.DataFrame, filters: Dict) -> pd.DataFrame:
        """
        Apply active filters to data
        
        Args:
            data: Original DataFrame
            filters: Dictionary of active filters
            
        Returns:
            Filtered DataFrame
        """
        
        filtered_data = data.copy()
        rows_before = len(filtered_data)
        
        logger.info(f"Applying {len(filters)} filters to {rows_before} rows")
        
        for filter_name, filter_config in filters.items():
            filter_type = filter_config['type']
            
            try:
                if filter_type == FilterType.DATE_RANGE:
                    filtered_data = self._apply_date_range_filter(filtered_data, filter_config)
                
                elif filter_type == FilterType.DATE_PRESET:
                    filtered_data = self._apply_date_preset_filter(filtered_data, filter_config)
                
                elif filter_type in [FilterType.CHANNEL, FilterType.CAMPAIGN, FilterType.DEVICE]:
                    column = filter_config.get('column', filter_type.value)
                    filtered_data = self._apply_dimension_filter(
                        filtered_data,
                        column=column,
                        values=filter_config['values']
                    )
                
                elif filter_type == FilterType.METRIC_THRESHOLD:
                    filtered_data = self._apply_metric_threshold_filter(filtered_data, filter_config)
                
                elif filter_type == FilterType.PERFORMANCE_TIER:
                    filtered_data = self._apply_performance_tier_filter(filtered_data, filter_config)
                
                elif filter_type == FilterType.BENCHMARK_RELATIVE:
                    filtered_data = self._apply_benchmark_filter(filtered_data, filter_config)
                
                elif filter_type == FilterType.STATISTICAL:
                    filtered_data = self._apply_statistical_filter(filtered_data, filter_config)
                
                elif filter_type == FilterType.ANOMALY:
                    filtered_data = self._apply_anomaly_filter(filtered_data, filter_config)
            
            except Exception as e:
                logger.error(f"Error applying filter {filter_name}: {e}")
                continue
        
        rows_after = len(filtered_data)
        
        # Store filter application in history
        self.filter_history.append({
            'timestamp': datetime.now(),
            'filters': filters,
            'rows_before': rows_before,
            'rows_after': rows_after,
            'reduction_pct': (1 - rows_after/rows_before) * 100 if rows_before > 0 else 0
        })
        
        logger.info(f"Filters applied: {rows_before} → {rows_after} rows ({(1 - rows_after/rows_before) * 100:.1f}% reduction)")
        
        return filtered_data
    
    def _apply_date_range_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Apply date range filter"""
        date_col = self._get_date_column(data)
        if not date_col:
            return data
        
        data = data.copy()
        data[date_col] = pd.to_datetime(data[date_col], errors='coerce')
        
        start_date = pd.to_datetime(config['start_date'])
        end_date = pd.to_datetime(config['end_date'])
        
        return data[(data[date_col] >= start_date) & (data[date_col] <= end_date)]
    
    def _apply_date_preset_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Apply preset date filters (last 7 days, last 30 days, etc.)"""
        date_col = self._get_date_column(data)
        if not date_col:
            return data
        
        data = data.copy()
        data[date_col] = pd.to_datetime(data[date_col], errors='coerce')
        
        preset = config['preset']
        max_date = data[date_col].max()
        
        preset_mappings = {
            'today': timedelta(days=0),
            'yesterday': timedelta(days=1),
            'last_7_days': timedelta(days=7),
            'last_14_days': timedelta(days=14),
            'last_30_days': timedelta(days=30),
            'last_90_days': timedelta(days=90),
            'this_month': None,
            'last_month': None,
            'this_quarter': None,
            'last_quarter': None
        }
        
        if preset in preset_mappings and preset_mappings[preset]:
            start_date = max_date - preset_mappings[preset]
            return data[data[date_col] >= start_date]
        
        elif preset == 'this_month':
            start_date = max_date.replace(day=1)
            return data[data[date_col] >= start_date]
        
        elif preset == 'last_month':
            last_month_end = max_date.replace(day=1) - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            return data[(data[date_col] >= last_month_start) & (data[date_col] <= last_month_end)]
        
        return data
    
    def _apply_dimension_filter(self, data: pd.DataFrame, column: str, values: Any) -> pd.DataFrame:
        """Apply filter on categorical dimension"""
        if column not in data.columns:
            return data
        
        if isinstance(values, list):
            return data[data[column].isin(values)]
        else:
            return data[data[column] == values]
    
    def _apply_metric_threshold_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Apply threshold filters on metrics"""
        filtered_data = data.copy()
        
        for condition in config.get('conditions', []):
            metric = condition['metric']
            operator = condition['operator']
            value = condition['value']
            
            if metric not in filtered_data.columns:
                continue
            
            if operator == '>':
                filtered_data = filtered_data[filtered_data[metric] > value]
            elif operator == '>=':
                filtered_data = filtered_data[filtered_data[metric] >= value]
            elif operator == '<':
                filtered_data = filtered_data[filtered_data[metric] < value]
            elif operator == '<=':
                filtered_data = filtered_data[filtered_data[metric] <= value]
            elif operator == '==':
                filtered_data = filtered_data[filtered_data[metric] == value]
            elif operator == '!=':
                filtered_data = filtered_data[filtered_data[metric] != value]
            elif operator == 'between':
                min_val, max_val = value
                filtered_data = filtered_data[
                    (filtered_data[metric] >= min_val) &
                    (filtered_data[metric] <= max_val)
                ]
        
        return filtered_data
    
    def _apply_performance_tier_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Filter by performance tier (top/middle/bottom performers)"""
        tier = config['tier']
        metric = config.get('metric', 'conversions')
        
        if metric not in data.columns:
            return data
        
        # Calculate percentile thresholds
        top_threshold = data[metric].quantile(0.8)  # Top 20%
        bottom_threshold = data[metric].quantile(0.2)  # Bottom 20%
        
        if tier == 'top':
            return data[data[metric] >= top_threshold]
        elif tier == 'bottom':
            return data[data[metric] <= bottom_threshold]
        elif tier == 'middle':
            return data[(data[metric] > bottom_threshold) & (data[metric] < top_threshold)]
        
        return data
    
    def _apply_benchmark_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Filter by performance relative to benchmark"""
        comparison = config['comparison']
        benchmarks = config['benchmarks']
        tolerance = config.get('tolerance', 0.1)
        
        filtered_data = data.copy()
        
        for metric, benchmark_value in benchmarks.items():
            if metric not in filtered_data.columns:
                continue
            
            if comparison == 'above':
                filtered_data = filtered_data[filtered_data[metric] > benchmark_value]
            elif comparison == 'below':
                filtered_data = filtered_data[filtered_data[metric] < benchmark_value]
            elif comparison == 'at':
                lower_bound = benchmark_value * (1 - tolerance)
                upper_bound = benchmark_value * (1 + tolerance)
                filtered_data = filtered_data[
                    (filtered_data[metric] >= lower_bound) &
                    (filtered_data[metric] <= upper_bound)
                ]
        
        return filtered_data
    
    def _apply_statistical_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Filter for statistically significant results only"""
        if 'p_value' in data.columns:
            alpha = config.get('alpha', 0.05)
            return data[data['p_value'] < alpha]
        
        return data
    
    def _apply_anomaly_filter(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Filter to show only anomalies or normal data"""
        from scipy import stats
        
        mode = config['mode']  # 'anomalies_only', 'normal_only', 'all'
        
        if mode == 'all':
            return data
        
        # Detect anomalies using Z-score method
        numeric_cols = data.select_dtypes(include=['number']).columns
        key_metric = config.get('metric', numeric_cols[0] if len(numeric_cols) > 0 else None)
        
        if not key_metric or key_metric not in data.columns:
            return data
        
        z_scores = np.abs(stats.zscore(data[key_metric].fillna(0)))
        threshold = config.get('threshold', 2)  # 2 standard deviations
        
        is_anomaly = z_scores > threshold
        
        if mode == 'anomalies_only':
            return data[is_anomaly]
        elif mode == 'normal_only':
            return data[~is_anomaly]
        
        return data
    
    def get_filter_impact_summary(self) -> Dict:
        """Provide summary of how filters have impacted the data"""
        if not self.filter_history:
            return {'message': 'No filters applied yet'}
        
        latest = self.filter_history[-1]
        
        return {
            'rows_original': latest['rows_before'],
            'rows_filtered': latest['rows_after'],
            'rows_removed': latest['rows_before'] - latest['rows_after'],
            'reduction_percentage': latest['reduction_pct'],
            'filters_applied': len(latest['filters']),
            'warnings': self._generate_filter_warnings(latest)
        }
    
    def _generate_filter_warnings(self, filter_result: Dict) -> List[Dict]:
        """Generate warnings if filters are too aggressive or ineffective"""
        warnings = []
        
        reduction_pct = filter_result['reduction_pct']
        
        if reduction_pct > 90:
            warnings.append({
                'severity': 'high',
                'message': f'Filters removed {reduction_pct:.1f}% of data. Consider relaxing filters.',
                'suggestion': 'Try removing some threshold filters or expanding date range'
            })
        elif reduction_pct < 5:
            warnings.append({
                'severity': 'low',
                'message': f'Filters only removed {reduction_pct:.1f}% of data.',
                'suggestion': 'Filters may not be providing meaningful segmentation'
            })
        
        if filter_result['rows_after'] < 30:
            warnings.append({
                'severity': 'medium',
                'message': f'Only {filter_result["rows_after"]} rows remaining after filtering.',
                'suggestion': 'Sample size may be too small for reliable insights'
            })
        
        return warnings
    
    # Helper methods
    def _get_date_column(self, data: pd.DataFrame) -> Optional[str]:
        """Find the date column in data"""
        for col in ['date', 'timestamp', 'datetime', 'day', 'Date', 'Timestamp']:
            if col in data.columns:
                return col
        return None
    
    def _get_column(self, data: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """Find column from list of possible names (case-insensitive)"""
        data_cols_lower = {col.lower(): col for col in data.columns}
        
        for name in possible_names:
            if name.lower() in data_cols_lower:
                return data_cols_lower[name.lower()]
        
        return None
    
    def _generate_date_presets(self, days_span: int) -> List[str]:
        """Generate relevant date presets based on data span"""
        presets = ['last_7_days', 'last_30_days']
        
        if days_span >= 90:
            presets.append('last_90_days')
        if days_span >= 180:
            presets.extend(['this_quarter', 'last_quarter'])
        if days_span >= 365:
            presets.append('this_month')
            presets.append('last_month')
        
        return presets
    
    def _has_sufficient_data_for_anomaly_detection(self, data: pd.DataFrame) -> bool:
        """Check if there's enough data for meaningful anomaly detection"""
        return len(data) >= 30  # Need at least 30 data points
