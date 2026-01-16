"""
Advanced Data Processing Engine
Handles multi-granularity, time-series, and media-specific data processing
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import warnings
warnings.filterwarnings('ignore')


class MediaDataProcessor:
    """Advanced processor for media campaign data with multi-granularity support."""
    
    # Platform hierarchies
    PLATFORM_HIERARCHIES = {
        "google_ads": ["Account", "Campaign", "Ad_Group", "Ad", "Keyword"],
        "meta_ads": ["Account", "Campaign", "Ad_Set", "Ad", "Creative"],
        "linkedin_ads": ["Account", "Campaign", "Campaign_Group", "Ad", "Creative"],
        "dv360": ["Advertiser", "Insertion_Order", "Line_Item", "Creative", "Placement"],
        "cm360": ["Advertiser", "Campaign", "Placement", "Creative", "Site"],
        "snapchat_ads": ["Account", "Campaign", "Ad_Squad", "Ad", "Creative"],
        "tiktok_ads": ["Account", "Campaign", "Ad_Group", "Ad", "Creative"],
        "twitter_ads": ["Account", "Campaign", "Ad_Group", "Ad", "Creative"],
        "pinterest_ads": ["Account", "Campaign", "Ad_Group", "Ad", "Pin"]
    }
    
    # Standard dimension mappings
    DIMENSION_MAPPINGS = {
        "campaign": ["campaign", "campaign_name", "campaign_id", "campaignname"],
        "ad_group": ["ad_group", "adgroup", "ad_set", "adset", "ad_squad", "adsquad"],
        "ad": ["ad", "ad_name", "ad_id", "adname"],
        "creative": ["creative", "creative_name", "creative_id", "creativename"],
        "placement": ["placement", "placement_name", "placement_id", "site"],
        "keyword": ["keyword", "keyword_text", "search_term"],
        "device": ["device", "device_type", "device_category"],
        "location": ["location", "geo", "region", "country", "state", "city"],
        "age": ["age", "age_range", "age_group"],
        "gender": ["gender"],
        "audience": ["audience", "audience_name", "segment"],
        "platform": ["platform", "channel", "source"]
    }
    
    # Metric calculations
    CALCULATED_METRICS = {
        "CTR": lambda df: (df.get("Clicks", 0) / df.get("Impressions", 1)) * 100,
        "CPC": lambda df: df.get("Spend", 0) / df.get("Clicks", 1),
        "CPM": lambda df: (df.get("Spend", 0) / df.get("Impressions", 1)) * 1000,
        "CPA": lambda df: df.get("Spend", 0) / df.get("Conversions", 1),
        "ROAS": lambda df: df.get("Revenue", 0) / df.get("Spend", 1),
        "Conversion_Rate": lambda df: (df.get("Conversions", 0) / df.get("Clicks", 1)) * 100,
        "Cost_Per_Click": lambda df: df.get("Spend", 0) / df.get("Clicks", 1),
        "Revenue_Per_Click": lambda df: df.get("Revenue", 0) / df.get("Clicks", 1),
        "Frequency": lambda df: df.get("Impressions", 0) / df.get("Reach", 1),
        "Engagement_Rate": lambda df: (df.get("Engagements", 0) / df.get("Impressions", 1)) * 100
    }
    
    def __init__(self):
        """Initialize the processor."""
        self.df = None
        self.date_column = None
        self.time_granularity = None
        logger.info("Initialized MediaDataProcessor")
    
    def load_data(self, df: pd.DataFrame, auto_detect: bool = True) -> pd.DataFrame:
        """
        Load and prepare data with automatic type detection.
        
        Args:
            df: Input DataFrame
            auto_detect: Automatically detect and fix data types
            
        Returns:
            Processed DataFrame
        """
        logger.info(f"Loading data: {len(df)} rows, {len(df.columns)} columns")
        
        # Create a copy
        self.df = df.copy()
        
        if auto_detect:
            self.df = self._auto_detect_types(self.df)
            self.df = self._detect_date_column(self.df)
            self.df = self._standardize_column_names(self.df)
            self.df = self._calculate_missing_metrics(self.df)
        
        logger.info(f"Data loaded successfully with {len(self.df.columns)} columns")
        return self.df
    
    def _auto_detect_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Automatically detect and convert data types."""
        logger.info("Auto-detecting data types...")
        
        for col in df.columns:
            # Skip if already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            
            # Try to convert to numeric
            if df[col].dtype == 'object':
                # Remove common formatting
                cleaned = df[col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('%', '')
                
                try:
                    # Try integer first
                    numeric_values = pd.to_numeric(cleaned, errors='coerce')
                    if numeric_values.notna().sum() > len(df) * 0.8:  # 80% threshold
                        df[col] = numeric_values
                        logger.debug(f"Converted {col} to numeric")
                except Exception as e:
                    logger.debug(f"Type detection failed for {col}: {e}")
        
        return df
    
    def _detect_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and parse date columns."""
        date_keywords = ['date', 'time', 'day', 'week', 'month', 'year', 'period']
        
        for col in df.columns:
            if any(keyword in col.lower() for keyword in date_keywords):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if df[col].notna().sum() > 0:
                        self.date_column = col
                        logger.info(f"Detected date column: {col}")
                        
                        # Add derived time columns
                        df['Year'] = df[col].dt.year
                        df['Month'] = df[col].dt.month
                        df['Week'] = df[col].dt.isocalendar().week
                        df['Day_of_Week'] = df[col].dt.day_name()
                        df['Quarter'] = df[col].dt.quarter
                        df['Year_Month'] = df[col].dt.to_period('M').astype(str)
                        df['Year_Week'] = df[col].dt.to_period('W').astype(str)
                        
                        break
                except Exception as e:
                    logger.debug(f"Date conversion failed for {col}: {e}")
                    continue
        
        return df
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for easier processing."""
        # Create mapping of found columns to standard names
        column_mapping = {}
        
        for standard_name, variations in self.DIMENSION_MAPPINGS.items():
            for col in df.columns:
                if col.lower().replace('_', '').replace(' ', '') in [v.lower().replace('_', '').replace(' ', '') for v in variations]:
                    column_mapping[col] = standard_name.title()
                    break
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
            logger.info(f"Standardized {len(column_mapping)} column names")
        
        return df
    
    def _calculate_missing_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate missing KPIs from available data."""
        calculated = []
        
        for metric_name, calc_func in self.CALCULATED_METRICS.items():
            if metric_name not in df.columns:
                try:
                    df[metric_name] = calc_func(df)
                    # Replace inf and nan
                    df[metric_name] = df[metric_name].replace([np.inf, -np.inf], np.nan)
                    if df[metric_name].notna().sum() > 0:
                        calculated.append(metric_name)
                except Exception as e:
                    logger.debug(f"Could not calculate {metric_name}: {e}")
        
        if calculated:
            logger.info(f"Calculated metrics: {', '.join(calculated)}")
        
        return df
    
    def get_time_granularity(self) -> str:
        """Detect time granularity of the data."""
        if self.date_column is None or self.date_column not in self.df.columns:
            return "unknown"
        
        dates = self.df[self.date_column].dropna()
        if len(dates) < 2:
            return "single_point"
        
        # Calculate date differences
        date_diffs = dates.sort_values().diff().dropna()
        median_diff = date_diffs.median()
        
        if median_diff <= pd.Timedelta(days=1):
            return "daily"
        elif median_diff <= pd.Timedelta(days=7):
            return "weekly"
        elif median_diff <= pd.Timedelta(days=31):
            return "monthly"
        elif median_diff <= pd.Timedelta(days=92):
            return "quarterly"
        else:
            return "yearly"
    
    def aggregate_by_time(
        self,
        period: str = "month",
        metrics: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by time period.
        
        Args:
            period: Time period (day, week, month, quarter, year)
            metrics: Metrics to aggregate
            
        Returns:
            Aggregated DataFrame
        """
        if self.date_column is None:
            logger.warning("No date column found, cannot aggregate by time")
            return self.df
        
        # Determine grouping column
        period_map = {
            "day": self.date_column,
            "week": "Year_Week",
            "month": "Year_Month",
            "quarter": "Quarter",
            "year": "Year"
        }
        
        group_col = period_map.get(period.lower(), "Year_Month")
        
        if group_col not in self.df.columns:
            logger.warning(f"Column {group_col} not found")
            return self.df
        
        # Determine metrics to aggregate
        if metrics is None:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            metrics = [col for col in numeric_cols if col not in ['Year', 'Month', 'Week', 'Quarter']]
        
        # Aggregate
        agg_dict = {metric: 'sum' for metric in metrics if metric in self.df.columns}
        
        if agg_dict:
            result = self.df.groupby(group_col).agg(agg_dict).reset_index()
            logger.info(f"Aggregated data by {period}: {len(result)} periods")
            return result
        
        return self.df
    
    def compare_periods(
        self,
        period_type: str = "month",
        num_periods: int = 2,
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compare performance across time periods.
        
        Args:
            period_type: Type of period (month, week, quarter)
            num_periods: Number of recent periods to compare
            metrics: Metrics to compare
            
        Returns:
            Comparison results
        """
        if self.date_column is None:
            return {"error": "No date column found"}
        
        # Get aggregated data
        agg_data = self.aggregate_by_time(period_type, metrics)
        
        if len(agg_data) < num_periods:
            return {"error": f"Not enough data for {num_periods} periods"}
        
        # Get last N periods
        recent_periods = agg_data.tail(num_periods)
        
        # Calculate changes
        comparisons = {}
        numeric_cols = recent_periods.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            values = recent_periods[col].tolist()
            if len(values) >= 2:
                latest = values[-1]
                previous = values[-2]
                
                if previous != 0:
                    change_pct = ((latest - previous) / previous) * 100
                else:
                    change_pct = 0
                
                comparisons[col] = {
                    "latest": float(latest),
                    "previous": float(previous),
                    "change": float(latest - previous),
                    "change_pct": float(change_pct),
                    "trend": "up" if change_pct > 0 else "down" if change_pct < 0 else "flat"
                }
        
        return {
            "period_type": period_type,
            "num_periods": num_periods,
            "periods": recent_periods.iloc[:, 0].tolist(),
            "comparisons": comparisons
        }
    
    def detect_seasonality(self, metric: str = "Spend") -> Dict[str, Any]:
        """
        Detect seasonality patterns in the data.
        
        Args:
            metric: Metric to analyze for seasonality
            
        Returns:
            Seasonality analysis
        """
        if self.date_column is None or metric not in self.df.columns:
            return {"error": "Cannot detect seasonality"}
        
        # Aggregate by month
        monthly = self.df.groupby('Month')[metric].sum()
        
        # Calculate statistics
        seasonality = {
            "metric": metric,
            "by_month": monthly.to_dict(),
            "peak_month": int(monthly.idxmax()),
            "low_month": int(monthly.idxmin()),
            "peak_value": float(monthly.max()),
            "low_value": float(monthly.min()),
            "variation_pct": float(((monthly.max() - monthly.min()) / monthly.mean()) * 100)
        }
        
        # Day of week analysis
        if 'Day_of_Week' in self.df.columns:
            dow = self.df.groupby('Day_of_Week')[metric].sum()
            seasonality["by_day_of_week"] = dow.to_dict()
            seasonality["peak_day"] = dow.idxmax()
            seasonality["low_day"] = dow.idxmin()
        
        return seasonality
    
    def group_by_dimensions(
        self,
        dimensions: List[str],
        metrics: Optional[List[str]] = None,
        top_n: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Group data by specified dimensions.
        
        Args:
            dimensions: List of dimensions to group by
            metrics: Metrics to aggregate
            top_n: Return only top N results
            
        Returns:
            Grouped DataFrame
        """
        # Standardize dimension names
        available_dims = []
        for dim in dimensions:
            dim_lower = dim.lower().replace(' ', '_')
            # Find matching column
            for col in self.df.columns:
                if dim_lower in col.lower().replace(' ', '_'):
                    available_dims.append(col)
                    break
        
        if not available_dims:
            logger.warning(f"No matching dimensions found for: {dimensions}")
            return self.df
        
        # Determine metrics
        if metrics is None:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            metrics = [col for col in numeric_cols if col not in available_dims]
        
        # Filter metrics that exist
        available_metrics = [m for m in metrics if m in self.df.columns]
        
        if not available_metrics:
            return self.df
        
        # Aggregate
        agg_dict = {metric: 'sum' for metric in available_metrics}
        result = self.df.groupby(available_dims).agg(agg_dict).reset_index()
        
        # Sort by first metric and get top N
        if top_n and len(available_metrics) > 0:
            result = result.nlargest(top_n, available_metrics[0])
        
        logger.info(f"Grouped by {available_dims}: {len(result)} groups")
        return result
    
    def get_hierarchy_data(
        self,
        platform: str,
        levels: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Get data at different hierarchy levels for a platform.
        
        Args:
            platform: Platform name
            levels: Specific levels to return
            
        Returns:
            Dictionary of DataFrames by level
        """
        platform_key = platform.lower().replace(' ', '_')
        
        if platform_key not in self.PLATFORM_HIERARCHIES:
            logger.warning(f"Unknown platform: {platform}")
            return {}
        
        hierarchy = self.PLATFORM_HIERARCHIES[platform_key]
        
        if levels:
            hierarchy = [h for h in hierarchy if h in levels]
        
        result = {}
        
        for level in hierarchy:
            # Find matching columns
            level_cols = [col for col in self.df.columns if level.lower() in col.lower()]
            
            if level_cols:
                # Group by this level
                grouped = self.group_by_dimensions([level_cols[0]])
                result[level] = grouped
        
        return result
    
    def calculate_overall_kpis(self) -> Dict[str, float]:
        """Calculate KPIs at overall level."""
        kpis = {}
        
        # Sum metrics
        sum_metrics = ['Spend', 'Impressions', 'Clicks', 'Conversions', 'Revenue', 'Reach', 'Engagements']
        for metric in sum_metrics:
            if metric in self.df.columns:
                kpis[f"Total_{metric}"] = float(self.df[metric].sum())
        
        # Calculate ratios
        if 'Impressions' in self.df.columns and 'Clicks' in self.df.columns:
            total_impr = self.df['Impressions'].sum()
            total_clicks = self.df['Clicks'].sum()
            kpis['Overall_CTR'] = (total_clicks / total_impr * 100) if total_impr > 0 else 0
        
        if 'Spend' in self.df.columns and 'Clicks' in self.df.columns:
            total_spend = self.df['Spend'].sum()
            total_clicks = self.df['Clicks'].sum()
            kpis['Overall_CPC'] = (total_spend / total_clicks) if total_clicks > 0 else 0
        
        if 'Spend' in self.df.columns and 'Impressions' in self.df.columns:
            total_spend = self.df['Spend'].sum()
            total_impr = self.df['Impressions'].sum()
            kpis['Overall_CPM'] = (total_spend / total_impr * 1000) if total_impr > 0 else 0
        
        if 'Revenue' in self.df.columns and 'Spend' in self.df.columns:
            total_revenue = self.df['Revenue'].sum() if 'Revenue' in self.df.columns else self.df['Conversions'].sum() * 100
            total_spend = self.df['Spend'].sum()
            kpis['Overall_ROAS'] = (total_revenue / total_spend) if total_spend > 0 else 0
        
        if 'Spend' in self.df.columns and 'Conversions' in self.df.columns:
            total_spend = self.df['Spend'].sum()
            total_conv = self.df['Conversions'].sum()
            kpis['Overall_CPA'] = (total_spend / total_conv) if total_conv > 0 else 0
        
        if 'Conversions' in self.df.columns and 'Clicks' in self.df.columns:
            total_conv = self.df['Conversions'].sum()
            total_clicks = self.df['Clicks'].sum()
            kpis['Overall_Conversion_Rate'] = (total_conv / total_clicks * 100) if total_clicks > 0 else 0
        
        return kpis
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get comprehensive data summary."""
        summary = {
            "total_rows": len(self.df),
            "total_columns": len(self.df.columns),
            "date_range": None,
            "time_granularity": self.get_time_granularity(),
            "dimensions_found": [],
            "metrics_found": [],
            "platforms_found": [],
            "hierarchy_levels": {}
        }
        
        # Date range
        if self.date_column and self.date_column in self.df.columns:
            summary["date_range"] = {
                "start": self.df[self.date_column].min().isoformat() if pd.notna(self.df[self.date_column].min()) else None,
                "end": self.df[self.date_column].max().isoformat() if pd.notna(self.df[self.date_column].max()) else None,
                "days": (self.df[self.date_column].max() - self.df[self.date_column].min()).days if pd.notna(self.df[self.date_column].min()) else 0
            }
        
        # Dimensions
        for dim_name, variations in self.DIMENSION_MAPPINGS.items():
            for col in self.df.columns:
                if any(var.lower() in col.lower() for var in variations):
                    summary["dimensions_found"].append(col)
                    break
        
        # Metrics
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
        summary["metrics_found"] = numeric_cols
        
        # Platforms
        if 'Platform' in self.df.columns:
            try:
                summary["platforms_found"] = self.df['Platform'].unique().tolist()
            except AttributeError:
                # Handle case where self.df might not be set correctly
                summary["platforms_found"] = []
                logger.warning("Could not extract unique platforms - DataFrame issue")
        
        return summary
