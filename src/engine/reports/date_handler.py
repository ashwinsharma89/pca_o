"""
Date Handler - Intelligent date parsing, period detection, and filtering.

Features:
- Auto-detect date columns in any format
- Parse various date formats
- Detect granularity (daily, weekly, monthly)
- Filter data by date range
- Generate date-based aggregations
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import re

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DateHandler:
    """
    Intelligent date handling for report generation.
    
    Handles:
    - Multi-format date parsing
    - Period detection (daily/weekly/monthly)
    - Date range filtering
    - Date-based aggregation grouping
    """
    
    # Common date formats to try
    DATE_FORMATS = [
        "%Y-%m-%d",           # 2024-01-15
        "%d-%m-%Y",           # 15-01-2024
        "%m/%d/%Y",           # 01/15/2024
        "%d/%m/%Y",           # 15/01/2024
        "%Y/%m/%d",           # 2024/01/15
        "%Y-%m-%d %H:%M:%S",  # 2024-01-15 10:30:00
        "%d %b %Y",           # 15 Jan 2024
        "%d %B %Y",           # 15 January 2024
        "%b %d, %Y",          # Jan 15, 2024
        "%B %d, %Y",          # January 15, 2024
    ]
    
    # Date column name patterns
    DATE_COLUMN_PATTERNS = [
        r'^date$',
        r'^day$',
        r'^report_date$',
        r'^metric_date$',
        r'_date$',
        r'^period$',
        r'^week_start',
        r'^week_end',
        r'^month$',
    ]
    
    def __init__(self):
        self._date_patterns = [re.compile(p, re.IGNORECASE) for p in self.DATE_COLUMN_PATTERNS]
    
    def detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect columns that contain date values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of date column names
        """
        date_cols = []
        
        for col in df.columns:
            # Check column name
            if any(p.search(col) for p in self._date_patterns):
                date_cols.append(col)
                continue
            
            # Check if already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_cols.append(col)
                continue
            
            # Try to parse sample values
            if df[col].dtype == 'object':
                sample = df[col].dropna().head(5).tolist()
                if sample and self._looks_like_date(sample):
                    date_cols.append(col)
        
        logger.debug(f"Detected date columns: {date_cols}")
        return date_cols
    
    def _looks_like_date(self, values: List) -> bool:
        """Check if values look like dates."""
        parsed = 0
        for val in values:
            if isinstance(val, (datetime, pd.Timestamp)):
                parsed += 1
            elif isinstance(val, str):
                for fmt in self.DATE_FORMATS[:5]:  # Try common formats
                    try:
                        datetime.strptime(val, fmt)
                        parsed += 1
                        break
                    except ValueError:
                        continue
        return parsed >= len(values) * 0.5
    
    def parse_dates(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Parse date column to datetime.
        
        Args:
            df: Input DataFrame
            date_col: Column name to parse
            
        Returns:
            DataFrame with parsed date column
        """
        df = df.copy()
        
        if pd.api.types.is_datetime64_any_dtype(df[date_col]):
            return df
        
        # Try pandas auto-parsing first
        try:
            df[date_col] = pd.to_datetime(df[date_col])
            return df
        except Exception:
            pass
        
        # Try explicit formats
        for fmt in self.DATE_FORMATS:
            try:
                df[date_col] = pd.to_datetime(df[date_col], format=fmt)
                logger.debug(f"Parsed {date_col} with format: {fmt}")
                return df
            except Exception:
                continue
        
        logger.warning(f"Could not parse dates in column: {date_col}")
        return df
    
    def detect_granularity(self, df: pd.DataFrame, date_col: str) -> str:
        """
        Detect the time granularity of the data.
        
        Args:
            df: DataFrame with parsed dates
            date_col: Date column name
            
        Returns:
            Granularity string: 'daily', 'weekly', 'monthly', 'yearly'
        """
        if date_col not in df.columns:
            return "unknown"
        
        dates = pd.to_datetime(df[date_col], errors='coerce').dropna().sort_values()
        
        if len(dates) < 2:
            return "unknown"
        
        # Calculate median gap between dates
        diffs = dates.diff().dropna()
        median_diff = diffs.median()
        
        if median_diff <= timedelta(days=1):
            return "daily"
        elif median_diff <= timedelta(days=7):
            return "weekly"
        elif median_diff <= timedelta(days=31):
            return "monthly"
        elif median_diff <= timedelta(days=365):
            return "yearly"
        
        return "irregular"
    
    def filter_by_date_range(self, df: pd.DataFrame, date_col: str,
                            start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Filter DataFrame by date range.
        
        Args:
            df: Input DataFrame
            date_col: Date column name
            start_date: Start date string (inclusive)
            end_date: End date string (inclusive)
            
        Returns:
            Filtered DataFrame
        """
        df = self.parse_dates(df, date_col)
        
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df[date_col] >= start]
        
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df[date_col] <= end]
        
        return df
    
    def add_period_columns(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Add period columns for aggregation (week, month, year).
        
        Args:
            df: Input DataFrame
            date_col: Date column name
            
        Returns:
            DataFrame with additional period columns
        """
        df = self.parse_dates(df, date_col).copy()
        
        if date_col not in df.columns:
            return df
        
        dates = pd.to_datetime(df[date_col], errors='coerce')
        
        df['_year'] = dates.dt.year
        df['_month'] = dates.dt.to_period('M').astype(str)
        df['_week'] = dates.dt.to_period('W').astype(str)
        df['_day'] = dates.dt.date
        
        return df
    
    def get_date_range(self, df: pd.DataFrame, date_col: str) -> Dict[str, Any]:
        """
        Get the date range of the data.
        
        Args:
            df: Input DataFrame
            date_col: Date column name
            
        Returns:
            Dict with min_date, max_date, total_days
        """
        df = self.parse_dates(df, date_col)
        
        if date_col not in df.columns:
            return {"min_date": None, "max_date": None, "total_days": 0}
        
        dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
        
        if len(dates) == 0:
            return {"min_date": None, "max_date": None, "total_days": 0}
        
        min_date = dates.min()
        max_date = dates.max()
        
        return {
            "min_date": min_date.strftime("%Y-%m-%d"),
            "max_date": max_date.strftime("%Y-%m-%d"),
            "total_days": (max_date - min_date).days + 1,
        }
