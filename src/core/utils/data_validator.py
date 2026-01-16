"""
Robust Data Validation and Normalization System
Handles multiple data types, formats, and edge cases
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple
from loguru import logger
import re


class DataValidator:
    """Comprehensive data validation and normalization."""
    
    # Date format patterns to try
    DATE_FORMATS = [
        '%d-%m-%Y',      # 13-01-2024
        '%d/%m/%Y',      # 13/01/2024
        '%m-%d-%Y',      # 01-13-2024
        '%m/%d/%Y',      # 01/13/2024
        '%Y-%m-%d',      # 2024-01-13
        '%Y/%m/%d',      # 2024/01/13
        '%d.%m.%Y',      # 13.01.2024
        '%d-%b-%Y',      # 13-Jan-2024
        '%d %b %Y',      # 13 Jan 2024
        '%d-%B-%Y',      # 13-January-2024
        '%d %B %Y',      # 13 January 2024
        '%b %d, %Y',     # Jan 13, 2024
        '%B %d, %Y',     # January 13, 2024
        '%Y%m%d',        # 20240113
        '%d%m%Y',        # 13012024
    ]
    
    def __init__(self):
        """Initialize validator."""
        self.validation_stats = {
            'total_rows': 0,
            'cleaned_rows': 0,
            'errors': [],
            'warnings': [],
            'conversions': {}
        }
    
    def validate_and_clean_dataframe(self, df: pd.DataFrame, apply_campaign_normalization: bool = True) -> Tuple[pd.DataFrame, Dict]:
        """
        Validate and clean entire DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (cleaned_df, validation_report)
        """
        logger.info(f"Starting validation of DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        self.validation_stats['total_rows'] = len(df)
        cleaned_df = df.copy()
        
        # Step 1: Apply campaign-specific column normalization first
        if apply_campaign_normalization:
            cleaned_df, column_mappings = self._normalize_campaign_columns(cleaned_df)
            if column_mappings:
                self.validation_stats['conversions']['Column Mappings'] = f"{len(column_mappings)} columns renamed"
        
        # Step 2: Detect and normalize column types
        for col in cleaned_df.columns:
            try:
                cleaned_df[col] = self._normalize_column(cleaned_df[col], col)
            except Exception as e:
                logger.warning(f"Error normalizing column {col}: {e}")
                self.validation_stats['warnings'].append(f"Column {col}: {str(e)}")
        
        self.validation_stats['cleaned_rows'] = len(cleaned_df)
        
        report = self._generate_report(cleaned_df)
        
        logger.info(f"Validation complete. Cleaned {self.validation_stats['cleaned_rows']} rows")
        
        return cleaned_df, report
    
    def _normalize_column(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize a column based on its content.
        
        Args:
            series: Column data
            col_name: Column name
            
        Returns:
            Normalized series
        """
        # Skip if all null
        if series.isna().all():
            return series
        
        # Detect column type
        col_type = self._detect_column_type(series, col_name)
        
        logger.debug(f"Column '{col_name}' detected as type: {col_type}")
        
        # Normalize based on type
        if col_type == 'date':
            return self._normalize_dates(series, col_name)
        elif col_type == 'numeric':
            return self._normalize_numeric(series, col_name)
        elif col_type == 'currency':
            return self._normalize_currency(series, col_name)
        elif col_type == 'percentage':
            return self._normalize_percentage(series, col_name)
        elif col_type == 'boolean':
            return self._normalize_boolean(series, col_name)
        elif col_type == 'categorical':
            return self._normalize_categorical(series, col_name)
        else:
            return self._normalize_string(series, col_name)
    
    def _detect_column_type(self, series: pd.Series, col_name: str) -> str:
        """
        Detect the type of data in a column.
        
        Args:
            series: Column data
            col_name: Column name
            
        Returns:
            Detected type
        """
        # Remove nulls for detection
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return 'unknown'
        
        # Check column name hints
        col_lower = col_name.lower()
        
        if any(word in col_lower for word in ['date', 'time', 'day', 'month', 'year', 'period']):
            return 'date'
        
        if any(word in col_lower for word in ['spend', 'cost', 'revenue', 'price', 'amount', 'cpc', 'cpa', 'cpm']):
            return 'currency'
        
        if any(word in col_lower for word in ['rate', 'ctr', 'cvr', 'roas', 'roi', 'percent']):
            return 'percentage'
        
        # Sample values for content detection
        sample = non_null.head(100)
        
        # Check if already numeric
        if pd.api.types.is_numeric_dtype(series):
            return 'numeric'
        
        # Try to detect dates
        if self._is_date_column(sample):
            return 'date'
        
        # Try to detect currency
        if self._is_currency_column(sample):
            return 'currency'
        
        # Try to detect percentage
        if self._is_percentage_column(sample):
            return 'percentage'
        
        # Try to detect boolean
        if self._is_boolean_column(sample):
            return 'boolean'
        
        # Try to detect numeric
        if self._is_numeric_column(sample):
            return 'numeric'
        
        # Check if categorical (low cardinality)
        if len(non_null.unique()) < len(non_null) * 0.5 and len(non_null.unique()) < 50:
            return 'categorical'
        
        return 'string'
    
    def _is_date_column(self, sample: pd.Series) -> bool:
        """Check if column contains dates."""
        try:
            # Try pandas auto-detection
            pd.to_datetime(sample.head(10), errors='coerce')
            success_rate = pd.to_datetime(sample, errors='coerce').notna().sum() / len(sample)
            return success_rate > 0.7
        except Exception:
            return False
    
    def _is_currency_column(self, sample: pd.Series) -> bool:
        """Check if column contains currency values."""
        sample_str = sample.astype(str)
        currency_pattern = r'[\$£€¥₹]|USD|EUR|GBP'
        return sample_str.str.contains(currency_pattern, regex=True).sum() > len(sample) * 0.3
    
    def _is_percentage_column(self, sample: pd.Series) -> bool:
        """Check if column contains percentages."""
        sample_str = sample.astype(str)
        return sample_str.str.contains('%', regex=False).sum() > len(sample) * 0.3
    
    def _is_boolean_column(self, sample: pd.Series) -> bool:
        """Check if column contains boolean values."""
        unique_vals = set(str(v).lower() for v in sample.unique())
        bool_vals = {'true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'}
        return len(unique_vals - bool_vals) == 0
    
    def _is_numeric_column(self, sample: pd.Series) -> bool:
        """Check if column contains numeric values."""
        try:
            pd.to_numeric(sample, errors='coerce')
            success_rate = pd.to_numeric(sample, errors='coerce').notna().sum() / len(sample)
            return success_rate > 0.7
        except Exception:
            return False
    
    def _normalize_dates(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize date column with multiple format support.
        
        Args:
            series: Date column
            col_name: Column name
            
        Returns:
            Normalized datetime series
        """
        logger.info(f"Normalizing dates in column '{col_name}'")
        
        # Try pandas auto-detection first
        try:
            result = pd.to_datetime(series, format='mixed', dayfirst=True, errors='coerce')
            success_rate = result.notna().sum() / len(series.dropna())
            
            if success_rate > 0.9:
                self.validation_stats['conversions'][col_name] = f"Date (auto-detected, {success_rate:.1%} success)"
                return result
        except Exception as e:
            logger.debug(f"Auto-detection failed: {e}")
        
        # Try each format manually
        for date_format in self.DATE_FORMATS:
            try:
                result = pd.to_datetime(series, format=date_format, errors='coerce')
                success_rate = result.notna().sum() / len(series.dropna())
                
                if success_rate > 0.9:
                    logger.info(f"Successfully parsed dates with format: {date_format}")
                    self.validation_stats['conversions'][col_name] = f"Date ({date_format}, {success_rate:.1%} success)"
                    return result
            except Exception as e:
                logger.debug(f"Date format {date_format} failed: {e}")
                continue
        
        # Last resort: try to parse each value individually
        def parse_flexible(val):
            if pd.isna(val):
                return pd.NaT
            
            val_str = str(val).strip()
            
            # Try each format
            for fmt in self.DATE_FORMATS:
                try:
                    return datetime.strptime(val_str, fmt)
                except:
                    continue
            
            # Try pandas as last resort
            try:
                return pd.to_datetime(val_str, dayfirst=True)
            except:
                return pd.NaT
        
        result = series.apply(parse_flexible)
        success_rate = result.notna().sum() / len(series.dropna())
        
        self.validation_stats['conversions'][col_name] = f"Date (flexible parsing, {success_rate:.1%} success)"
        
        if success_rate < 0.5:
            self.validation_stats['warnings'].append(
                f"Column '{col_name}': Low date parsing success rate ({success_rate:.1%})"
            )
        
        return result
    
    def _normalize_numeric(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize numeric column.
        
        Args:
            series: Numeric column
            col_name: Column name
            
        Returns:
            Normalized numeric series
        """
        logger.info(f"Normalizing numeric values in column '{col_name}'")
        
        def clean_numeric(val):
            if pd.isna(val):
                return np.nan
            
            # If already numeric, return as is
            if isinstance(val, (int, float)):
                return float(val)
            
            # Convert to string and clean
            val_str = str(val).strip()
            
            # Remove common separators
            val_str = val_str.replace(',', '')  # 1,000 -> 1000
            val_str = val_str.replace(' ', '')  # 1 000 -> 1000
            
            # Handle negative numbers
            is_negative = val_str.startswith('-') or val_str.startswith('(')
            val_str = val_str.replace('-', '').replace('(', '').replace(')', '')
            
            # Try to convert
            try:
                result = float(val_str)
                return -result if is_negative else result
            except:
                return np.nan
        
        result = series.apply(clean_numeric)
        success_rate = result.notna().sum() / len(series.dropna())
        
        self.validation_stats['conversions'][col_name] = f"Numeric ({success_rate:.1%} success)"
        
        return result
    
    def _normalize_currency(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize currency column.
        
        Args:
            series: Currency column
            col_name: Column name
            
        Returns:
            Normalized numeric series
        """
        logger.info(f"Normalizing currency values in column '{col_name}'")
        
        def clean_currency(val):
            if pd.isna(val):
                return np.nan
            
            # If already numeric, return as is
            if isinstance(val, (int, float)):
                return float(val)
            
            # Convert to string and clean
            val_str = str(val).strip()
            
            # Remove currency symbols and text
            val_str = re.sub(r'[\$£€¥₹]', '', val_str)
            val_str = re.sub(r'USD|EUR|GBP|INR|JPY', '', val_str, flags=re.IGNORECASE)
            
            # Remove separators
            val_str = val_str.replace(',', '')
            val_str = val_str.replace(' ', '')
            
            # Handle negative
            is_negative = '(' in val_str or val_str.startswith('-')
            val_str = val_str.replace('(', '').replace(')', '').replace('-', '')
            
            try:
                result = float(val_str)
                return -result if is_negative else result
            except:
                return np.nan
        
        result = series.apply(clean_currency)
        success_rate = result.notna().sum() / len(series.dropna())
        
        self.validation_stats['conversions'][col_name] = f"Currency ({success_rate:.1%} success)"
        
        return result
    
    def _normalize_percentage(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize percentage column.
        
        Args:
            series: Percentage column
            col_name: Column name
            
        Returns:
            Normalized numeric series (as decimal)
        """
        logger.info(f"Normalizing percentage values in column '{col_name}'")
        
        def clean_percentage(val):
            if pd.isna(val):
                return np.nan
            
            # If already numeric, check if it's already a decimal
            if isinstance(val, (int, float)):
                # If value is between 0-1, assume it's already decimal
                if 0 <= val <= 1:
                    return float(val)
                # If value is > 1, assume it's percentage
                else:
                    return float(val) / 100
            
            # Convert to string and clean
            val_str = str(val).strip()
            
            # Remove % symbol
            val_str = val_str.replace('%', '')
            val_str = val_str.replace(' ', '')
            
            try:
                result = float(val_str)
                # If value is > 1, assume it's percentage
                if result > 1:
                    return result / 100
                return result
            except:
                return np.nan
        
        result = series.apply(clean_percentage)
        success_rate = result.notna().sum() / len(series.dropna())
        
        self.validation_stats['conversions'][col_name] = f"Percentage ({success_rate:.1%} success)"
        
        return result
    
    def _normalize_boolean(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize boolean column.
        
        Args:
            series: Boolean column
            col_name: Column name
            
        Returns:
            Normalized boolean series
        """
        logger.info(f"Normalizing boolean values in column '{col_name}'")
        
        def clean_boolean(val):
            if pd.isna(val):
                return np.nan
            
            val_str = str(val).strip().lower()
            
            true_vals = {'true', 'yes', '1', 't', 'y', 'on', 'enabled', 'active'}
            false_vals = {'false', 'no', '0', 'f', 'n', 'off', 'disabled', 'inactive'}
            
            if val_str in true_vals:
                return True
            elif val_str in false_vals:
                return False
            else:
                return np.nan
        
        result = series.apply(clean_boolean)
        success_rate = result.notna().sum() / len(series.dropna())
        
        self.validation_stats['conversions'][col_name] = f"Boolean ({success_rate:.1%} success)"
        
        return result
    
    def _normalize_categorical(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize categorical column.
        
        Args:
            series: Categorical column
            col_name: Column name
            
        Returns:
            Normalized categorical series
        """
        logger.info(f"Normalizing categorical values in column '{col_name}'")
        
        # Clean strings
        result = series.astype(str).str.strip()
        
        # Standardize case for common categories
        if col_name.lower() in ['platform', 'channel', 'device', 'status']:
            result = result.str.title()
        
        self.validation_stats['conversions'][col_name] = f"Categorical ({len(result.unique())} unique values)"
        
        return result
    
    def _normalize_string(self, series: pd.Series, col_name: str) -> pd.Series:
        """
        Normalize string column.
        
        Args:
            series: String column
            col_name: Column name
            
        Returns:
            Normalized string series
        """
        logger.info(f"Normalizing string values in column '{col_name}'")
        
        # Convert to string and clean
        result = series.astype(str).str.strip()
        
        # Remove multiple spaces
        result = result.str.replace(r'\s+', ' ', regex=True)
        
        self.validation_stats['conversions'][col_name] = "String (cleaned)"
        
        return result
    
    def _generate_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate validation report.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Validation report dictionary
        """
        report = {
            'summary': {
                'total_rows': self.validation_stats['total_rows'],
                'cleaned_rows': self.validation_stats['cleaned_rows'],
                'total_columns': len(df.columns),
                'success_rate': self.validation_stats['cleaned_rows'] / self.validation_stats['total_rows'] if self.validation_stats['total_rows'] > 0 else 0
            },
            'columns': {},
            'conversions': self.validation_stats['conversions'],
            'warnings': self.validation_stats['warnings'],
            'errors': self.validation_stats['errors']
        }
        
        # Column-level stats
        for col in df.columns:
            try:
                col_series = df[col]
                report['columns'][col] = {
                    'dtype': str(col_series.dtype),
                    'null_count': int(col_series.isna().sum()),
                    'null_percentage': float(col_series.isna().sum() / len(df) * 100) if len(df) > 0 else 0.0,
                    'unique_values': int(col_series.nunique()),
                    'sample_values': col_series.dropna().head(3).tolist()
                }
            except Exception as e:
                logger.warning(f"Error generating stats for column '{col}': {e}")
                report['columns'][col] = {
                    'dtype': 'unknown',
                    'null_count': 0,
                    'null_percentage': 0.0,
                    'unique_values': 0,
                    'sample_values': []
                }
        
        return report


    def _normalize_campaign_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Normalize campaign-specific column names with channel and funnel terminology.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (normalized_df, column_mappings)
        """
        logger.info("Applying campaign-specific column normalization")
        
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        
        def _norm(name: str) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower())
        
        # Comprehensive column mapping with channel and funnel terminology
        column_mapping = {
            # Campaign identifiers
            "campaign_name": "Campaign_Name",
            "campaign": "Campaign_Name",
            "campaignid": "Campaign_Name",
            "campaign_id": "Campaign_Name",
            "campaign_name_full": "Campaign_Name",
            "ad_name": "Campaign_Name",
            "adset_name": "Campaign_Name",
            
            # Platform/Channel (with variations)
            "platform": "Platform",
            "channel": "Platform",
            "publisher": "Platform",
            "network": "Platform",
            "source": "Platform",
            "media_channel": "Platform",
            "ad_platform": "Platform",
            
            # Spend/Cost
            "spend": "Spend",
            "total_spend": "Spend",
            "total_spent": "Spend",
            "media_spend": "Spend",
            "ad_spend": "Spend",
            "cost": "Spend",
            "costs": "Spend",
            "amount_spent": "Spend",
            "budget": "Spend",
            
            # Conversions (with funnel terminology)
            "conversions": "Conversions",
            "conv": "Conversions",
            "site_visit": "Conversions",
            "site_visits": "Conversions",
            "conversion": "Conversions",
            "leads": "Conversions",
            "signups": "Conversions",
            "purchases": "Conversions",
            "transactions": "Conversions",
            
            # Revenue
            "revenue": "Revenue",
            "conversion_value": "Revenue",
            "total_revenue": "Revenue",
            "sales": "Revenue",
            "purchase_value": "Revenue",
            
            # Impressions
            "impressions": "Impressions",
            "impr": "Impressions",
            "impression": "Impressions",
            "views": "Impressions",
            "reach": "Impressions",
            
            # Clicks
            "clicks": "Clicks",
            "click": "Clicks",
            "link_clicks": "Clicks",
            
            # Date/Time
            "date": "Date",
            "day": "Date",
            "report_date": "Date",
            "date_start": "Date",
            "date_stop": "Date",
            "period": "Date",
            
            # Placement
            "placement": "Placement",
            "ad_placement": "Placement",
            "position": "Placement",
            
            # Funnel Stage
            "funnel_stage": "Funnel_Stage",
            "funnel": "Funnel_Stage",
            "stage": "Funnel_Stage",
            "campaign_type": "Funnel_Stage",
            "objective": "Funnel_Stage",
            
            # Audience
            "audience": "Audience",
            "audience_name": "Audience",
            "target_audience": "Audience",
            "targeting": "Audience",
            
            # Device
            "device": "Device",
            "device_type": "Device",
            "platform_device": "Device",
            
            # Age
            "age": "Age",
            "age_group": "Age",
            "age_range": "Age",
            "age_bucket": "Age",
            
            # Gender
            "gender": "Gender",
            
            # Creative
            "creative": "Creative",
            "creative_name": "Creative",
            "ad_creative": "Creative",
            
            # Ad Type
            "ad_type": "Ad Type", # Changed from Ad_Type
            "format": "Ad Type",
            "ad_format": "Ad Type",
            "creative_type": "Ad Type", # Added support
        }
        
        # Convert columns and apply mapping
        existing = set(str(c) for c in df.columns)
        rename_map = {}
        
        for col in df.columns:
            key = _norm(col)
            target = column_mapping.get(key)
            if target and target not in existing:
                rename_map[str(col)] = target
        
        if rename_map:
            df = df.rename(columns=rename_map)
            logger.info(f"Renamed {len(rename_map)} columns: {list(rename_map.items())[:5]}...")
        
        # Normalize Funnel_Stage values if column exists
        if 'Funnel_Stage' in df.columns:
            df['Funnel_Stage'] = self._normalize_funnel_values(df['Funnel_Stage'])
            logger.info("Normalized Funnel_Stage values")
        
        return df, rename_map
    
    def _normalize_funnel_values(self, series: pd.Series) -> pd.Series:
        """
        Normalize funnel stage values to standard: Awareness, Consideration, Conversion.
        
        Args:
            series: Funnel stage column
            
        Returns:
            Normalized series
        """
        def normalize_funnel(val):
            if pd.isna(val):
                return val
            
            val_str = str(val).strip().lower()
            
            # Funnel stage mappings
            funnel_mapping = {
                # Upper Funnel / Top of Funnel → Awareness
                'upper': 'Awareness',
                'upper funnel': 'Awareness',
                'top': 'Awareness',
                'top of funnel': 'Awareness',
                'tofu': 'Awareness',
                'awareness': 'Awareness',
                'brand awareness': 'Awareness',
                'reach': 'Awareness',
                'impression': 'Awareness',
                'discovery': 'Awareness',
                'prospecting': 'Awareness',
                
                # Middle Funnel → Consideration
                'middle': 'Consideration',
                'middle funnel': 'Consideration',
                'mid': 'Consideration',
                'mid funnel': 'Consideration',
                'mofu': 'Consideration',
                'consideration': 'Consideration',
                'interest': 'Consideration',
                'engagement': 'Consideration',
                'evaluation': 'Consideration',
                'lead generation': 'Consideration',
                'leads': 'Consideration',
                
                # Lower Funnel / Bottom of Funnel → Conversion
                'lower': 'Conversion',
                'lower funnel': 'Conversion',
                'bottom': 'Conversion',
                'bottom of funnel': 'Conversion',
                'bofu': 'Conversion',
                'conversion': 'Conversion',
                'purchase': 'Conversion',
                'sale': 'Conversion',
                'transaction': 'Conversion',
                'action': 'Conversion',
                'acquisition': 'Conversion',
                'retargeting': 'Conversion',
                'remarketing': 'Conversion',
                
                # Additional mappings
                'retention': 'Retention',
                'loyalty': 'Retention',
                'repeat': 'Retention',
            }
            
            # Direct lookup
            if val_str in funnel_mapping:
                return funnel_mapping[val_str]
            
            # Partial match for compound terms
            for key, value in funnel_mapping.items():
                if key in val_str:
                    return value
            
            # Return original if no match (keep as is)
            return str(val).strip()
        
        result = series.apply(normalize_funnel)
        
        # Log the normalization
        unique_before = series.dropna().unique()
        unique_after = result.dropna().unique()
        
        if len(unique_before) != len(unique_after):
            logger.info(f"Funnel stages normalized: {list(unique_before)} → {list(unique_after)}")
        
        return result


# Convenience function
def validate_and_clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Validate and clean DataFrame with comprehensive normalization.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned_df, validation_report)
    """
    # Defensive check: ensure df is actually a DataFrame
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df).__name__}")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    validator = DataValidator()
    return validator.validate_and_clean_dataframe(df)
