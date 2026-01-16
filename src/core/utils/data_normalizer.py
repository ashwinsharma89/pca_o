"""
Data Normalizer Module for Reporting Agent

Handles:
1. Column name variations (typos, different naming conventions)
2. Data type detection and conversion
3. Messy date format parsing
4. Numeric value cleaning (currency symbols, commas, etc.)
5. Fuzzy matching for column mapping
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
from difflib import SequenceMatcher


# =============================================================================
# COLUMN NAME NORMALIZATION & FUZZY MATCHING
# =============================================================================

# Common column name aliases (canonical -> variations)
COLUMN_ALIASES = {
    'date': ['date', 'dt', 'day', 'report_date', 'reporting_date', 'data_date', 'period'],
    'platform': ['platform', 'source', 'channel', 'network', 'media', 'ad_platform', 'advertising_platform'],
    'campaign': ['campaign', 'campaign_name', 'campaign_id', 'camp', 'campaign_title'],
    'adgroup': ['adgroup', 'ad_group', 'adgroup_name', 'ad_group_name', 'ad_set', 'adset', 'group'],
    'ad': ['ad', 'ad_name', 'ad_id', 'creative', 'creative_name', 'ad_title'],
    'impressions': ['impressions', 'impr', 'imps', 'impression', 'views', 'imp'],
    'clicks': ['clicks', 'click', 'clks', 'link_clicks', 'total_clicks'],
    'spend': ['spend', 'cost', 'amount_spent', 'spend_usd', 'cost_usd', 'budget_spent', 'ad_spend', 'total_spend'],
    'conversions': ['conversions', 'conv', 'conversion', 'converts', 'results', 'actions', 'purchases'],
    'revenue': ['revenue', 'rev', 'income', 'sales', 'revenue_usd', 'total_revenue', 'conversion_value'],
    'ctr': ['ctr', 'click_through_rate', 'ctr_%', 'ctr_percent', 'clickthrough_rate'],
    'cpc': ['cpc', 'cost_per_click', 'cpc_usd', 'avg_cpc', 'average_cpc'],
    'cpa': ['cpa', 'cost_per_acquisition', 'cost_per_conversion', 'cpa_usd', 'cost_per_action'],
    'cpm': ['cpm', 'cost_per_mille', 'cost_per_thousand', 'cpm_usd'],
    'roas': ['roas', 'return_on_ad_spend', 'return_on_spend', 'roi'],
    'reach': ['reach', 'unique_reach', 'people_reached', 'unique_users'],
    'frequency': ['frequency', 'freq', 'avg_frequency', 'average_frequency'],
    'video_views': ['video_views', 'views', 'video_plays', 'video_starts', 'thruplay'],
}


def normalize_column_name(name: str) -> str:
    """
    Normalize a column name for comparison.
    
    - Lowercase
    - Replace separators with spaces
    - Remove special characters
    - Strip whitespace
    """
    if not isinstance(name, str):
        name = str(name)
    
    # Lowercase
    normalized = name.lower()
    
    # Replace common separators with space
    normalized = re.sub(r'[_\-\.\/\\]', ' ', normalized)
    
    # Remove special characters except spaces
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
    
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def get_canonical_name(column_name: str) -> Optional[str]:
    """
    Get the canonical name for a column based on known aliases.
    
    Returns None if no match found.
    """
    normalized = normalize_column_name(column_name)
    
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if normalize_column_name(alias) == normalized:
                return canonical
            # Also check if the normalized name contains the alias
            if normalized in normalize_column_name(alias) or normalize_column_name(alias) in normalized:
                return canonical
    
    return None


def similarity_score(s1: str, s2: str) -> float:
    """
    Calculate similarity score between two strings (0.0 to 1.0).
    Uses SequenceMatcher for fuzzy matching.
    """
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def find_best_match(
    target: str,
    candidates: List[str],
    threshold: float = 0.6,
    use_aliases: bool = True
) -> Tuple[Optional[str], float]:
    """
    Find the best matching column name from candidates.
    
    Args:
        target: The column name to match
        candidates: List of available column names
        threshold: Minimum similarity score (0.0 to 1.0)
        use_aliases: Whether to use known column aliases
    
    Returns:
        Tuple of (best_match, score) or (None, 0.0) if no match found
    """
    if not candidates:
        return None, 0.0
    
    target_normalized = normalize_column_name(target)
    target_canonical = get_canonical_name(target)
    
    best_match = None
    best_score = 0.0
    
    for candidate in candidates:
        candidate_normalized = normalize_column_name(candidate)
        candidate_canonical = get_canonical_name(candidate)
        
        # Exact match after normalization
        if target_normalized == candidate_normalized:
            return candidate, 1.0
        
        # Canonical name match (both map to same canonical)
        if use_aliases and target_canonical and candidate_canonical:
            if target_canonical == candidate_canonical:
                return candidate, 0.95
        
        # Fuzzy match
        score = similarity_score(target_normalized, candidate_normalized)
        
        # Boost score if one contains the other
        if target_normalized in candidate_normalized or candidate_normalized in target_normalized:
            score = max(score, 0.8)
        
        if score > best_score:
            best_score = score
            best_match = candidate
    
    if best_score >= threshold:
        return best_match, best_score
    
    return None, 0.0


def auto_map_columns(
    source_columns: List[str],
    target_columns: List[str],
    threshold: float = 0.6
) -> Dict[str, Dict[str, Any]]:
    """
    Automatically map source columns to target columns.
    
    Returns:
        Dict mapping source column -> {target, score, method}
    """
    mappings = {}
    used_targets = set()
    
    for source in source_columns:
        best_match, score = find_best_match(
            source,
            [t for t in target_columns if t not in used_targets],
            threshold=threshold
        )
        
        if best_match:
            method = 'exact' if score == 1.0 else 'canonical' if score >= 0.95 else 'fuzzy'
            mappings[source] = {
                'target': best_match,
                'score': score,
                'method': method
            }
            used_targets.add(best_match)
        else:
            mappings[source] = {
                'target': None,
                'score': 0.0,
                'method': 'unmatched'
            }
    
    return mappings


# =============================================================================
# DATA TYPE DETECTION & CONVERSION
# =============================================================================

def detect_column_type(series: pd.Series) -> str:
    """
    Detect the semantic type of a column.
    
    Returns one of: 'date', 'numeric', 'currency', 'percentage', 'text', 'boolean'
    """
    # Skip if all null
    if series.isna().all():
        return 'text'
    
    # Get non-null sample
    sample = series.dropna()
    if len(sample) == 0:
        return 'text'
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'date'
    
    # Check if numeric
    if pd.api.types.is_numeric_dtype(series):
        col_name = series.name.lower() if series.name else ''
        
        # Check for percentage indicators
        if '%' in col_name or 'rate' in col_name or 'percent' in col_name:
            return 'percentage'
        
        # Check for currency indicators
        if any(kw in col_name for kw in ['usd', 'cost', 'spend', 'revenue', 'price', 'amount', 'budget']):
            return 'currency'
        
        return 'numeric'
    
    # Check if boolean
    if series.dtype == bool:
        return 'boolean'
    
    # String analysis
    sample_str = sample.astype(str)
    
    # Check for date patterns
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY or DD/MM/YYYY
        r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY or DD-MM-YYYY
        r'\d{1,2}/\d{1,2}/\d{2,4}',  # M/D/YY or M/D/YYYY
    ]
    
    date_matches = sum(1 for val in sample_str.head(10) if any(re.match(p, str(val)) for p in date_patterns))
    if date_matches >= len(sample_str.head(10)) * 0.7:
        return 'date'
    
    # Check for currency patterns
    currency_pattern = r'^[\$€£¥₹]?\s*[\d,]+\.?\d*$'
    currency_matches = sum(1 for val in sample_str.head(10) if re.match(currency_pattern, str(val).strip()))
    if currency_matches >= len(sample_str.head(10)) * 0.7:
        return 'currency'
    
    # Check for percentage patterns
    pct_pattern = r'^[\d.]+\s*%?$'
    pct_matches = sum(1 for val in sample_str.head(10) if re.match(pct_pattern, str(val).strip()))
    if pct_matches >= len(sample_str.head(10)) * 0.7:
        return 'percentage'
    
    return 'text'


# =============================================================================
# DATE PARSING
# =============================================================================

# Common date formats to try
DATE_FORMATS = [
    '%Y-%m-%d',           # 2024-01-15
    '%Y/%m/%d',           # 2024/01/15
    '%d-%m-%Y',           # 15-01-2024
    '%d/%m/%Y',           # 15/01/2024
    '%m-%d-%Y',           # 01-15-2024
    '%m/%d/%Y',           # 01/15/2024
    '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
    '%d-%b-%Y',           # 15-Jan-2024
    '%d %b %Y',           # 15 Jan 2024
    '%b %d, %Y',          # Jan 15, 2024
    '%B %d, %Y',          # January 15, 2024
    '%Y%m%d',             # 20240115
    '%d.%m.%Y',           # 15.01.2024
    '%Y.%m.%d',           # 2024.01.15
]


def parse_date(value: Any) -> Optional[datetime]:
    """
    Parse a date value from various formats.
    
    Returns datetime object or None if parsing fails.
    """
    if pd.isna(value):
        return None
    
    if isinstance(value, (datetime, pd.Timestamp)):
        return value
    
    value_str = str(value).strip()
    
    if not value_str:
        return None
    
    # Try pandas first (handles many formats)
    try:
        return pd.to_datetime(value_str)
    except:
        pass
    
    # Try explicit formats
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value_str, fmt)
        except ValueError:
            continue
    
    return None


def normalize_date_column(series: pd.Series) -> pd.Series:
    """
    Normalize a date column to datetime type.
    
    Handles messy date formats and returns a clean datetime series.
    """
    return series.apply(parse_date)


# =============================================================================
# NUMERIC VALUE CLEANING
# =============================================================================

def clean_numeric_value(value: Any) -> Optional[float]:
    """
    Clean a numeric value, handling:
    - Currency symbols ($, €, £, etc.)
    - Thousand separators (commas)
    - Percentage signs
    - Whitespace
    - Parentheses for negatives
    """
    if pd.isna(value):
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    value_str = str(value).strip()
    
    if not value_str:
        return None
    
    # Check for parentheses (negative)
    is_negative = value_str.startswith('(') and value_str.endswith(')')
    if is_negative:
        value_str = value_str[1:-1]
    
    # Remove currency symbols and whitespace
    value_str = re.sub(r'[\$€£¥₹\s]', '', value_str)
    
    # Remove percentage sign
    value_str = value_str.replace('%', '')
    
    # Remove thousand separators (but keep decimal point)
    # Handle both comma and period as thousand separators
    if ',' in value_str and '.' in value_str:
        # Assume comma is thousand separator, period is decimal
        value_str = value_str.replace(',', '')
    elif ',' in value_str:
        # Could be thousand separator or decimal
        # If comma is followed by exactly 2 digits at end, it's decimal
        if re.match(r'^[\d,]+,\d{2}$', value_str):
            value_str = value_str.replace(',', '.', 1)
            value_str = value_str.replace(',', '')
        else:
            value_str = value_str.replace(',', '')
    
    try:
        result = float(value_str)
        return -result if is_negative else result
    except ValueError:
        return None


def normalize_numeric_column(series: pd.Series) -> pd.Series:
    """
    Normalize a numeric column, cleaning currency symbols, commas, etc.
    """
    return series.apply(clean_numeric_value)


# =============================================================================
# DATAFRAME NORMALIZATION
# =============================================================================

def normalize_dataframe(
    df: pd.DataFrame,
    date_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    auto_detect: bool = True
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Normalize a DataFrame for reporting.
    
    Args:
        df: Input DataFrame
        date_columns: Columns to parse as dates (optional)
        numeric_columns: Columns to parse as numeric (optional)
        auto_detect: Whether to auto-detect column types
    
    Returns:
        Tuple of (normalized_df, column_types_dict)
    """
    df = df.copy()
    column_types = {}
    
    for col in df.columns:
        # Detect type
        if auto_detect:
            col_type = detect_column_type(df[col])
        else:
            col_type = 'text'
        
        # Override with explicit columns
        if date_columns and col in date_columns:
            col_type = 'date'
        if numeric_columns and col in numeric_columns:
            col_type = 'numeric'
        
        column_types[col] = col_type
        
        # Apply normalization
        if col_type == 'date':
            df[col] = normalize_date_column(df[col])
        elif col_type in ('numeric', 'currency', 'percentage'):
            df[col] = normalize_numeric_column(df[col])
    
    return df, column_types


def get_column_mapping_report(
    source_df: pd.DataFrame,
    target_columns: List[str],
    threshold: float = 0.6
) -> pd.DataFrame:
    """
    Generate a report of column mappings between source and target.
    
    Returns a DataFrame with mapping details.
    """
    mappings = auto_map_columns(list(source_df.columns), target_columns, threshold)
    
    rows = []
    for source, info in mappings.items():
        source_type = detect_column_type(source_df[source])
        rows.append({
            'Source Column': source,
            'Source Type': source_type,
            'Target Column': info['target'] or '(unmatched)',
            'Match Score': f"{info['score']:.0%}",
            'Match Method': info['method'],
            'Sample Values': str(source_df[source].dropna().head(3).tolist())[:50]
        })
    
    return pd.DataFrame(rows)


# =============================================================================
# VALIDATION & QUALITY CHECKS
# =============================================================================

def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data quality and return a report.
    """
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'issues': [],
        'warnings': [],
        'column_stats': {}
    }
    
    for col in df.columns:
        col_stats = {
            'null_count': int(df[col].isna().sum()),
            'null_pct': f"{df[col].isna().mean():.1%}",
            'unique_count': int(df[col].nunique()),
            'dtype': str(df[col].dtype),
            'detected_type': detect_column_type(df[col])
        }
        report['column_stats'][col] = col_stats
        
        # Check for high null rate
        if df[col].isna().mean() > 0.5:
            report['warnings'].append(f"Column '{col}' has >50% null values")
        
        # Check for all nulls
        if df[col].isna().all():
            report['issues'].append(f"Column '{col}' is entirely null")
    
    # Check for duplicate rows
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        report['warnings'].append(f"Found {dup_count} duplicate rows")
    
    return report
