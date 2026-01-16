"""
Semantic Column Resolution Engine

Maps user terms to actual database column names with intelligent matching.
Supports fuzzy matching, semantic aliases, and context-aware resolution.
"""

from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
from loguru import logger
import re


class ColumnResolver:
    """
    Maps user terms to actual column names with fuzzy matching.
    Enterprise-grade column resolution for natural language queries.
    """
    
    # Semantic mappings: user_term -> [possible_column_names]
    # Order matters - first match is preferred
    SEMANTIC_MAPPING: Dict[str, List[str]] = {
        # Campaign identifiers
        'campaign': ['Campaign_Name_Full', 'campaign_name', 'Campaign', 'campaign_id', 'Campaign_Name'],
        'campaign name': ['Campaign_Name_Full', 'campaign_name', 'Campaign_Name', 'Campaign'],
        'campaign_name': ['Campaign_Name_Full', 'campaign_name', 'Campaign_Name'],
        'ad': ['Campaign_Name_Full', 'Ad_Name', 'ad_name', 'Creative'],
        'ad name': ['Campaign_Name_Full', 'Ad_Name', 'ad_name'],
        'creative': ['Creative', 'Ad_Name', 'Campaign_Name_Full'],
        
        # Spend/Cost variations
        'spend': ['Total Spent', 'Total_Spent', 'Spend', 'Cost', 'spend', 'cost', 'Budget'],
        'cost': ['Total Spent', 'Cost', 'cost', 'Total_Spent', 'Spend'],
        'total spent': ['Total Spent', 'Total_Spent'],
        'total_spent': ['Total Spent', 'Total_Spent'],
        'budget': ['Total Spent', 'Budget', 'budget', 'Spend'],
        'investment': ['Total Spent', 'Spend', 'Cost', 'Budget'],
        'amount spent': ['Total Spent', 'Spend'],
        'money spent': ['Total Spent', 'Spend'],
        
        # Click metrics
        'clicks': ['Clicks', 'clicks', 'Link_Clicks', 'Total_Clicks'],
        'click': ['Clicks', 'clicks'],
        'taps': ['Clicks', 'clicks'],
        'link clicks': ['Link_Clicks', 'Clicks'],
        
        # Impression metrics
        'impressions': ['Impressions', 'impressions', 'Impr', 'Views', 'Reach'],
        'impression': ['Impressions', 'impressions'],
        'views': ['Impressions', 'Views', 'impressions', 'Video_Views'],
        'reach': ['Reach', 'Impressions', 'impressions'],
        'eyeballs': ['Impressions', 'Reach'],
        
        # Conversion metrics
        'conversions': ['Site Visit', 'Conversions', 'conversions', 'Site_Visit', 'Purchases', 'Leads', 'Actions', 'Results'],
        'conversion': ['Site Visit', 'Conversions', 'conversions'],
        'site visit': ['Site Visit', 'Site_Visit', 'Conversions'],
        'site_visit': ['Site Visit', 'Site_Visit'],
        'site visits': ['Site Visit', 'Site_Visit', 'Conversions'],
        'leads': ['Leads', 'Conversions', 'Site Visit', 'Inquiries'],
        'sales': ['Conversions', 'Purchases', 'Site Visit', 'Revenue', 'Orders'],
        'acquisitions': ['Site Visit', 'Conversions', 'Purchases', 'Customers'],
        'purchases': ['Purchases', 'Conversions', 'Site Visit', 'Orders'],
        
        # Revenue metrics (including year-specific columns)
        'revenue': ['Revenue_2024', 'Revenue_2025', 'Revenue', 'Conversion_Value', 'revenue', 'Value', 'Sales'],
        'value': ['Revenue_2024', 'Revenue_2025', 'Conversion_Value', 'Revenue', 'Value'],
        'conversion value': ['Revenue_2024', 'Revenue_2025', 'Conversion_Value', 'Revenue'],
        'conversion_value': ['Revenue_2024', 'Revenue_2025', 'Conversion_Value', 'Revenue'],
        'sales revenue': ['Revenue_2024', 'Revenue_2025', 'Revenue', 'Conversion_Value'],
        
        # Channel/Platform dimensions
        'channel': ['Channel', 'channel', 'Marketing_Channel', 'Medium'],
        'marketing channel': ['Channel', 'Marketing_Channel'],
        'medium': ['Channel', 'Medium', 'Marketing_Channel'],
        'platform': ['Platform', 'platform', 'Ad_Platform', 'Network', 'Source'],
        'network': ['Platform', 'Network', 'Ad_Platform'],
        'source': ['Platform', 'Source', 'Channel'],
        'ad network': ['Platform', 'Ad_Platform', 'Network'],
        'ad platform': ['Platform', 'Ad_Platform'],
        
        # Funnel dimensions
        'funnel': ['Funnel', 'funnel', 'Funnel_Stage', 'Stage', 'Marketing_Stage'],
        'funnel stage': ['Funnel', 'Funnel_Stage', 'Stage'],
        'funnel_stage': ['Funnel', 'Funnel_Stage'],
        'stage': ['Funnel', 'Stage', 'Funnel_Stage'],
        'journey stage': ['Funnel', 'Funnel_Stage', 'Stage'],
        'tofu': ['Funnel', 'Funnel_Stage'],
        'mofu': ['Funnel', 'Funnel_Stage'],
        'bofu': ['Funnel', 'Funnel_Stage'],
        
        # Device dimensions
        'device': ['Device_Type', 'Device', 'device', 'device_type'],
        'device type': ['Device_Type', 'Device', 'device_type'],
        'device_type': ['Device_Type', 'device_type'],
        'mobile': ['Device_Type', 'Device'],
        'desktop': ['Device_Type', 'Device'],
        'tablet': ['Device_Type', 'Device'],
        
        # Ad type dimensions
        'ad type': ['Ad_Type', 'Ad Type', 'ad_type', 'Creative_Type'],
        'ad_type': ['Ad_Type', 'Ad Type', 'ad_type'],
        'creative type': ['Ad_Type', 'Creative_Type'],
        
        # Campaign objective
        'objective': ['Campaign_Objective', 'Objective', 'objective', 'Goal'],
        'campaign objective': ['Campaign_Objective', 'Objective'],
        'goal': ['Campaign_Objective', 'Objective', 'Goal'],
        
        # Temporal columns
        'date': ['Date', 'Week Range', 'Week', 'Day', 'Period', 'date', 'Time'],
        'week': ['Week Range', 'Week', 'week', 'Date'],
        'week range': ['Week Range', 'Week'],
        'month': ['Month', 'month', 'Date', 'Period'],
        'year': ['Year', 'year', 'Date'],
        'day': ['Day', 'Date', 'day'],
        'period': ['Period', 'Date', 'Week Range'],
        'time': ['Date', 'Time', 'Period', 'Week Range'],
        
        # Calculated metrics (for reference)
        'ctr': ['CTR', 'ctr', 'Click_Through_Rate'],
        'click through rate': ['CTR', 'Click_Through_Rate'],
        'cpc': ['CPC', 'cpc', 'Cost_Per_Click'],
        'cost per click': ['CPC', 'Cost_Per_Click'],
        'cpm': ['CPM', 'cpm', 'Cost_Per_Mille'],
        'cost per mille': ['CPM', 'Cost_Per_Mille'],
        'cpa': ['CPA', 'cpa', 'Cost_Per_Acquisition'],
        'cost per acquisition': ['CPA', 'Cost_Per_Acquisition'],
        'roas': ['ROAS', 'roas', 'Return_On_Ad_Spend'],
        'return on ad spend': ['ROAS', 'Return_On_Ad_Spend'],
        
        # Other dimensions
        'language': ['Language_Targeting', 'Language', 'language'],
        'targeting': ['Language_Targeting', 'Audience_Targeting'],
        'landing page': ['Landing_Page_Type', 'Landing_Page'],
        'dayparting': ['Dayparting_Schedule', 'Dayparting'],
    }
    
    # Common typos and misspellings
    TYPO_CORRECTIONS: Dict[str, str] = {
        'campain': 'campaign',
        'campaing': 'campaign',
        'campagin': 'campaign',
        'chanell': 'channel',
        'chanel': 'channel',
        'platfrom': 'platform',
        'plateform': 'platform',
        'impresions': 'impressions',
        'impressons': 'impressions',
        'converison': 'conversion',
        'convesion': 'conversion',
        'revnue': 'revenue',
        'reveune': 'revenue',
        'spendings': 'spend',
        'spendt': 'spend',
        'clikcs': 'clicks',
        'clciks': 'clicks',
    }
    
    def __init__(self, schema_columns: List[str] = None):
        """
        Initialize the column resolver.
        
        Args:
            schema_columns: List of actual column names in the database
        """
        self.schema_columns = schema_columns or []
        self._column_cache: Dict[str, str] = {}
        
    def set_schema(self, columns: List[str]) -> None:
        """Update the schema columns and clear cache."""
        self.schema_columns = columns
        self._column_cache.clear()
        logger.debug(f"ColumnResolver schema updated with {len(columns)} columns")
    
    def get_categorical_columns(self) -> List[str]:
        """
        Identify categorical/dimensional columns that should not have hardcoded values.
        
        These are columns where values change over time and should be queried dynamically
        using GROUP BY rather than hardcoded in WHERE clauses.
        
        Returns:
            List of categorical column names
        """
        categorical_patterns = [
            # Funnel/Stage columns
            'funnel', 'stage',
            # Platform/Channel columns
            'platform', 'channel', 'network', 'source', 'medium',
            # Device columns
            'device',
            # Ad/Creative columns
            'ad_type', 'ad type', 'creative_type', 'creative_format',
            # Geographic columns
            'region', 'geographic', 'country', 'state', 'dma', 'city',
            # Audience columns
            'audience', 'segment', 'demographic', 'age_group', 'gender',
            # Campaign attributes
            'objective', 'goal', 'targeting_type', 'bid_strategy',
            # Placement columns
            'placement',
            # Language columns
            'language',
            # Other categorical dimensions
            'customer_lifecycle', 'seasonality', 'competitive_strategy'
        ]
        
        categorical_cols = []
        for col in self.schema_columns:
            col_lower = col.lower().replace('_', ' ').replace('-', ' ')
            # Check if column name contains any categorical pattern
            if any(pattern in col_lower for pattern in categorical_patterns):
                categorical_cols.append(col)
        
        return categorical_cols
        
    def resolve(self, term: str, available_columns: List[str] = None) -> Optional[str]:
        """
        Find the best matching column for a user term.
        
        Args:
            term: User's term (e.g., 'cost', 'spend', 'campaign_name')
            available_columns: Override schema columns if provided
            
        Returns:
            Best matching column name, or None if no match found
        """
        columns = available_columns or self.schema_columns
        if not columns:
            logger.warning("No columns available for resolution")
            return None
            
        # Check cache first
        cache_key = f"{term.lower()}:{','.join(sorted(columns))}"
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]
        
        normalized_term = self._normalize_term(term)
        result = None
        
        # 1. Exact match (case-insensitive)
        for col in columns:
            if col.lower() == normalized_term.lower():
                result = col
                break
                
        # 2. Semantic mapping
        if not result:
            result = self._semantic_match(normalized_term, columns)
            
        # 3. Partial match (column contains term or vice versa)
        if not result:
            result = self._partial_match(normalized_term, columns)
            
        # 4. Fuzzy match (Levenshtein-based)
        if not result:
            result = self._fuzzy_match(normalized_term, columns, threshold=0.6)
        
        # Cache the result
        if result:
            self._column_cache[cache_key] = result
            logger.debug(f"Resolved '{term}' → '{result}'")
        else:
            logger.debug(f"Could not resolve '{term}', available: {columns[:5]}...")
            
        return result
    
    def resolve_multiple(self, terms: List[str], available_columns: List[str] = None) -> Dict[str, Optional[str]]:
        """
        Resolve multiple terms at once.
        
        Args:
            terms: List of user terms
            available_columns: Override schema columns if provided
            
        Returns:
            Dictionary mapping each term to its resolved column (or None)
        """
        return {term: self.resolve(term, available_columns) for term in terms}
    
    def suggest_alternatives(self, term: str, available_columns: List[str] = None, top_k: int = 3) -> List[str]:
        """
        Suggest alternative column names for a term that couldn't be resolved exactly.
        
        Args:
            term: User's term
            available_columns: Override schema columns if provided
            top_k: Number of suggestions to return
            
        Returns:
            List of suggested column names
        """
        columns = available_columns or self.schema_columns
        normalized_term = self._normalize_term(term)
        
        # Get similarity scores for all columns
        scores = []
        for col in columns:
            # Combine multiple similarity measures
            name_sim = self._similarity(normalized_term, col.lower())
            
            # Check if term appears in semantic mappings that map to this column
            semantic_bonus = 0.0
            for mapping_term, mapping_cols in self.SEMANTIC_MAPPING.items():
                if col in mapping_cols:
                    term_sim = self._similarity(normalized_term, mapping_term)
                    semantic_bonus = max(semantic_bonus, term_sim * 0.3)
            
            scores.append((col, name_sim + semantic_bonus))
        
        # Sort by score and return top_k
        scores.sort(key=lambda x: x[1], reverse=True)
        return [col for col, _ in scores[:top_k]]
    
    def get_column_description(self, column: str) -> str:
        """
        Get a human-readable description of what a column represents.
        
        Args:
            column: Column name
            
        Returns:
            Description string
        """
        # Reverse lookup: find which semantic terms map to this column
        related_terms = []
        for term, cols in self.SEMANTIC_MAPPING.items():
            if column in cols:
                related_terms.append(term)
        
        if related_terms:
            return f"{column} (also known as: {', '.join(related_terms[:3])})"
        return column
    
    def _normalize_term(self, term: str) -> str:
        """Normalize a user term for matching."""
        # Fix common typos
        normalized = term.lower().strip()
        if normalized in self.TYPO_CORRECTIONS:
            normalized = self.TYPO_CORRECTIONS[normalized]
        
        # Remove common noise words
        normalized = re.sub(r'\b(the|a|an|by|for|of|in|on|to)\b', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _semantic_match(self, term: str, columns: List[str]) -> Optional[str]:
        """Match using semantic mappings."""
        if term in self.SEMANTIC_MAPPING:
            for candidate in self.SEMANTIC_MAPPING[term]:
                # Check exact match
                if candidate in columns:
                    return candidate
                # Check case-insensitive match
                for col in columns:
                    if col.lower() == candidate.lower():
                        return col
        return None
    
    def _partial_match(self, term: str, columns: List[str]) -> Optional[str]:
        """Match if term is contained in column name or vice versa."""
        term_lower = term.lower().replace(' ', '_').replace('-', '_')
        
        for col in columns:
            col_lower = col.lower().replace(' ', '_').replace('-', '_')
            
            # Term is contained in column name
            if term_lower in col_lower:
                return col
            
            # Column name is contained in term
            if col_lower in term_lower and len(col_lower) > 3:
                return col
                
        return None
    
    def _fuzzy_match(self, term: str, columns: List[str], threshold: float = 0.6) -> Optional[str]:
        """Match using fuzzy string similarity."""
        best_match = None
        best_score = threshold
        
        term_lower = term.lower().replace(' ', '_').replace('-', '_')
        
        for col in columns:
            col_lower = col.lower().replace(' ', '_').replace('-', '_')
            score = self._similarity(term_lower, col_lower)
            
            if score > best_score:
                best_score = score
                best_match = col
                
        return best_match
    
    @staticmethod
    def _similarity(a: str, b: str) -> float:
        """Calculate string similarity using SequenceMatcher."""
        return SequenceMatcher(None, a, b).ratio()


# Pre-built instance for convenience
_default_resolver = ColumnResolver()


def resolve_column(term: str, available_columns: List[str]) -> Optional[str]:
    """
    Convenience function to resolve a column name.
    
    Args:
        term: User's term
        available_columns: List of actual column names
        
    Returns:
        Best matching column name, or None
    """
    _default_resolver.set_schema(available_columns)
    return _default_resolver.resolve(term)


def suggest_columns(term: str, available_columns: List[str], top_k: int = 3) -> List[str]:
    """
    Convenience function to suggest column alternatives.
    
    Args:
        term: User's term
        available_columns: List of actual column names
        top_k: Number of suggestions
        
    Returns:
        List of suggested column names
    """
    _default_resolver.set_schema(available_columns)
    return _default_resolver.suggest_alternatives(term, top_k=top_k)
