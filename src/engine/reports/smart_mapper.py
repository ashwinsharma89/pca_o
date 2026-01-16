"""
Smart Mapper - Fuzzy-match template headers to data columns.

Features:
- Fuzzy string matching (Levenshtein, token overlap)
- Synonym recognition
- Abbreviation expansion
- Confidence scoring
"""

import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


@dataclass
class ColumnMatch:
    """Represents a match between template header and data column."""
    template_header: str
    data_column: str
    confidence: float
    match_type: str  # exact, synonym, fuzzy, calculated


class SmartMapper:
    """
    Intelligent column mapper using fuzzy matching and synonyms.
    
    Maps template headers to available data columns even when
    naming conventions differ.
    """
    
    # Comprehensive synonym database
    SYNONYMS = {
        # Spend/Cost
        "spend": ["cost", "total_spent", "amount_spent", "budget", "expenditure", 
                  "media_spend", "ad_spend", "total_cost", "amount"],
        "cost": ["spend", "total_spent", "amount_spent"],
        
        # Volume metrics
        "impressions": ["imps", "imp", "views", "displays", "total_impressions"],
        "clicks": ["click", "total_clicks", "link_clicks", "outbound_clicks"],
        "reach": ["unique_reach", "unique_users", "users_reached"],
        
        # Conversions
        "conversions": ["convs", "conv", "total_conversions", "site_visit", 
                       "site_visits", "purchases", "leads", "signups", "actions"],
        "revenue": ["sales", "value", "total_revenue", "income", "gross_revenue",
                   "purchase_value", "conversion_value"],
        
        # Efficiency metrics
        "ctr": ["click_through_rate", "click_rate", "clickthrough"],
        "cpc": ["cost_per_click", "avg_cpc", "average_cpc"],
        "cpm": ["cost_per_mille", "cost_per_thousand", "cost_per_impression"],
        "cpa": ["cost_per_acquisition", "cost_per_action", "cost_per_conversion"],
        "roas": ["return_on_ad_spend", "revenue_over_spend", "return_on_spend"],
        "roi": ["return_on_investment"],
        "cvr": ["conversion_rate", "conv_rate"],
        
        # Dimensions
        "platform": ["channel", "source", "network", "medium", "publisher"],
        "channel": ["platform", "source", "network"],
        "campaign": ["campaign_name", "campaign_id"],
        "ad_group": ["adset", "ad_set", "adgroup"],
        "ad": ["ad_name", "creative", "ad_id"],
        
        # Time
        "date": ["day", "report_date", "period", "reporting_date", "metric_date"],
        "week": ["week_of", "week_number", "week_start"],
        "month": ["month_of", "month_name"],
        
        # Demographics
        "age": ["age_group", "age_range", "age_bracket"],
        "gender": ["sex", "gender_group"],
        "device": ["device_type", "device_category"],
        "region": ["geo", "geography", "location", "country", "city"],
        
        # Funnel
        "funnel": ["funnel_stage", "objective", "campaign_objective"],
    }
    
    # Abbreviation expansions
    ABBREVIATIONS = {
        "ctr": "click through rate",
        "cpc": "cost per click",
        "cpm": "cost per mille",
        "cpa": "cost per acquisition",
        "roas": "return on ad spend",
        "roi": "return on investment",
        "cvr": "conversion rate",
        "imps": "impressions",
        "convs": "conversions",
        "rev": "revenue",
    }
    
    # Calculated metrics (can be derived from other columns)
    CALCULATED_METRICS = {
        "ctr": {"formula": "clicks / impressions * 100", "requires": ["clicks", "impressions"]},
        "cpc": {"formula": "spend / clicks", "requires": ["spend", "clicks"]},
        "cpm": {"formula": "spend / impressions * 1000", "requires": ["spend", "impressions"]},
        "cpa": {"formula": "spend / conversions", "requires": ["spend", "conversions"]},
        "roas": {"formula": "revenue / spend", "requires": ["revenue", "spend"]},
        "cvr": {"formula": "conversions / clicks * 100", "requires": ["conversions", "clicks"]},
    }
    
    def __init__(self, min_confidence: float = 0.5):
        self.min_confidence = min_confidence
        self._build_reverse_synonyms()
    
    def _build_reverse_synonyms(self):
        """Build reverse mapping from synonyms to canonical names."""
        self.reverse_synonyms = {}
        for canonical, syns in self.SYNONYMS.items():
            for syn in syns:
                self.reverse_synonyms[syn.lower()] = canonical
    
    def map(self, template_structure: Dict[str, Any], 
            data_columns: List[str]) -> Dict[str, ColumnMatch]:
        """
        Map template headers to data columns.
        
        Args:
            template_structure: Output from TemplateAnalyzer
            data_columns: List of available data column names
            
        Returns:
            Dictionary mapping template headers to ColumnMatch objects
        """
        mappings = {}
        data_cols_lower = {c.lower(): c for c in data_columns}
        
        # Collect all headers from all tables
        all_headers = set()
        for sheet in template_structure.get("sheets", []):
            for table in sheet.get("tables", []):
                all_headers.update(table.get("headers", []))
        
        for header in all_headers:
            match = self._find_best_match(header, data_columns, data_cols_lower)
            if match:
                mappings[header] = match
        
        logger.info(f"Mapped {len(mappings)}/{len(all_headers)} template headers to data columns")
        return mappings
    
    def _find_best_match(self, header: str, data_columns: List[str],
                        data_cols_lower: Dict[str, str]) -> Optional[ColumnMatch]:
        """Find the best matching data column for a template header."""
        header_clean = self._normalize(header)
        
        # 1. Exact match
        if header_clean in data_cols_lower:
            return ColumnMatch(
                template_header=header,
                data_column=data_cols_lower[header_clean],
                confidence=1.0,
                match_type="exact"
            )
        
        # 2. Synonym match
        canonical = self.reverse_synonyms.get(header_clean)
        if canonical and canonical in data_cols_lower:
            return ColumnMatch(
                template_header=header,
                data_column=data_cols_lower[canonical],
                confidence=0.95,
                match_type="synonym"
            )
        
        # Check if data columns contain a synonym
        for data_col in data_columns:
            data_clean = self._normalize(data_col)
            data_canonical = self.reverse_synonyms.get(data_clean)
            
            if data_canonical and data_canonical == header_clean:
                return ColumnMatch(
                    template_header=header,
                    data_column=data_col,
                    confidence=0.95,
                    match_type="synonym"
                )
        
        # 3. Check for calculated metrics
        if header_clean in self.CALCULATED_METRICS:
            return ColumnMatch(
                template_header=header,
                data_column=f"__CALCULATED__{header_clean}",
                confidence=0.9,
                match_type="calculated"
            )
        
        # 4. Fuzzy match
        best_match = None
        best_score = 0
        
        for data_col in data_columns:
            score = self._similarity_score(header_clean, self._normalize(data_col))
            if score > best_score:
                best_score = score
                best_match = data_col
        
        if best_match and best_score >= self.min_confidence:
            return ColumnMatch(
                template_header=header,
                data_column=best_match,
                confidence=best_score,
                match_type="fuzzy"
            )
        
        return None
    
    def _normalize(self, text: str) -> str:
        """Normalize column name for matching."""
        # Lowercase
        text = text.lower()
        # Remove special characters, keep alphanumeric and underscore
        text = re.sub(r'[^a-z0-9_]', '_', text)
        # Collapse multiple underscores
        text = re.sub(r'_+', '_', text)
        # Strip leading/trailing underscores
        text = text.strip('_')
        return text
    
    def _similarity_score(self, s1: str, s2: str) -> float:
        """Calculate similarity score between two strings."""
        # SequenceMatcher ratio
        ratio = SequenceMatcher(None, s1, s2).ratio()
        
        # Token overlap bonus
        tokens1 = set(s1.split('_'))
        tokens2 = set(s2.split('_'))
        if tokens1 and tokens2:
            overlap = len(tokens1.intersection(tokens2)) / max(len(tokens1), len(tokens2))
            ratio = max(ratio, overlap)
        
        # Substring bonus
        if s1 in s2 or s2 in s1:
            ratio = max(ratio, 0.8)
        
        return ratio
    
    def get_mapping_report(self, mappings: Dict[str, ColumnMatch]) -> str:
        """Generate human-readable mapping report."""
        lines = ["Column Mapping Report", "=" * 50]
        
        by_type = {}
        for header, match in mappings.items():
            by_type.setdefault(match.match_type, []).append((header, match))
        
        for match_type in ["exact", "synonym", "calculated", "fuzzy"]:
            if match_type in by_type:
                lines.append(f"\n{match_type.upper()} MATCHES:")
                for header, match in by_type[match_type]:
                    lines.append(f"  {header} -> {match.data_column} ({match.confidence:.0%})")
        
        return "\n".join(lines)
