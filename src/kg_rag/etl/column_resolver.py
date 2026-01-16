"""
KG-RAG Column Resolver

Dynamically resolves platform-specific column names to canonical names.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from functools import lru_cache


logger = logging.getLogger(__name__)


class ColumnResolver:
    """
    Resolves platform column names to canonical names using the alias registry.
    
    Usage:
        resolver = ColumnResolver()
        canonical = resolver.resolve("Amount Spent")  # Returns "spend"
        resolver.resolve_dataframe(df, platform="meta")  # Renames columns in-place
    """
    
    def __init__(self, aliases_path: Optional[str] = None):
        """
        Initialize with column aliases.
        
        Args:
            aliases_path: Path to column_aliases.json (defaults to config dir)
        """
        if aliases_path is None:
            config_dir = Path(__file__).parent.parent / "config"
            aliases_path = config_dir / "column_aliases.json"
        
        self._aliases_path = Path(aliases_path)
        self._aliases = self._load_aliases()
        self._reverse_map = self._build_reverse_map()
    
    def _load_aliases(self) -> Dict[str, Dict[str, Any]]:
        """Load aliases from JSON file."""
        try:
            with open(self._aliases_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Aliases file not found: {self._aliases_path}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in aliases file: {e}")
            return {}
    
    def _build_reverse_map(self) -> Dict[str, str]:
        """Build reverse mapping from aliases to canonical names."""
        reverse_map = {}
        for canonical, config in self._aliases.items():
            # Add canonical name itself
            reverse_map[canonical.lower()] = canonical
            
            # Add all aliases
            for alias in config.get("aliases", []):
                reverse_map[alias.lower()] = canonical
        
        return reverse_map
    
    def resolve(self, column_name: str) -> Optional[str]:
        """
        Resolve a column name to its canonical form.
        
        Args:
            column_name: Platform-specific column name
            
        Returns:
            Canonical name or None if not found
        """
        return self._reverse_map.get(column_name.lower())
    
    def resolve_with_type(self, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a column and get its type and aggregation info.
        
        Args:
            column_name: Platform-specific column name
            
        Returns:
            Dict with canonical, type, and aggregation
        """
        canonical = self.resolve(column_name)
        if canonical and canonical in self._aliases:
            config = self._aliases[canonical]
            return {
                "canonical": canonical,
                "type": config.get("type", "string"),
                "aggregation": config.get("aggregation", "none"),
            }
        return None
    
    def get_all_aliases(self, canonical: str) -> List[str]:
        """Get all aliases for a canonical name."""
        if canonical in self._aliases:
            return self._aliases[canonical].get("aliases", [])
        return []
    
    def get_canonical_names(self) -> List[str]:
        """Get all canonical column names."""
        return list(self._aliases.keys())
    
    def resolve_columns(self, columns: List[str]) -> Dict[str, str]:
        """
        Resolve a list of columns to canonical names.
        
        Args:
            columns: List of column names
            
        Returns:
            Dict mapping original -> canonical (only for resolved columns)
        """
        resolved = {}
        for col in columns:
            canonical = self.resolve(col)
            if canonical:
                resolved[col] = canonical
        return resolved
    
    def resolve_dataframe_columns(
        self,
        columns: List[str],
        keep_unresolved: bool = True
    ) -> Dict[str, str]:
        """
        Create column rename mapping for a DataFrame.
        
        Args:
            columns: List of DataFrame columns
            keep_unresolved: If True, keep columns that can't be resolved
            
        Returns:
            Dict for DataFrame.rename()
        """
        rename_map = {}
        for col in columns:
            canonical = self.resolve(col)
            if canonical:
                rename_map[col] = canonical
            elif keep_unresolved:
                rename_map[col] = col
        return rename_map
    
    def find_column(self, columns: List[str], canonical: str) -> Optional[str]:
        """
        Find which column in a list matches a canonical name.
        
        Args:
            columns: List of column names to search
            canonical: Canonical name to find
            
        Returns:
            The matching column name or None
        """
        # Get all possible aliases
        aliases = set([canonical] + self.get_all_aliases(canonical))
        aliases_lower = {a.lower() for a in aliases}
        
        for col in columns:
            if col.lower() in aliases_lower:
                return col
        
        return None
    
    def detect_platform(self, columns: List[str]) -> Optional[str]:
        """
        Attempt to detect platform from column names.
        
        Args:
            columns: List of column names
            
        Returns:
            Detected platform ID or None
        """
        columns_lower = {c.lower() for c in columns}
        
        # Platform-specific indicators
        indicators = {
            "meta": ["amount_spent", "amount spent", "link clicks", "thruplay"],
            "google_ads": ["avg. cpc", "impr.", "quality score", "conv. value"],
            "linkedin": ["total spent", "lead gen form opens"],
            "tiktok": ["video views p100", "profile visits"],
            "dv360": ["media cost", "insertion order", "line item"],
            "cm360": ["floodlight", "placement id", "rich media"],
            "amazon_sponsored": ["acos", "attributed sales"],
            "snapchat": ["swipe ups", "ecpsu"],
        }
        
        for platform, terms in indicators.items():
            matches = sum(1 for term in terms if term in columns_lower)
            if matches >= 2:
                return platform
        
        return None


# Singleton instance
_resolver: Optional[ColumnResolver] = None


def get_column_resolver() -> ColumnResolver:
    """Get the column resolver singleton."""
    global _resolver
    if _resolver is None:
        _resolver = ColumnResolver()
    return _resolver
