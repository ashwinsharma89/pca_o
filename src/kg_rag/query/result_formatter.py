"""
KG-RAG Result Formatter

Formats Neo4j query results into structured JSON for API responses.
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import math


logger = logging.getLogger(__name__)


@dataclass
class FormattedResult:
    """Container for formatted query results."""
    success: bool
    data: List[Dict[str, Any]]
    summary: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "summary": self.summary,
            "metadata": self.metadata,
        }


class ResultFormatter:
    """
    Format Neo4j query results for API responses.
    
    Handles:
    - Numeric formatting (rounding, currency)
    - Null handling
    - Summary statistics
    - Column ordering
    
    Usage:
        formatter = ResultFormatter()
        formatted = formatter.format(results, query_type="platform_performance")
    """
    
    # Columns to round to 2 decimal places
    CURRENCY_COLUMNS = ["spend", "revenue", "cpc", "cpm", "cpa", "budget"]
    
    # Columns to round to 2 decimal places (percentages)
    PERCENTAGE_COLUMNS = ["ctr", "cvr", "roas", "vtr", "viewability"]
    
    # Column display order
    COLUMN_ORDER = [
        "period", "channel", "platform", "campaign_id", "campaign_name",
        "device", "age_range", "country", "interest", "bid_strategy", "funnel_stage",
        "campaigns", "impressions", "clicks", "spend",
        "conversions", "revenue", "ctr", "cpc", "cpm", "cpa", "roas",
        "date", "week_start", "month"
    ]
    
    def format(
        self,
        results: List[Dict[str, Any]],
        query_type: Optional[str] = None,
        include_summary: bool = True
    ) -> FormattedResult:
        """
        Format query results.
        
        Args:
            results: Raw Neo4j results
            query_type: Type of query for context-specific formatting
            include_summary: Include summary statistics
            
        Returns:
            FormattedResult with data and metadata
        """
        if not results:
            return FormattedResult(
                success=True,
                data=[],
                summary={"count": 0},
                metadata={"query_type": query_type}
            )
        
        # Format data
        formatted_data = [self._format_row(row) for row in results]
        
        # Reorder columns
        formatted_data = [self._reorder_columns(row) for row in formatted_data]
        
        # Calculate summary
        summary = {}
        if include_summary:
            summary = self._calculate_summary(formatted_data)
        
        return FormattedResult(
            success=True,
            data=formatted_data,
            summary=summary,
            metadata={
                "query_type": query_type,
                "row_count": len(formatted_data),
                "columns": list(formatted_data[0].keys()) if formatted_data else [],
            }
        )
    
    def _format_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Format a single result row."""
        formatted = {}
        
        for key, value in row.items():
            # Handle None
            if value is None:
                formatted[key] = None
                continue
            
            # Sanitize NaN/Inf for JSON
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                formatted[key] = None
                continue
            
            # ... continue with existing logic
            
            # Convert Neo4j types to JSON-serializable
            value = self._convert_neo4j_type(value)
            
            # Format based on column type
            if key.lower() in self.CURRENCY_COLUMNS:
                formatted[key] = self._format_currency(value)
            elif key.lower() in self.PERCENTAGE_COLUMNS:
                formatted[key] = self._format_percentage(value)
            elif isinstance(value, float):
                formatted[key] = round(value, 2)
            else:
                formatted[key] = value
        
        return formatted
    
    def _convert_neo4j_type(self, value: Any) -> Any:
        """Convert Neo4j-specific types to JSON-serializable formats."""
        # Handle Neo4j Date/Time types
        type_name = type(value).__name__
        
        if type_name == 'Date':
            # neo4j.time.Date
            return value.iso_format()
        elif type_name == 'DateTime':
            # neo4j.time.DateTime
            return value.iso_format()
        elif type_name == 'Time':
            return value.iso_format()
        elif type_name == 'Duration':
            return str(value)
        elif type_name == 'Node':
            # Neo4j Node - return properties dict
            return dict(value)
        elif type_name == 'Relationship':
            return dict(value)
        elif type_name == 'Path':
            return str(value)
        elif hasattr(value, 'iso_format'):
            # Generic date/time with iso_format method
            return value.iso_format()
        elif hasattr(value, 'isoformat'):
            # Python datetime types
            return value.isoformat()
        
        return value
    
    def _format_currency(self, value: Any) -> Optional[float]:
        """Format currency value."""
        if value is None:
            return None
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None
    
    def _format_percentage(self, value: Any) -> Optional[float]:
        """Format percentage value."""
        if value is None:
            return None
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None
    
    def _reorder_columns(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Reorder columns based on COLUMN_ORDER."""
        reordered = {}
        
        # Add columns in preferred order
        for col in self.COLUMN_ORDER:
            if col in row:
                reordered[col] = row[col]
        
        # Add remaining columns
        for col, value in row.items():
            if col not in reordered:
                reordered[col] = value
        
        return reordered
    
    def _calculate_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics."""
        summary = {
            "count": len(data),
        }
        
        if not data:
            return summary
        
        # Aggregate numeric columns
        numeric_cols = ["impressions", "clicks", "spend", "conversions", "revenue"]
        
        for col in numeric_cols:
            values = [row[col] for row in data if col in row and row[col] is not None]
            if values:
                summary[f"total_{col}"] = round(sum(values), 2)
        
        # Calculate derived metrics from totals
        if "total_impressions" in summary and summary["total_impressions"] > 0:
            if "total_clicks" in summary:
                summary["avg_ctr"] = round(
                    summary["total_clicks"] / summary["total_impressions"] * 100, 2
                )
        
        if "total_clicks" in summary and summary["total_clicks"] > 0:
            if "total_spend" in summary:
                summary["avg_cpc"] = round(
                    summary["total_spend"] / summary["total_clicks"], 2
                )
        
        if "total_spend" in summary and summary["total_spend"] > 0:
            if "total_revenue" in summary:
                summary["avg_roas"] = round(
                    summary["total_revenue"] / summary["total_spend"], 2
                )
        
        if "total_conversions" in summary and summary["total_conversions"] > 0:
            if "total_spend" in summary:
                summary["avg_cpa"] = round(
                    summary["total_spend"] / summary["total_conversions"], 2
                )
        
        return summary
    
    def format_for_llm(
        self,
        results: List[Dict[str, Any]],
        max_rows: int = 10
    ) -> str:
        """
        Format results as text for LLM context.
        
        Args:
            results: Query results
            max_rows: Maximum rows to include
            
        Returns:
            Text representation of results
        """
        if not results:
            return "No results found."
        
        # Format results
        formatted = self.format(results).data[:max_rows]
        
        # Build text representation
        lines = []
        
        # Header
        columns = list(formatted[0].keys())
        lines.append(" | ".join(columns))
        lines.append("-" * len(lines[0]))
        
        # Data rows
        for row in formatted:
            values = [str(row.get(col, "")) for col in columns]
            lines.append(" | ".join(values))
        
        if len(results) > max_rows:
            lines.append(f"\n... and {len(results) - max_rows} more rows")
        
        return "\n".join(lines)
    
    def to_chart_data(
        self,
        results: List[Dict[str, Any]],
        x_column: str,
        y_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Format results for chart visualization.
        
        Args:
            results: Query results
            x_column: Column for x-axis
            y_columns: Columns for y-axis series
            
        Returns:
            Chart-ready data structure
        """
        formatted = self.format(results).data
        
        return {
            "labels": [row.get(x_column, "") for row in formatted],
            "datasets": [
                {
                    "name": col,
                    "data": [row.get(col, 0) for row in formatted]
                }
                for col in y_columns
            ]
        }
