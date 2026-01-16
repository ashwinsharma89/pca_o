"""
Dynamic Aggregator - Aggregate and pivot data to match template requirements.

Features:
- Auto-detect aggregation granularity from template
- Group by any dimension(s)
- Calculate derived KPIs (CTR, CPA, ROAS, etc.)
- Pivot data for cross-tab templates
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class AggregationSpec:
    """Specification for an aggregation operation."""
    group_by: List[str]
    metrics: Dict[str, str]  # column -> aggregation type
    pivot_rows: Optional[str] = None
    pivot_cols: Optional[str] = None
    pivot_values: Optional[str] = None


class DynamicAggregator:
    """
    Dynamically aggregate and pivot data based on template requirements.
    
    Handles:
    - Multi-dimensional grouping
    - Sum/Average/Weighted Average aggregations
    - Pivot table generation
    - Calculated metric derivation
    """
    
    # Aggregation rules for metrics
    METRIC_AGGREGATIONS = {
        # Sum metrics
        "spend": "sum",
        "cost": "sum",
        "impressions": "sum",
        "clicks": "sum",
        "conversions": "sum",
        "revenue": "sum",
        "reach": "sum",
        "value": "sum",
        
        # Average metrics (will use weighted average where applicable)
        "ctr": "weighted_avg",
        "cpc": "weighted_avg",
        "cpm": "weighted_avg",
        "cpa": "weighted_avg",
        "roas": "weighted_avg",
        "cvr": "weighted_avg",
        "frequency": "mean",
    }
    
    # Formulas for calculated metrics
    CALCULATED_METRICS = {
        "ctr": lambda df: (df["clicks"] / df["impressions"] * 100).replace([np.inf, -np.inf], 0).fillna(0),
        "cpc": lambda df: (df["spend"] / df["clicks"]).replace([np.inf, -np.inf], 0).fillna(0),
        "cpm": lambda df: (df["spend"] / df["impressions"] * 1000).replace([np.inf, -np.inf], 0).fillna(0),
        "cpa": lambda df: (df["spend"] / df["conversions"]).replace([np.inf, -np.inf], 0).fillna(0),
        "roas": lambda df: (df["revenue"] / df["spend"]).replace([np.inf, -np.inf], 0).fillna(0),
        "cvr": lambda df: (df["conversions"] / df["clicks"] * 100).replace([np.inf, -np.inf], 0).fillna(0),
    }
    
    # Column name normalization mapping
    COLUMN_ALIASES = {
        "total_spent": "spend",
        "amount_spent": "spend",
        "cost": "spend",
        "imps": "impressions",
        "imp": "impressions",
        "convs": "conversions",
        "conv": "conversions",
        "site_visit": "conversions",
    }
    
    def __init__(self):
        pass
    
    def aggregate(self, df: pd.DataFrame, template_structure: Dict[str, Any],
                  column_mappings: Dict[str, Any] = None) -> Dict[str, pd.DataFrame]:
        """
        Aggregate data to match template requirements.
        
        Args:
            df: Input DataFrame with raw data
            template_structure: Output from TemplateAnalyzer
            column_mappings: Output from SmartMapper (optional)
            
        Returns:
            Dictionary of DataFrames, keyed by table identifier
        """
        # Normalize column names
        df = self._normalize_columns(df)
        
        results = {}
        
        for sheet in template_structure.get("sheets", []):
            sheet_name = sheet["name"]
            
            for i, table in enumerate(sheet.get("tables", [])):
                table_key = f"{sheet_name}_table_{i}"
                
                # Determine aggregation needs
                spec = self._build_aggregation_spec(table, df.columns.tolist())
                
                if table.get("is_pivot"):
                    # Handle pivot table
                    result_df = self._create_pivot(df, table)
                else:
                    # Regular aggregation
                    result_df = self._aggregate(df, spec)
                
                results[table_key] = result_df
                logger.info(f"Aggregated {table_key}: {len(result_df)} rows, {len(result_df.columns)} cols")
        
        return results
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to standard format."""
        df = df.copy()
        
        # Lowercase and clean
        df.columns = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
        
        # Apply aliases
        rename_map = {}
        for old, new in self.COLUMN_ALIASES.items():
            if old in df.columns and new not in df.columns:
                rename_map[old] = new
        
        if rename_map:
            df = df.rename(columns=rename_map)
        
        return df
    
    def _build_aggregation_spec(self, table: Dict, available_columns: List[str]) -> AggregationSpec:
        """Build aggregation specification from table structure."""
        dimension_cols = [d.lower() for d in table.get("dimension_cols", [])]
        kpi_cols = [k.lower() for k in table.get("kpi_cols", [])]
        
        # Filter to available columns
        group_by = [d for d in dimension_cols if d in available_columns]
        
        # Build metric aggregation rules
        metrics = {}
        for kpi in kpi_cols:
            kpi_lower = kpi.lower()
            if kpi_lower in available_columns:
                agg_type = self.METRIC_AGGREGATIONS.get(kpi_lower, "sum")
                metrics[kpi_lower] = agg_type
            elif kpi_lower in self.CALCULATED_METRICS:
                # Mark as calculated
                metrics[kpi_lower] = "calculated"
        
        return AggregationSpec(
            group_by=group_by,
            metrics=metrics,
        )
    
    def _aggregate(self, df: pd.DataFrame, spec: AggregationSpec) -> pd.DataFrame:
        """Perform aggregation based on specification."""
        if not spec.group_by:
            # Total aggregation (no grouping)
            result = {}
            for metric, agg_type in spec.metrics.items():
                if agg_type == "calculated":
                    continue  # Handle after summing
                elif metric in df.columns:
                    if agg_type == "sum":
                        result[metric] = df[metric].sum()
                    elif agg_type == "mean":
                        result[metric] = df[metric].mean()
                    elif agg_type == "weighted_avg":
                        result[metric] = df[metric].sum()  # Will recalculate
            
            result_df = pd.DataFrame([result])
        else:
            # Group by dimensions
            available_group_by = [g for g in spec.group_by if g in df.columns]
            
            if not available_group_by:
                logger.warning("No valid group-by columns found, returning aggregate")
                return self._aggregate(df, AggregationSpec(group_by=[], metrics=spec.metrics))
            
            # Build aggregation dict
            agg_dict = {}
            for metric, agg_type in spec.metrics.items():
                if metric in df.columns and agg_type != "calculated":
                    if agg_type == "weighted_avg":
                        agg_dict[metric] = "sum"  # Sum first, then calculate ratio
                    else:
                        agg_dict[metric] = agg_type
            
            if agg_dict:
                result_df = df.groupby(available_group_by, as_index=False).agg(agg_dict)
            else:
                result_df = df[available_group_by].drop_duplicates()
        
        # Add calculated metrics
        result_df = self._add_calculated_metrics(result_df, list(spec.metrics.keys()))
        
        return result_df
    
    def _add_calculated_metrics(self, df: pd.DataFrame, required_metrics: List[str]) -> pd.DataFrame:
        """Add calculated metrics to DataFrame."""
        df = df.copy()
        
        for metric in required_metrics:
            if metric in self.CALCULATED_METRICS and metric not in df.columns:
                formula = self.CALCULATED_METRICS[metric]
                try:
                    df[metric] = formula(df)
                    logger.debug(f"Calculated {metric}")
                except Exception as e:
                    logger.warning(f"Could not calculate {metric}: {e}")
                    df[metric] = 0
        
        return df
    
    def _create_pivot(self, df: pd.DataFrame, table: Dict) -> pd.DataFrame:
        """Create a pivot table based on table structure."""
        row_dim = table.get("row_dimension", "").lower()
        headers = [h.lower() for h in table.get("headers", [])]
        
        # Detect what the column dimension should be
        # Usually it's the first header
        if row_dim and row_dim in df.columns:
            # Get unique values for columns (from headers after the first)
            col_values = headers[1:] if len(headers) > 1 else []
            
            # Try to find a dimension that has these values
            for col in df.columns:
                if df[col].dtype == 'object':
                    unique_vals = df[col].str.lower().unique()
                    overlap = set(col_values).intersection(set(unique_vals))
                    if len(overlap) >= 2:
                        # Found the column dimension
                        # Create pivot
                        value_col = self._find_value_column(df)
                        if value_col:
                            pivot = df.pivot_table(
                                index=row_dim,
                                columns=col,
                                values=value_col,
                                aggfunc='sum',
                                fill_value=0
                            ).reset_index()
                            return pivot
        
        # Fallback: return aggregated by first dimension
        if row_dim and row_dim in df.columns:
            return df.groupby(row_dim, as_index=False).sum(numeric_only=True)
        
        return df
    
    def _find_value_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the best column to use as values in pivot."""
        priority = ["spend", "revenue", "impressions", "clicks", "conversions"]
        for col in priority:
            if col in df.columns:
                return col
        
        # Fallback to first numeric column
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        return numeric_cols[0] if numeric_cols else None
    
    def aggregate_by(self, df: pd.DataFrame, dimensions: List[str],
                     metrics: List[str] = None) -> pd.DataFrame:
        """
        Public method to aggregate by specific dimensions.
        
        Args:
            df: Input DataFrame
            dimensions: List of dimension columns to group by
            metrics: List of metric columns to aggregate (auto-detect if None)
            
        Returns:
            Aggregated DataFrame
        """
        df = self._normalize_columns(df)
        
        if metrics is None:
            # Auto-detect numeric columns as metrics
            metrics = df.select_dtypes(include=[np.number]).columns.tolist()
        
        spec = AggregationSpec(
            group_by=[d.lower() for d in dimensions],
            metrics={m.lower(): self.METRIC_AGGREGATIONS.get(m.lower(), "sum") for m in metrics}
        )
        
        return self._aggregate(df, spec)
    
    def pivot_by(self, df: pd.DataFrame, row_dim: str, col_dim: str,
                 value_col: str, agg_func: str = "sum") -> pd.DataFrame:
        """
        Public method to create a pivot table.
        
        Args:
            df: Input DataFrame
            row_dim: Column for row labels
            col_dim: Column for column labels
            value_col: Column for values
            agg_func: Aggregation function
            
        Returns:
            Pivot table DataFrame
        """
        df = self._normalize_columns(df)
        
        return df.pivot_table(
            index=row_dim.lower(),
            columns=col_dim.lower(),
            values=value_col.lower(),
            aggfunc=agg_func,
            fill_value=0
        ).reset_index()
