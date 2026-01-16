"""
Smart Error Detector - AI-powered data validation and error detection.

Features:
- Detect missing required columns
- Validate data types
- Identify outliers and anomalies
- Check for data quality issues
- Suggest fixes for common problems
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a detected validation error."""
    severity: str  # critical, warning, info
    category: str  # missing_column, data_type, outlier, quality
    message: str
    column: Optional[str] = None
    suggestion: Optional[str] = None
    affected_rows: int = 0


@dataclass
class ValidationReport:
    """Complete validation report."""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    info: List[ValidationError] = field(default_factory=list)
    data_quality_score: float = 100.0


class SmartErrorDetector:
    """
    AI-powered data validation and error detection.
    
    Validates data against template requirements and
    detects potential issues before report generation.
    """
    
    # Expected data types for common columns
    EXPECTED_TYPES = {
        "spend": ["float", "int"],
        "cost": ["float", "int"],
        "impressions": ["float", "int"],
        "clicks": ["float", "int"],
        "conversions": ["float", "int"],
        "revenue": ["float", "int"],
        "date": ["datetime64", "object"],
        "platform": ["object"],
        "channel": ["object"],
        "campaign": ["object"],
    }
    
    # Reasonable ranges for common metrics
    REASONABLE_RANGES = {
        "ctr": (0, 100),  # Percentage
        "cpc": (0, 1000),  # Dollars
        "cpm": (0, 500),  # Dollars
        "cpa": (0, 10000),  # Dollars
        "roas": (0, 100),  # Ratio
        "cvr": (0, 100),  # Percentage
        "spend": (0, 100000000),
        "impressions": (0, 10000000000),
    }
    
    def __init__(self):
        pass
    
    def validate(self, df: pd.DataFrame, 
                template_structure: Dict[str, Any] = None,
                column_mappings: Dict[str, Any] = None) -> ValidationReport:
        """
        Validate data against template requirements.
        
        Args:
            df: Input DataFrame
            template_structure: Optional template structure
            column_mappings: Optional column mappings
            
        Returns:
            ValidationReport with errors and warnings
        """
        errors = []
        warnings = []
        info = []
        
        # Check for empty data
        if df.empty:
            errors.append(ValidationError(
                severity="critical",
                category="empty_data",
                message="DataFrame is empty - no data to process",
                suggestion="Ensure data source contains records"
            ))
            return ValidationReport(is_valid=False, errors=errors)
        
        # Check required columns from template
        if template_structure:
            col_errors = self._check_required_columns(df, template_structure)
            errors.extend(col_errors)
        
        # Check data types
        type_warnings = self._check_data_types(df)
        warnings.extend(type_warnings)
        
        # Check for missing values
        null_warnings = self._check_null_values(df)
        warnings.extend(null_warnings)
        
        # Check for outliers
        outlier_warnings = self._check_outliers(df)
        warnings.extend(outlier_warnings)
        
        # Check for duplicates
        dup_info = self._check_duplicates(df)
        info.extend(dup_info)
        
        # Check for negative values in positive-only columns
        neg_warnings = self._check_negative_values(df)
        warnings.extend(neg_warnings)
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(df, errors, warnings)
        
        return ValidationReport(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            info=info,
            data_quality_score=quality_score
        )
    
    def _check_required_columns(self, df: pd.DataFrame, 
                               template_structure: Dict[str, Any]) -> List[ValidationError]:
        """Check if required columns are present."""
        errors = []
        df_cols_lower = {c.lower() for c in df.columns}
        
        for sheet in template_structure.get("sheets", []):
            for table in sheet.get("tables", []):
                # Check dimension columns
                for dim in table.get("dimension_cols", []):
                    dim_lower = dim.lower()
                    if dim_lower not in df_cols_lower:
                        errors.append(ValidationError(
                            severity="critical",
                            category="missing_column",
                            message=f"Required dimension column '{dim}' not found in data",
                            column=dim,
                            suggestion=f"Add column '{dim}' or ensure it exists under a different name"
                        ))
                
                # Check KPI columns (less strict - may be calculable)
                for kpi in table.get("kpi_cols", []):
                    kpi_lower = kpi.lower()
                    if kpi_lower not in df_cols_lower:
                        # Check if it's a calculable metric
                        calculable = ["ctr", "cpc", "cpm", "cpa", "roas", "cvr"]
                        if any(k in kpi_lower for k in calculable):
                            continue  # Can be calculated
                        errors.append(ValidationError(
                            severity="warning",
                            category="missing_column",
                            message=f"KPI column '{kpi}' not found in data",
                            column=kpi,
                            suggestion=f"Add column '{kpi}' or it will be filled with zeros"
                        ))
        
        return errors
    
    def _check_data_types(self, df: pd.DataFrame) -> List[ValidationError]:
        """Check for data type issues."""
        warnings = []
        
        for col in df.columns:
            col_lower = col.lower()
            expected = self.EXPECTED_TYPES.get(col_lower)
            
            if expected:
                actual_type = str(df[col].dtype)
                if not any(t in actual_type for t in expected):
                    warnings.append(ValidationError(
                        severity="warning",
                        category="data_type",
                        message=f"Column '{col}' has unexpected type '{actual_type}'",
                        column=col,
                        suggestion=f"Expected types: {expected}"
                    ))
        
        return warnings
    
    def _check_null_values(self, df: pd.DataFrame) -> List[ValidationError]:
        """Check for null/missing values."""
        warnings = []
        
        for col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = null_count / len(df) * 100
            
            if null_pct > 0:
                severity = "warning" if null_pct < 20 else "critical" if null_pct > 50 else "warning"
                warnings.append(ValidationError(
                    severity=severity,
                    category="null_values",
                    message=f"Column '{col}' has {null_count} null values ({null_pct:.1f}%)",
                    column=col,
                    affected_rows=int(null_count),
                    suggestion="Fill nulls with 0 for metrics or drop rows for dimensions"
                ))
        
        return warnings
    
    def _check_outliers(self, df: pd.DataFrame) -> List[ValidationError]:
        """Check for statistical outliers."""
        warnings = []
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            col_lower = col.lower()
            
            # Check against known reasonable ranges
            if col_lower in self.REASONABLE_RANGES:
                min_val, max_val = self.REASONABLE_RANGES[col_lower]
                outside = ((df[col] < min_val) | (df[col] > max_val)).sum()
                
                if outside > 0:
                    warnings.append(ValidationError(
                        severity="warning",
                        category="outlier",
                        message=f"Column '{col}' has {outside} values outside expected range ({min_val}-{max_val})",
                        column=col,
                        affected_rows=int(outside),
                        suggestion="Review outlier values for data entry errors"
                    ))
            else:
                # Use IQR method for unknown columns
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)).sum()
                
                if outliers > len(df) * 0.05:  # More than 5% outliers
                    warnings.append(ValidationError(
                        severity="info",
                        category="outlier",
                        message=f"Column '{col}' has {outliers} statistical outliers (>1.5 IQR)",
                        column=col,
                        affected_rows=int(outliers)
                    ))
        
        return warnings
    
    def _check_duplicates(self, df: pd.DataFrame) -> List[ValidationError]:
        """Check for duplicate rows."""
        info = []
        
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            info.append(ValidationError(
                severity="info",
                category="duplicates",
                message=f"Found {dup_count} duplicate rows",
                affected_rows=int(dup_count),
                suggestion="Consider deduplicating before report generation"
            ))
        
        return info
    
    def _check_negative_values(self, df: pd.DataFrame) -> List[ValidationError]:
        """Check for negative values in positive-only columns."""
        warnings = []
        
        positive_only = ["spend", "cost", "impressions", "clicks", "conversions", "revenue"]
        
        for col in df.columns:
            col_lower = col.lower()
            if any(p in col_lower for p in positive_only):
                if df[col].dtype in [np.float64, np.int64, float, int]:
                    neg_count = (df[col] < 0).sum()
                    if neg_count > 0:
                        warnings.append(ValidationError(
                            severity="warning",
                            category="negative_values",
                            message=f"Column '{col}' has {neg_count} negative values",
                            column=col,
                            affected_rows=int(neg_count),
                            suggestion="Replace negative values with 0 or investigate data source"
                        ))
        
        return warnings
    
    def _calculate_quality_score(self, df: pd.DataFrame, 
                                errors: List[ValidationError],
                                warnings: List[ValidationError]) -> float:
        """Calculate overall data quality score."""
        score = 100.0
        
        # Deduct for critical errors
        score -= len([e for e in errors if e.severity == "critical"]) * 20
        
        # Deduct for warnings
        score -= len([w for w in warnings if w.severity == "warning"]) * 5
        
        # Calculate completeness
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isna().sum().sum()
        completeness = (1 - null_cells / total_cells) * 100 if total_cells > 0 else 0
        
        # Weighted score
        score = max(0, min(100, score * 0.7 + completeness * 0.3))
        
        return round(score, 1)
    
    def get_validation_summary(self, report: ValidationReport) -> str:
        """Generate human-readable validation summary."""
        lines = ["Data Validation Summary", "=" * 40]
        
        lines.append(f"Status: {'✅ VALID' if report.is_valid else '❌ INVALID'}")
        lines.append(f"Quality Score: {report.data_quality_score}/100")
        
        if report.errors:
            lines.append(f"\n🔴 Critical Errors ({len(report.errors)}):")
            for e in report.errors[:5]:
                lines.append(f"  - {e.message}")
                if e.suggestion:
                    lines.append(f"    → {e.suggestion}")
        
        if report.warnings:
            lines.append(f"\n🟡 Warnings ({len(report.warnings)}):")
            for w in report.warnings[:5]:
                lines.append(f"  - {w.message}")
        
        if report.info:
            lines.append(f"\nℹ️ Info ({len(report.info)}):")
            for i in report.info[:3]:
                lines.append(f"  - {i.message}")
        
        return "\n".join(lines)
