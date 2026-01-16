"""
Intelligent Report Engine - Orchestrates the entire report generation flow.

Usage:
    engine = IntelligentReportEngine()
    output_path = engine.generate(template_path, data_df)
"""

import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

import pandas as pd

from .template_analyzer import TemplateAnalyzer
from .smart_mapper import SmartMapper
from .dynamic_aggregator import DynamicAggregator
from .universal_populator import UniversalPopulator
from .date_handler import DateHandler
from .ai_interpreter import AITemplateInterpreter, TemplateIntent
from .error_detector import SmartErrorDetector, ValidationReport

logger = logging.getLogger(__name__)


class IntelligentReportEngine:
    """
    IQ-160 level intelligent report generation.
    
    Automatically:
    1. Analyzes template structure (tables, headers, dimensions, KPIs)
    2. Interprets template intent using NLP (report type, focus, time period)
    3. Validates data with smart error detection
    4. Handles dates intelligently (parsing, ranges, periods)
    5. Maps template columns to data columns (fuzzy matching + synonyms)
    6. Aggregates data to required granularity (group by, pivot, calculate)
    7. Populates template while preserving formatting
    """
    
    def __init__(self, output_dir: str = "data/pacing_reports"):
        self.analyzer = TemplateAnalyzer()
        self.mapper = SmartMapper()
        self.aggregator = DynamicAggregator()
        self.populator = UniversalPopulator(output_dir=output_dir)
        self.date_handler = DateHandler()
        self.interpreter = AITemplateInterpreter()
        self.error_detector = SmartErrorDetector()
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate(self, template_path: str, data: pd.DataFrame,
                 output_name: str = None,
                 verbose: bool = False) -> Dict[str, Any]:
        """
        Generate a report from template and data.
        
        Args:
            template_path: Path to Excel template file
            data: pandas DataFrame with source data
            output_name: Optional custom output filename
            verbose: Print detailed mapping report
            
        Returns:
            Dictionary with:
            - output_path: Path to generated report
            - structure: Detected template structure
            - mappings: Column mappings used
            - stats: Generation statistics
        """
        logger.info(f"Generating report from template: {template_path}")
        
        # Step 1: Analyze template
        logger.info("Step 1: Analyzing template structure...")
        structure = self.analyzer.analyze(template_path)
        
        tables_found = structure.get("total_tables", 0)
        logger.info(f"  Found {tables_found} table(s) in {len(structure.get('sheets', []))} sheet(s)")
        
        # Step 2: Map columns
        logger.info("Step 2: Mapping template columns to data...")
        mappings = self.mapper.map(structure, data.columns.tolist())
        
        mapped_count = len(mappings)
        total_headers = sum(len(t.get("headers", [])) for s in structure.get("sheets", []) 
                          for t in s.get("tables", []))
        logger.info(f"  Mapped {mapped_count}/{total_headers} columns")
        
        if verbose:
            print(self.mapper.get_mapping_report(mappings))
        
        # Step 3: Aggregate data
        logger.info("Step 3: Aggregating data to match template granularity...")
        aggregated = self.aggregator.aggregate(data, structure, mappings)
        
        for key, df in aggregated.items():
            logger.info(f"  {key}: {len(df)} rows")
        
        # Step 4: Populate template
        logger.info("Step 4: Populating template...")
        output_path = self.populator.populate(
            template_path=template_path,
            aggregated_data=aggregated,
            template_structure=structure,
            column_mappings=mappings,
            output_name=output_name
        )
        
        logger.info(f"Report generated: {output_path}")
        
        return {
            "output_path": output_path,
            "structure": structure,
            "mappings": {k: {"column": v.data_column, "confidence": v.confidence, "type": v.match_type} 
                        for k, v in mappings.items()},
            "stats": {
                "tables_detected": tables_found,
                "columns_mapped": mapped_count,
                "data_rows": len(data),
                "sheets_processed": len(structure.get("sheets", [])),
            }
        }
    
    def analyze_template(self, template_path: str) -> Dict[str, Any]:
        """
        Analyze a template without generating a report.
        
        Useful for understanding template structure before providing data.
        
        Args:
            template_path: Path to Excel template
            
        Returns:
            Template structure dictionary
        """
        return self.analyzer.analyze(template_path)
    
    def preview_mapping(self, template_path: str, data: pd.DataFrame) -> str:
        """
        Preview how template columns would map to data columns.
        
        Args:
            template_path: Path to Excel template
            data: pandas DataFrame with source data
            
        Returns:
            Human-readable mapping report
        """
        structure = self.analyzer.analyze(template_path)
        mappings = self.mapper.map(structure, data.columns.tolist())
        return self.mapper.get_mapping_report(mappings)
    
    def generate_simple(self, template_path: str, data: pd.DataFrame,
                       sheet_name: str = None, output_name: str = None) -> str:
        """
        Simplified generation for single-table templates.
        
        Args:
            template_path: Path to template
            data: DataFrame to write
            sheet_name: Target sheet (first if None)
            output_name: Output filename
            
        Returns:
            Path to generated report
        """
        return self.populator.populate_simple(
            template_path=template_path,
            data=data,
            sheet_name=sheet_name,
            output_name=output_name
        )
    
    def aggregate_data(self, data: pd.DataFrame, 
                      dimensions: List[str],
                      metrics: List[str] = None) -> pd.DataFrame:
        """
        Public method to aggregate data by dimensions.
        
        Args:
            data: Input DataFrame
            dimensions: List of dimension columns to group by
            metrics: List of metrics to aggregate (auto-detect if None)
            
        Returns:
            Aggregated DataFrame
        """
        return self.aggregator.aggregate_by(data, dimensions, metrics)
    
    def pivot_data(self, data: pd.DataFrame,
                   row_dim: str, col_dim: str, value_col: str) -> pd.DataFrame:
        """
        Create a pivot table from data.
        
        Args:
            data: Input DataFrame
            row_dim: Column for row labels
            col_dim: Column for column labels  
            value_col: Column for values
            
        Returns:
            Pivot table DataFrame
        """
        return self.aggregator.pivot_by(data, row_dim, col_dim, value_col)
    
    # ==================== NEW AI-POWERED METHODS ====================
    
    def interpret_template(self, template_path: str) -> TemplateIntent:
        """
        Use NLP to interpret template intent.
        
        Args:
            template_path: Path to Excel template
            
        Returns:
            TemplateIntent with report type, focus, and time period
        """
        structure = self.analyzer.analyze(template_path)
        return self.interpreter.interpret(structure)
    
    def validate_data(self, data: pd.DataFrame, 
                     template_path: str = None) -> ValidationReport:
        """
        Validate data with smart error detection.
        
        Args:
            data: Input DataFrame
            template_path: Optional template to validate against
            
        Returns:
            ValidationReport with errors, warnings, and quality score
        """
        structure = None
        if template_path:
            structure = self.analyzer.analyze(template_path)
        
        return self.error_detector.validate(data, structure)
    
    def get_validation_summary(self, data: pd.DataFrame,
                              template_path: str = None) -> str:
        """
        Get human-readable validation summary.
        
        Args:
            data: Input DataFrame
            template_path: Optional template path
            
        Returns:
            Formatted validation summary string
        """
        report = self.validate_data(data, template_path)
        return self.error_detector.get_validation_summary(report)
    
    def detect_date_columns(self, data: pd.DataFrame) -> List[str]:
        """
        Auto-detect date columns in data.
        
        Args:
            data: Input DataFrame
            
        Returns:
            List of date column names
        """
        return self.date_handler.detect_date_columns(data)
    
    def filter_by_date(self, data: pd.DataFrame, date_col: str = None,
                      start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Filter data by date range.
        
        Args:
            data: Input DataFrame
            date_col: Date column name (auto-detect if None)
            start_date: Start date string
            end_date: End date string
            
        Returns:
            Filtered DataFrame
        """
        if date_col is None:
            date_cols = self.detect_date_columns(data)
            if date_cols:
                date_col = date_cols[0]
            else:
                logger.warning("No date column detected")
                return data
        
        return self.date_handler.filter_by_date_range(data, date_col, start_date, end_date)
    
    def get_date_info(self, data: pd.DataFrame, date_col: str = None) -> Dict[str, Any]:
        """
        Get date range and granularity info from data.
        
        Args:
            data: Input DataFrame
            date_col: Date column name (auto-detect if None)
            
        Returns:
            Dict with date range and granularity
        """
        if date_col is None:
            date_cols = self.detect_date_columns(data)
            date_col = date_cols[0] if date_cols else None
        
        if not date_col:
            return {"error": "No date column found"}
        
        date_range = self.date_handler.get_date_range(data, date_col)
        granularity = self.date_handler.detect_granularity(data, date_col)
        
        return {
            **date_range,
            "granularity": granularity,
            "date_column": date_col
        }
    
    def generate_with_validation(self, template_path: str, data: pd.DataFrame,
                                output_name: str = None,
                                date_filter: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Full intelligent report generation with validation and date handling.
        
        Args:
            template_path: Path to Excel template
            data: Input DataFrame
            output_name: Optional output filename
            date_filter: Optional dict with start_date and end_date
            
        Returns:
            Dict with output_path, validation, intent, and stats
        """
        # Validate data first
        logger.info("Pre-generation validation...")
        validation = self.validate_data(data, template_path)
        
        if not validation.is_valid:
            logger.error("Data validation failed!")
            return {
                "success": False,
                "validation": validation,
                "error": "Data validation failed - see validation.errors"
            }
        
        # Apply date filtering if requested
        if date_filter:
            data = self.filter_by_date(
                data, 
                start_date=date_filter.get("start_date"),
                end_date=date_filter.get("end_date")
            )
        
        # Interpret template intent
        intent = self.interpret_template(template_path)
        logger.info(f"Template intent: {intent.report_type} ({intent.primary_focus})")
        
        # Generate report
        result = self.generate(template_path, data, output_name)
        
        return {
            "success": True,
            "output_path": result["output_path"],
            "intent": {
                "report_type": intent.report_type,
                "focus": intent.primary_focus,
                "time_period": intent.time_period,
            },
            "validation": {
                "quality_score": validation.data_quality_score,
                "warnings": len(validation.warnings),
            },
            **result
        }
