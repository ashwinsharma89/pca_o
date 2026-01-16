"""
Intelligent Report Generation System.

Provides IQ-160 level smart reporting that handles any template,
any KPI, any dimension, and any granularity.

Features:
- Template Analysis: Auto-detect tables, headers, dimensions, KPIs
- Smart Mapping: Fuzzy match with synonyms and abbreviations  
- Dynamic Aggregation: Group by any dimension, auto-calculate derived metrics
- Date Handling: Auto-detect dates, filter by range, detect granularity
- AI Interpretation: NLP to understand template intent
- Smart Validation: Error detection, outliers, quality scoring

Usage:
    from src.engine.reports import IntelligentReportEngine
    
    engine = IntelligentReportEngine()
    
    # Full intelligent generation
    result = engine.generate_with_validation("template.xlsx", df)
    
    # Simple generation
    result = engine.generate("template.xlsx", df)
    
    # Analyze template
    structure = engine.analyze_template("template.xlsx")
    
    # Validate data
    report = engine.validate_data(df, "template.xlsx")
    print(engine.get_validation_summary(df))
"""

from .template_analyzer import TemplateAnalyzer
from .smart_mapper import SmartMapper, ColumnMatch
from .dynamic_aggregator import DynamicAggregator, AggregationSpec
from .universal_populator import UniversalPopulator
from .date_handler import DateHandler
from .ai_interpreter import AITemplateInterpreter, TemplateIntent
from .error_detector import SmartErrorDetector, ValidationReport, ValidationError
from .intelligent_engine import IntelligentReportEngine

__all__ = [
    # Main Engine
    "IntelligentReportEngine",
    
    # Core Components
    "TemplateAnalyzer",
    "SmartMapper",
    "ColumnMatch",
    "DynamicAggregator",
    "AggregationSpec",
    "UniversalPopulator",
    
    # AI Components
    "DateHandler",
    "AITemplateInterpreter",
    "TemplateIntent",
    "SmartErrorDetector",
    "ValidationReport",
    "ValidationError",
]
