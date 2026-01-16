"""
AI Template Interpreter - NLP-powered template understanding and intent detection.

Features:
- Understand template intent from sheet/header names
- NLP-based column classification
- Semantic similarity matching
- Generate insights about template structure
- AI-powered data validation suggestions
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TemplateIntent:
    """Represents the detected intent of a template."""
    report_type: str  # pacing, performance, attribution, executive, etc.
    primary_focus: str  # spend, conversions, engagement, etc.
    time_period: str  # daily, weekly, monthly, custom
    comparison_type: Optional[str]  # vs_target, vs_previous, trend
    confidence: float


class AITemplateInterpreter:
    """
    AI-powered template interpretation using NLP.
    
    Uses keyword analysis, semantic patterns, and heuristics
    to understand what a template is trying to accomplish.
    """
    
    # Report type keywords
    REPORT_TYPE_KEYWORDS = {
        "pacing": ["pacing", "budget", "tracking", "spend", "allocation", "remaining", "variance"],
        "performance": ["performance", "metrics", "kpi", "results", "analysis", "efficiency"],
        "attribution": ["attribution", "touchpoint", "journey", "path", "conversion path"],
        "executive": ["executive", "summary", "overview", "dashboard", "highlights"],
        "comparison": ["comparison", "vs", "versus", "benchmark", "compare"],
        "trend": ["trend", "over time", "historical", "growth", "change"],
        "forecast": ["forecast", "prediction", "projected", "expected", "estimate"],
        "breakdown": ["breakdown", "by", "segmented", "split", "distribution"],
    }
    
    # Focus area keywords
    FOCUS_KEYWORDS = {
        "spend": ["spend", "cost", "budget", "investment", "expenditure"],
        "conversions": ["conversion", "lead", "signup", "purchase", "action"],
        "engagement": ["engagement", "click", "view", "impression", "reach"],
        "revenue": ["revenue", "sales", "income", "value", "roas"],
        "efficiency": ["cpa", "cpc", "cpm", "roas", "roi", "efficiency"],
    }
    
    # Time period keywords
    TIME_PERIOD_KEYWORDS = {
        "daily": ["daily", "day", "today", "yesterday"],
        "weekly": ["weekly", "week", "this week", "last week"],
        "monthly": ["monthly", "month", "mtd", "month to date"],
        "quarterly": ["quarterly", "quarter", "qtd", "q1", "q2", "q3", "q4"],
        "yearly": ["yearly", "annual", "year", "ytd", "year to date"],
    }
    
    # Common abbreviation expansions
    ABBREVIATIONS = {
        "mtd": "month to date",
        "ytd": "year to date",
        "qtd": "quarter to date",
        "wtd": "week to date",
        "yoy": "year over year",
        "mom": "month over month",
        "wow": "week over week",
        "dod": "day over day",
        "cpa": "cost per acquisition",
        "cpc": "cost per click",
        "cpm": "cost per mille",
        "roas": "return on ad spend",
        "ctr": "click through rate",
        "cvr": "conversion rate",
    }
    
    def __init__(self):
        pass
    
    def interpret(self, template_structure: Dict[str, Any]) -> TemplateIntent:
        """
        Interpret template intent from its structure.
        
        Args:
            template_structure: Output from TemplateAnalyzer
            
        Returns:
            TemplateIntent with detected report type and focus
        """
        # Collect all text from template
        text_corpus = self._collect_text(template_structure)
        
        # Detect report type
        report_type, type_confidence = self._detect_report_type(text_corpus)
        
        # Detect primary focus
        primary_focus = self._detect_focus(text_corpus)
        
        # Detect time period
        time_period = self._detect_time_period(text_corpus)
        
        # Detect comparison type
        comparison_type = self._detect_comparison(text_corpus)
        
        return TemplateIntent(
            report_type=report_type,
            primary_focus=primary_focus,
            time_period=time_period,
            comparison_type=comparison_type,
            confidence=type_confidence
        )
    
    def _collect_text(self, structure: Dict[str, Any]) -> str:
        """Collect all text from template structure."""
        texts = [structure.get("file", "")]
        
        for sheet in structure.get("sheets", []):
            texts.append(sheet["name"])
            
            for table in sheet.get("tables", []):
                texts.extend(table.get("headers", []))
        
        for ph in structure.get("placeholders", []):
            texts.append(ph.get("name", ""))
        
        return " ".join(texts).lower()
    
    def _detect_report_type(self, text: str) -> Tuple[str, float]:
        """Detect the type of report from text."""
        scores = {}
        
        for report_type, keywords in self.REPORT_TYPE_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                scores[report_type] = score
        
        if not scores:
            return "general", 0.5
        
        best_type = max(scores, key=scores.get)
        confidence = min(scores[best_type] / 3, 1.0)  # Normalize
        
        return best_type, confidence
    
    def _detect_focus(self, text: str) -> str:
        """Detect the primary focus area."""
        scores = {}
        
        for focus, keywords in self.FOCUS_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                scores[focus] = score
        
        if not scores:
            return "general"
        
        return max(scores, key=scores.get)
    
    def _detect_time_period(self, text: str) -> str:
        """Detect the time period from text."""
        for period, keywords in self.TIME_PERIOD_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                return period
        return "custom"
    
    def _detect_comparison(self, text: str) -> Optional[str]:
        """Detect if template involves comparisons."""
        if any(kw in text for kw in ["vs target", "variance", "pacing"]):
            return "vs_target"
        elif any(kw in text for kw in ["vs previous", "change", "yoy", "mom", "wow"]):
            return "vs_previous"
        elif any(kw in text for kw in ["trend", "over time", "historical"]):
            return "trend"
        return None
    
    def generate_insights(self, template_structure: Dict[str, Any]) -> List[str]:
        """
        Generate natural language insights about the template.
        
        Args:
            template_structure: Output from TemplateAnalyzer
            
        Returns:
            List of insight strings
        """
        insights = []
        intent = self.interpret(template_structure)
        
        # Report type insight
        insights.append(f"This appears to be a **{intent.report_type}** report focused on **{intent.primary_focus}**.")
        
        # Time period insight
        if intent.time_period != "custom":
            insights.append(f"Data granularity is **{intent.time_period}**.")
        
        # Comparison insight
        if intent.comparison_type == "vs_target":
            insights.append("The template compares **actual vs target** values.")
        elif intent.comparison_type == "vs_previous":
            insights.append("The template shows **period-over-period** comparisons.")
        
        # Structure insights
        total_tables = structure.get("total_tables", 0)
        total_sheets = len(structure.get("sheets", []))
        insights.append(f"Contains **{total_tables} table(s)** across **{total_sheets} sheet(s)**.")
        
        return insights
    
    def suggest_data_requirements(self, template_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Suggest what data columns are needed to populate this template.
        
        Args:
            template_structure: Output from TemplateAnalyzer
            
        Returns:
            Dictionary with required and optional columns
        """
        required = set()
        optional = set()
        
        for sheet in template_structure.get("sheets", []):
            for table in sheet.get("tables", []):
                # Dimension columns are required
                for dim in table.get("dimension_cols", []):
                    required.add(dim.lower())
                
                # KPI columns - check if calculable
                for kpi in table.get("kpi_cols", []):
                    kpi_lower = kpi.lower()
                    
                    # Derived metrics are optional if base metrics present
                    if any(k in kpi_lower for k in ["ctr", "cpc", "cpm", "cpa", "roas", "cvr", "pacing"]):
                        optional.add(kpi_lower)
                    else:
                        required.add(kpi_lower)
        
        return {
            "required_columns": list(required),
            "optional_columns": list(optional),
            "calculable_metrics": ["ctr", "cpc", "cpm", "cpa", "roas", "cvr"],
        }
    
    def expand_abbreviations(self, text: str) -> str:
        """
        Expand common abbreviations in text.
        
        Args:
            text: Input text with abbreviations
            
        Returns:
            Text with expanded abbreviations
        """
        result = text.lower()
        for abbr, expansion in self.ABBREVIATIONS.items():
            result = re.sub(rf'\b{abbr}\b', expansion, result, flags=re.IGNORECASE)
        return result
