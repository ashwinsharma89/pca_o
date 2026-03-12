"""
Temporal Parser for Marketing Queries

Parses natural language temporal expressions into structured objects
suitable for SQL generation and comparative analysis.
"""

import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger


class TemporalIntent(Enum):
    POINT_IN_TIME = "point_in_time"
    RANGE = "range"
    COMPARISON = "comparison"
    GROWTH_CALCULATION = "growth"
    TREND = "trend"
    LAG_ANALYSIS = "lag"
    UNKNOWN = "unknown"


@dataclass
class DateRange:
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    label: str = ""
    sql_interval: Optional[str] = None


@dataclass
class TemporalContext:
    intent: TemporalIntent = TemporalIntent.UNKNOWN
    primary_period: Optional[DateRange] = None
    comparison_period: Optional[DateRange] = None
    is_period_over_period: bool = False
    is_year_over_year: bool = False
    granularity: str = "daily"  # daily, weekly, monthly, yearly
    raw_query: str = ""


class TemporalParser:
    """
    Parses temporal expressions like 'last 2 months', 'MoM', 'YoY vs previous year'.
    """

    # Common patterns
    RE_LAST_N_MONTHS = re.compile(r"last\s+(\d+)\s+months?", re.IGNORECASE)
    RE_PAST_N_DAYS = re.compile(r"past\s+(\d+)\s+days?", re.IGNORECASE)
    RE_MOM = re.compile(r"\b(mom|month\s+over\s+month)\b", re.IGNORECASE)
    RE_YOY = re.compile(r"\b(yoy|year\s+over\s+year)\b", re.IGNORECASE)
    RE_COMPARE = re.compile(r"\b(compare|vs|versus|growth|change)\b", re.IGNORECASE)

    def __init__(self, reference_date: Optional[datetime] = None):
        self.reference_date = reference_date or datetime.now()

    def parse(self, query: str) -> TemporalContext:
        """Parse natural language query for temporal context."""
        context = TemporalContext(raw_query=query)
        
        # 1. Identify Intent
        if self.RE_COMPARE.search(query) or self.RE_MOM.search(query) or self.RE_YOY.search(query):
            context.intent = TemporalIntent.COMPARISON
            if self.RE_MOM.search(query):
                context.is_period_over_period = True
                context.granularity = "monthly"
            if self.RE_YOY.search(query):
                context.is_year_over_year = True
                context.granularity = "yearly"
        elif "trend" in query.lower():
            context.intent = TemporalIntent.TREND
        else:
            context.intent = TemporalIntent.RANGE

        # 2. Extract Primary Period
        match_months = self.RE_LAST_N_MONTHS.search(query)
        match_days = self.RE_PAST_N_DAYS.search(query)
        
        if match_months:
            n = int(match_months.group(1))
            context.primary_period = DateRange(
                label=f"last {n} months",
                sql_interval=f"{n} MONTH"
            )
            # Rough dates for reasoning (if absolute tracking isn't available)
            context.primary_period.end_date = self.reference_date
            context.primary_period.start_date = self.reference_date - timedelta(days=n * 30)
        
        elif match_days:
            n = int(match_days.group(1))
            context.primary_period = DateRange(
                label=f"past {n} days",
                sql_interval=f"{n} DAY"
            )
            context.primary_period.end_date = self.reference_date
            context.primary_period.start_date = self.reference_date - timedelta(days=n)
        
        # 3. Handle Comparisons (P2)
        if context.intent == TemporalIntent.COMPARISON or context.is_period_over_period:
            if context.primary_period:
                # Default comparison is the previous period of the same length
                context.comparison_period = DateRange(
                    label=f"previous {context.primary_period.label}"
                )
                if context.primary_period.start_date and context.primary_period.end_date:
                    duration = context.primary_period.end_date - context.primary_period.start_date
                    context.comparison_period.end_date = context.primary_period.start_date
                    context.comparison_period.start_date = context.primary_period.start_date - duration
            
            elif context.is_period_over_period:
                # Default MoM if no specific range mentioned
                context.primary_period = DateRange(label="this month", sql_interval="1 MONTH")
                context.comparison_period = DateRange(label="previous month")
        
        return context

    def get_sql_cte_hints(self, context: TemporalContext) -> str:
        """Generate SQL hints for PromptBuilder to create CTEs."""
        if context.intent != TemporalIntent.COMPARISON:
            return ""
            
        p1_label = context.primary_period.label if context.primary_period else "Current Period"
        p2_label = context.comparison_period.label if context.comparison_period else "Previous Period"
        
        hint = f"-- Comparison Mode Detected: {p1_label} vs {p2_label}\n"
        hint += "-- Request: Use CTEs (period1, period2) and JOIN them to calculate growth metrics.\n"
        
        if context.primary_period and context.primary_period.sql_interval:
            hint += f"-- Period 1 Filter: date >= (SELECT MAX(date) FROM campaigns) - INTERVAL {context.primary_period.sql_interval}\n"
            hint += f"-- Period 2 Filter: date >= (SELECT MAX(date) FROM campaigns) - INTERVAL {context.primary_period.sql_interval} * 2 "
            hint += f"AND date < (SELECT MAX(date) FROM campaigns) - INTERVAL {context.primary_period.sql_interval}\n"
            
        return hint
