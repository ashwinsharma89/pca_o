"""
Temporal Parser for NL-to-SQL Query Engine

Parses natural language temporal expressions into structured objects for comparison queries.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class TemporalIntent(Enum):
    POINT_IN_TIME_SNAPSHOT = "snapshot"  # "on Feb 1st"
    TIME_RANGE_AGGREGATION = "range"      # "last 30 days"
    TREND_ANALYSIS = "trend"             # "over time", "monthly trend"
    COMPARISON = "comparison"             # "vs last month", "MoM", "YoY"
    GROWTH_CALCULATION = "growth"         # "growth rate", "% change"

class Granularity(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

@dataclass
class TimePeriod:
    """Represents a structured time period."""
    label: str
    offset_value: int = 0
    offset_unit: str = "days"
    duration_value: Optional[int] = None
    duration_unit: Optional[str] = None
    is_relative: bool = True
    fixed_date: Optional[str] = None

@dataclass
class TemporalAnalysis:
    """Result of temporal parsing."""
    intent: TemporalIntent = TemporalIntent.TIME_RANGE_AGGREGATION
    primary_period: Optional[TimePeriod] = None
    comparison_period: Optional[TimePeriod] = None
    granularity: Optional[Granularity] = None
    is_period_over_period: bool = False
    metric_intent: list[str] = field(default_factory=list)

class TemporalParser:
    """
    Parses natural language queries to extract temporal context.

    Supports complex relative time expressions and comparison intents.
    """

    # Intent Detection Patterns
    COMPARISON_PATTERNS = [
        r"\bcompare\b", r"\bvs\b", r"\bversus\b", r"\bagainst\b",
        r"\bcompared\s+to\b", r"\bdifference\b", r"\bdelta\b"
    ]

    GROWTH_PATTERNS = [
        r"\bgrowth\b", r"\bchange\b", r"\b%\b", r"\bpercent\b", r"\bincrease\b", r"\bdecrease\b"
    ]

    TREND_PATTERNS = [
        r"\btrend\b", r"\bover\s+time\b", r"\bdaily\b", r"\bweekly\b", r"\bmonthly\b", r"\bquarterly\b"
    ]

    # Relative Time Patterns
    RELATIVE_PATTERNS = {
        "last_n_days": r"last\s+(\d+)\s+days?",
        "last_n_weeks": r"last\s+(\d+)\s+weeks?",
        "last_n_months": r"last\s+(\d+)\s+months?",
        "past_n_days": r"past\s+(\d+)\s+days?",
        "this_week": r"this\s+week",
        "last_week": r"last\s+week",
        "this_month": r"this\s+month",
        "last_month": r"last\s+month",
        "last_quarter": r"last\s+quarter",
        "this_year": r"this\s+year",
        "last_year": r"last\s+year",
        "ytd": r"\bytd\b",
        "mtd": r"\bmtd\b",
        "wtd": r"\bwtd\b"
    }

    # PoP Abbreviations
    POP_PATTERNS = {
        "wow": r"\bwow\b|\bweek\s+over\s+week\b",
        "mom": r"\bmom\b|\bmonth\s+over\s+month\b",
        "qoq": r"\bqoq\b|\bquarter\s+over\s+quarter\b",
        "yoy": r"\byoy\b|\byear\s+over\s+year\b"
    }

    def __init__(self):
        self._compiled_relative = {k: re.compile(v, re.IGNORECASE) for k, v in self.RELATIVE_PATTERNS.items()}
        self._compiled_pop = {k: re.compile(v, re.IGNORECASE) for k, v in self.POP_PATTERNS.items()}

    def parse(self, query: str) -> TemporalAnalysis:
        """
        Main entry point for parsing temporal context from a query.
        """
        analysis = TemporalAnalysis()
        query_lower = query.lower()

        # 1. Detect Intent
        is_comp = any(re.search(p, query_lower) for p in self.COMPARISON_PATTERNS)
        is_growth = any(re.search(p, query_lower) for p in self.GROWTH_PATTERNS)
        is_trend = any(re.search(p, query_lower) for p in self.TREND_PATTERNS)

        if is_comp:
            analysis.intent = TemporalIntent.COMPARISON
        if is_growth:
            # Growth often implies comparison
            analysis.intent = TemporalIntent.GROWTH_CALCULATION if not is_comp else TemporalIntent.COMPARISON
        elif is_trend:
            analysis.intent = TemporalIntent.TREND_ANALYSIS

        # 2. Extract Relative Periods
        found_periods = self._extract_periods(query_lower)

        # 3. Handle Period-over-Period Abbreviations
        pop_match = self._check_pop(query_lower)
        if pop_match:
            analysis.is_period_over_period = True
            analysis.intent = TemporalIntent.COMPARISON
            analysis.granularity = pop_match
            # Auto-fill periods if not explicitly mentioned
            if not found_periods:
                if pop_match == Granularity.WEEK:
                    found_periods = [TimePeriod("last week", duration_value=1, duration_unit="week")]
                elif pop_match == Granularity.MONTH:
                    found_periods = [TimePeriod("last month", duration_value=1, duration_unit="month")]
                elif pop_match == Granularity.QUARTER:
                    found_periods = [TimePeriod("last quarter", duration_value=1, duration_unit="quarter")]
                elif pop_match == Granularity.YEAR:
                    found_periods = [TimePeriod("last year", duration_value=1, duration_unit="year")]

        # 4. Assign Primary and Comparison Periods
        if found_periods:
            analysis.primary_period = found_periods[0]
            if len(found_periods) > 1:
                analysis.comparison_period = found_periods[1]
            elif analysis.intent in [TemporalIntent.COMPARISON, TemporalIntent.GROWTH_CALCULATION]:
                # Auto-generate comparison period (the period immediately preceding)
                analysis.comparison_period = self._get_previous_period(analysis.primary_period)
                analysis.is_period_over_period = True

        # 5. Detect Granularity
        if not analysis.granularity:
            analysis.granularity = self._detect_granularity(query_lower, analysis.primary_period)

        return analysis

    def _extract_periods(self, query: str) -> list[TimePeriod]:
        periods = []

        # Check "last N days/weeks/months"
        match_days = self._compiled_relative["last_n_days"].search(query) or self._compiled_relative["past_n_days"].search(query)
        if match_days:
            n = int(match_days.group(1))
            periods.append(TimePeriod(f"last {n} days", duration_value=n, duration_unit="day"))

        match_weeks = self._compiled_relative["last_n_weeks"].search(query)
        if match_weeks:
            n = int(match_weeks.group(1))
            periods.append(TimePeriod(f"last {n} weeks", duration_value=n, duration_unit="week"))

        match_months = self._compiled_relative["last_n_months"].search(query)
        if match_months:
            n = int(match_months.group(1))
            periods.append(TimePeriod(f"last {n} months", duration_value=n, duration_unit="month"))

        # Check singular keywords
        if "last month" in query and not any(p.label == "last 1 months" for p in periods):
            periods.append(TimePeriod("last month", duration_value=1, duration_unit="month"))
        if "this month" in query:
            periods.append(TimePeriod("this month", offset_value=0, duration_unit="month"))
        if "last week" in query and not any(p.label == "last 1 weeks" for p in periods):
            periods.append(TimePeriod("last week", duration_value=1, duration_unit="week"))
        if "this week" in query:
            periods.append(TimePeriod("this week", offset_value=0, duration_unit="week"))
        if "ytd" in query:
            periods.append(TimePeriod("YTD", duration_unit="year"))

        return periods

    def _check_pop(self, query: str) -> Optional[Granularity]:
        if self._compiled_pop["wow"].search(query): return Granularity.WEEK
        if self._compiled_pop["mom"].search(query): return Granularity.MONTH
        if self._compiled_pop["qoq"].search(query): return Granularity.QUARTER
        if self._compiled_pop["yoy"].search(query): return Granularity.YEAR
        return None

    def _get_previous_period(self, period: TimePeriod) -> TimePeriod:
        """Calculates the period immediately preceding the given one."""
        prev = TimePeriod(
            label=f"previous {period.duration_value or ''} {period.duration_unit or ''}".strip(),
            duration_value=period.duration_value,
            duration_unit=period.duration_unit,
            offset_value=(period.duration_value or 1) + (period.offset_value or 0),
            offset_unit=period.duration_unit or "day"
        )
        return prev

    def _detect_granularity(self, query: str, period: Optional[TimePeriod]) -> Optional[Granularity]:
        if "daily" in query or "by day" in query: return Granularity.DAY
        if "weekly" in query or "by week" in query: return Granularity.WEEK
        if "monthly" in query or "by month" in query: return Granularity.MONTH

        # Infer from period
        if period and period.duration_unit:
            unit = period.duration_unit.lower()
            if "day" in unit: return Granularity.DAY
            if "week" in unit: return Granularity.WEEK
            if "month" in unit: return Granularity.MONTH

        return None
