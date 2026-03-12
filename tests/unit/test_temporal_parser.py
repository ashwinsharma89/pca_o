import pytest
from datetime import datetime
from src.platform.query_engine.temporal_parser import TemporalParser, TemporalIntent

def test_parse_last_n_months():
    parser = TemporalParser(reference_date=datetime(2024, 6, 1))
    context = parser.parse("What was the spend for the last 2 months?")
    
    assert context.intent == TemporalIntent.RANGE
    assert context.primary_period.label == "last 2 months"
    assert context.primary_period.sql_interval == "2 MONTH"

def test_parse_mom():
    parser = TemporalParser(reference_date=datetime(2024, 6, 1))
    context = parser.parse("Give me a MoM comparison of ROAS")
    
    assert context.intent == TemporalIntent.COMPARISON
    assert context.is_period_over_period == True
    assert context.granularity == "monthly"
    assert context.primary_period.label == "this month"
    assert context.comparison_period.label == "previous month"

def test_parse_yoy_growth():
    parser = TemporalParser(reference_date=datetime(2024, 6, 1))
    context = parser.parse("What is the YoY growth in revenue?")
    
    assert context.intent == TemporalIntent.COMPARISON
    assert context.is_year_over_year == True
    assert context.granularity == "yearly"

def test_parse_past_n_days():
    parser = TemporalParser(reference_date=datetime(2024, 6, 1))
    context = parser.parse("past 14 days performance")
    
    assert context.intent == TemporalIntent.RANGE
    assert context.primary_period.label == "past 14 days"
    assert context.primary_period.sql_interval == "14 DAY"

def test_sql_cte_hints():
    parser = TemporalParser(reference_date=datetime(2024, 6, 1))
    context = parser.parse("Compare last 3 months vs previous period")
    hints = parser.get_sql_cte_hints(context)
    
    assert "Comparison Mode Detected" in hints
    assert "Use CTEs (period1, period2)" in hints
    assert "INTERVAL 3 MONTH" in hints
