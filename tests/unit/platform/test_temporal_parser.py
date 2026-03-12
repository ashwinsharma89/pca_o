import pytest

from src.platform.query_engine.temporal_parser import Granularity, TemporalIntent, TemporalParser


def test_parse_compare_last_2_months():
    parser = TemporalParser()
    query = "Compare last 2 months spend by channel"
    analysis = parser.parse(query)

    assert analysis.intent == TemporalIntent.COMPARISON
    assert analysis.primary_period.label == "last 2 months"
    assert analysis.primary_period.duration_value == 2
    assert analysis.primary_period.duration_unit == "month"

    # Auto-generated comparison period should be the 2 months before that
    assert analysis.comparison_period is not None
    assert analysis.comparison_period.offset_value == 2
    assert analysis.comparison_period.duration_value == 2
    assert analysis.comparison_period.duration_unit == "month"

    assert analysis.granularity == Granularity.MONTH

def test_parse_mom_growth():
    parser = TemporalParser()
    query = "What is the MoM growth in conversions?"
    analysis = parser.parse(query)

    assert analysis.intent == TemporalIntent.COMPARISON
    assert analysis.is_period_over_period is True
    assert analysis.granularity == Granularity.MONTH
    assert analysis.primary_period.label == "last month"
    assert analysis.comparison_period.label == "previous 1 month"

def test_parse_last_30_days_trend():
    parser = TemporalParser()
    query = "Show me spend trend for the last 30 days"
    analysis = parser.parse(query)

    assert analysis.intent == TemporalIntent.TREND_ANALYSIS
    assert analysis.primary_period.duration_value == 30
    assert analysis.primary_period.duration_unit == "day"
    assert analysis.granularity == Granularity.DAY

def test_parse_comparison_explicit():
    parser = TemporalParser()
    query = "last week vs this week performance"
    analysis = parser.parse(query)

    # Simple parser might pick up "last week" first
    # and then identify "this week" as a separate period if it was more advanced.
    # Currently it takes the first period and then auto-generates if only one found.
    # Let's see what it does.

    assert analysis.intent == TemporalIntent.COMPARISON
    # If it found [last week], it might not find [this week] if it returns after first match.
    # My _extract_periods finds all.
    assert analysis.primary_period.label == "last week"
    assert analysis.comparison_period.label == "this week"

if __name__ == "__main__":
    pytest.main([__file__])
