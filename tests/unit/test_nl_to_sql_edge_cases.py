"""
Tests for Query Engine Edge Cases and Internal Logic (Phase A.4).
Focuses on SQL validation rules and sanitization without live LLM.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine

@pytest.fixture
def engine():
    """Create a query engine with a mocked API key."""
    return NaturalLanguageQueryEngine(api_key="mock-key")

class TestQueryEngineInternalLogic:
    """Tests for internal SQL validation and sanitization logic."""

    def test_validate_marketing_rules_pass(self, engine):
        """Test SQL that passes all marketing rules."""
        sql = "SELECT SUM(spend)/NULLIF(SUM(conversions), 0) FROM table WHERE date <= (SELECT MAX(date) FROM table)"
        result = engine._validate_marketing_sql_rules(sql)
        assert result['all_passed'] is True
        assert len(result['failed_rules']) == 0

    def test_validate_marketing_rules_fail_avg_rate(self, engine):
        """Test failure for AVG() on rate columns."""
        sql = "SELECT AVG(CTR) FROM campaigns"
        result = engine._validate_marketing_sql_rules(sql)
        assert result['all_passed'] is False
        assert any("Rule 1" in r for r in result['failed_rules'])

    def test_validate_marketing_rules_fail_division(self, engine):
        """Test failure for division without NULLIF."""
        # The internal regex expects a trailing comma, newline, or parenthesis
        sql = "SELECT (spend/conversions) FROM campaigns"
        result = engine._validate_marketing_sql_rules(sql)
        assert result['all_passed'] is False
        assert any("Rule 2" in r for r in result['failed_rules'])

    def test_validate_marketing_rules_fail_current_date(self, engine):
        """Test failure for CURRENT_DATE usage."""
        sql = "SELECT * FROM campaigns WHERE date = CURRENT_DATE"
        result = engine._validate_marketing_sql_rules(sql)
        assert result['all_passed'] is False
        assert any("Rule 3" in r for r in result['failed_rules'])

    def test_validate_marketing_rules_fail_threshold(self, engine):
        """Test failure for arbitrary thresholds."""
        sql = "SELECT * FROM campaigns WHERE ROAS > 3"
        result = engine._validate_marketing_sql_rules(sql)
        assert result['all_passed'] is False
        assert any("Rule 4" in r for r in result['failed_rules'])

    def test_sanitize_sql_double_quotes(self, engine):
        """Test sanitization of over-escaped quotes."""
        engine.schema_info = {} # Initialize it
        sql = 'SELECT ""Campaign_Name" FROM table'
        sanitized = engine._sanitize_sql(sql)
        assert '""Campaign_Name"' not in sanitized
        assert '"Campaign_Name' in sanitized

    def test_sanitize_sql_column_mapping(self, engine):
        """Test automatic column name standardization."""
        # Setup schema info to trigger mapping
        engine.schema_info = {'columns': ['spend', 'conversions', 'date']}
        
        sql = "SELECT Total_Spent, Site_Visit, Date FROM table"
        sanitized = engine._sanitize_sql(sql)
        
        # Should lowercase and standardize based on patterns in _sanitize_sql
        assert "spend" in sanitized.lower()
        assert "conversions" in sanitized.lower()
        assert "date" in sanitized.lower()

    def test_sanitize_sql_missing_revenue(self, engine):
        """Test handling of missing Revenue column (ROAS fallback)."""
        engine.schema_info = {'columns': ['spend', 'conversions']} # No Revenue
        
        # The internal regex is quite specific: r'ROUND\s*\(\s*SUM\s*\(\s*Revenue\s*\)[^)]*\)\s+AS\s+ROAS'
        sql = "SELECT ROUND(SUM(Revenue), 2) AS ROAS FROM table"
        sanitized = engine._sanitize_sql(sql)
        
        assert "NULL AS ROAS" in sanitized

    def test_load_data_basic(self, engine):
        """Test data loading into DuckDB."""
        df = pd.DataFrame({'campaign_id': ['A', 'B'], 'spend': [100, 200]})
        engine.load_data(df, "test_campaigns")
        
        # Verify directly with duckdb
        conn = engine.conn
        result = conn.execute("SELECT COUNT(*) FROM test_campaigns").fetchone()
        assert result[0] == 2
