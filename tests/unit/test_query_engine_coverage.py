"""
Tests for Query Engine coverage (Phase 4.1 & 4.2).
Covers validator.py and query_templates.py.
"""

import pytest
from unittest.mock import Mock, patch
import sqlglot

from src.platform.query_engine.validator import SQLValidator
from src.platform.query_engine.query_templates import (
    QueryTemplate, find_matching_template, get_suggested_questions, QUERY_TEMPLATES
)

class TestSQLValidator:
    """Tests for SQLValidator security and injection logic."""
    
    @pytest.fixture
    def validator(self):
        return SQLValidator(dialect="duckdb")

    def test_valid_queries(self, validator):
        """Test safe SELECT queries."""
        valid_queries = [
            "SELECT * FROM campaigns",
            "SELECT name, spend FROM campaigns WHERE spend > 100",
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ]
        for sql in valid_queries:
            is_valid, error = validator.validate(sql)
            assert is_valid is True, f"Failed on: {sql}. Error: {error}"

    def test_security_violations(self, validator):
        """Test forbidden commands and multi-statements."""
        violations = [
            ("DROP TABLE campaigns", "Security Violation"),
            ("DELETE FROM campaigns", "Security Violation"),
            ("UPDATE campaigns SET spend = 0", "Security Violation"),
            ("INSERT INTO campaigns VALUES (1)", "Security Violation"),
            ("SELECT * FROM campaigns; DROP TABLE users", "Multiple SQL statements"),
            ("SELECT * FROM information_schema.tables", "Access to system table"),
            ("SELECT * FROM pg_catalog.pg_tables", "Access to system table"),
        ]
        for sql, expected_error in violations:
            is_valid, error = validator.validate(sql)
            assert is_valid is False
            assert expected_error.lower() in error.lower()

    def test_empty_query(self, validator):
        """Test empty string validation."""
        is_valid, error = validator.validate("")
        assert is_valid is False
        assert "empty" in error.lower()

    def test_parse_error(self, validator):
        """Test malformed SQL."""
        is_valid, error = validator.validate("SELECT FROM WHERE")
        assert is_valid is False
        assert "parse error" in error.lower()

    def test_inject_limit(self, validator):
        """Test LIMIT injection."""
        # Missing limit
        sql = "SELECT * FROM campaigns"
        injected = validator.inject_limit(sql, limit=100)
        assert "LIMIT 100" in injected.upper()

        # Already has limit
        sql_with_limit = "SELECT * FROM campaigns LIMIT 50"
        not_injected = validator.inject_limit(sql_with_limit, limit=100)
        assert "LIMIT 50" in not_injected.upper()
        assert "LIMIT 100" not in not_injected.upper()

        # Non-SELECT (should return original)
        assert validator.inject_limit("UNION SELECT 1", limit=10) == "UNION SELECT 1"

class TestQueryTemplates:
    """Tests for pre-built query templates."""
    
    def test_template_matching(self):
        """Test pattern matching for templates."""
        template = QueryTemplate(
            name="Test", 
            patterns=["top", "best"], 
            sql="SELECT 1", 
            description="desc"
        )
        assert template.matches("Show me the top campaigns") is True
        assert template.matches("Worst performance") is False

    def test_find_matching_template(self):
        """Test finding template from global list."""
        # Match funnel
        t = find_matching_template("Show me the funnel analysis")
        assert t is not None
        assert t.name == "Marketing Funnel Analysis"

        # No match
        assert find_matching_template("Random question") is None

    def test_get_suggested_questions(self):
        """Test suggestions list."""
        suggestions = get_suggested_questions()
        assert len(suggestions) > 0
        assert "question" in suggestions[0]
        assert "description" in suggestions[0]

    def test_all_templates_valid_sql(self):
        """Basic check if all template SQLs are parsable."""
        validator = SQLValidator()
        for key, template in QUERY_TEMPLATES.items():
            # Some templates might use tables like 'all_campaigns' instead of 'campaigns'
            # We just check if they are syntactically valid SQL
            try:
                sqlglot.parse(template.sql, read="duckdb")
            except Exception as e:
                pytest.fail(f"Template '{key}' has invalid SQL: {e}")
