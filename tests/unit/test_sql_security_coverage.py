"""
Tests for SQL Security (Phase A.1).
Exhaustive testing of SQLSanitizer and safe_sql_format.
"""

import pytest
from src.core.utils.sql_security import SQLSanitizer, sql_sanitizer, safe_sql_format

class TestSQLSanitizer:
    """Tests for SQLSanitizer validation and sanitization."""

    def test_sanitize_string_basics(self):
        """Test basic string sanitization."""
        # Single quotes escape
        assert sql_sanitizer.sanitize_string("O'Reilly") == "O''Reilly"
        # Null bytes
        assert sql_sanitizer.sanitize_string("null\x00byte") == "nullbyte"
        # Comment markers
        assert sql_sanitizer.sanitize_string("SELECT * -- comment") == "SELECT *  comment"
        assert sql_sanitizer.sanitize_string("/* block */") == " block "
        # Semicolons
        assert sql_sanitizer.sanitize_string("SELECT 1; DROP TABLE users") == "SELECT 1 DROP TABLE users"

    def test_sanitize_string_max_length(self):
        """Test length truncation."""
        long_str = "a" * 2000
        sanitized = sql_sanitizer.sanitize_string(long_str, max_length=100)
        assert len(sanitized) == 100

    def test_is_valid_identifier(self):
        """Test SQL identifier validation."""
        valid = ["campaign_name", "spend_1", "Platform", "_private"]
        invalid = ["campaign name", "1_spend", "spend!", "'; DROP", "", "a" * 130]
        
        for identifier in valid:
            assert sql_sanitizer.is_valid_identifier(identifier) is True
        for identifier in invalid:
            assert sql_sanitizer.is_valid_identifier(identifier) is False

    def test_validate_column_name(self):
        """Test column name validation against allowlist."""
        allowed = ["campaign_name", "spend", "clicks"]
        assert sql_sanitizer.validate_column_name("SPEND", allowed) == "spend"
        assert sql_sanitizer.validate_column_name("unknown", allowed) is None
        assert sql_sanitizer.validate_column_name(None, allowed) is None

    def test_validate_platform_and_channel(self):
        """Test platform and channel validation."""
        # Platform
        assert sql_sanitizer.validate_platform("Google Ads") == "Google Ads"
        assert sql_sanitizer.validate_platform("invalid_platform") is None
        # Channel
        assert sql_sanitizer.validate_channel("search") == "search"
        assert sql_sanitizer.validate_channel("hacking") is None

    def test_sanitize_number(self):
        """Test numeric sanitization."""
        assert sql_sanitizer.sanitize_number(123) == 123.0
        assert sql_sanitizer.sanitize_number("45.6") == 45.6
        assert sql_sanitizer.sanitize_number("invalid") is None
        assert sql_sanitizer.sanitize_number(float('inf')) is None
        assert sql_sanitizer.sanitize_number(None) is None

    def test_build_in_clause(self):
        """Test safe IN clause builder."""
        clause = sql_sanitizer.build_in_clause(["google", "meta"], "platform")
        assert clause == "platform IN ('google', 'meta')"
        
        # Invalid identifier
        assert sql_sanitizer.build_in_clause(["v"], "platform;--") == "1=1"
        
        # Sanitize values in IN
        clause = sql_sanitizer.build_in_clause(["O'Reilly"], "author")
        assert clause == "author IN ('O''Reilly')"

    def test_build_date_filter(self):
        """Test safe date filter builder."""
        clause = sql_sanitizer.build_date_filter("2024-01-01", "2024-01-31", "created_at")
        assert clause == "created_at >= '2024-01-01' AND created_at <= '2024-01-31'"
        
        # Malformed dates
        clause = sql_sanitizer.build_date_filter("bad-date", "2024-01-31")
        assert clause == "date <= '2024-01-31'"
        
        # No dates
        assert sql_sanitizer.build_date_filter(None, None) == "1=1"

    def test_contains_sql_injection(self):
        """Test injection pattern detection."""
        suspicious = [
            "SELECT * FROM users",
            "1=1",
            "admin' OR '1'='1",
            "0x414243",
            "--;",
            "DROP TABLE",
            "UNION SELECT"
        ]
        clean = [
            "blue campaigns",
            "2024-05-10",
            "standard_user_123"
        ]
        
        for val in suspicious:
            assert sql_sanitizer.contains_sql_injection(val) is True, f"Failed to detect: {val}"
        for val in clean:
            assert sql_sanitizer.contains_sql_injection(val) is False

def test_safe_sql_format():
    """Test safe_sql_format utility."""
    # String injection
    query = safe_sql_format(
        "SELECT * FROM table WHERE name = '{name}'",
        name="O'Reilly; DROP TABLE users"
    )
    assert "O''Reilly DROP TABLE users" in query
    assert ";" not in query
    
    # Numbers
    query = safe_sql_format(
        "SELECT * FROM table WHERE id = {id}",
        id="123.5"
    )
    assert "id = 123.5" in query
    
    # Lists
    query = safe_sql_format(
        "SELECT * FROM table WHERE tags = {tags}",
        tags=["a", "b'c"]
    )
    # Note: formatting a list directly in string might not be standard SQL but check sanitization
    assert "b''c" in str(query)
