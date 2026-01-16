"""
SQL Security Utilities

Provides input sanitization and injection prevention for SQL queries.
Used in DuckDB analytical queries and dynamic SQL generation.
"""

import re
from typing import Any, List, Optional, Union
from loguru import logger


class SQLSanitizer:
    """
    SQL input sanitizer for preventing injection attacks.
    
    Usage:
        from src.core.utils.sql_security import sql_sanitizer
        
        # Sanitize a value
        safe_value = sql_sanitizer.sanitize_string("user_input")
        
        # Validate identifier (table/column name)
        if sql_sanitizer.is_valid_identifier(column_name):
            query = f"SELECT {column_name} FROM table"
        
        # Build safe WHERE clause
        clause = sql_sanitizer.build_where_clause(filters)
    """
    
    # Allowed characters in identifiers (table/column names)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    # SQL keywords that should never appear in user input
    SQL_KEYWORDS = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE',
        'CREATE', 'ALTER', 'GRANT', 'REVOKE', 'UNION', 'EXEC',
        'EXECUTE', 'XP_', 'SP_', '--', ';', '/*', '*/'
    }
    
    # Allowed aggregation functions
    ALLOWED_AGGREGATES = {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'COALESCE'}
    
    # Allowed platforms for filtering
    ALLOWED_PLATFORMS = {
        'google ads', 'google_ads', 'meta', 'facebook', 'instagram',
        'linkedin', 'twitter', 'x', 'tiktok', 'pinterest', 'snapchat',
        'microsoft', 'bing', 'amazon', 'youtube', 'display', 'search',
        'social', 'video', 'shopping', 'programmatic'
    }
    
    # Allowed channels
    ALLOWED_CHANNELS = {
        'search', 'display', 'social', 'video', 'shopping', 'email',
        'programmatic', 'native', 'affiliate', 'influencer', 'pmax',
        'performance max', 'retargeting', 'prospecting', 'brand', 'generic'
    }
    
    @classmethod
    def sanitize_string(cls, value: str, max_length: int = 1000) -> str:
        """
        Sanitize a string value for SQL usage.
        
        Args:
            value: Input string
            max_length: Maximum allowed length
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            value = str(value)
        
        # Truncate to max length
        value = value[:max_length]
        
        # Remove null bytes
        value = value.replace('\x00', '')
        
        # Escape single quotes (SQL injection prevention)
        value = value.replace("'", "''")
        
        # Remove SQL comment markers
        value = value.replace('--', '')
        value = value.replace('/*', '')
        value = value.replace('*/', '')
        
        # Remove semicolons (prevents statement termination)
        value = value.replace(';', '')
        
        return value
    
    @classmethod
    def is_valid_identifier(cls, identifier: str) -> bool:
        """
        Check if a string is a valid SQL identifier.
        
        Args:
            identifier: Table or column name
            
        Returns:
            True if valid identifier
        """
        if not identifier or not isinstance(identifier, str):
            return False
        
        # Max identifier length (most DBs support 128+)
        if len(identifier) > 128:
            return False
        
        return bool(cls.IDENTIFIER_PATTERN.match(identifier))
    
    @classmethod
    def validate_column_name(cls, column: str, allowed_columns: List[str]) -> Optional[str]:
        """
        Validate a column name against an allowed list.
        
        Args:
            column: Column name to validate
            allowed_columns: List of allowed column names
            
        Returns:
            Validated column name or None
        """
        if not column:
            return None
        
        # Case-insensitive matching
        column_lower = column.lower()
        
        for allowed in allowed_columns:
            if allowed.lower() == column_lower:
                return allowed  # Return the canonical form
        
        return None
    
    @classmethod
    def validate_platform(cls, platform: str) -> Optional[str]:
        """
        Validate a platform name.
        
        Args:
            platform: Platform name to validate
            
        Returns:
            Validated platform name or None
        """
        if not platform:
            return None
        
        platform_lower = platform.lower().strip()
        
        if platform_lower in cls.ALLOWED_PLATFORMS:
            return platform
        
        # Check for partial matches
        for allowed in cls.ALLOWED_PLATFORMS:
            if allowed in platform_lower or platform_lower in allowed:
                return platform
        
        logger.warning(f"Invalid platform rejected: {platform}")
        return None
    
    @classmethod
    def validate_channel(cls, channel: str) -> Optional[str]:
        """
        Validate a channel name.
        
        Args:
            channel: Channel name to validate
            
        Returns:
            Validated channel name or None
        """
        if not channel:
            return None
        
        channel_lower = channel.lower().strip()
        
        if channel_lower in cls.ALLOWED_CHANNELS:
            return channel
        
        logger.warning(f"Invalid channel rejected: {channel}")
        return None
    
    @classmethod
    def sanitize_number(cls, value: Any) -> Optional[float]:
        """
        Sanitize a numeric value.
        
        Args:
            value: Numeric input
            
        Returns:
            Float value or None
        """
        if value is None:
            return None
        
        try:
            num = float(value)
            # Check for infinity and NaN
            if num != num or num == float('inf') or num == float('-inf'):
                return None
            return num
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def build_in_clause(cls, values: List[str], column: str) -> str:
        """
        Build a safe IN clause with sanitized values.
        
        Args:
            values: List of values
            column: Column name
            
        Returns:
            Safe SQL IN clause
        """
        if not values or not cls.is_valid_identifier(column):
            return "1=1"  # No-op condition
        
        sanitized = [f"'{cls.sanitize_string(v)}'" for v in values]
        return f"{column} IN ({', '.join(sanitized)})"
    
    @classmethod
    def build_date_filter(cls, start_date: str, end_date: str, column: str = 'date') -> str:
        """
        Build a safe date range filter.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            column: Date column name
            
        Returns:
            Safe SQL date clause
        """
        if not cls.is_valid_identifier(column):
            column = 'date'
        
        # Validate date format
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        
        conditions = []
        
        if start_date and date_pattern.match(start_date):
            conditions.append(f"{column} >= '{start_date}'")
        
        if end_date and date_pattern.match(end_date):
            conditions.append(f"{column} <= '{end_date}'")
        
        if not conditions:
            return "1=1"
        
        return " AND ".join(conditions)
    
    @classmethod
    def contains_sql_injection(cls, value: str) -> bool:
        """
        Check if a value contains potential SQL injection patterns.
        
        Args:
            value: Input string
            
        Returns:
            True if suspicious patterns found
        """
        if not value:
            return False
        
        value_upper = value.upper()
        
        # Check for SQL keywords
        for keyword in cls.SQL_KEYWORDS:
            if keyword in value_upper:
                return True
        
        # Check for suspicious patterns
        suspicious_patterns = [
            r"'\s*OR\s+'",      # ' OR '
            r"'\s*AND\s+'",     # ' AND '
            r"1\s*=\s*1",       # 1=1
            r"'\s*=\s*'",       # '='
            r"0x[0-9a-fA-F]+",  # Hex values
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        
        return False


# Global instance
sql_sanitizer = SQLSanitizer()


def safe_sql_format(template: str, **kwargs) -> str:
    """
    Format SQL template with sanitized parameters.
    
    Usage:
        query = safe_sql_format(
            "SELECT * FROM campaigns WHERE platform = '{platform}'",
            platform=user_input
        )
    
    Args:
        template: SQL template with {placeholders}
        **kwargs: Values to substitute
        
    Returns:
        Formatted and sanitized SQL
    """
    sanitized = {}
    
    for key, value in kwargs.items():
        if isinstance(value, str):
            sanitized[key] = sql_sanitizer.sanitize_string(value)
        elif isinstance(value, (int, float)):
            sanitized[key] = sql_sanitizer.sanitize_number(value)
        elif isinstance(value, list):
            sanitized[key] = [sql_sanitizer.sanitize_string(str(v)) for v in value]
        else:
            sanitized[key] = value
    
    return template.format(**sanitized)


# Export for easy import
__all__ = ['SQLSanitizer', 'sql_sanitizer', 'safe_sql_format']
