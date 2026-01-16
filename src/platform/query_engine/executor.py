"""
Query Executor for NL-to-SQL Query Engine

Handles safe SQL execution with validation, error handling, and result formatting.
Separated from the main engine for testability and single responsibility.

Pattern: Command Pattern
Depends on: DuckDB connection, SQL Validator
Used by: NL-to-SQL Engine
"""

import polars as pl
from typing import Dict, Any, Optional, Tuple, List
from loguru import logger
import re


class QueryExecutor:
    """
    Executes SQL queries safely with validation and error handling.
    
    Responsibilities:
    - Sanitize SQL queries
    - Execute with timeout/limits
    - Format results
    - Handle errors gracefully
    """
    
    def __init__(self, conn=None):
        """
        Initialize query executor.
        
        Args:
            conn: DuckDB connection (optional, can be set later)
        """
        self.conn = conn
        self.last_error: Optional[str] = None
        self.last_query: Optional[str] = None
    
    def set_connection(self, conn) -> None:
        """Set the bank connection."""
        self.conn = conn
    
    def execute(
        self,
        sql_query: str,
        analyze_plan: bool = False,
        max_rows: int = 10000
    ) -> Tuple[Optional[pl.DataFrame], Optional[str]]:
        """
        Execute SQL query and return results.
        
        Args:
            sql_query: SQL query to execute
            analyze_plan: If True, analyze query plan with EXPLAIN
            max_rows: Maximum rows to return
        
        Returns:
            Tuple of (DataFrame or None, error_message or None)
        """
        if not self.conn:
            return (None, "No database connection available")
        
        self.last_query = sql_query
        self.last_error = None
        
        try:
            # Sanitize the query
            sanitized_query = self._sanitize_sql(sql_query)
            
            # Validate basic structure
            if not self._validate_query_structure(sanitized_query):
                return (None, "Invalid query structure")
            
            # Optional query plan analysis
            if analyze_plan:
                try:
                    plan = self.conn.execute(f"EXPLAIN {sanitized_query}").fetchall()
                    logger.info(f"Query plan: {plan}")
                except Exception as e:
                    logger.warning(f"Could not analyze query plan: {e}")
            
            # Execute the query
            # Use .pl() to fetch directly as Polars DataFrame
            result = self.conn.execute(sanitized_query).pl()
            
            # Limit results
            if result.height > max_rows:
                logger.warning(f"Result set truncated from {result.height} to {max_rows} rows")
                result = result.head(max_rows)
            
            return (result, None)
            
        except Exception as e:
            error_msg = str(e)
            self.last_error = error_msg
            logger.error(f"Query execution failed: {error_msg}")
            return (None, error_msg)
    
    def _sanitize_sql(self, sql_query: str) -> str:
        """
        Sanitize SQL query to fix common issues.
        
        Args:
            sql_query: Original SQL query
        
        Returns:
            Sanitized SQL query
        """
        # Remove markdown code blocks
        sql_query = re.sub(r'^```(?:sql)?\n?', '', sql_query, flags=re.MULTILINE)
        sql_query = re.sub(r'\n?```$', '', sql_query, flags=re.MULTILINE)
        
        # Strip whitespace
        sql_query = sql_query.strip()
        
        # Remove trailing semicolon (DuckDB handles it but can cause issues)
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1].strip()
        
        # Fix common LLM mistakes
        
        # 1. Wrong date function names
        sql_query = re.sub(r'\bDATEDIFF\s*\(', 'DATE_DIFF(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bDATEADD\s*\(', 'DATE_ADD(', sql_query, flags=re.IGNORECASE)
        
        # 2. Wrong interval syntax
        sql_query = re.sub(
            r"INTERVAL\s+'?(\d+)'?\s+(DAY|WEEK|MONTH|YEAR)S?",
            r"INTERVAL \1 \2",
            sql_query,
            flags=re.IGNORECASE
        )
        
        # 3. GETDATE() -> CURRENT_DATE (DuckDB syntax)
        sql_query = re.sub(r'\bGETDATE\s*\(\)', 'CURRENT_DATE', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bNOW\s*\(\)', 'CURRENT_TIMESTAMP', sql_query, flags=re.IGNORECASE)
        
        # 4. Fix STRFTIME format for DuckDB
        # DuckDB uses %b for abbreviated month, not MON
        sql_query = re.sub(r"'%MON-%YY'", "'%b-%y'", sql_query, flags=re.IGNORECASE)
        
        return sql_query
    
    def _validate_query_structure(self, sql_query: str) -> bool:
        """
        Basic validation of query structure.
        
        Args:
            sql_query: SQL query to validate
        
        Returns:
            True if valid, False otherwise
        """
        query_upper = sql_query.upper().strip()
        
        # Must start with SELECT, WITH, or EXPLAIN
        valid_starts = ('SELECT', 'WITH', 'EXPLAIN')
        if not any(query_upper.startswith(start) for start in valid_starts):
            logger.warning(f"Query does not start with valid keyword: {query_upper[:50]}")
            return False
        
        # Must not contain dangerous operations
        dangerous_patterns = [
            r'\bDROP\s+TABLE\b',
            r'\bDELETE\s+FROM\b',
            r'\bTRUNCATE\b',
            r'\bALTER\s+TABLE\b',
            r'\bCREATE\s+TABLE\b',
            r'\bINSERT\s+INTO\b',
            r'\bUPDATE\s+\w+\s+SET\b',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper):
                logger.error(f"Dangerous operation detected: {pattern}")
                return False
        
        return True
    
    def format_results(
        self,
        df: Optional[pl.DataFrame],
        format_type: str = "dict"
    ) -> Any:
        """
        Format results for output.
        
        Args:
            df: Query results DataFrame (Polars)
            format_type: "dict", "records", "json", or "markdown"
        
        Returns:
            Formatted results
        """
        if df is None or df.is_empty():
            return None
        
        if format_type == "dict":
            # Polars doesn't have a direct to_dict like pandas default, 
            # usually `to_dict(as_series=False)` or similar is what we want (columnar)
            # But let's check what compatibility is needed.
            # Pandas `to_dict()` default is dict of dicts/series (col -> {index -> value}).
            # `to_dict(orient='records')` is list of dicts.
            # Let's assume 'records' is the most useful/safe unless specified.
            # If the original code allowed 'dict' as default pandas behavior, strictly it's "column -> {index -> value}".
            # Polars `to_dict(as_series=False)` gives "column -> list of values".
            return df.to_dict(as_series=False)
        elif format_type == "records":
            return df.to_dicts()
        elif format_type == "json":
            return df.write_json(row_oriented=True)
        elif format_type == "markdown":
            return str(df) # Simple string representation
        else:
            return df.rows()
    
    def get_result_summary(self, df: Optional[pl.DataFrame]) -> Dict[str, Any]:
        """
        Get a summary of query results for LLM context.
        
        Args:
            df: Query results DataFrame (Polars)
        
        Returns:
            Dictionary with summary statistics
        """
        if df is None or df.is_empty():
            return {"row_count": 0, "columns": []}
        
        summary = {
            "row_count": df.height,
            "columns": df.columns,
            "sample": df.head(5).to_dicts()
        }
        
        # Add numeric summaries
        # Select numeric columns
        numeric_cols = [
            col for col, dtype in zip(df.columns, df.dtypes) 
            if dtype in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)
        ]
        
        if numeric_cols:
            stats = {}
            for col in numeric_cols:
                stats[col] = {
                    "min": float(df[col].min() or 0),
                    "max": float(df[col].max() or 0),
                    "mean": float(df[col].mean() or 0),
                    "sum": float(df[col].sum() or 0)
                }
            summary["numeric_summary"] = stats
        
        return summary


def get_query_executor(conn=None) -> QueryExecutor:
    """Get a query executor instance."""
    return QueryExecutor(conn)
