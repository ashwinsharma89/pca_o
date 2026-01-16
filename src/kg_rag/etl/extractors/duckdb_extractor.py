"""
DuckDB Extractor for KG-RAG ETL

Extracts campaign data from DuckDB for graph ingestion.
"""

import logging
from typing import Optional, List, Dict, Any, Iterator, TYPE_CHECKING
from datetime import date, datetime
from pathlib import Path

# NOTE: duckdb imported lazily inside methods to avoid C++ mutex locks at import time
# import duckdb  # REMOVED - causes import-time DuckDB file locks

# Type checking only (no runtime import)
if TYPE_CHECKING:
    import duckdb

from src.kg_rag.etl.column_resolver import get_column_resolver


logger = logging.getLogger(__name__)



class DuckDBExtractor:
    """
    Extract campaign data from DuckDB for Knowledge Graph ingestion.
    
    Usage:
        extractor = DuckDBExtractor('/path/to/campaigns.duckdb')
        for batch in extractor.extract_campaigns(batch_size=1000):
            # Process batch
    """
    
    def __init__(self, db_path: str):
        """
        Initialize extractor with DuckDB path.
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._resolver = get_column_resolver()
    
    @property
    def connection(self):
        """Get or create DuckDB connection."""
        if self._conn is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"DuckDB file not found: {self.db_path}")
            import duckdb  # Lazy import to avoid C++ mutex lock at module load
            self._conn = duckdb.connect(str(self.db_path), read_only=True)
        return self._conn

    
    def get_tables(self) -> List[str]:
        """Get list of tables in the database."""
        result = self.connection.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    
    def get_columns(self, table: str) -> List[str]:
        """Get column names for a table."""
        result = self.connection.execute(f"DESCRIBE {table}").fetchall()
        return [row[0] for row in result]
    
    def detect_platform(self, table: str) -> Optional[str]:
        """Detect platform from table columns."""
        columns = self.get_columns(table)
        return self._resolver.detect_platform(columns)
    
    def extract_campaigns(
        self,
        table: str = "campaigns",
        batch_size: int = 1000,
        platform_filter: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Extract campaigns in batches.
        
        Args:
            table: Table name
            batch_size: Rows per batch
            platform_filter: Optional platform to filter
            date_from: Optional start date filter
            date_to: Optional end date filter
            
        Yields:
            Batches of campaign records with resolved column names
        """
        # Build query
        query = f"SELECT * FROM {table}"
        conditions = []
        params = {}
        
        if platform_filter:
            # Try to find platform column
            columns = self.get_columns(table)
            platform_col = self._resolver.find_column(columns, "platform")
            if platform_col:
                conditions.append(f'"{platform_col}" = $platform')
                params["platform"] = platform_filter
        
        if date_from:
            columns = self.get_columns(table)
            date_col = self._resolver.find_column(columns, "date")
            if date_col:
                conditions.append(f'"{date_col}" >= $date_from')
                params["date_from"] = date_from
        
        if date_to:
            columns = self.get_columns(table)
            date_col = self._resolver.find_column(columns, "date")
            if date_col:
                conditions.append(f'"{date_col}" <= $date_to')
                params["date_to"] = date_to
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Execute with batching
        logger.info(f"Extracting from {table} with batch_size={batch_size}")
        
        result = self.connection.execute(query, params)
        columns = [desc[0] for desc in result.description]
        
        # Resolve column names
        column_map = self._resolver.resolve_dataframe_columns(columns)
        
        batch = []
        for row in result.fetchall():
            # Create record with resolved column names
            record = {}
            for i, (orig_col, value) in enumerate(zip(columns, row)):
                canonical = column_map.get(orig_col, orig_col)
                # Handle datetime conversion
                if isinstance(value, datetime):
                    value = value.date() if hasattr(value, 'date') else value
                record[canonical] = value
            
            batch.append(record)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining
        if batch:
            yield batch
    
    def extract_metrics(
        self,
        table: str = "metrics",
        batch_size: int = 1000,
        campaign_ids: Optional[List[str]] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Extract daily metrics in batches.
        
        Args:
            table: Metrics table name
            batch_size: Rows per batch
            campaign_ids: Optional campaign ID filter
            date_from: Optional start date
            date_to: Optional end date
            
        Yields:
            Batches of metric records
        """
        columns = self.get_columns(table)
        
        query = f"SELECT * FROM {table}"
        conditions = []
        params = {}
        
        # Find campaign_id column
        campaign_col = self._resolver.find_column(columns, "campaign_id")
        if campaign_ids and campaign_col:
            placeholders = ", ".join([f"${i}" for i in range(len(campaign_ids))])
            conditions.append(f'"{campaign_col}" IN ({placeholders})')
            for i, cid in enumerate(campaign_ids):
                params[str(i)] = cid
        
        # Date filters
        date_col = self._resolver.find_column(columns, "date")
        if date_from and date_col:
            conditions.append(f'"{date_col}" >= $date_from')
            params["date_from"] = date_from
        if date_to and date_col:
            conditions.append(f'"{date_col}" <= $date_to')
            params["date_to"] = date_to
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        logger.info(f"Extracting metrics from {table}")
        
        result = self.connection.execute(query, params)
        columns = [desc[0] for desc in result.description]
        column_map = self._resolver.resolve_dataframe_columns(columns)
        
        batch = []
        for row in result.fetchall():
            record = {}
            for orig_col, value in zip(columns, row):
                canonical = column_map.get(orig_col, orig_col)
                if isinstance(value, datetime):
                    value = value.date()
                record[canonical] = value
            batch.append(record)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch
    
    def get_campaign_count(self, table: str = "campaigns") -> int:
        """Get total campaign count."""
        result = self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return result[0] if result else 0
    
    def get_date_range(self, table: str = "metrics") -> Dict[str, Optional[date]]:
        """Get date range in metrics table."""
        columns = self.get_columns(table)
        date_col = self._resolver.find_column(columns, "date")
        
        if not date_col:
            return {"min": None, "max": None}
        
        result = self.connection.execute(
            f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM {table}'
        ).fetchone()
        
        return {
            "min": result[0] if result else None,
            "max": result[1] if result else None
        }
    
    def close(self):
        """Close the connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
