"""
KG-RAG Database Connection Management

Provides connection interface for KùzuDB (embedded graph database).
"""

import logging
from typing import Dict, Any, List, Optional

import kuzu


logger = logging.getLogger(__name__)


class KuzuConnection:
    """
    Wrapper around KùzuDB connection.
    
    KùzuDB is an embedded graph database with no server.
    The connection manages query execution against local database.
    """

    def __init__(self, db_path: str = "./kuzu_db"):
        """
        Initialize KùzuDB connection.
        
        Args:
            db_path: Path to KùzuDB directory (created if absent)
        """
        self._db_path = db_path
        self._db = None
        self._conn = None
        self._initialized = False

    def connect(self) -> None:
        """Open database connection."""
        if self._initialized:
            return
        try:
            self._db = kuzu.Database(self._db_path)
            self._conn = kuzu.Connection(self._db)
            self._initialized = True
            logger.info(f"Connected to KùzuDB at {self._db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to KùzuDB: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn = None
        if self._db:
            self._db = None
        self._initialized = False
        logger.info("Disconnected from KùzuDB")

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a single query and return results.
        
        Args:
            query: Cypher query string
            params: Query parameters (optional)
            
        Returns:
            List of result dictionaries
        """
        if not self._initialized:
            self.connect()
        try:
            result = self._conn.execute(query, params or {})
            return result.get_as_list()
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {query}")
            raise

    def execute_write(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a write query (CREATE, MERGE, SET, DELETE, etc.).
        For KùzuDB, this is the same as execute_query.
        
        Args:
            query: Cypher query string
            params: Query parameters (optional)
            
        Returns:
            List of result dictionaries
        """
        return self.execute_query(query, params)

    def execute_batch(
        self,
        query: str,
        batch: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Execute a batch operation (UNWIND).
        
        Args:
            query: Cypher query with $batch parameter
            batch: List of records to UNWIND
            
        Returns:
            Dictionary with execution stats
        """
        if not self._initialized:
            self.connect()
        try:
            result = self._conn.execute(query, {"batch": batch})
            stats = {
                "nodes_created": 0,
                "properties_set": 0,
            }
            # KùzuDB returns query result; extract stats if available
            return stats
        except Exception as e:
            logger.error(f"Batch execution failed: {e}\nQuery: {query}")
            raise

    def show_tables(self) -> List[str]:
        """List all node and relationship tables."""
        if not self._initialized:
            self.connect()
        try:
            result = self._conn.execute("SHOW TABLES")
            return [row["name"] for row in result.get_as_list()]
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []

    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get schema information about the database.
        
        Returns:
            Dictionary with 'labels' and 'relations' keys
        """
        if not self._initialized:
            self.connect()
        try:
            tables = self.show_tables()
            # Separate node tables from relation tables
            labels = []
            relations = []
            
            # Node tables are typically uppercase first letter
            # Relation tables are typically all uppercase
            for table in tables:
                if table.isupper():
                    relations.append(table)
                else:
                    labels.append(table)
            
            return {
                "labels": labels,
                "relations": relations,
            }
        except Exception as e:
            logger.error(f"Failed to get schema info: {e}")
            return {"labels": [], "relations": []}

    def health_check(self) -> Dict[str, Any]:
        """
        Check database health status.
        
        Returns:
            Dictionary with health status
        """
        try:
            if not self._initialized:
                self.connect()
            # Try a simple query to verify connection
            self._conn.execute("RETURN 1")
            return {
                "status": "healthy",
                "connected": True,
                "uri": self._db_path,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "uri": self._db_path,
                "error": str(e),
            }

    def drop_table(self, table_name: str) -> None:
        """Drop a table."""
        if not self._initialized:
            self.connect()
        try:
            self._conn.execute(f"DROP TABLE {table_name}")
            logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            raise

    def is_connected(self) -> bool:
        """Check if connected."""
        return self._initialized and self._conn is not None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# Singleton instance
_connection: Optional[KuzuConnection] = None


def get_kuzu_connection() -> KuzuConnection:
    """Get or create KùzuDB connection singleton."""
    global _connection
    if _connection is None:
        from src.kg_rag.config.settings import get_kg_rag_settings
        settings = get_kg_rag_settings()
        _connection = KuzuConnection(settings.kuzu_db_path)
        _connection.connect()
    return _connection


def close_kuzu_connection() -> None:
    """Close the KùzuDB connection."""
    global _connection
    if _connection:
        _connection.disconnect()
        _connection = None
