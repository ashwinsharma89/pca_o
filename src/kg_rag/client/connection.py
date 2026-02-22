"""
KG-RAG Neo4j Connection Manager

Manages Neo4j driver lifecycle and provides connection pooling.
"""

import logging
from typing import Optional, Any, Dict, List
from contextlib import contextmanager

from neo4j import GraphDatabase, Driver, Session, Result
from neo4j.exceptions import ServiceUnavailable, AuthError

from src.kg_rag.config.settings import get_kg_rag_settings, KGRAGSettings


logger = logging.getLogger(__name__)


class Neo4jConnection:
    """
    Neo4j connection manager with connection pooling.
    
    Usage:
        neo4j = Neo4jConnection()
        with neo4j.session() as session:
            result = session.run("MATCH (n) RETURN count(n)")
    """
    
    _instance: Optional["Neo4jConnection"] = None
    _driver: Optional[Driver] = None
    
    def __new__(cls) -> "Neo4jConnection":
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize connection (only once due to singleton)."""
        if self._driver is None:
            self._settings = get_kg_rag_settings()
            # connection is lazy, will connect on first access
    
    def _connect(self) -> None:
        """Establish connection to Neo4j."""
        try:
            self._driver = GraphDatabase.driver(
                self._settings.neo4j_uri,
                auth=(self._settings.neo4j_user, self._settings.neo4j_password),
                max_connection_lifetime=self._settings.neo4j_max_connection_lifetime,
                max_connection_pool_size=self._settings.neo4j_max_connection_pool_size,
                connection_acquisition_timeout=self._settings.neo4j_connection_acquisition_timeout,
            )
            # Verify connectivity
            self._driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {self._settings.neo4j_uri}")
        except AuthError as e:
            logger.error(f"Neo4j authentication failed: {e}")
            raise
        except ServiceUnavailable as e:
            logger.error(f"Neo4j service unavailable: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    @property
    def driver(self) -> Driver:
        """Get the Neo4j driver."""
        if self._driver is None:
            self._connect()
        return self._driver
    
    @contextmanager
    def session(self, database: Optional[str] = None):
        """
        Get a Neo4j session context manager.
        
        Args:
            database: Optional database name (defaults to settings)
            
        Yields:
            Neo4j Session
        """
        db = database or self._settings.neo4j_database
        session = self.driver.session(database=db)
        try:
            yield session
        finally:
            session.close()
    
    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results as list of dicts.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            database: Optional database name
            
        Returns:
            List of result records as dictionaries
        """
        with self.session(database) as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a write query and return summary.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            database: Optional database name
            
        Returns:
            Query summary with counters
        """
        with self.session(database) as session:
            result = session.run(query, parameters or {})
            summary = result.consume()
            return {
                "nodes_created": summary.counters.nodes_created,
                "nodes_deleted": summary.counters.nodes_deleted,
                "relationships_created": summary.counters.relationships_created,
                "relationships_deleted": summary.counters.relationships_deleted,
                "properties_set": summary.counters.properties_set,
            }
    
    def execute_batch(
        self,
        query: str,
        batch: List[Dict[str, Any]],
        batch_param_name: str = "batch",
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a batch write using UNWIND.
        
        Args:
            query: Cypher query with UNWIND $batch_param_name AS row
            batch: List of records to process
            batch_param_name: Name of batch parameter in query
            database: Optional database name
            
        Returns:
            Query summary with total counters
        """
        with self.session(database) as session:
            result = session.run(query, {batch_param_name: batch})
            summary = result.consume()
            return {
                "nodes_created": summary.counters.nodes_created,
                "relationships_created": summary.counters.relationships_created,
                "properties_set": summary.counters.properties_set,
                "batch_size": len(batch),
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check Neo4j connection health.
        
        Returns:
            Health status dict
        """
        try:
            result = self.execute_query("RETURN 1 AS health")
            return {
                "status": "healthy",
                "connected": True,
                "uri": self._settings.neo4j_uri,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
            }
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get schema information from Neo4j."""
        labels = self.execute_query("CALL db.labels()")
        rel_types = self.execute_query("CALL db.relationshipTypes()")
        indexes = self.execute_query("SHOW INDEXES")
        constraints = self.execute_query("SHOW CONSTRAINTS")
        
        return {
            "labels": [r["label"] for r in labels],
            "relationship_types": [r["relationshipType"] for r in rel_types],
            "index_count": len(indexes),
            "constraint_count": len(constraints),
        }
    
    def close(self) -> None:
        """Close the driver connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed")
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()


# Convenience function
def get_neo4j_connection() -> Neo4jConnection:
    """Get the Neo4j connection singleton."""
    return Neo4jConnection()
