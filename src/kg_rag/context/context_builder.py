"""
KG-RAG Context Builder

Builds context from the knowledge graph for LLM prompts.
"""

import logging
from typing import Dict, Any, List, Optional

from src.kg_rag.client.connection import get_kuzu_connection, KuzuConnection
from src.kg_rag.schema.edges import EDGE_DEFINITIONS
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


class ContextBuilder:
    """
    Build context from the knowledge graph.
    """

    def __init__(self, connection: Optional[KuzuConnection] = None):
        self._conn = connection or get_kuzu_connection()
        self._settings = get_kg_rag_settings()
        self._node_info: Optional[Dict[str, Any]] = None
        self._relationship_info: Optional[Dict[str, Any]] = None

    @property
    def connection(self) -> KuzuConnection:
        """Get the database connection."""
        return self._conn

    def get_schema_context(self) -> Dict[str, Any]:
        """Get schema information for context."""
        if self._node_info is None:
            self._node_info = self._get_node_info()
        if self._relationship_info is None:
            self._relationship_info = self._get_relationship_info()

        return {
            "nodes": self._node_info,
            "relationships": self._relationship_info,
        }

    def _get_node_info(self) -> Dict[str, Any]:
        """Get node table information."""
        try:
            tables = self._conn.show_tables()
            node_tables = {}
            for table in tables:
                # Filter to known node types
                node_types = [
                    "Channel", "Platform", "Account", "Campaign",
                    "Targeting", "Metric", "EntityGroup", "Creative",
                    "Keyword", "Placement", "Audience"
                ]
                if table in node_types:
                    node_tables[table] = {"name": table, "properties": []}
            return node_tables
        except Exception as e:
            logger.error(f"Failed to get node info: {e}")
            return {}

    def _get_relationship_info(self) -> Dict[str, Any]:
        """Get relationship definitions from schema."""
        try:
            relationships = {}
            for rel_type, from_node, to_node in EDGE_DEFINITIONS:
                relationships[rel_type] = {
                    "name": rel_type,
                    "from": from_node,
                    "to": to_node,
                }
            return relationships
        except Exception as e:
            logger.error(f"Failed to get relationship info: {e}")
            return {}

    def get_sample_campaigns(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get sample campaigns for context."""
        limit = limit or self._settings.max_sample_campaigns

        query = f"""
        MATCH (c:Campaign)
        RETURN c.id, c.name, c.status, c.objective, c.budget
        LIMIT {limit}
        """
        try:
            result = self._conn.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Failed to get sample campaigns: {e}")
            return []

    def build_context_string(self) -> str:
        """Build a context string for LLM prompts."""
        schema = self.get_schema_context()
        samples = self.get_sample_campaigns()

        context_parts = [
            "Knowledge Graph Schema:",
            f"Nodes: {', '.join(schema['nodes'].keys())}",
            f"Relationships: {', '.join(schema['relationships'].keys())}",
            "\nSample Campaigns:",
        ]

        for campaign in samples:
            context_parts.append(f"  - {campaign.get('name', 'Unknown')}")

        return "\n".join(context_parts)
