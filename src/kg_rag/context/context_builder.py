"""
KG-RAG Context Builder

Builds optimized context from Knowledge Graph for LLM consumption.
"""

import logging
from typing import Dict, Any, List, Optional

from src.kg_rag.client.connection import get_neo4j_connection, Neo4jConnection
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


class ContextBuilder:
    """
    Build context from Knowledge Graph for LLM consumption.
    
    Creates compact, relevant context including:
    - Schema information
    - Sample data
    - Relevant subgraphs
    
    Usage:
        builder = ContextBuilder()
        context = builder.build_schema_context()
        context = builder.build_query_context("What campaigns are on Meta?")
    """
    
    def __init__(self, connection: Optional[Neo4jConnection] = None):
        """
        Initialize builder.
        
        Args:
            connection: Neo4j connection
        """
        self._conn = connection
        self._settings = get_kg_rag_settings()
    
    @property
    def connection(self) -> Neo4jConnection:
        """Get Neo4j connection (lazy load)."""
        if self._conn is None:
            self._conn = get_neo4j_connection()
        return self._conn
    
    def build_schema_context(self) -> str:
        """
        Build schema context for LLM.
        
        Returns:
            Formatted schema description
        """
        lines = [
            "# Knowledge Graph Schema",
            "",
            "## Nodes",
        ]
        
        # Node labels and properties
        nodes = self._get_node_info()
        for node in nodes:
            props = ", ".join(node["properties"][:10])  # Limit properties
            if len(node["properties"]) > 10:
                props += f", ... ({len(node['properties'])} total)"
            lines.append(f"- **{node['label']}** ({node['count']} nodes): {props}")
        
        lines.extend([
            "",
            "## Relationships",
        ])
        
        # Relationships
        rels = self._get_relationship_info()
        for rel in rels:
            lines.append(f"- (:{rel['from']})-[:{rel['type']}]->(:{rel['to']}) [{rel['count']}]")
        
        lines.extend([
            "",
            "## Sample Data",
        ])
        
        # Sample platforms
        platforms = self._get_sample_platforms()
        if platforms:
            lines.append(f"Platforms: {', '.join(platforms)}")
        
        return "\n".join(lines)
    
    def build_query_context(
        self,
        query: str,
        max_campaigns: int = 10
    ) -> str:
        """
        Build context relevant to a specific query.
        
        Args:
            query: Natural language query
            max_campaigns: Max sample campaigns
            
        Returns:
            Formatted context string
        """
        lines = [
            "# Query Context",
            f"Question: {query}",
            "",
        ]
        
        # Detect relevant entities
        query_lower = query.lower()
        
        # Platform-specific context
        platforms = ["meta", "google", "linkedin", "tiktok", "dv360"]
        for platform in platforms:
            if platform in query_lower:
                platform_context = self._get_platform_context(platform)
                if platform_context:
                    lines.extend([
                        f"## {platform.title()} Context",
                        platform_context,
                        ""
                    ])
        
        # General stats
        stats = self._get_general_stats()
        if stats:
            lines.extend([
                "## General Statistics",
                f"Total Campaigns: {stats.get('campaigns', 0)}",
                f"Total Spend: ${stats.get('spend', 0):,.2f}",
                f"Date Range: {stats.get('min_date')} to {stats.get('max_date')}",
                ""
            ])
        
        # Sample campaigns
        samples = self._get_sample_campaigns(max_campaigns)
        if samples:
            lines.extend([
                "## Sample Campaigns",
            ])
            for c in samples:
                lines.append(f"- {c['name']} ({c['platform']}): ${c['spend']:,.2f} spend")
        
        return "\n".join(lines)
    
    def build_compact_context(
        self,
        max_tokens: int = 2000
    ) -> str:
        """
        Build compact context within token limit.
        
        Args:
            max_tokens: Approximate token limit
            
        Returns:
            Compact context string
        """
        # Estimate 4 chars per token
        max_chars = max_tokens * 4
        
        # Build components
        components = []
        
        # Schema (essential)
        schema = self._get_compact_schema()
        components.append(schema)
        
        # Stats
        stats = self._get_general_stats()
        if stats:
            components.append(
                f"Stats: {stats.get('campaigns', 0)} campaigns, "
                f"${stats.get('spend', 0):,.0f} total spend"
            )
        
        # Platforms
        platforms = self._get_sample_platforms()
        if platforms:
            components.append(f"Platforms: {', '.join(platforms)}")
        
        # Join and truncate
        context = "\n".join(components)
        if len(context) > max_chars:
            context = context[:max_chars] + "..."
        
        return context
    
    def _get_node_info(self) -> List[Dict[str, Any]]:
        """Get node label info."""
        try:
            query = """
            CALL db.labels() YIELD label
            CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) as count, keys(n) as props LIMIT 1', {})
            YIELD value
            RETURN label, value.count as count, value.props as properties
            ORDER BY value.count DESC
            """
            return self.connection.execute_query(query)
        except Exception:
            # Fallback without APOC
            labels = self.connection.execute_query("CALL db.labels()")
            result = []
            for row in labels:
                label = row.get("label", "")
                count_result = self.connection.execute_query(
                    f"MATCH (n:{label}) RETURN count(n) as count"
                )
                count = count_result[0]["count"] if count_result else 0
                result.append({
                    "label": label,
                    "count": count,
                    "properties": []
                })
            return result
    
    def _get_relationship_info(self) -> List[Dict[str, Any]]:
        """Get relationship type info."""
        try:
            query = """
            CALL db.relationshipTypes() YIELD relationshipType
            RETURN relationshipType as type
            """
            rels = self.connection.execute_query(query)
            
            result = []
            for r in rels:
                rel_type = r["type"]
                # Get sample for from/to
                sample_query = f"""
                MATCH (a)-[r:{rel_type}]->(b)
                RETURN labels(a)[0] as from_label, labels(b)[0] as to_label, count(r) as count
                LIMIT 1
                """
                sample = self.connection.execute_query(sample_query)
                if sample:
                    result.append({
                        "type": rel_type,
                        "from": sample[0].get("from_label", "?"),
                        "to": sample[0].get("to_label", "?"),
                        "count": sample[0].get("count", 0)
                    })
            return result
        except Exception as e:
            logger.warning(f"Failed to get relationship info: {e}")
            return []
    
    def _get_sample_platforms(self) -> List[str]:
        """Get sample platform names."""
        try:
            query = "MATCH (p:Platform) RETURN p.name as name ORDER BY p.name LIMIT 10"
            result = self.connection.execute_query(query)
            return [r["name"] for r in result]
        except Exception:
            return []
    
    def _get_platform_context(self, platform_id: str) -> Optional[str]:
        """Get context for a specific platform."""
        try:
            query = """
            MATCH (p:Platform {id: $platform_id})<-[:BELONGS_TO]-(c:Campaign)
            RETURN p.name as platform,
                   count(c) as campaigns,
                   SUM(c.spend_total) as spend,
                   p.supports_keywords as has_keywords,
                   p.supports_placements as has_placements
            """
            result = self.connection.execute_query(query, {"platform_id": platform_id})
            
            if result:
                r = result[0]
                return (
                    f"Platform: {r['platform']}\n"
                    f"Campaigns: {r['campaigns']}\n"
                    f"Total Spend: ${r.get('spend', 0):,.2f}\n"
                    f"Supports Keywords: {r.get('has_keywords', False)}\n"
                    f"Supports Placements: {r.get('has_placements', False)}"
                )
            return None
        except Exception:
            return None
    
    def _get_general_stats(self) -> Dict[str, Any]:
        """Get general statistics."""
        try:
            query = """
            MATCH (c:Campaign)
            OPTIONAL MATCH (c)-[:HAS_PERFORMANCE]->(m:Metric)
            RETURN count(DISTINCT c) as campaigns,
                   SUM(c.spend_total) as spend,
                   MIN(m.date) as min_date,
                   MAX(m.date) as max_date
            """
            result = self.connection.execute_query(query)
            if result:
                stats = result[0]
                # Convert Neo4j Date types to ISO strings
                if stats.get("min_date"):
                    stats["min_date"] = stats["min_date"].iso_format() if hasattr(stats["min_date"], 'iso_format') else str(stats["min_date"])
                if stats.get("max_date"):
                    stats["max_date"] = stats["max_date"].iso_format() if hasattr(stats["max_date"], 'iso_format') else str(stats["max_date"])
                return stats
            return {}
        except Exception:
            return {}
    
    def _get_sample_campaigns(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample campaigns."""
        try:
            query = """
            MATCH (c:Campaign)
            RETURN c.name as name,
                   c.platform_id as platform,
                   c.spend_total as spend
            ORDER BY c.spend_total DESC
            LIMIT $limit
            """
            return self.connection.execute_query(query, {"limit": limit})
        except Exception:
            return []
    
    def _get_compact_schema(self) -> str:
        """Get compact schema description."""
        return """Schema:
- Channel -> Platform -> Campaign -> Metric (daily)
- Campaign -> Targeting (device, age, geo, interests)
- Campaign -> EntityGroup -> Placement/Keyword
Key metrics: impressions, clicks, spend, conversions, revenue
Calculate: CTR=clicks/impr*100, CPC=spend/clicks, ROAS=revenue/spend, CPA=spend/conv"""
