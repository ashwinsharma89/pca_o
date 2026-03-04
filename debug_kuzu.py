"""
KùzuDB Debug Utility

Provides interactive debugging and inspection of the knowledge graph.
"""

import logging
from typing import Optional

from src.kg_rag.client.connection import get_neo4j_connection, KuzuConnection
from src.kg_rag.config.settings import get_kg_rag_settings


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class KuzuDebugger:
    """Interactive debugger for KùzuDB."""

    def __init__(self, connection: Optional[KuzuConnection] = None):
        self._conn = connection or get_neo4j_connection()
        self._settings = get_kg_rag_settings()

    def show_schema(self) -> None:
        """Display the database schema."""
        try:
            tables = self._conn.show_tables()
            print("\nKùzuDB Tables:")
            print("=" * 50)
            for table in tables:
                print(f"  - {table}")
            print("=" * 50)
        except Exception as e:
            logger.error(f"Failed to show schema: {e}")

    def show_node_counts(self) -> None:
        """Display node counts for each node type."""
        node_types = [
            "Channel", "Platform", "Account", "Campaign",
            "Targeting", "Metric", "EntityGroup", "Creative",
            "Keyword", "Placement", "Audience"
        ]

        print("\nNode Counts:")
        print("=" * 50)

        for node_type in node_types:
            try:
                result = self._conn.execute_query(f"MATCH (n:{node_type}) RETURN count(n) AS cnt")
                count = result[0]["cnt"] if result else 0
                print(f"  {node_type}: {count}")
            except Exception as e:
                logger.warning(f"Could not count {node_type}: {e}")

        print("=" * 50)

    def show_relationship_counts(self) -> None:
        """Display relationship counts."""
        rel_types = [
            "CATEGORIZES", "HOSTS", "OWNS", "BELONGS_TO",
            "HAS_TARGETING", "HAS_PERFORMANCE", "CONTAINS",
            "HAS_CREATIVE", "HAS_KEYWORD", "HAS_PLACEMENT",
            "OVERLAPS_WITH", "SIMILAR_TO"
        ]

        print("\nRelationship Counts:")
        print("=" * 50)

        for rel_type in rel_types:
            try:
                result = self._conn.execute_query(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
                count = result[0]["cnt"] if result else 0
                print(f"  {rel_type}: {count}")
            except Exception as e:
                logger.warning(f"Could not count {rel_type}: {e}")

        print("=" * 50)

    def sample_query(self, limit: int = 5) -> None:
        """Show sample campaigns."""
        try:
            result = self._conn.execute_query(
                f"MATCH (c:Campaign) RETURN c.id, c.name, c.status, c.objective LIMIT {limit}"
            )
            print("\nSample Campaigns:")
            print("=" * 50)
            for row in result:
                print(f"  {row}")
            print("=" * 50)
        except Exception as e:
            logger.error(f"Failed to sample campaigns: {e}")

    def run_interactive(self) -> None:
        """Run interactive query loop."""
        print("\nKùzuDB Interactive Debugger")
        print("Commands: show_schema, show_nodes, show_rels, sample, exit")
        print("Or enter a Cypher query directly.\n")

        while True:
            try:
                command = input("kuzu> ").strip()

                if not command:
                    continue
                elif command == "show_schema":
                    self.show_schema()
                elif command == "show_nodes":
                    self.show_node_counts()
                elif command == "show_rels":
                    self.show_relationship_counts()
                elif command == "sample":
                    self.sample_query()
                elif command == "exit":
                    break
                else:
                    # Treat as Cypher query
                    result = self._conn.execute_query(command)
                    for row in result:
                        print(row)
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                logger.error(f"Error: {e}")


def main():
    """Main entry point."""
    settings = get_kg_rag_settings()
    print(f"Connecting to KùzuDB at: {settings.kuzu_db_path}")

    debugger = KuzuDebugger()

    # Show overview
    debugger.show_schema()
    debugger.show_node_counts()
    debugger.show_relationship_counts()
    debugger.sample_query()

    # Interactive mode
    try:
        debugger.run_interactive()
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main()
