"""
KùzuDB Schema Initialization Script

Creates node and relationship tables from DDL definitions.
Run this once to set up the knowledge graph.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.kg_rag.client.connection import KuzuConnection
from src.kg_rag.config.settings import get_kg_rag_settings
from src.kg_rag.schema.nodes import KUZU_NODE_DDL
from src.kg_rag.schema.edges import KUZU_REL_DDL


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def initialize_schema() -> None:
    """Initialize KùzuDB schema."""
    settings = get_kg_rag_settings()
    logger.info(f"Initializing KùzuDB at: {settings.kuzu_db_path}")

    conn = KuzuConnection(settings.kuzu_db_path)
    conn.connect()

    try:
        # Create node tables
        logger.info("Creating node tables...")
        for ddl in KUZU_NODE_DDL:
            try:
                conn.execute_query(ddl)
                logger.debug(f"Executed: {ddl[:50]}...")
            except Exception as e:
                logger.warning(f"Node table creation warning: {e}")

        logger.info(f"Created {len(KUZU_NODE_DDL)} node tables")

        # Create relationship tables
        logger.info("Creating relationship tables...")
        for ddl in KUZU_REL_DDL:
            try:
                conn.execute_query(ddl)
                logger.debug(f"Executed: {ddl[:50]}...")
            except Exception as e:
                logger.warning(f"Relationship table creation warning: {e}")

        logger.info(f"Created {len(KUZU_REL_DDL)} relationship tables")

        # Verify schema
        tables = conn.show_tables()
        logger.info(f"Database now contains {len(tables)} tables:")
        for table in tables:
            logger.info(f"  - {table}")

        logger.info("Schema initialization complete!")

    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        raise
    finally:
        conn.disconnect()


if __name__ == "__main__":
    try:
        initialize_schema()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)
