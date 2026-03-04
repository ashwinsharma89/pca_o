"""
KG-RAG Query Router

Routes natural language queries to template-based or LLM-based Cypher generation.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple

from langchain.llms.base import BaseLanguageModel
from langchain.prompts.prompt import PromptTemplate

from src.kg_rag.client.connection import get_kuzu_connection, KuzuConnection
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


ROUTING_PROMPT_TEMPLATE = """You are a Cypher expert for KùzuDB.
Analyze the natural language query and determine if it matches a known template.

Known templates:
1. Campaign status filter: "campaigns with status X"
2. Metric aggregation: "total spend by campaign"
3. Performance comparison: "campaigns with high ROI"

If the query matches a template, respond with: TEMPLATE: [template_name]
If not, respond with: NOVEL_QUERY

Query: {query}
"""


class QueryRouter:
    """
    Route queries to appropriate handler (template or LLM).
    """

    def __init__(
        self,
        llm: BaseLanguageModel,
        connection: Optional[KuzuConnection] = None
    ):
        self.llm = llm
        self._conn = connection or get_kuzu_connection()
        self._settings = get_kg_rag_settings()
        self.routing_prompt = PromptTemplate(
            input_variables=["query"],
            template=ROUTING_PROMPT_TEMPLATE
        )

    def route(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """
        Route query to handler and execute.
        
        Returns:
            (query_type, results)
        """
        if not self._settings.kg_rag_enabled:
            return "disabled", {}

        # Determine routing
        routing_result = self.llm.predict(
            text=self.routing_prompt.format(query=query)
        )

        if "TEMPLATE:" in routing_result:
            return self._handle_template(query, routing_result)
        else:
            return self._handle_novel_query(query)

    def _handle_template(self, query: str, template_name: str) -> Tuple[str, Dict[str, Any]]:
        """Execute template-based query."""
        logger.info(f"Using template: {template_name}")
        # Template implementations would go here
        return "template", {}

    def _handle_novel_query(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """Handle novel query with LLM."""
        if not self._settings.kg_rag_use_llm_for_novel_queries:
            logger.warning("Novel query routing disabled")
            return "disabled", {}

        logger.info("Routing to LLM for novel query")
        # LLM-based Cypher generation would go here
        return "llm", {}

    def execute_cypher(self, cypher_query: str) -> List[Dict[str, Any]]:
        """Execute a Cypher query."""
        try:
            result = self._conn.execute_query(cypher_query)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            if self._settings.kg_rag_fallback_to_sql:
                logger.info("Falling back to SQL")
            raise
