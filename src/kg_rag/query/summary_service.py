"""
KG-RAG Summary Service

Generates summaries of query results using LLM.
"""

import logging
from typing import Dict, Any, List, Optional

from langchain.llms.base import BaseLanguageModel

from src.kg_rag.client.connection import get_kuzu_connection


logger = logging.getLogger(__name__)


class SummaryService:
    """
    Generate natural language summaries of query results.
    """

    def __init__(self, llm: BaseLanguageModel):
        self.llm = llm
        self._conn = get_kuzu_connection()

    def summarize_results(
        self,
        results: List[Dict[str, Any]],
        query_context: str
    ) -> str:
        """
        Summarize query results in natural language.
        
        Args:
            results: Query result set
            query_context: Original natural language query
            
        Returns:
            Natural language summary
        """
        if not results:
            return "No results found."

        summary_prompt = f"""
Summarize these query results in natural language:

Query: {query_context}

Results:
{self._format_results(results)}

Provide a concise, human-readable summary.
"""
        try:
            summary = self.llm.predict(text=summary_prompt)
            return summary
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            return "Could not generate summary."

    def _format_results(self, results: List[Dict[str, Any]]) -> str:
        """Format results for LLM consumption."""
        lines = []
        for i, result in enumerate(results[:10]):  # Limit to 10 for brevity
            lines.append(f"{i+1}. {result}")
        if len(results) > 10:
            lines.append(f"... and {len(results) - 10} more results")
        return "\n".join(lines)
