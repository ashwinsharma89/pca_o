"""
Cypher Query Generator

Generates Cypher queries for KùzuDB from natural language using LLM.
"""

import logging
from typing import Optional, Dict, Any

from langchain.llms.base import BaseLanguageModel
from langchain.prompts.prompt import PromptTemplate


logger = logging.getLogger(__name__)


CYPHER_SYSTEM_PROMPT = """You are a Cypher query expert for KùzuDB.
KùzuDB is an embedded graph database that supports Cypher.

Given a natural language question, generate a valid Cypher query.

Node types: Campaign, Targeting, Metric, EntityGroup, Creative, Keyword, Placement, Audience, Platform, Account, Channel

Common query patterns:
1. Find campaigns by status: MATCH (c:Campaign {status: $status}) RETURN c
2. Find metrics for date range: MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric) WHERE m.date >= date($start_date) AND m.date <= date($end_date) RETURN m
3. Sum metrics by campaign: MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric) RETURN c.name, SUM(m.spend) AS total_spend
4. Period comparison (Growth): 
   MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
   WHERE m.date >= date($p1_start) AND m.date <= date($p2_end)
   WITH m, CASE WHEN m.date <= date($p1_end) THEN 'Previous' ELSE 'Current' END AS period
   WITH period, SUM(m.spend) AS total_spend
   RETURN period, total_spend
5. Trend Analysis: MATCH (m:Metric) RETURN date.truncate('month', m.date) AS month, SUM(m.spend) ORDER BY month

Return ONLY the Cypher query, no explanation.
"""


class CypherGenerator:
    """Generate Cypher queries from natural language."""

    def __init__(self, llm: BaseLanguageModel):
        self.llm = llm
        self.prompt = PromptTemplate(
            input_variables=["question"],
            template=CYPHER_SYSTEM_PROMPT + "\n\nQuestion: {question}\n\nCypher Query:"
        )

    def generate(self, question: str) -> str:
        """Generate a Cypher query from a natural language question."""
        try:
            response = self.llm.predict(
                text=self.prompt.format(question=question)
            )
            query = response.strip()
            logger.debug(f"Generated query: {query}")
            return query
        except Exception as e:
            logger.error(f"Failed to generate query: {e}")
            raise

    def validate_query(self, query: str) -> bool:
        """Basic validation of generated query."""
        required_keywords = ["MATCH", "RETURN"]
        return all(keyword in query.upper() for keyword in required_keywords)
