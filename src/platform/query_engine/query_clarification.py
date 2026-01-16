"""
Query Clarification Module - Human-in-the-Loop
Generates multiple interpretations of user queries for clarification
Uses Claude Sonnet 4.5 for multi-step agentic reasoning
"""
import os
import json
from typing import List, Dict, Any
import logging
from src.core.config.llm_router import LLMRouter, TaskType
from .sql_knowledge import SQLKnowledgeHelper

logger = logging.getLogger(__name__)


class QueryClarifier:
    """Generates multiple interpretations of natural language queries using optimal LLM."""
    
    def __init__(self):
        """Initialize the query clarifier with LLM router."""
        # Use Claude Sonnet 4.5 for multi-step agentic reasoning
        self.client, self.model, self.config = LLMRouter.get_client(TaskType.QUERY_INTERPRETATION)
        logger.info(f"QueryClarifier initialized with {self.model} for agentic reasoning")
        self.sql_helper = SQLKnowledgeHelper(enable_hybrid=True)
    
    def generate_interpretations(
        self, 
        query: str, 
        schema_info: Dict[str, Any],
        num_interpretations: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Generate multiple interpretations of a user query.
        
        Args:
            query: The natural language query from user
            schema_info: Information about available tables and columns
            num_interpretations: Number of interpretations to generate (default 5)
            
        Returns:
            List of interpretation dictionaries with:
            - interpretation: Human-readable interpretation
            - reasoning: Why this interpretation makes sense
            - confidence: Confidence score (0-1)
            - sql_hint: Hint about what SQL would look like
        """
        
        sql_context = self.sql_helper.build_context(query, schema_info)

        prompt = f"""You are a data analyst helping clarify what a user wants to know about their campaign data.

Available Data Schema:
{json.dumps(schema_info, indent=2)}

SQL Knowledge & Reference:
{sql_context}

User Query: "{query}"

Generate {num_interpretations} different possible interpretations of what the user might be asking.
Each interpretation should be:
1. Specific and actionable
2. Based on the available data
3. Realistic for campaign analysis

For each interpretation, provide:
- A clear, specific interpretation statement
- Brief reasoning for why this interpretation makes sense
- A confidence score (0.0 to 1.0)
- A hint about what the SQL query would focus on

Return ONLY a JSON array with this structure:
[
  {{
    "interpretation": "Show campaigns where total spend exceeds $10,000",
    "reasoning": "User said 'high spend' which typically means above a threshold",
    "confidence": 0.85,
    "sql_hint": "Filter on SUM(Spend) > 10000"
  }},
  ...
]

Order interpretations by confidence score (highest first).
Make interpretations diverse - cover different aspects like thresholds, comparisons, trends, rankings, etc.
"""

        try:
            # Use Claude Sonnet 4.5 via LLM Router for multi-step reasoning
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.config.get("max_tokens", 4096),
                temperature=self.config.get("temperature", 0.7),
                messages=[{"role": "user", "content": prompt}]
            )
            content = response.content[0].text
            
            # Extract JSON from response
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            interpretations = json.loads(content)
            
            # Add index to each interpretation
            for i, interp in enumerate(interpretations):
                interp['index'] = i
            
            logger.info(f"Generated {len(interpretations)} interpretations for query: {query}")
            return interpretations
            
        except Exception as e:
            logger.error(f"Error generating interpretations: {e}")
            # Return a default interpretation
            return [{
                "index": 0,
                "interpretation": f"Analyze: {query}",
                "reasoning": "Using original query as-is",
                "confidence": 0.5,
                "sql_hint": "Direct query translation"
            }]
    
    def refine_interpretation(
        self,
        original_query: str,
        selected_interpretation: Dict[str, Any],
        user_feedback: str = None
    ) -> str:
        """
        Refine the selected interpretation based on user feedback.
        
        Args:
            original_query: Original user query
            selected_interpretation: The interpretation user selected
            user_feedback: Optional additional feedback from user
            
        Returns:
            Refined query string
        """
        
        if not user_feedback:
            return selected_interpretation['interpretation']
        
        prompt = f"""Refine this query interpretation based on user feedback.

Original Query: "{original_query}"
Selected Interpretation: "{selected_interpretation['interpretation']}"
User Feedback: "{user_feedback}"

Provide a refined, more specific interpretation that incorporates the user's feedback.
Return ONLY the refined interpretation as a single clear statement.
"""

        try:
            # Use Claude Sonnet 4.5 for refinement reasoning
            response = self.client.messages.create(
                model=self.model,
                max_tokens=200,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )
            refined = response.content[0].text.strip()
            
            return refined
            
        except Exception as e:
            logger.error(f"Error refining interpretation: {e}")
            return selected_interpretation['interpretation']


def format_interpretations_for_display(interpretations: List[Dict[str, Any]]) -> str:
    """
    Format interpretations for display in Streamlit.
    
    Args:
        interpretations: List of interpretation dictionaries
        
    Returns:
        Formatted string for display
    """
    output = []
    for i, interp in enumerate(interpretations, 1):
        confidence_bar = "🟢" * int(interp['confidence'] * 5)
        output.append(f"""
**Option {i}** {confidence_bar} ({interp['confidence']:.0%} confidence)

**What it means:** {interp['interpretation']}

**Why:** {interp['reasoning']}

**SQL Focus:** {interp['sql_hint']}
""")
    
    return "\n---\n".join(output)
