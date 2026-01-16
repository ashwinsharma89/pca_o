"""
Query Orchestration Layer
Intelligently routes queries through interpretation or direct execution
Handles errors gracefully and manages the complete query flow
"""
from typing import Dict, Any, Optional, List
from loguru import logger
import pandas as pd


class QueryOrchestrator:
    """Orchestrates the query flow with intelligent routing and error handling."""
    
    def __init__(self, query_engine, interpreter=None):
        """
        Initialize orchestrator.
        
        Args:
            query_engine: NaturalLanguageQueryEngine for SQL generation
            interpreter: Optional SmartQueryInterpreter for ambiguous queries
        """
        self.query_engine = query_engine
        self.interpreter = interpreter
        self.use_interpretation = interpreter is not None
    
    def process_query(
        self,
        query: str,
        schema_info: Dict[str, Any],
        force_direct: bool = False
    ) -> Dict[str, Any]:
        """
        Process a query with intelligent routing.
        
        Args:
            query: User's natural language query
            schema_info: Schema information
            force_direct: If True, skip interpretation and execute directly
            
        Returns:
            Dict with:
            - success: bool
            - mode: "direct" or "interpretation"
            - interpretations: List[Dict] (if mode=interpretation)
            - result: Query result (if executed)
            - error: Error message (if failed)
        """
        
        # Force direct execution if requested or no interpreter available
        if force_direct or not self.use_interpretation:
            return self._execute_direct(query)
        
        # Check if query needs interpretation
        needs_interpretation = self._needs_interpretation(query, schema_info)
        
        if not needs_interpretation:
            logger.info(f"Query is clear, executing directly: {query}")
            return self._execute_direct(query)
        
        # Try interpretation
        logger.info(f"Query is ambiguous, generating interpretations: {query}")
        return self._generate_interpretations(query, schema_info)
    
    def execute_interpretation(
        self,
        interpretation: str
    ) -> Dict[str, Any]:
        """
        Execute a selected interpretation.
        
        Args:
            interpretation: The selected interpretation text
            
        Returns:
            Query result dict
        """
        return self._execute_direct(interpretation)
    
    def _needs_interpretation(self, query: str, schema_info: Dict[str, Any]) -> bool:
        """
        Determine if a query needs interpretation.
        
        Args:
            query: User query
            schema_info: Schema information
            
        Returns:
            True if query is ambiguous and needs clarification
        """
        query_lower = query.lower()
        
        # Vague terms that need clarification
        vague_terms = [
            'high', 'low', 'best', 'worst', 'good', 'bad', 
            'top', 'bottom', 'performance', 'effective'
        ]
        
        # Check for vague terms
        has_vague_terms = any(term in query_lower for term in vague_terms)
        
        # Check if specific columns are mentioned
        columns = schema_info.get('columns', [])
        has_specific_columns = any(col.lower() in query_lower for col in columns)
        
        # Check for multiple possible intents
        intent_keywords = ['compare', 'trend', 'rank', 'group', 'filter', 'show', 'list']
        intent_count = sum(1 for kw in intent_keywords if kw in query_lower)
        
        # Needs interpretation if:
        # 1. Has vague terms AND no specific columns mentioned
        # 2. OR has multiple intents (>2)
        if has_vague_terms and not has_specific_columns:
            return True
        if intent_count > 2:
            return True
        
        return False
    
    def _execute_direct(self, query: str) -> Dict[str, Any]:
        """
        Execute query directly without interpretation.
        
        Args:
            query: Natural language query
            
        Returns:
            Result dict
        """
        try:
            result = self.query_engine.ask(query)
            
            return {
                "success": True,
                "mode": "direct",
                "result": result,
                "sql": result.get("sql_query") or result.get("sql", ""),
                "data": result.get("results"),
                "answer": result.get("answer")
            }
        except Exception as e:
            logger.error(f"Direct execution failed: {e}", exc_info=True)
            return {
                "success": False,
                "mode": "direct",
                "error": str(e)
            }
    
    def _generate_interpretations(
        self,
        query: str,
        schema_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate interpretations for ambiguous query.
        
        Args:
            query: User query
            schema_info: Schema information
            
        Returns:
            Result dict with interpretations
        """
        try:
            interpretations = self.interpreter.generate_interpretations(
                query=query,
                schema_info=schema_info,
                num_interpretations=3
            )
            
            return {
                "success": True,
                "mode": "interpretation",
                "interpretations": interpretations,
                "original_query": query
            }
        except Exception as e:
            logger.error(f"Interpretation generation failed: {e}", exc_info=True)
            
            # Fallback to direct execution
            logger.info("Falling back to direct execution")
            return self._execute_direct(query)
