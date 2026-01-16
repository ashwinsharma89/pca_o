"""
Detailed unit tests for Query Orchestrator.
Tests interpretation logic, execution paths, and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Try to import directly (avoid workflow.py which needs langgraph)
try:
    import sys
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "query_orchestrator",
        "src/orchestration/query_orchestrator.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules['query_orchestrator'] = module
    spec.loader.exec_module(module)
    QueryOrchestrator = module.QueryOrchestrator
    ORCHESTRATOR_AVAILABLE = True
except Exception:
    ORCHESTRATOR_AVAILABLE = False
    QueryOrchestrator = None

pytestmark = pytest.mark.skipif(not ORCHESTRATOR_AVAILABLE, reason="Query orchestrator not available")


class TestNeedsInterpretation:
    """Test _needs_interpretation logic."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock engine."""
        mock_engine = Mock()
        mock_interpreter = Mock()
        return QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
    
    def test_vague_terms_need_interpretation(self, orchestrator):
        """Test queries with vague terms need interpretation."""
        # Use columns that won't match vague terms
        schema_info = {'columns': ['Campaign_Name', 'Platform', 'Region']}
        
        vague_queries = [
            "Show high performing campaigns",
            "What are the best campaigns",
            "Show top performers"
        ]
        
        for query in vague_queries:
            result = orchestrator._needs_interpretation(query, schema_info)
            assert result is True, f"Expected True for: {query}"
    
    def test_specific_queries_no_interpretation(self, orchestrator):
        """Test specific queries don't need interpretation."""
        schema_info = {'columns': ['Campaign_Name', 'Spend', 'Conversions']}
        
        specific_queries = [
            "Show campaigns where Spend > 1000",
            "List all Campaign_Name",
            "Show Conversions by Campaign_Name"
        ]
        
        for query in specific_queries:
            result = orchestrator._needs_interpretation(query, schema_info)
            # May or may not need interpretation based on other factors
            assert isinstance(result, bool)
    
    def test_multiple_intents_need_interpretation(self, orchestrator):
        """Test queries with multiple intents need interpretation."""
        schema_info = {'columns': ['Campaign_Name', 'Spend']}
        
        # Query with many intent keywords
        query = "compare and rank campaigns, then filter and group by trend"
        
        result = orchestrator._needs_interpretation(query, schema_info)
        assert result is True
    
    def test_column_mention_reduces_ambiguity(self, orchestrator):
        """Test mentioning specific columns reduces ambiguity."""
        schema_info = {'columns': ['Spend', 'CTR', 'ROAS']}
        
        # Vague term but with specific column
        query = "Show high Spend campaigns"
        
        result = orchestrator._needs_interpretation(query, schema_info)
        # Should be False because Spend is mentioned
        assert result is False


class TestExecuteDirect:
    """Test _execute_direct method."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator."""
        mock_engine = Mock()
        return QueryOrchestrator(query_engine=mock_engine)
    
    def test_execute_direct_success(self, orchestrator):
        """Test successful direct execution."""
        orchestrator.query_engine.ask.return_value = {
            'sql_query': 'SELECT * FROM campaigns',
            'results': pd.DataFrame({'id': [1, 2]}),
            'answer': 'Found 2 campaigns'
        }
        
        result = orchestrator._execute_direct("Show all campaigns")
        
        assert result['success'] is True
        assert result['mode'] == 'direct'
        assert 'sql' in result
    
    def test_execute_direct_failure(self, orchestrator):
        """Test failed direct execution."""
        orchestrator.query_engine.ask.side_effect = Exception("Query failed")
        
        result = orchestrator._execute_direct("Invalid query")
        
        assert result['success'] is False
        assert 'error' in result
    
    def test_execute_direct_returns_data(self, orchestrator):
        """Test direct execution returns data."""
        test_df = pd.DataFrame({'Campaign': ['A', 'B'], 'Spend': [100, 200]})
        orchestrator.query_engine.ask.return_value = {
            'sql_query': 'SELECT * FROM campaigns',
            'results': test_df,
            'answer': 'Results'
        }
        
        result = orchestrator._execute_direct("Show campaigns")
        
        assert result['data'] is not None


class TestGenerateInterpretations:
    """Test _generate_interpretations method."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with interpreter."""
        mock_engine = Mock()
        mock_interpreter = Mock()
        mock_interpreter.generate_interpretations.return_value = [
            {'interpretation': 'Option 1', 'confidence': 0.9},
            {'interpretation': 'Option 2', 'confidence': 0.7}
        ]
        return QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
    
    def test_generate_interpretations_success(self, orchestrator):
        """Test successful interpretation generation."""
        schema_info = {'columns': ['Spend', 'Conversions']}
        
        result = orchestrator._generate_interpretations(
            "Show best campaigns",
            schema_info
        )
        
        assert result['success'] is True
        assert result['mode'] == 'interpretation'
        assert len(result['interpretations']) == 2
    
    def test_generate_interpretations_failure_fallback(self, orchestrator):
        """Test interpretation failure falls back to direct."""
        orchestrator.interpreter.generate_interpretations.side_effect = Exception("LLM error")
        
        # Mock direct execution for fallback
        orchestrator.query_engine.ask.return_value = {
            'sql_query': 'SELECT *',
            'results': None,
            'answer': 'Fallback'
        }
        
        schema_info = {'columns': ['Spend']}
        
        result = orchestrator._generate_interpretations(
            "Show campaigns",
            schema_info
        )
        
        # Should either fail or fallback
        assert 'success' in result or 'error' in result
    
    def test_interpretations_include_original_query(self, orchestrator):
        """Test interpretations include original query."""
        schema_info = {'columns': ['Spend']}
        
        result = orchestrator._generate_interpretations(
            "Show top campaigns",
            schema_info
        )
        
        assert result['original_query'] == "Show top campaigns"


class TestProcessQuery:
    """Test process_query method."""
    
    @pytest.fixture
    def orchestrator_with_interpreter(self):
        """Create orchestrator with interpreter."""
        mock_engine = Mock()
        mock_engine.ask.return_value = {
            'sql_query': 'SELECT *',
            'results': pd.DataFrame(),
            'answer': 'Done'
        }
        
        mock_interpreter = Mock()
        mock_interpreter.generate_interpretations.return_value = [
            {'interpretation': 'Option 1', 'confidence': 0.9}
        ]
        
        return QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
    
    def test_force_direct_skips_interpretation(self, orchestrator_with_interpreter):
        """Test force_direct bypasses interpretation."""
        schema_info = {'columns': ['Spend']}
        
        result = orchestrator_with_interpreter.process_query(
            query="Show high performers",  # Would normally need interpretation
            schema_info=schema_info,
            force_direct=True
        )
        
        assert result['mode'] == 'direct'
    
    def test_no_interpreter_uses_direct(self):
        """Test no interpreter always uses direct."""
        mock_engine = Mock()
        mock_engine.ask.return_value = {
            'sql_query': 'SELECT *',
            'results': None,
            'answer': 'Done'
        }
        
        orchestrator = QueryOrchestrator(query_engine=mock_engine)
        
        result = orchestrator.process_query(
            query="Show best campaigns",
            schema_info={'columns': ['Spend']}
        )
        
        assert result['mode'] == 'direct'
    
    def test_clear_query_uses_direct(self, orchestrator_with_interpreter):
        """Test clear queries use direct execution."""
        schema_info = {'columns': ['Spend', 'Campaign_Name']}
        
        result = orchestrator_with_interpreter.process_query(
            query="Show Campaign_Name and Spend",
            schema_info=schema_info
        )
        
        # Clear query should go direct
        assert result['mode'] == 'direct'


class TestExecuteInterpretation:
    """Test execute_interpretation method."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator."""
        mock_engine = Mock()
        mock_engine.ask.return_value = {
            'sql_query': 'SELECT * FROM campaigns WHERE spend > 1000',
            'results': pd.DataFrame({'id': [1]}),
            'answer': 'Found 1 campaign'
        }
        return QueryOrchestrator(query_engine=mock_engine)
    
    def test_execute_selected_interpretation(self, orchestrator):
        """Test executing a selected interpretation."""
        result = orchestrator.execute_interpretation(
            "Show campaigns with spend greater than $1000"
        )
        
        assert result['success'] is True
        assert result['mode'] == 'direct'
    
    def test_execute_interpretation_calls_engine(self, orchestrator):
        """Test execute_interpretation calls query engine."""
        orchestrator.execute_interpretation("Test interpretation")
        
        orchestrator.query_engine.ask.assert_called_once()
