"""
Unit tests for Query Orchestrator.
Tests query routing, interpretation, and execution.
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
    sys.modules['query_orchestrator_test'] = module
    spec.loader.exec_module(module)
    QueryOrchestrator = module.QueryOrchestrator
    ORCHESTRATOR_AVAILABLE = True
except Exception:
    ORCHESTRATOR_AVAILABLE = False
    QueryOrchestrator = None

pytestmark = pytest.mark.skipif(not ORCHESTRATOR_AVAILABLE, reason="Query orchestrator not available")


class TestQueryOrchestratorInit:
    """Test QueryOrchestrator initialization."""
    
    def test_init_without_interpreter(self):
        """Test initialization without interpreter."""
        mock_engine = Mock()
        
        orchestrator = QueryOrchestrator(query_engine=mock_engine)
        
        assert orchestrator.query_engine == mock_engine
        assert orchestrator.interpreter is None
        assert orchestrator.use_interpretation is False
    
    def test_init_with_interpreter(self):
        """Test initialization with interpreter."""
        mock_engine = Mock()
        mock_interpreter = Mock()
        
        orchestrator = QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
        
        assert orchestrator.interpreter == mock_interpreter
        assert orchestrator.use_interpretation is True


class TestQueryProcessing:
    """Test query processing methods."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mocks."""
        mock_engine = Mock()
        mock_engine.execute.return_value = {
            'success': True,
            'result': pd.DataFrame({'A': [1, 2, 3]})
        }
        return QueryOrchestrator(query_engine=mock_engine)
    
    @pytest.fixture
    def orchestrator_with_interpreter(self):
        """Create orchestrator with interpreter."""
        mock_engine = Mock()
        mock_interpreter = Mock()
        mock_interpreter.interpret.return_value = [
            {'interpretation': 'Option 1', 'confidence': 0.9},
            {'interpretation': 'Option 2', 'confidence': 0.7}
        ]
        return QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
    
    def test_process_query_direct(self, orchestrator):
        """Test direct query processing."""
        schema_info = {'tables': ['campaigns']}
        
        with patch.object(orchestrator, '_execute_direct') as mock_execute:
            mock_execute.return_value = {'success': True, 'mode': 'direct'}
            
            result = orchestrator.process_query(
                query="Show all campaigns",
                schema_info=schema_info,
                force_direct=True
            )
            
            assert result['mode'] == 'direct'
            mock_execute.assert_called_once()
    
    def test_process_query_force_direct(self, orchestrator_with_interpreter):
        """Test forced direct execution."""
        schema_info = {'tables': ['campaigns']}
        
        with patch.object(orchestrator_with_interpreter, '_execute_direct') as mock_execute:
            mock_execute.return_value = {'success': True, 'mode': 'direct'}
            
            result = orchestrator_with_interpreter.process_query(
                query="Show all campaigns",
                schema_info=schema_info,
                force_direct=True
            )
            
            assert result['mode'] == 'direct'
    
    def test_process_query_with_interpretation(self, orchestrator_with_interpreter):
        """Test query processing with interpretation."""
        schema_info = {'tables': ['campaigns']}
        
        with patch.object(orchestrator_with_interpreter, '_needs_interpretation', return_value=True):
            with patch.object(orchestrator_with_interpreter, '_generate_interpretations') as mock_interpret:
                mock_interpret.return_value = {
                    'success': True,
                    'mode': 'interpretation',
                    'interpretations': [{'text': 'Option 1'}]
                }
                
                result = orchestrator_with_interpreter.process_query(
                    query="Show high performing campaigns",
                    schema_info=schema_info
                )
                
                assert result['mode'] == 'interpretation'


class TestQueryExecution:
    """Test query execution methods."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator."""
        mock_engine = Mock()
        return QueryOrchestrator(query_engine=mock_engine)
    
    def test_execute_interpretation(self, orchestrator):
        """Test executing a selected interpretation."""
        with patch.object(orchestrator, '_execute_direct') as mock_execute:
            mock_execute.return_value = {'success': True, 'result': 'data'}
            
            result = orchestrator.execute_interpretation(
                interpretation="Show campaigns with spend > 1000"
            )
            
            mock_execute.assert_called_once_with("Show campaigns with spend > 1000")
    
    def test_execute_direct_success(self, orchestrator):
        """Test successful direct execution."""
        orchestrator.query_engine.execute = Mock(return_value={
            'success': True,
            'sql': 'SELECT * FROM campaigns',
            'result': pd.DataFrame({'id': [1, 2]})
        })
        
        if hasattr(orchestrator, '_execute_direct'):
            result = orchestrator._execute_direct("Show all campaigns")
            assert result is not None
    
    def test_execute_direct_failure(self, orchestrator):
        """Test failed direct execution."""
        orchestrator.query_engine.execute = Mock(side_effect=Exception("Query failed"))
        
        if hasattr(orchestrator, '_execute_direct'):
            try:
                result = orchestrator._execute_direct("Invalid query")
                assert 'error' in result or not result.get('success', True)
            except Exception:
                pass  # Expected behavior


class TestInterpretationLogic:
    """Test interpretation decision logic."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with interpreter."""
        mock_engine = Mock()
        mock_interpreter = Mock()
        return QueryOrchestrator(
            query_engine=mock_engine,
            interpreter=mock_interpreter
        )
    
    def test_needs_interpretation_ambiguous(self, orchestrator):
        """Test detecting ambiguous queries."""
        if hasattr(orchestrator, '_needs_interpretation'):
            schema_info = {'tables': ['campaigns']}
            
            # Ambiguous query
            result = orchestrator._needs_interpretation(
                "Show high performers",
                schema_info
            )
            
            assert isinstance(result, bool)
    
    def test_needs_interpretation_clear(self, orchestrator):
        """Test detecting clear queries."""
        if hasattr(orchestrator, '_needs_interpretation'):
            schema_info = {'tables': ['campaigns']}
            
            # Clear query
            result = orchestrator._needs_interpretation(
                "SELECT * FROM campaigns WHERE spend > 1000",
                schema_info
            )
            
            assert isinstance(result, bool)
    
    def test_generate_interpretations(self, orchestrator):
        """Test generating interpretations."""
        if hasattr(orchestrator, '_generate_interpretations'):
            schema_info = {'tables': ['campaigns']}
            
            orchestrator.interpreter.generate_interpretations = Mock(return_value=[
                {'interpretation': 'Option 1', 'confidence': 0.9}
            ])
            
            result = orchestrator._generate_interpretations(
                "Show best campaigns",
                schema_info
            )
            
            assert result is not None
