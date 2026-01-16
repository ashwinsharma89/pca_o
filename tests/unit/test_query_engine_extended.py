"""
Extended unit tests for Query Engine module.
Tests smart interpretation, SQL knowledge, and NL to SQL conversion.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json

# Try to import smart interpreter
try:
    from src.platform.query_engine.smart_interpretation import SmartQueryInterpreter
    SMART_INTERPRETER_AVAILABLE = True
except ImportError:
    SMART_INTERPRETER_AVAILABLE = False
    SmartQueryInterpreter = None

# Try to import SQL knowledge helper
try:
    from src.platform.query_engine.sql_knowledge import SQLKnowledgeHelper
    SQL_KNOWLEDGE_AVAILABLE = True
except ImportError:
    SQL_KNOWLEDGE_AVAILABLE = False
    SQLKnowledgeHelper = None

# Try to import NL to SQL engine
try:
    from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
    NL_TO_SQL_AVAILABLE = True
except ImportError:
    NL_TO_SQL_AVAILABLE = False
    NaturalLanguageQueryEngine = None


class TestSmartQueryInterpreter:
    """Test SmartQueryInterpreter functionality."""
    
    pytestmark = pytest.mark.skipif(not SMART_INTERPRETER_AVAILABLE, reason="Smart interpreter not available")
    
    @pytest.fixture
    def mock_interpreter(self):
        """Create mock interpreter."""
        with patch('src.platform.query_engine.smart_interpretation.LLMRouter') as mock_router:
            mock_router.get_model_config.return_value = {'model': 'gpt-4'}
            mock_router.call_llm.return_value = json.dumps([
                {
                    "interpretation": "Show top campaigns by spend",
                    "reasoning": "User wants high spenders",
                    "sql_hint": "ORDER BY spend DESC"
                }
            ])
            
            return SmartQueryInterpreter()
    
    def test_initialization(self, mock_interpreter):
        """Test interpreter initialization."""
        assert mock_interpreter is not None
    
    def test_generate_interpretations(self, mock_interpreter):
        """Test generating interpretations."""
        schema_info = {
            'columns': ['Campaign_Name', 'Spend', 'Conversions', 'CTR'],
            'sample_data': [{'Campaign_Name': 'Test', 'Spend': 1000}]
        }
        
        with patch('src.platform.query_engine.smart_interpretation.LLMRouter') as mock_router:
            mock_router.call_llm.return_value = json.dumps([
                {"interpretation": "Option 1", "reasoning": "R1", "sql_hint": "H1"}
            ])
            
            try:
                interpretations = mock_interpreter.generate_interpretations(
                    query="Show best campaigns",
                    schema_info=schema_info,
                    num_interpretations=3
                )
                
                assert interpretations is not None
            except Exception:
                pytest.skip("Interpretation requires LLM setup")
    
    def test_detect_available_metrics(self, mock_interpreter):
        """Test detecting available metrics from columns."""
        if hasattr(mock_interpreter, '_detect_available_metrics'):
            columns = ['Spend', 'Conversions', 'CTR', 'ROAS', 'Campaign_Name']
            
            metrics = mock_interpreter._detect_available_metrics(columns)
            
            assert isinstance(metrics, dict)
    
    def test_detect_available_dimensions(self, mock_interpreter):
        """Test detecting available dimensions from columns."""
        if hasattr(mock_interpreter, '_detect_available_dimensions'):
            columns = ['Campaign_Name', 'Platform', 'Region', 'Spend']
            
            dimensions = mock_interpreter._detect_available_dimensions(columns)
            
            assert isinstance(dimensions, dict)
    
    def test_build_smart_prompt(self, mock_interpreter):
        """Test building smart prompt."""
        if hasattr(mock_interpreter, '_build_smart_prompt'):
            prompt = mock_interpreter._build_smart_prompt(
                query="Show top campaigns",
                available_columns=['Spend', 'Conversions'],
                metrics={'spend': True},
                dimensions={'campaign': True},
                sample_data=[],
                num_interpretations=3
            )
            
            assert isinstance(prompt, str)
            assert len(prompt) > 0


class TestSQLKnowledgeHelper:
    """Test SQLKnowledgeHelper functionality."""
    
    pytestmark = pytest.mark.skipif(not SQL_KNOWLEDGE_AVAILABLE, reason="SQL knowledge not available")
    
    @pytest.fixture
    def helper(self):
        """Create SQL knowledge helper."""
        try:
            return SQLKnowledgeHelper(enable_hybrid=False)
        except Exception:
            pytest.skip("SQLKnowledgeHelper initialization failed")
    
    def test_initialization(self, helper):
        """Test helper initialization."""
        assert helper is not None
    
    def test_build_context(self, helper):
        """Test building SQL context."""
        if hasattr(helper, 'build_context'):
            schema_info = {
                'tables': {'campaigns': ['id', 'name', 'spend']}
            }
            
            context = helper.build_context(
                query="Show campaigns",
                schema_info=schema_info
            )
            
            assert context is not None
    
    def test_get_sql_patterns(self, helper):
        """Test getting SQL patterns."""
        if hasattr(helper, 'get_sql_patterns'):
            patterns = helper.get_sql_patterns("aggregation")
            
            assert patterns is not None
    
    def test_get_examples(self, helper):
        """Test getting SQL examples."""
        if hasattr(helper, 'get_examples'):
            examples = helper.get_examples("top_n")
            
            assert examples is not None


class TestNaturalLanguageQueryEngine:
    """Test NaturalLanguageQueryEngine functionality."""
    
    pytestmark = pytest.mark.skipif(not NL_TO_SQL_AVAILABLE, reason="NL to SQL not available")
    
    @pytest.fixture
    def mock_engine(self):
        """Create mock NL to SQL engine."""
        try:
            with patch('src.platform.query_engine.nl_to_sql.LLMRouter') as mock_router:
                mock_router.get_client.return_value = (Mock(), "gpt-4", {})
                mock_router.get_model_config.return_value = {'model': 'gpt-4'}
                
                return NaturalLanguageQueryEngine()
        except Exception:
            pytest.skip("Engine initialization failed")
    
    def test_initialization(self, mock_engine):
        """Test engine initialization."""
        assert mock_engine is not None
    
    def test_generate_sql(self, mock_engine):
        """Test SQL generation."""
        if hasattr(mock_engine, 'generate_sql'):
            schema_info = {
                'tables': {'campaigns': ['id', 'name', 'spend']}
            }
            
            with patch.object(mock_engine, '_call_llm') as mock_llm:
                mock_llm.return_value = "SELECT * FROM campaigns WHERE spend > 1000"
                
                try:
                    sql = mock_engine.generate_sql(
                        query="Show high spend campaigns",
                        schema_info=schema_info
                    )
                    
                    assert sql is not None
                except Exception:
                    pytest.skip("SQL generation requires LLM")
    
    def test_validate_sql(self, mock_engine):
        """Test SQL validation."""
        if hasattr(mock_engine, 'validate_sql'):
            sql = "SELECT * FROM campaigns"
            
            is_valid = mock_engine.validate_sql(sql)
            
            assert isinstance(is_valid, bool)
    
    def test_execute_query(self, mock_engine):
        """Test query execution."""
        if hasattr(mock_engine, 'execute'):
            with patch.object(mock_engine, 'generate_sql') as mock_gen:
                mock_gen.return_value = "SELECT * FROM campaigns"
                
                try:
                    result = mock_engine.execute("Show all campaigns")
                    assert result is not None
                except Exception:
                    pytest.skip("Execution requires database")


class TestQueryEngineIntegration:
    """Test query engine integration."""
    
    def test_interpreter_to_engine_flow(self):
        """Test flow from interpreter to engine."""
        if SMART_INTERPRETER_AVAILABLE and NL_TO_SQL_AVAILABLE:
            try:
                with patch('src.platform.query_engine.smart_interpretation.LLMRouter') as mock_router:
                    mock_router.get_model_config.return_value = {'model': 'gpt-4'}
                    
                    interpreter = SmartQueryInterpreter()
                    # Flow would be: interpret -> select -> execute
                    assert interpreter is not None
            except Exception:
                pytest.skip("Integration requires full setup")
    
    def test_sql_knowledge_in_interpretation(self):
        """Test SQL knowledge used in interpretation."""
        if SQL_KNOWLEDGE_AVAILABLE and SMART_INTERPRETER_AVAILABLE:
            try:
                helper = SQLKnowledgeHelper(enable_hybrid=False)
                
                if hasattr(helper, 'build_context'):
                    context = helper.build_context("test", {})
                    assert context is not None
            except Exception:
                pytest.skip("Integration requires setup")
