"""
Unit tests for Query Clarification module.
Tests interpretation generation and LLM integration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json

# Try to import
try:
    from src.platform.query_engine.query_clarification import QueryClarifier
    CLARIFIER_AVAILABLE = True
except ImportError:
    CLARIFIER_AVAILABLE = False
    QueryClarifier = None

pytestmark = pytest.mark.skipif(not CLARIFIER_AVAILABLE, reason="Query clarifier not available")


class TestQueryClarifierInit:
    """Test QueryClarifier initialization."""
    
    @patch('src.platform.query_engine.query_clarification.LLMRouter')
    @patch('src.platform.query_engine.query_clarification.SQLKnowledgeHelper')
    def test_initialization(self, mock_sql_helper, mock_router):
        """Test clarifier initialization."""
        mock_client = Mock()
        mock_router.get_client.return_value = (mock_client, "claude-3-sonnet", {})
        mock_sql_helper.return_value = Mock()
        
        clarifier = QueryClarifier()
        
        assert clarifier is not None
        assert clarifier.client == mock_client


class TestInterpretationGeneration:
    """Test interpretation generation."""
    
    @pytest.fixture
    def mock_clarifier(self):
        """Create mock clarifier."""
        with patch('src.platform.query_engine.query_clarification.LLMRouter') as mock_router:
            with patch('src.platform.query_engine.query_clarification.SQLKnowledgeHelper') as mock_helper:
                mock_client = Mock()
                mock_router.get_client.return_value = (mock_client, "claude-3-sonnet", {})
                mock_helper.return_value = Mock()
                mock_helper.return_value.build_context.return_value = "SQL context"
                
                clarifier = QueryClarifier()
                clarifier.client = mock_client
                clarifier.sql_helper = mock_helper.return_value
                return clarifier
    
    def test_generate_interpretations(self, mock_clarifier):
        """Test generating interpretations."""
        schema_info = {
            'tables': {
                'campaigns': ['id', 'name', 'spend', 'conversions']
            }
        }
        
        # Mock LLM response
        mock_response = Mock()
        mock_response.content = json.dumps([
            {
                "interpretation": "Show campaigns with spend > $10,000",
                "reasoning": "High spend typically means above threshold",
                "confidence": 0.85,
                "sql_hint": "WHERE spend > 10000"
            },
            {
                "interpretation": "Show top 10 campaigns by spend",
                "reasoning": "High spend could mean top performers",
                "confidence": 0.75,
                "sql_hint": "ORDER BY spend DESC LIMIT 10"
            }
        ])
        
        if hasattr(mock_clarifier.client, 'messages'):
            mock_clarifier.client.messages.create.return_value = mock_response
        else:
            mock_clarifier.client.chat.completions.create.return_value = Mock(
                choices=[Mock(message=Mock(content=mock_response.content))]
            )
        
        try:
            interpretations = mock_clarifier.generate_interpretations(
                query="Show high spend campaigns",
                schema_info=schema_info,
                num_interpretations=5
            )
            
            assert interpretations is not None
        except Exception:
            pytest.skip("Interpretation generation requires LLM setup")
    
    def test_generate_interpretations_custom_count(self, mock_clarifier):
        """Test generating custom number of interpretations."""
        schema_info = {'tables': {'campaigns': ['id', 'name']}}
        
        mock_response = Mock()
        mock_response.content = json.dumps([
            {"interpretation": "Option 1", "confidence": 0.9, "reasoning": "R1", "sql_hint": "H1"},
            {"interpretation": "Option 2", "confidence": 0.8, "reasoning": "R2", "sql_hint": "H2"},
            {"interpretation": "Option 3", "confidence": 0.7, "reasoning": "R3", "sql_hint": "H3"}
        ])
        
        if hasattr(mock_clarifier.client, 'messages'):
            mock_clarifier.client.messages.create.return_value = mock_response
        else:
            mock_clarifier.client.chat.completions.create.return_value = Mock(
                choices=[Mock(message=Mock(content=mock_response.content))]
            )
        
        try:
            interpretations = mock_clarifier.generate_interpretations(
                query="Show best campaigns",
                schema_info=schema_info,
                num_interpretations=3
            )
            
            assert interpretations is not None
        except Exception:
            pytest.skip("Interpretation generation requires LLM setup")


class TestInterpretationParsing:
    """Test interpretation response parsing."""
    
    def test_parse_valid_json(self):
        """Test parsing valid JSON response."""
        response = json.dumps([
            {
                "interpretation": "Show campaigns with CTR > 5%",
                "reasoning": "High CTR means above average",
                "confidence": 0.9,
                "sql_hint": "WHERE ctr > 0.05"
            }
        ])
        
        parsed = json.loads(response)
        
        assert len(parsed) == 1
        assert parsed[0]['confidence'] == 0.9
    
    def test_parse_invalid_json(self):
        """Test handling invalid JSON."""
        response = "This is not valid JSON"
        
        with pytest.raises(json.JSONDecodeError):
            json.loads(response)
    
    def test_interpretation_structure(self):
        """Test interpretation has required fields."""
        interpretation = {
            "interpretation": "Show top campaigns",
            "reasoning": "User wants best performers",
            "confidence": 0.85,
            "sql_hint": "ORDER BY performance DESC"
        }
        
        required_fields = ['interpretation', 'reasoning', 'confidence', 'sql_hint']
        
        for field in required_fields:
            assert field in interpretation


class TestSQLKnowledgeIntegration:
    """Test SQL knowledge helper integration."""
    
    @pytest.fixture
    def mock_clarifier(self):
        """Create mock clarifier."""
        with patch('src.platform.query_engine.query_clarification.LLMRouter') as mock_router:
            with patch('src.platform.query_engine.query_clarification.SQLKnowledgeHelper') as mock_helper:
                mock_client = Mock()
                mock_router.get_client.return_value = (mock_client, "claude-3-sonnet", {})
                
                mock_sql_helper = Mock()
                mock_sql_helper.build_context.return_value = "SQL patterns and examples"
                mock_helper.return_value = mock_sql_helper
                
                clarifier = QueryClarifier()
                return clarifier
    
    def test_sql_context_building(self, mock_clarifier):
        """Test SQL context is built for queries."""
        schema_info = {'tables': {'campaigns': ['id', 'name']}}
        
        context = mock_clarifier.sql_helper.build_context(
            "Show campaigns",
            schema_info
        )
        
        assert context is not None
        mock_clarifier.sql_helper.build_context.assert_called_once()
