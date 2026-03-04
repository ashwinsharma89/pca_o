
import pytest
import pandas as pd
import sys
from unittest.mock import MagicMock, patch
from datetime import datetime

# --- Import Target Classes ---
from src.platform.knowledge.semantic_cache import SemanticCache, get_semantic_cache
from src.platform.query_engine.schema_manager import SchemaManager, get_schema_manager

# --- Fixtures ---

@pytest.fixture
def mock_lancedb():
    """Mock the LanceDBManager and its methods."""
    mock_db = MagicMock()
    # Default behavior: empty search results
    mock_db.semantic_search.return_value = []
    # Mock add_documents to return True/None
    mock_db.add_documents.return_value = None
    return mock_db

@pytest.fixture
def mock_duckdb_conn():
    """Mock a DuckDB connection."""
    mock_conn = MagicMock()
    return mock_conn

@pytest.fixture
def schema_manager(mock_duckdb_conn):
    """Instance of SchemaManager with mocked connection."""
    return SchemaManager(conn=mock_duckdb_conn)

# --- SemanticCache Tests ---

class TestSemanticCache:
    
    def test_init(self, mock_lancedb):
        """Test initialization ensures table exists."""
        cache = SemanticCache(db_manager=mock_lancedb)
        assert cache.db_manager == mock_lancedb
        
    def test_singleton_accessor(self):
        """Test the get_semantic_cache singleton helper."""
        with patch('src.platform.knowledge.semantic_cache.get_lancedb_manager') as mock_get_mgr:
            cache = get_semantic_cache()
            assert isinstance(cache, SemanticCache)
            assert mock_get_mgr.called

    def test_get_hit(self, mock_lancedb):
        """Test successful cache hit."""
        cache = SemanticCache(db_manager=mock_lancedb)
        
        # Setup mock return for semantic_search
        mock_lancedb.semantic_search.return_value = [{
            'text': 'original question',
            'score': 0.98, # Above 0.95 threshold
            'sql_query': 'SELECT * FROM items',
            'explanation': 'Test explanation'
        }]
        
        result = cache.get("test question")
        
        assert result is not None
        assert result['sql'] == 'SELECT * FROM items'
        assert result['similarity'] == 0.98

    def test_get_miss_low_score(self, mock_lancedb):
        """Test cache miss due to low similarity score."""
        cache = SemanticCache(db_manager=mock_lancedb)
        
        # Score below threshold (0.95)
        mock_lancedb.semantic_search.return_value = [{
            'text': 'somewhat similar',
            'score': 0.80 
        }]
        
        result = cache.get("test question")
        assert result is None

    def test_get_miss_no_results(self, mock_lancedb):
        """Test cache miss when no results found."""
        cache = SemanticCache(db_manager=mock_lancedb)
        mock_lancedb.semantic_search.return_value = []
        
        result = cache.get("test question")
        assert result is None

    def test_get_error_handling(self, mock_lancedb):
        """Test error handling during get."""
        cache = SemanticCache(db_manager=mock_lancedb)
        mock_lancedb.semantic_search.side_effect = Exception("DB Error")
        
        result = cache.get("test question")
        assert result is None # Should swallow error and return None

    def test_set_success(self, mock_lancedb):
        """Test setting a value in cache."""
        cache = SemanticCache(db_manager=mock_lancedb)
        
        cache.set("question", "SELECT 1", "explanation")
        
        mock_lancedb.add_documents.assert_called_once()
        args, kwargs = mock_lancedb.add_documents.call_args
        assert kwargs['documents'] == ["question"]
        assert kwargs['metadata'][0]['sql_query'] == "SELECT 1"

    def test_set_error_handling(self, mock_lancedb):
        """Test error handling during set."""
        cache = SemanticCache(db_manager=mock_lancedb)
        mock_lancedb.add_documents.side_effect = Exception("DB Error")
        
        # Should log error but not raise
        try:
            cache.set("question", "SELECT 1")
        except Exception:
            pytest.fail("SemanticCache.set raised exception instead of catching it")


# --- SchemaManager Tests ---

class TestSchemaManager:

    def test_init_and_set_connection(self):
        """Test initialization and connection setter."""
        mgr = get_schema_manager() # Test singleton helper too
        assert mgr.conn is None
        
        mock_conn = MagicMock()
        mgr.set_connection(mock_conn)
        assert mgr.conn == mock_conn

    def test_extract_schema(self, schema_manager):
        """Test schema extraction from DataFrame."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'amount': [10.5, 20.0, 30.0]
        })
        
        schema = schema_manager.extract_schema(df, "test_table")
        
        assert schema['table_name'] == "test_table"
        assert 'id' in schema['columns']
        assert 'name' in schema['columns']
        assert schema['row_count'] == 3
        # Check sample data extraction
        assert len(schema['sample_data']) == 3
        assert schema['sample_data'][0]['id'] == 1

    def test_get_schema_for_prompt_success(self, schema_manager):
        """Test formatting schema for prompt."""
        df = pd.DataFrame({'id': [1], 'category': ['A']})
        schema_manager.extract_schema(df)
        
        # Mock connection return for unique values to avoid real DB call issues
        # But for this test, let's keep it simple first
        prompt_text = schema_manager.get_schema_for_prompt()
        
        assert "Table: campaigns" in prompt_text # Default name if not overwritten in extract? No we passed default in extract
        assert "- id" in prompt_text
        assert "- category" in prompt_text
        assert "Sample rows:" in prompt_text

    def test_get_schema_for_prompt_missing(self, schema_manager):
        """Test error when schema info is missing."""
        with pytest.raises(ValueError, match="Schema information not available"):
            schema_manager.get_schema_for_prompt()

    def test_get_categorical_unique_values(self, schema_manager, mock_duckdb_conn):
        """Test fetching unique values for categorical columns."""
        schema_manager.schema_info = {
            'table_name': 'campaigns',
            'columns': ['platform', 'id'] # platform is categorical pattern, id is not
        }
        
        # Mock result for platform query
        mock_result_df = pd.DataFrame({'platform': ['FB', 'IG']})
        
        # Setup mock fetchdf return
        mock_duckdb_conn.execute.return_value.fetchdf.return_value = mock_result_df
        
        # Only 'platform' should be queried
        unique_vals = schema_manager._get_categorical_unique_values(['platform', 'id'])
        
        assert 'platform' in unique_vals
        assert unique_vals['platform'] == ['FB', 'IG']
        assert 'id' not in unique_vals
        
        # Verify query structure
        # We can't easily check exact string matches due to sanitization calls potentially, 
        # but we can check the execute call happened.
        assert mock_duckdb_conn.execute.called

    def test_get_categorical_unique_values_error(self, schema_manager, mock_duckdb_conn):
        """Test error resilience in unique value fetching."""
        schema_manager.schema_info = {'table_name': 'campaigns'}
        mock_duckdb_conn.execute.side_effect = Exception("DB Fail")
        
        vals = schema_manager._get_categorical_unique_values(['platform'])
        assert vals == {}

    def test_column_detection_helpers(self, schema_manager):
        """Test helper methods for column logical types."""
        schema_manager.schema_info = {
            'columns': ['date', 'impressions', 'spend', 'platform', 'random_col']
        }
        
        # Date detection
        assert schema_manager.get_date_column() == 'date'
        
        # Metric detection
        metrics = schema_manager.get_metric_columns()
        assert 'spend' in metrics
        assert 'impressions' in metrics
        assert 'platform' not in metrics
        
        # Dimension detection
        dims = schema_manager.get_dimension_columns()
        assert 'platform' in dims
        assert 'spend' not in dims
        
    def test_column_detection_none(self, schema_manager):
        """Test helpers when patterns don't match."""
        schema_manager.schema_info = {'columns': ['random_col']}
        assert schema_manager.get_date_column() is None
        assert schema_manager.get_metric_columns() == []
        assert schema_manager.get_dimension_columns() == []

    def test_validate_column(self, schema_manager):
        """Test column validation logic."""
        schema_manager.schema_info = {'columns': ['Spend', 'Clicks']}
        
        # Exact match
        exists, name = schema_manager.validate_column('Spend')
        assert exists
        assert name == 'Spend'
        
        # Case insensitive
        exists, name = schema_manager.validate_column('spend')
        assert exists
        assert name == 'Spend'
        
        # Miss
        exists, name = schema_manager.validate_column('Revenue')
        assert not exists
        assert name is None

    def test_no_conn_for_unique_values(self):
        """Test unique value fetcher returns empty if no connection."""
        mgr = SchemaManager(conn=None) # Explicitly None
        vals = mgr._get_categorical_unique_values(['platform'])
        assert vals == {}

    def test_unique_values_column_with_space(self, schema_manager, mock_duckdb_conn):
        """Test unique values for column with space (testing quote logic)."""
        schema_manager.schema_info = {
            'table_name': 'campaigns',
            'columns': ['Ad Type'] # Contains space, matches categorical pattern
        }
        
        mock_result_df = pd.DataFrame({'Ad Type': ['Video', 'Image']})
        mock_duckdb_conn.execute.return_value.fetchdf.return_value = mock_result_df
        
        vals = schema_manager._get_categorical_unique_values(['Ad Type'])
        
        assert 'Ad Type' in vals
        assert vals['Ad Type'] == ['Video', 'Image']
        
        # Verify query used quotes
        args, _ = mock_duckdb_conn.execute.call_args
        assert '"Ad Type"' in args[0]

    def test_unique_values_nested_error_logging(self, schema_manager, mock_duckdb_conn):
        """Test exception handling within the column loop."""
        schema_manager.schema_info = {'table_name': 'campaigns'}
        
        # Make one execution fail without raising top level
        mock_duckdb_conn.execute.side_effect = Exception("Column missing")
        
        # Use simple pattern match
        vals = schema_manager._get_categorical_unique_values(['platform'])
        # Should catch debug log and continue/return empty for that col
        assert vals == {}

    def test_extract_schema_empty_df(self, schema_manager):
        """Test schema extraction with empty DataFrame."""
        df = pd.DataFrame({'id': []}) # Empty
        schema = schema_manager.extract_schema(df)
        assert schema['row_count'] == 0
        assert schema['sample_data'] == []

    def test_get_schema_for_prompt_with_unique_values(self, schema_manager):
        """Test schema prompt includes unique values when available."""
        schema_manager.schema_info = {
            'table_name': 'campaigns', 
            'columns': ['platform'], 
            'dtypes': {'platform': 'object'},
            'sample_data': []
        }
        
        # Mock the internal helper to return values
        with patch.object(schema_manager, '_get_categorical_unique_values', return_value={'platform': ['FB']}):
            prompt = schema_manager.get_schema_for_prompt()
            assert "IMPORTANT - Actual values" in prompt
            assert "FB" in prompt

    def test_unique_values_outer_error(self, schema_manager):
        """Test outer exception handling in unique values."""
        # Force error in the setup part (e.g., SafeQueryExecutor import or similar)
        # We can mock sanitize_identifier to raise
        with patch('src.platform.query_engine.safe_query.SafeQueryExecutor.sanitize_identifier', side_effect=Exception("Major Fail")):
            schema_manager.schema_info = {'table_name': 'tbl'}
            # connect must be present to enter the try block
            schema_manager.conn = MagicMock()
            
            vals = schema_manager._get_categorical_unique_values(['col'])
            assert vals == {}

    def test_unique_values_empty_results(self, schema_manager, mock_duckdb_conn):
        """Test unique values edge cases (empty df, empty values)."""
        schema_manager.schema_info = {'table_name': 'campaigns'}
        
        # Case 1: Empty DataFrame result
        mock_duckdb_conn.execute.return_value.fetchdf.return_value = pd.DataFrame()
        vals = schema_manager._get_categorical_unique_values(['platform'])
        assert 'platform' not in vals
        
        # Case 2: DataFrame with NaNs (empty list after dropna)
        mock_duckdb_conn.execute.return_value.fetchdf.return_value = pd.DataFrame({'platform': [None, None]})
        vals = schema_manager._get_categorical_unique_values(['platform'])
        assert 'platform' not in vals

    def test_get_schema_for_prompt_no_columns(self, schema_manager):
        """Test schema prompt generation with valid schema but no columns."""
        schema_manager.schema_info = {
            'table_name': 'campaigns',
            'columns': [], # Empty columns
            'dtypes': {},
            'sample_data': []
        }
        prompt = schema_manager.get_schema_for_prompt()
        assert "Table: campaigns" in prompt
        assert "Columns:" not in prompt
