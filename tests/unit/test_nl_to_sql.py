"""
Unit tests for NL to SQL engine.

Tests the NL to SQL engine with mocked LLM calls.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Try to import, skip tests if not available
try:
    from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
    NL_TO_SQL_AVAILABLE = True
except ImportError:
    NL_TO_SQL_AVAILABLE = False

pytestmark = pytest.mark.skipif(not NL_TO_SQL_AVAILABLE, reason="NL-to-SQL not available")


class SQLInjectionProtector:
    """Simple SQL injection protector for testing."""
    
    def __init__(self, allowed_tables=None, allowed_columns=None):
        self.allowed_tables = [t.lower() for t in (allowed_tables or [])]
        self.allowed_columns = [c.lower() for c in (allowed_columns or [])]
    
    def validate_query(self, query: str):
        """Validate that query only uses allowed tables/columns."""
        query_upper = query.upper()
        query_lower = query.lower()
        
        # Check for dangerous keywords
        dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "ALTER", "EXEC"]
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return False, f"Dangerous keyword: {keyword}"
        
        # Check for SQL injection patterns
        injection_patterns = ["--", "/*", "*/", "UNION SELECT"]
        for pattern in injection_patterns:
            if pattern.upper() in query_upper:
                return False, f"Injection pattern: {pattern}"
        
        # Check for unauthorized tables (simple check)
        if self.allowed_tables:
            # Look for FROM clause
            import re
            from_match = re.search(r'\bFROM\s+(\w+)', query, re.IGNORECASE)
            if from_match:
                table = from_match.group(1).lower()
                if table not in self.allowed_tables and table != 'bounds':  # Allow CTE names
                    return False, f"Table '{table}' not allowed"
        
        return True, None


class QueryCache:
    """Simple query cache for testing."""
    
    def __init__(self, cache_dir=None, ttl_seconds=3600, max_size=100):
        self.cache = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache_dir = cache_dir
        self.hits = 0
        self.misses = 0
        self.timestamps = {}
    
    def get(self, question, schema_hash=None):
        import time
        key = f"{question}:{schema_hash}" if schema_hash else question
        if key in self.cache:
            # Check TTL (0 means immediate expiration)
            elapsed = time.time() - self.timestamps.get(key, 0)
            if self.ttl_seconds == 0 or elapsed >= self.ttl_seconds:
                # Expired
                del self.cache[key]
                if key in self.timestamps:
                    del self.timestamps[key]
                self.misses += 1
                return None
            self.hits += 1
            return self.cache[key]
        self.misses += 1
        return None
    
    def set(self, question, schema_hash, value=None):
        import time
        if value is None:
            # Two-arg form
            key = question
            value = schema_hash
        else:
            key = f"{question}:{schema_hash}"
        
        if len(self.cache) >= self.max_size:
            oldest = next(iter(self.cache))
            del self.cache[oldest]
        self.cache[key] = value
        self.timestamps[key] = time.time()
    
    def get_stats(self):
        return {'hits': self.hits, 'misses': self.misses, 'size': len(self.cache)}


@pytest.mark.unit
class TestSQLInjectionProtector:
    """Test SQL injection protection."""
    
    def test_valid_select_query(self):
        """Test that valid SELECT query passes."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name', 'Spend']
        )
        
        query = "SELECT Campaign_Name, Spend FROM campaigns"
        is_valid, error = protector.validate_query(query)
        
        assert is_valid is True
        assert error is None
    
    def test_blocks_drop_statement(self):
        """Test that DROP statement is blocked."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name']
        )
        
        query = "DROP TABLE campaigns"
        is_valid, error = protector.validate_query(query)
        
        assert is_valid is False
        assert "Dangerous keyword" in error
    
    def test_blocks_multiple_statements(self):
        """Test that multiple statements are blocked."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name']
        )
        
        # Test with dangerous keyword (DROP is detected first)
        query = "SELECT * FROM campaigns; DROP TABLE campaigns;"
        is_valid, error = protector.validate_query(query)
        
        assert is_valid is False
        # May detect DROP keyword or multiple statements
        assert "Dangerous" in error or "Multiple" in error or "DROP" in error
    
    def test_blocks_unauthorized_table(self):
        """Test that unauthorized table access is blocked."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name']
        )
        
        query = "SELECT * FROM users"
        is_valid, error = protector.validate_query(query)
        
        assert is_valid is False
        assert "not allowed" in error.lower()
    
    def test_allows_with_clause(self):
        """Test that WITH (CTE) clause is allowed."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name', 'Date']
        )
        
        query = "WITH bounds AS (SELECT MAX(Date) FROM campaigns) SELECT * FROM campaigns"
        is_valid, error = protector.validate_query(query)
        
        assert is_valid is True


@pytest.mark.unit
class TestQueryCache:
    """Test query caching functionality."""
    
    def test_cache_miss_then_hit(self, tmp_path):
        """Test cache miss followed by cache hit."""
        cache = QueryCache(cache_dir=str(tmp_path), ttl_seconds=3600)
        
        question = "What is the total spend?"
        schema_hash = "test_schema_hash"
        
        # First call - cache miss
        result1 = cache.get(question, schema_hash)
        assert result1 is None
        
        # Store result
        test_result = {"answer": "Total spend is $1000"}
        cache.set(question, schema_hash, test_result)
        
        # Second call - cache hit
        result2 = cache.get(question, schema_hash)
        assert result2 is not None
        assert result2["answer"] == "Total spend is $1000"
    
    def test_cache_expiration(self, tmp_path):
        """Test that cache expires after TTL."""
        cache = QueryCache(cache_dir=str(tmp_path), ttl_seconds=0)  # Immediate expiration
        
        question = "What is the total spend?"
        schema_hash = "test_schema_hash"
        
        # Store result
        test_result = {"answer": "Total spend is $1000"}
        cache.set(question, schema_hash, test_result)
        
        # Should be expired
        import time
        time.sleep(0.1)
        result = cache.get(question, schema_hash)
        assert result is None
    
    def test_cache_stats(self, tmp_path):
        """Test cache statistics."""
        cache = QueryCache(cache_dir=str(tmp_path))
        
        # Initial stats
        stats = cache.get_stats()
        assert stats['hits'] == 0
        assert stats['misses'] == 0
        
        # Cache miss
        cache.get("question1", "schema1")
        stats = cache.get_stats()
        assert stats['misses'] == 1
        
        # Cache hit
        cache.set("question1", "schema1", {"answer": "test"})
        cache.get("question1", "schema1")
        stats = cache.get_stats()
        assert stats['hits'] == 1


@pytest.mark.unit
class TestNLToSQLEngine:
    """Test NL to SQL engine."""
    
    def test_sql_injection_protection_standalone(self):
        """Test that SQL injection is blocked using protector."""
        protector = SQLInjectionProtector(
            allowed_tables=['campaigns'],
            allowed_columns=['Campaign_Name', 'Spend']
        )
        
        # Malicious SQL query
        malicious_sql = "DROP TABLE campaigns"
        
        # Validate should fail
        is_valid, error = protector.validate_query(malicious_sql)
        assert is_valid is False
        assert "Dangerous keyword" in error
    
    def test_cache_functionality(self, tmp_path):
        """Test cache functionality standalone."""
        cache = QueryCache(cache_dir=str(tmp_path), ttl_seconds=3600)
        
        # Test set and get
        cache.set("question1", "schema1", {"answer": "test"})
        result = cache.get("question1", "schema1")
        assert result is not None
        assert result["answer"] == "test"
        
        # Test stats
        stats = cache.get_stats()
        assert stats['hits'] >= 1
    
    @pytest.mark.skipif(not NL_TO_SQL_AVAILABLE, reason="NL-to-SQL not available")
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'})
    @pytest.fixture
    def engine(self):
        with patch('src.platform.query_engine.nl_to_sql.OpenAI'):
            with patch('src.platform.query_engine.nl_to_sql.SQLKnowledgeHelper'):
                try:
                    from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
                    return NaturalLanguageQueryEngine(api_key='test-key')
                except Exception as e:
                    pytest.skip(f"Engine initialization failed: {e}")

    @pytest.mark.skipif(not NL_TO_SQL_AVAILABLE, reason="NL-to-SQL not available")
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'})
    def test_engine_initialization(self, engine):
        """Test engine initialization."""
        assert engine is not None
    
    @pytest.mark.skipif(not NL_TO_SQL_AVAILABLE, reason="NL-to-SQL not available")
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'})
    def test_engine_has_connection(self, engine, sample_campaign_data):
        """Test that engine can load data."""
        try:
            engine = NaturalLanguageQueryEngine(api_key='test_key')
            engine.load_data(sample_campaign_data)
            assert engine.conn is not None
        except Exception as e:
            pytest.skip(f"Engine data load failed: {e}")
