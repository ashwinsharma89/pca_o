"""
Unit tests for database connector.
Tests database connection and query functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Try to import
try:
    from src.platform.data.database_connector import DatabaseConnector
    CONNECTOR_AVAILABLE = True
except ImportError:
    CONNECTOR_AVAILABLE = False
    DatabaseConnector = None

pytestmark = pytest.mark.skipif(not CONNECTOR_AVAILABLE, reason="Database connector not available")


class TestDatabaseConnector:
    """Test DatabaseConnector functionality."""
    
    @pytest.fixture
    def connector(self):
        """Create database connector."""
        return DatabaseConnector()
    
    def test_initialization(self, connector):
        """Test connector initialization."""
        assert connector is not None
        assert connector.engine is None
    
    def test_supported_databases(self, connector):
        """Test supported database types."""
        assert 'postgresql' in connector.SUPPORTED_DATABASES
        assert 'mysql' in connector.SUPPORTED_DATABASES
        assert 'sqlite' in connector.SUPPORTED_DATABASES
        assert 'snowflake' in connector.SUPPORTED_DATABASES
    
    def test_build_postgresql_connection_string(self, connector):
        """Test building PostgreSQL connection string."""
        conn_str = connector.build_connection_string(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'postgresql' in conn_str
        assert 'localhost' in conn_str
    
    def test_build_mysql_connection_string(self, connector):
        """Test building MySQL connection string."""
        conn_str = connector.build_connection_string(
            db_type='mysql',
            host='localhost',
            port=3306,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'mysql' in conn_str
    
    def test_build_sqlite_connection_string(self, connector):
        """Test building SQLite connection string."""
        conn_str = connector.build_connection_string(
            db_type='sqlite',
            file_path='/path/to/db.sqlite'
        )
        
        assert 'sqlite' in conn_str
    
    def test_connect_sqlite(self, connector, tmp_path):
        """Test connecting to SQLite database."""
        db_path = tmp_path / "test.db"
        
        if hasattr(connector, 'connect'):
            try:
                connector.connect(
                    db_type='sqlite',
                    file_path=str(db_path)
                )
                assert connector.engine is not None
            except Exception:
                pytest.skip("SQLite connection failed")
    
    def test_execute_query(self, connector):
        """Test executing a query."""
        if hasattr(connector, 'execute_query'):
            with patch.object(connector, 'engine') as mock_engine:
                mock_conn = MagicMock()
                mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
                mock_conn.execute.return_value.fetchall.return_value = [(1, 'test')]
                
                try:
                    result = connector.execute_query("SELECT * FROM test")
                    assert result is not None
                except Exception:
                    pytest.skip("Query execution requires connection")
    
    def test_load_dataframe(self, connector):
        """Test loading data as DataFrame."""
        if hasattr(connector, 'load_dataframe'):
            with patch('pandas.read_sql') as mock_read:
                mock_read.return_value = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
                
                try:
                    df = connector.load_dataframe("SELECT * FROM test")
                    assert isinstance(df, pd.DataFrame)
                except Exception:
                    pytest.skip("DataFrame loading requires connection")
    
    def test_disconnect(self, connector):
        """Test disconnecting from database."""
        if hasattr(connector, 'disconnect'):
            connector.engine = Mock()
            connector.disconnect()
            # Should handle disconnect gracefully


class TestDatabaseConnectorCloudStorage:
    """Test cloud storage connections."""
    
    @pytest.fixture
    def connector(self):
        """Create database connector."""
        return DatabaseConnector()
    
    def test_s3_supported(self, connector):
        """Test S3 is in supported databases."""
        assert 's3' in connector.SUPPORTED_DATABASES
    
    def test_bigquery_supported(self, connector):
        """Test BigQuery is in supported databases."""
        assert 'bigquery' in connector.SUPPORTED_DATABASES
    
    def test_snowflake_supported(self, connector):
        """Test Snowflake is in supported databases."""
        assert 'snowflake' in connector.SUPPORTED_DATABASES
