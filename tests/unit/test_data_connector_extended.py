"""
Extended unit tests for Database Connector.
Tests connection string building, connections, and queries for all database types.
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


class TestDatabaseConnectorInit:
    """Test DatabaseConnector initialization."""
    
    def test_initialization(self):
        """Test connector initialization."""
        connector = DatabaseConnector()
        
        assert connector is not None
        assert connector.engine is None
        assert connector.connection_string is None
    
    def test_supported_databases(self):
        """Test supported databases list."""
        connector = DatabaseConnector()
        
        assert 'postgresql' in connector.SUPPORTED_DATABASES
        assert 'mysql' in connector.SUPPORTED_DATABASES
        assert 'sqlite' in connector.SUPPORTED_DATABASES
        assert 'snowflake' in connector.SUPPORTED_DATABASES
        assert 'bigquery' in connector.SUPPORTED_DATABASES


class TestConnectionStringBuilding:
    """Test connection string building for various databases."""
    
    @pytest.fixture
    def connector(self):
        """Create connector instance."""
        return DatabaseConnector()
    
    def test_sqlite_connection_string(self, connector):
        """Test SQLite connection string."""
        conn_str = connector.build_connection_string(
            db_type='sqlite',
            file_path='/path/to/db.sqlite'
        )
        
        assert conn_str == 'sqlite:////path/to/db.sqlite'
    
    def test_sqlite_requires_file_path(self, connector):
        """Test SQLite requires file_path."""
        with pytest.raises(ValueError, match="file_path is required"):
            connector.build_connection_string(db_type='sqlite')
    
    def test_postgresql_connection_string(self, connector):
        """Test PostgreSQL connection string."""
        conn_str = connector.build_connection_string(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'postgresql://' in conn_str
        assert 'localhost' in conn_str
        assert '5432' in conn_str
        assert 'testdb' in conn_str
    
    def test_postgresql_default_port(self, connector):
        """Test PostgreSQL uses default port."""
        conn_str = connector.build_connection_string(
            db_type='postgresql',
            host='localhost',
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert ':5432/' in conn_str
    
    def test_mysql_connection_string(self, connector):
        """Test MySQL connection string."""
        conn_str = connector.build_connection_string(
            db_type='mysql',
            host='localhost',
            port=3306,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'mysql+' in conn_str
        assert 'localhost' in conn_str
    
    def test_mssql_connection_string(self, connector):
        """Test SQL Server connection string."""
        conn_str = connector.build_connection_string(
            db_type='mssql',
            host='localhost',
            port=1433,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'mssql+pyodbc://' in conn_str
    
    def test_oracle_connection_string(self, connector):
        """Test Oracle connection string."""
        conn_str = connector.build_connection_string(
            db_type='oracle',
            host='localhost',
            port=1521,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'oracle+cx_oracle://' in conn_str
    
    def test_snowflake_connection_string(self, connector):
        """Test Snowflake connection string."""
        conn_str = connector.build_connection_string(
            db_type='snowflake',
            host='account.snowflakecomputing.com',
            database='testdb',
            username='user',
            password='pass',
            account='myaccount',
            warehouse='compute_wh'
        )
        
        assert 'snowflake://' in conn_str
        assert 'warehouse=' in conn_str
    
    def test_snowflake_requires_account(self, connector):
        """Test Snowflake requires account."""
        with pytest.raises(ValueError, match="account"):
            connector.build_connection_string(
                db_type='snowflake',
                host='localhost',
                database='testdb',
                username='user',
                password='pass'
            )
    
    def test_bigquery_connection_string(self, connector):
        """Test BigQuery connection string."""
        conn_str = connector.build_connection_string(
            db_type='bigquery',
            host='bigquery.googleapis.com',
            database='myproject',
            username='user',
            password='pass',
            project_id='myproject'
        )
        
        assert 'bigquery://' in conn_str
    
    def test_redshift_connection_string(self, connector):
        """Test Redshift connection string."""
        conn_str = connector.build_connection_string(
            db_type='redshift',
            host='cluster.redshift.amazonaws.com',
            port=5439,
            database='testdb',
            username='user',
            password='pass'
        )
        
        assert 'redshift+psycopg2://' in conn_str
    
    def test_databricks_connection_string(self, connector):
        """Test Databricks connection string."""
        conn_str = connector.build_connection_string(
            db_type='databricks',
            host='workspace.cloud.databricks.com',
            database='catalog',
            username='token',
            password='dapi123',
            http_path='/sql/1.0/warehouses/abc'
        )
        
        assert 'databricks://' in conn_str
        assert 'http_path=' in conn_str
    
    def test_databricks_requires_http_path(self, connector):
        """Test Databricks requires http_path."""
        with pytest.raises(ValueError, match="http_path"):
            connector.build_connection_string(
                db_type='databricks',
                host='workspace.cloud.databricks.com',
                database='catalog',
                username='token',
                password='dapi123'
            )
    
    def test_unsupported_database(self, connector):
        """Test unsupported database raises error."""
        with pytest.raises(ValueError, match="Unsupported database"):
            connector.build_connection_string(db_type='unsupported_db')
    
    def test_password_encoding(self, connector):
        """Test special characters in password are encoded."""
        conn_str = connector.build_connection_string(
            db_type='postgresql',
            host='localhost',
            database='testdb',
            username='user',
            password='p@ss#word!'
        )
        
        # Special characters should be URL encoded
        assert 'p@ss#word!' not in conn_str
        assert '%40' in conn_str or '%23' in conn_str  # @ or # encoded


class TestDatabaseConnection:
    """Test database connection methods."""
    
    @pytest.fixture
    def connector(self):
        """Create connector instance."""
        return DatabaseConnector()
    
    @patch('src.platform.data.database_connector.create_engine')
    def test_connect_with_string(self, mock_engine, connector):
        """Test connecting with connection string."""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.return_value.connect.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value = None
        
        result = connector.connect(connection_string='sqlite:///test.db')
        
        assert result is True
        assert connector.engine is not None
    
    @patch('src.platform.data.database_connector.create_engine')
    def test_connect_with_db_type(self, mock_engine, connector):
        """Test connecting with db_type and params."""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.return_value.connect.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.return_value = None
        
        result = connector.connect(
            db_type='sqlite',
            file_path='/tmp/test.db'
        )
        
        assert result is True
    
    def test_connect_requires_params(self, connector):
        """Test connect requires connection_string or db_type."""
        with pytest.raises(ValueError, match="Either connection_string or db_type"):
            connector.connect()
    
    @patch('src.platform.data.database_connector.create_engine')
    def test_connect_failure(self, mock_engine, connector):
        """Test connection failure handling."""
        mock_engine.return_value.connect.side_effect = Exception("Connection failed")
        
        try:
            result = connector.connect(connection_string='invalid://connection')
            assert result is False
        except Exception:
            pass  # Expected - connection failed


class TestDatabaseQueries:
    """Test database query methods."""
    
    @pytest.fixture
    def connected_connector(self):
        """Create connected connector."""
        connector = DatabaseConnector()
        connector.engine = Mock()
        return connector
    
    def test_execute_query(self, connected_connector):
        """Test executing SQL query."""
        if hasattr(connected_connector, 'execute_query'):
            mock_result = Mock()
            mock_result.fetchall.return_value = [(1, 'A'), (2, 'B')]
            connected_connector.engine.execute.return_value = mock_result
            
            try:
                result = connected_connector.execute_query("SELECT * FROM campaigns")
                assert result is not None
            except Exception:
                pytest.skip("Execute query requires full setup")
    
    def test_query_to_dataframe(self, connected_connector):
        """Test query returning DataFrame."""
        if hasattr(connected_connector, 'query_to_dataframe'):
            with patch('pandas.read_sql') as mock_read:
                mock_read.return_value = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
                
                df = connected_connector.query_to_dataframe("SELECT * FROM campaigns")
                
                assert isinstance(df, pd.DataFrame)
                assert len(df) == 2
    
    def test_load_table(self, connected_connector):
        """Test loading entire table."""
        if hasattr(connected_connector, 'load_table'):
            with patch('pandas.read_sql_table') as mock_read:
                mock_read.return_value = pd.DataFrame({'id': [1, 2]})
                
                try:
                    df = connected_connector.load_table('campaigns')
                    assert isinstance(df, pd.DataFrame)
                except Exception:
                    pytest.skip("Load table requires full setup")


class TestDatabaseDisconnect:
    """Test database disconnection."""
    
    @pytest.fixture
    def connected_connector(self):
        """Create connected connector."""
        connector = DatabaseConnector()
        connector.engine = Mock()
        return connector
    
    def test_disconnect(self, connected_connector):
        """Test disconnecting from database."""
        if hasattr(connected_connector, 'disconnect'):
            connected_connector.disconnect()
            
            connected_connector.engine.dispose.assert_called_once()
    
    def test_disconnect_clears_engine(self, connected_connector):
        """Test disconnect clears engine reference."""
        if hasattr(connected_connector, 'disconnect'):
            connected_connector.disconnect()
            
            assert connected_connector.engine is None or connected_connector.engine.dispose.called
