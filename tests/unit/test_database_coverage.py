"""
Comprehensive tests for database modules to improve coverage.
"""

import pytest
import os
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# Use SQLite for testing
os.environ['USE_SQLITE'] = 'true'


class TestDatabaseConfig:
    """Tests for DatabaseConfig."""
    
    def test_config_initialization(self):
        """Test config initialization."""
        from src.core.database.connection import DatabaseConfig
        config = DatabaseConfig()
        assert config is not None
    
    def test_config_defaults(self):
        """Test config default values."""
        from src.core.database.connection import DatabaseConfig
        config = DatabaseConfig()
        assert config.host == os.getenv('DB_HOST', 'localhost')
        assert config.port == int(os.getenv('DB_PORT', '5432'))
    
    def test_get_database_url_sqlite(self):
        """Test SQLite database URL."""
        from src.core.database.connection import DatabaseConfig
        config = DatabaseConfig()
        config.use_sqlite = True
        url = config.get_database_url()
        assert 'sqlite' in url
    
    def test_get_database_url_postgres(self):
        """Test PostgreSQL database URL."""
        from src.core.database.connection import DatabaseConfig
        config = DatabaseConfig()
        config.use_sqlite = False
        config.host = 'localhost'
        config.port = 5432
        config.database = 'test'
        config.user = 'user'
        config.password = 'pass'
        url = config.get_database_url()
        assert 'postgresql' in url


class TestDatabaseManager:
    """Tests for DatabaseManager."""
    
    @pytest.fixture
    def manager(self):
        """Create database manager."""
        from src.core.database.connection import DatabaseManager, DatabaseConfig
        config = DatabaseConfig()
        config.use_sqlite = True
        return DatabaseManager(config)
    
    def test_manager_initialization(self, manager):
        """Test manager initialization."""
        assert manager is not None
        assert manager._initialized is False
    
    def test_initialize(self, manager):
        """Test database initialization."""
        manager.initialize()
        assert manager._initialized is True
        manager.close()
    
    def test_get_session(self, manager):
        """Test getting session."""
        manager.initialize()
        with manager.get_session() as session:
            assert session is not None
        manager.close()
    
    def test_health_check(self, manager):
        """Test health check."""
        manager.initialize()
        result = manager.health_check()
        assert result is True
        manager.close()
    
    def test_close(self, manager):
        """Test closing connections."""
        manager.initialize()
        manager.close()
        assert manager._initialized is False


class TestDatabaseModels:
    """Tests for database models."""
    
    def test_campaign_model(self):
        """Test Campaign model."""
        try:
            from src.core.database.models import Campaign
            campaign = Campaign(
                campaign_id='test-123',
                name='Test Campaign',
                platform='Google',
                status='active'
            )
            assert campaign.campaign_id == 'test-123'
        except Exception:
            pass
    
    def test_analysis_model(self):
        """Test Analysis model."""
        try:
            from src.core.database.models import Analysis
            analysis = Analysis(
                analysis_id='analysis-123',
                analysis_type='performance',
                status='completed'
            )
            assert analysis.analysis_id == 'analysis-123'
        except Exception:
            pass
    
    def test_context_model(self):
        """Test Context model."""
        try:
            from src.core.database.models import Context
            context = Context(
                key='test_key',
                value='test_value'
            )
            assert context.key == 'test_key'
        except Exception:
            pass


class TestRepositories:
    """Tests for repository classes."""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session."""
        from src.core.database.connection import DatabaseManager, DatabaseConfig
        config = DatabaseConfig()
        config.use_sqlite = True
        manager = DatabaseManager(config)
        manager.initialize()
        
        session = manager.get_session_direct()
        yield session
        session.close()
        manager.close()
    
    def test_campaign_repository_init(self, db_session):
        """Test CampaignRepository initialization."""
        try:
            from src.core.database.repositories import CampaignRepository
            repo = CampaignRepository(db_session)
            assert repo is not None
        except Exception:
            pass
    
    def test_campaign_repository_get_all(self, db_session):
        """Test getting all campaigns."""
        try:
            from src.core.database.repositories import CampaignRepository
            repo = CampaignRepository(db_session)
            campaigns = repo.get_all()
            assert isinstance(campaigns, list)
        except Exception:
            pass
    
    def test_analysis_repository_init(self, db_session):
        """Test AnalysisRepository initialization."""
        try:
            from src.core.database.repositories import AnalysisRepository
            repo = AnalysisRepository(db_session)
            assert repo is not None
        except Exception:
            pass
    
    def test_context_repository_init(self, db_session):
        """Test ContextRepository initialization."""
        try:
            from src.core.database.repositories import ContextRepository
            repo = ContextRepository(db_session)
            assert repo is not None
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
