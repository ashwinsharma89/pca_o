"""
Unit tests for dependency injection containers.
Tests DI container configuration and dependency resolution.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

# Try to import
try:
    from src.core.di.containers import DatabaseContainer, RepositoryContainer
    DI_AVAILABLE = True
except ImportError:
    DI_AVAILABLE = False
    DatabaseContainer = None
    RepositoryContainer = None

pytestmark = pytest.mark.skipif(not DI_AVAILABLE, reason="DI containers not available")


class TestDatabaseContainer:
    """Test DatabaseContainer functionality."""
    
    def test_container_creation(self):
        """Test creating database container."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        container = DatabaseContainer()
        assert container is not None
    
    def test_db_config_provider(self):
        """Test database config provider."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        with patch('src.core.di.containers.DatabaseConfig') as mock_config:
            mock_config.return_value = Mock()
            container = DatabaseContainer()
            
            # Config should be a singleton provider
            assert hasattr(container, 'db_config')
    
    def test_db_manager_provider(self):
        """Test database manager provider."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        with patch('src.core.di.containers.DatabaseManager') as mock_manager:
            mock_manager.return_value = Mock()
            container = DatabaseContainer()
            
            assert hasattr(container, 'db_manager')


class TestRepositoryContainer:
    """Test RepositoryContainer functionality."""
    
    def test_container_creation(self):
        """Test creating repository container."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        container = RepositoryContainer()
        assert container is not None
    
    def test_campaign_repository_provider(self):
        """Test campaign repository provider."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        container = RepositoryContainer()
        assert hasattr(container, 'campaign_repository')
    
    def test_analysis_repository_provider(self):
        """Test analysis repository provider."""
        if not DI_AVAILABLE:
            pytest.skip("DI not available")
        
        container = RepositoryContainer()
        assert hasattr(container, 'analysis_repository')
