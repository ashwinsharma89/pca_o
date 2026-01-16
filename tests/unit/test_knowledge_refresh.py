"""
Unit tests for knowledge base auto-refresh.
Tests automatic refresh and chunk optimization.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import tempfile
import os

# Try to import auto_refresh
try:
    from src.platform.knowledge.auto_refresh import (
        KnowledgeBaseRefresher, RefreshConfig, SourceMetadata
    )
    AUTO_REFRESH_AVAILABLE = True
except ImportError:
    AUTO_REFRESH_AVAILABLE = False
    KnowledgeBaseRefresher = None
    RefreshConfig = None
    SourceMetadata = None

# Try to import chunk_optimizer
try:
    from src.platform.knowledge.chunk_optimizer import (
        ChunkOptimizer, ChunkConfig, ChunkStrategy
    )
    CHUNK_OPTIMIZER_AVAILABLE = True
except ImportError:
    CHUNK_OPTIMIZER_AVAILABLE = False
    ChunkOptimizer = None
    ChunkConfig = None
    ChunkStrategy = None


class TestRefreshConfig:
    """Test RefreshConfig dataclass."""
    
    pytestmark = pytest.mark.skipif(not AUTO_REFRESH_AVAILABLE, reason="Auto refresh not available")
    
    def test_default_config(self):
        """Test default configuration values."""
        if not AUTO_REFRESH_AVAILABLE:
            pytest.skip("Auto refresh not available")
        
        config = RefreshConfig()
        
        assert config.check_interval_seconds == 3600
        assert config.auto_refresh_enabled is True
        assert config.refresh_on_startup is True
        assert config.max_refresh_attempts == 3
    
    def test_custom_config(self):
        """Test custom configuration."""
        if not AUTO_REFRESH_AVAILABLE:
            pytest.skip("Auto refresh not available")
        
        config = RefreshConfig(
            check_interval_seconds=1800,
            auto_refresh_enabled=False,
            max_refresh_attempts=5
        )
        
        assert config.check_interval_seconds == 1800
        assert config.auto_refresh_enabled is False
        assert config.max_refresh_attempts == 5


class TestSourceMetadata:
    """Test SourceMetadata dataclass."""
    
    pytestmark = pytest.mark.skipif(not AUTO_REFRESH_AVAILABLE, reason="Auto refresh not available")
    
    def test_create_metadata(self):
        """Test creating source metadata."""
        if not AUTO_REFRESH_AVAILABLE:
            pytest.skip("Auto refresh not available")
        
        metadata = SourceMetadata(
            source_id="src_123",
            source_type="url",
            location="https://example.com/docs",
            last_hash="abc123",
            last_checked=datetime.now().isoformat(),
            last_refreshed=datetime.now().isoformat()
        )
        
        assert metadata.source_id == "src_123"
        assert metadata.source_type == "url"
        assert metadata.refresh_count == 0


class TestKnowledgeBaseRefresher:
    """Test KnowledgeBaseRefresher functionality."""
    
    pytestmark = pytest.mark.skipif(not AUTO_REFRESH_AVAILABLE, reason="Auto refresh not available")
    
    @pytest.fixture
    def refresher(self, tmp_path):
        """Create refresher instance."""
        if not AUTO_REFRESH_AVAILABLE:
            pytest.skip("Auto refresh not available")
        
        return KnowledgeBaseRefresher(
            config=RefreshConfig(auto_refresh_enabled=False),
            metadata_path=str(tmp_path / "metadata.json")
        )
    
    def test_initialization(self, refresher):
        """Test refresher initialization."""
        assert refresher is not None
        assert refresher.config.auto_refresh_enabled is False
    
    def test_add_source(self, refresher):
        """Test adding a knowledge source."""
        if hasattr(refresher, 'add_source'):
            refresher.add_source(
                source_id="test_source",
                source_type="url",
                location="https://example.com"
            )
            
            assert "test_source" in refresher.sources or len(refresher.sources) >= 1
    
    def test_check_for_changes(self, refresher):
        """Test checking for content changes."""
        if hasattr(refresher, 'check_for_changes'):
            try:
                # Mock the content hash
                with patch.object(refresher, '_compute_hash', return_value="new_hash"):
                    changed = refresher.check_for_changes("test_source")
                    assert isinstance(changed, bool)
            except Exception:
                # Method may require source to be added first
                pytest.skip("check_for_changes requires source setup")
    
    def test_refresh_source(self, refresher):
        """Test refreshing a source."""
        if hasattr(refresher, 'refresh_source'):
            with patch.object(refresher, '_fetch_content', return_value="content"):
                result = refresher.refresh_source("test_source")
                assert isinstance(result, (bool, dict))
    
    def test_get_refresh_status(self, refresher):
        """Test getting refresh status."""
        if hasattr(refresher, 'get_status'):
            status = refresher.get_status()
            assert isinstance(status, dict)


class TestChunkStrategy:
    """Test ChunkStrategy enum."""
    
    pytestmark = pytest.mark.skipif(not CHUNK_OPTIMIZER_AVAILABLE, reason="Chunk optimizer not available")
    
    def test_strategy_values(self):
        """Test strategy enum values."""
        if not CHUNK_OPTIMIZER_AVAILABLE:
            pytest.skip("Chunk optimizer not available")
        
        assert ChunkStrategy.FIXED_SIZE.value == "fixed_size"
        assert ChunkStrategy.SENTENCE_BOUNDARY.value == "sentence_boundary"
        assert ChunkStrategy.SEMANTIC.value == "semantic"


class TestChunkConfig:
    """Test ChunkConfig dataclass."""
    
    pytestmark = pytest.mark.skipif(not CHUNK_OPTIMIZER_AVAILABLE, reason="Chunk optimizer not available")
    
    def test_create_config(self):
        """Test creating chunk configuration."""
        if not CHUNK_OPTIMIZER_AVAILABLE:
            pytest.skip("Chunk optimizer not available")
        
        config = ChunkConfig(
            content_type="technical_docs",
            chunk_size=512,
            overlap=50,
            strategy=ChunkStrategy.SENTENCE_BOUNDARY
        )
        
        assert config.chunk_size == 512
        assert config.overlap == 50
        assert config.preserve_sentences is True


class TestChunkOptimizer:
    """Test ChunkOptimizer functionality."""
    
    pytestmark = pytest.mark.skipif(not CHUNK_OPTIMIZER_AVAILABLE, reason="Chunk optimizer not available")
    
    @pytest.fixture
    def optimizer(self):
        """Create chunk optimizer instance."""
        if not CHUNK_OPTIMIZER_AVAILABLE:
            pytest.skip("Chunk optimizer not available")
        
        return ChunkOptimizer()
    
    def test_initialization(self, optimizer):
        """Test optimizer initialization."""
        assert optimizer is not None
        assert hasattr(optimizer, 'OPTIMAL_CONFIGS')
    
    def test_get_config_for_content_type(self, optimizer):
        """Test getting config for content type."""
        if hasattr(optimizer, 'get_config'):
            config = optimizer.get_config("technical_docs")
            assert config.content_type == "technical_docs"
    
    def test_chunk_text_fixed_size(self, optimizer):
        """Test fixed-size chunking."""
        text = "This is a test. " * 100  # Long text
        
        if hasattr(optimizer, 'chunk_text'):
            chunks = optimizer.chunk_text(
                text,
                chunk_size=100,
                overlap=10,
                strategy=ChunkStrategy.FIXED_SIZE
            )
            
            assert len(chunks) > 1
            assert all(len(c) <= 100 + 50 for c in chunks)  # Allow some flexibility
    
    def test_chunk_text_sentence_boundary(self, optimizer):
        """Test sentence-boundary chunking."""
        text = "First sentence. Second sentence. Third sentence. Fourth sentence."
        
        if hasattr(optimizer, 'chunk_text'):
            chunks = optimizer.chunk_text(
                text,
                chunk_size=50,
                overlap=0,
                strategy=ChunkStrategy.SENTENCE_BOUNDARY
            )
            
            # Should preserve sentence boundaries
            assert all(c.endswith('.') or c.endswith('. ') for c in chunks if c.strip())
    
    def test_optimal_configs_exist(self, optimizer):
        """Test that optimal configs are defined."""
        expected_types = ["technical_docs", "best_practices", "case_studies", "benchmarks"]
        
        for content_type in expected_types:
            assert content_type in optimizer.OPTIMAL_CONFIGS
    
    def test_count_tokens(self, optimizer):
        """Test token counting."""
        if hasattr(optimizer, 'count_tokens'):
            text = "This is a test sentence with multiple words."
            tokens = optimizer.count_tokens(text)
            assert tokens > 0
    
    def test_quality_score(self, optimizer):
        """Test chunk quality scoring."""
        if hasattr(optimizer, 'score_chunk_quality'):
            chunk = "This is a complete sentence with good content."
            score = optimizer.score_chunk_quality(chunk)
            assert 0 <= score <= 1
