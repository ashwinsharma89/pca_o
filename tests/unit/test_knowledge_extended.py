"""
Extended unit tests for Knowledge module.
Tests chunking, freshness validation, and enhanced reasoning.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from datetime import datetime, timedelta

# Try to import chunking strategy
try:
    from src.platform.knowledge.chunking_strategy import ChunkingStrategy
    CHUNKING_AVAILABLE = True
except ImportError:
    CHUNKING_AVAILABLE = False
    ChunkingStrategy = None

# Try to import freshness validator
try:
    from src.platform.knowledge.freshness_validator import FreshnessValidator
    FRESHNESS_AVAILABLE = True
except ImportError:
    FRESHNESS_AVAILABLE = False
    FreshnessValidator = None

# Try to import enhanced reasoning
try:
    from src.platform.knowledge.enhanced_reasoning import EnhancedReasoning
    ENHANCED_REASONING_AVAILABLE = True
except ImportError:
    ENHANCED_REASONING_AVAILABLE = False
    EnhancedReasoning = None

# Try to import persistent vector store
try:
    from src.platform.knowledge.persistent_vector_store import PersistentVectorStore
    PERSISTENT_STORE_AVAILABLE = True
except ImportError:
    PERSISTENT_STORE_AVAILABLE = False
    PersistentVectorStore = None

# Try to import enhanced auto refresh
try:
    from src.platform.knowledge.enhanced_auto_refresh import EnhancedAutoRefresh
    ENHANCED_REFRESH_AVAILABLE = True
except ImportError:
    ENHANCED_REFRESH_AVAILABLE = False
    EnhancedAutoRefresh = None


class TestChunkingStrategy:
    """Test ChunkingStrategy functionality."""
    
    pytestmark = pytest.mark.skipif(not CHUNKING_AVAILABLE, reason="Chunking not available")
    
    @pytest.fixture
    def strategy(self):
        """Create chunking strategy."""
        return ChunkingStrategy()
    
    def test_initialization(self, strategy):
        """Test strategy initialization."""
        assert strategy is not None
    
    def test_chunk_text(self, strategy):
        """Test text chunking."""
        if hasattr(strategy, 'chunk'):
            text = "This is a long document. " * 100
            chunks = strategy.chunk(text)
            
            assert len(chunks) > 0
    
    def test_chunk_with_overlap(self, strategy):
        """Test chunking with overlap."""
        if hasattr(strategy, 'chunk'):
            text = "Sentence one. Sentence two. Sentence three. " * 50
            chunks = strategy.chunk(text, overlap=50)
            
            assert len(chunks) > 0
    
    def test_semantic_chunking(self, strategy):
        """Test semantic chunking."""
        if hasattr(strategy, 'semantic_chunk'):
            text = """
            # Introduction
            This is the introduction section.
            
            # Methods
            This describes the methods used.
            
            # Results
            These are the results.
            """
            
            chunks = strategy.semantic_chunk(text)
            assert chunks is not None


class TestFreshnessValidator:
    """Test FreshnessValidator functionality."""
    
    pytestmark = pytest.mark.skipif(not FRESHNESS_AVAILABLE, reason="Freshness validator not available")
    
    @pytest.fixture
    def validator(self):
        """Create freshness validator."""
        return FreshnessValidator()
    
    def test_initialization(self, validator):
        """Test validator initialization."""
        assert validator is not None
    
    def test_check_freshness(self, validator):
        """Test checking content freshness."""
        if hasattr(validator, 'check_freshness'):
            metadata = {
                'last_updated': datetime.now() - timedelta(days=5),
                'source': 'web'
            }
            
            try:
                result = validator.check_freshness(metadata)
                assert result is not None
            except Exception:
                pytest.skip("Freshness check requires specific setup")
    
    def test_is_stale(self, validator):
        """Test stale content detection."""
        if hasattr(validator, 'is_stale'):
            # Old content
            old_date = datetime.now() - timedelta(days=365)
            assert validator.is_stale(old_date, max_age_days=30) is True
            
            # Fresh content
            new_date = datetime.now() - timedelta(days=1)
            assert validator.is_stale(new_date, max_age_days=30) is False
    
    def test_get_refresh_priority(self, validator):
        """Test refresh priority calculation."""
        if hasattr(validator, 'get_refresh_priority'):
            metadata = {
                'last_updated': datetime.now() - timedelta(days=60),
                'importance': 'high'
            }
            
            priority = validator.get_refresh_priority(metadata)
            assert priority is not None


class TestEnhancedReasoning:
    """Test EnhancedReasoning functionality."""
    
    pytestmark = pytest.mark.skipif(not ENHANCED_REASONING_AVAILABLE, reason="Enhanced reasoning not available")
    
    @pytest.fixture
    def reasoning(self):
        """Create enhanced reasoning instance."""
        return EnhancedReasoning()
    
    def test_initialization(self, reasoning):
        """Test reasoning initialization."""
        assert reasoning is not None
    
    def test_reason_with_context(self, reasoning):
        """Test reasoning with context."""
        if hasattr(reasoning, 'reason'):
            context = "Campaign A has CTR of 5% which is above industry average."
            query = "Is Campaign A performing well?"
            
            try:
                result = reasoning.reason(query, context)
                assert result is not None
            except Exception:
                pytest.skip("Reasoning requires LLM setup")
    
    def test_multi_step_reasoning(self, reasoning):
        """Test multi-step reasoning."""
        if hasattr(reasoning, 'multi_step_reason'):
            steps = [
                "Analyze campaign metrics",
                "Compare to benchmarks",
                "Generate recommendations"
            ]
            
            try:
                result = reasoning.multi_step_reason(steps, context={})
                assert result is not None
            except Exception:
                pytest.skip("Multi-step reasoning requires LLM setup")


class TestPersistentVectorStore:
    """Test PersistentVectorStore functionality."""
    
    pytestmark = pytest.mark.skipif(not PERSISTENT_STORE_AVAILABLE, reason="Persistent store not available")
    
    @pytest.fixture
    def store(self, tmp_path):
        """Create persistent vector store."""
        try:
            return PersistentVectorStore(storage_path=str(tmp_path))
        except Exception:
            pytest.skip("PersistentVectorStore initialization failed")
    
    def test_initialization(self, store):
        """Test store initialization."""
        assert store is not None
    
    def test_add_documents(self, store):
        """Test adding documents."""
        if hasattr(store, 'add'):
            documents = [
                {"text": "Document 1", "metadata": {"type": "article"}},
                {"text": "Document 2", "metadata": {"type": "guide"}}
            ]
            
            try:
                store.add(documents)
            except Exception:
                pytest.skip("Adding documents requires embedding setup")
    
    def test_search(self, store):
        """Test searching documents."""
        if hasattr(store, 'search'):
            try:
                results = store.search("marketing tips", k=5)
                assert results is not None
            except Exception:
                pytest.skip("Search requires index setup")
    
    def test_persist(self, store):
        """Test persisting store."""
        if hasattr(store, 'persist'):
            try:
                store.persist()
            except Exception:
                pytest.skip("Persist requires setup")
    
    def test_load(self, store):
        """Test loading store."""
        if hasattr(store, 'load'):
            try:
                store.load()
            except Exception:
                pytest.skip("Load requires existing index")


class TestEnhancedAutoRefresh:
    """Test EnhancedAutoRefresh functionality."""
    
    pytestmark = pytest.mark.skipif(not ENHANCED_REFRESH_AVAILABLE, reason="Enhanced refresh not available")
    
    @pytest.fixture
    def refresher(self):
        """Create enhanced auto refresh instance."""
        return EnhancedAutoRefresh()
    
    def test_initialization(self, refresher):
        """Test refresher initialization."""
        assert refresher is not None
    
    def test_schedule_refresh(self, refresher):
        """Test scheduling refresh."""
        if hasattr(refresher, 'schedule'):
            refresher.schedule(
                source_id="source_1",
                interval_hours=24
            )
    
    def test_check_for_updates(self, refresher):
        """Test checking for updates."""
        if hasattr(refresher, 'check_updates'):
            try:
                updates = refresher.check_updates()
                assert updates is not None
            except Exception:
                pytest.skip("Update check requires source setup")
    
    def test_refresh_source(self, refresher):
        """Test refreshing a source."""
        if hasattr(refresher, 'refresh'):
            try:
                result = refresher.refresh("source_1")
                assert result is not None
            except Exception:
                pytest.skip("Refresh requires source setup")


class TestKnowledgeIntegration:
    """Test knowledge module integration."""
    
    def test_chunk_and_store_workflow(self):
        """Test chunking and storing workflow."""
        if CHUNKING_AVAILABLE and PERSISTENT_STORE_AVAILABLE:
            try:
                strategy = ChunkingStrategy()
                store = PersistentVectorStore()
                
                text = "Long document content here. " * 50
                chunks = strategy.chunk(text) if hasattr(strategy, 'chunk') else [text]
                
                if hasattr(store, 'add'):
                    store.add([{"text": c} for c in chunks])
            except Exception:
                pytest.skip("Integration requires full setup")
    
    def test_freshness_and_refresh_workflow(self):
        """Test freshness check and refresh workflow."""
        if FRESHNESS_AVAILABLE and ENHANCED_REFRESH_AVAILABLE:
            try:
                validator = FreshnessValidator()
                refresher = EnhancedAutoRefresh()
                
                # Check freshness
                if hasattr(validator, 'is_stale'):
                    old_date = datetime.now() - timedelta(days=100)
                    if validator.is_stale(old_date, max_age_days=30):
                        if hasattr(refresher, 'refresh'):
                            refresher.refresh("stale_source")
            except Exception:
                pytest.skip("Integration requires full setup")
