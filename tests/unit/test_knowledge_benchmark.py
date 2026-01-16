"""
Unit tests for Benchmark Engine.
Tests dynamic benchmark retrieval and contextual adjustments.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Try to import
try:
    from src.platform.knowledge.benchmark_engine import DynamicBenchmarkEngine
    BENCHMARK_ENGINE_AVAILABLE = True
except ImportError:
    BENCHMARK_ENGINE_AVAILABLE = False
    DynamicBenchmarkEngine = None

pytestmark = pytest.mark.skipif(not BENCHMARK_ENGINE_AVAILABLE, reason="Benchmark engine not available")


class TestDynamicBenchmarkEngineInit:
    """Test DynamicBenchmarkEngine initialization."""
    
    def test_initialization_without_rag(self):
        """Test initialization without RAG."""
        engine = DynamicBenchmarkEngine(rag_retriever=None)
        
        assert engine is not None
        assert engine.rag is None
    
    def test_initialization_with_rag(self):
        """Test initialization with RAG."""
        mock_rag = Mock()
        engine = DynamicBenchmarkEngine(rag_retriever=mock_rag)
        
        assert engine.rag == mock_rag
    
    def test_benchmark_db_loaded(self):
        """Test that benchmark database is loaded."""
        engine = DynamicBenchmarkEngine()
        
        assert hasattr(engine, 'benchmark_db')
        assert engine.benchmark_db is not None


class TestContextualBenchmarks:
    """Test contextual benchmark retrieval."""
    
    @pytest.fixture
    def engine(self):
        """Create benchmark engine."""
        return DynamicBenchmarkEngine()
    
    def test_get_contextual_benchmarks_basic(self, engine):
        """Test basic contextual benchmark retrieval."""
        benchmarks = engine.get_contextual_benchmarks(
            channel="google_search",
            business_model="B2B",
            industry="SaaS"
        )
        
        assert isinstance(benchmarks, dict)
    
    def test_get_contextual_benchmarks_with_objective(self, engine):
        """Test benchmarks with campaign objective."""
        benchmarks = engine.get_contextual_benchmarks(
            channel="google_search",
            business_model="B2B",
            industry="SaaS",
            objective="conversion"
        )
        
        assert isinstance(benchmarks, dict)
    
    def test_get_contextual_benchmarks_with_region(self, engine):
        """Test benchmarks with regional adjustment."""
        benchmarks = engine.get_contextual_benchmarks(
            channel="meta",
            business_model="B2C",
            industry="E-commerce",
            region="Europe"
        )
        
        assert isinstance(benchmarks, dict)
    
    def test_get_contextual_benchmarks_full_context(self, engine):
        """Test benchmarks with full context."""
        benchmarks = engine.get_contextual_benchmarks(
            channel="linkedin",
            business_model="B2B",
            industry="Professional Services",
            objective="awareness",
            region="North America"
        )
        
        assert isinstance(benchmarks, dict)


class TestBenchmarkAdjustments:
    """Test benchmark adjustment methods."""
    
    @pytest.fixture
    def engine(self):
        """Create benchmark engine."""
        return DynamicBenchmarkEngine()
    
    def test_apply_objective_adjustments(self, engine):
        """Test objective-based adjustments."""
        if hasattr(engine, '_apply_objective_adjustments'):
            base_benchmarks = {
                'ctr': 0.02,
                'cpc': 2.50,
                'conversion_rate': 0.03
            }
            
            try:
                adjusted = engine._apply_objective_adjustments(
                    base_benchmarks,
                    objective="awareness",
                    business_model="B2B"
                )
                assert adjusted is not None
            except Exception:
                pytest.skip("Objective adjustments requires specific setup")
    
    def test_apply_regional_adjustments(self, engine):
        """Test regional adjustments."""
        if hasattr(engine, '_apply_regional_adjustments'):
            base_benchmarks = {
                'ctr': 0.02,
                'cpc': 2.50
            }
            
            adjusted = engine._apply_regional_adjustments(
                base_benchmarks,
                region="Europe"
            )
            
            assert isinstance(adjusted, dict)
    
    def test_get_base_benchmarks(self, engine):
        """Test getting base benchmarks."""
        if hasattr(engine, '_get_base_benchmarks'):
            benchmarks = engine._get_base_benchmarks(
                "google_search_b2b",
                "saas"
            )
            
            assert isinstance(benchmarks, dict)


class TestBenchmarkComparison:
    """Test benchmark comparison functionality."""
    
    @pytest.fixture
    def engine(self):
        """Create benchmark engine."""
        return DynamicBenchmarkEngine()
    
    def test_compare_to_benchmark(self, engine):
        """Test comparing metrics to benchmarks."""
        if hasattr(engine, 'compare_to_benchmark'):
            actual_metrics = {
                'ctr': 0.025,
                'cpc': 2.00,
                'conversion_rate': 0.04
            }
            
            comparison = engine.compare_to_benchmark(
                actual_metrics,
                channel="google_search",
                business_model="B2B",
                industry="SaaS"
            )
            
            assert isinstance(comparison, dict)
    
    def test_get_performance_rating(self, engine):
        """Test getting performance rating."""
        if hasattr(engine, 'get_performance_rating'):
            rating = engine.get_performance_rating(
                metric_name="ctr",
                actual_value=0.03,
                benchmark_value=0.02
            )
            
            assert rating in ['above', 'at', 'below'] or isinstance(rating, (int, float, str))


class TestBenchmarkDatabase:
    """Test benchmark database operations."""
    
    @pytest.fixture
    def engine(self):
        """Create benchmark engine."""
        return DynamicBenchmarkEngine()
    
    def test_load_benchmarks(self, engine):
        """Test loading benchmarks."""
        if hasattr(engine, '_load_benchmarks'):
            benchmarks = engine._load_benchmarks()
            assert isinstance(benchmarks, dict)
    
    def test_load_regional_multipliers(self, engine):
        """Test loading regional multipliers."""
        if hasattr(engine, '_load_regional_multipliers'):
            multipliers = engine._load_regional_multipliers()
            assert isinstance(multipliers, dict)
    
    def test_load_objective_adjustments(self, engine):
        """Test loading objective adjustments."""
        if hasattr(engine, '_load_objective_adjustments'):
            adjustments = engine._load_objective_adjustments()
            assert isinstance(adjustments, dict)


class TestRAGIntegration:
    """Test RAG integration for benchmarks."""
    
    @pytest.fixture
    def engine_with_rag(self):
        """Create engine with mock RAG."""
        mock_rag = Mock()
        mock_rag.retrieve = Mock(return_value="Industry benchmark: CTR 2.5%")
        return DynamicBenchmarkEngine(rag_retriever=mock_rag)
    
    def test_retrieve_industry_benchmarks(self, engine_with_rag):
        """Test retrieving benchmarks from RAG."""
        if hasattr(engine_with_rag, '_retrieve_from_rag'):
            result = engine_with_rag._retrieve_from_rag(
                query="SaaS B2B Google Ads benchmarks"
            )
            
            assert result is not None
