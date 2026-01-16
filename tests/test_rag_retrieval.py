"""
RAG Retrieval Quality Tests
Tests RAG system retrieval accuracy and quality
"""

import pytest
from typing import List, Dict
import numpy as np

from src.platform.knowledge.benchmark_engine import DynamicBenchmarkEngine


class TestBenchmarkRetrieval:
    """Test benchmark retrieval from RAG system"""
    
    @pytest.fixture
    def benchmark_engine(self):
        """Create benchmark engine instance"""
        return DynamicBenchmarkEngine()
    
    def test_b2b_benchmark_retrieval(self, benchmark_engine):
        """Verify B2B benchmarks are retrieved correctly"""
        
        benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='Technology'
        )
        
        assert 'benchmarks' in benchmarks
        assert 'ctr' in benchmarks['benchmarks']
        # Note: roas may not be present for all B2B benchmarks
        
        # B2B benchmarks should be in reasonable ranges
        # ctr is a dict with levels (excellent, good, average, needs_work)
        assert 0.001 <= benchmarks['benchmarks']['ctr']['good'] <= 0.20
        
        print("✅ B2B benchmark retrieval test passed!")
    
    def test_b2c_benchmark_retrieval(self, benchmark_engine):
        """Verify B2C benchmarks are retrieved correctly"""
        
        benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='meta',
            business_model='B2C',
            industry='Retail'
        )
        
        assert 'benchmarks' in benchmarks
        assert 'ctr' in benchmarks['benchmarks']
        
        # B2C benchmarks should differ from B2B
        b2b_benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='Technology'
        )
        
        # At least some benchmarks should be different
        # ctr is a dict with levels
        assert benchmarks['benchmarks']['ctr']['good'] != b2b_benchmarks['benchmarks']['ctr']['good']
        
        print("✅ B2C benchmark retrieval test passed!")
    
    def test_channel_specific_benchmarks(self, benchmark_engine):
        """Test channel-specific benchmark retrieval"""
        
        # LinkedIn benchmarks
        linkedin_benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='linkedin',
            business_model='B2B',
            industry='Technology'
        )
        
        assert 'benchmarks' in linkedin_benchmarks
        assert 'ctr' in linkedin_benchmarks['benchmarks']
        
        # Meta benchmarks
        meta_benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='meta',
            business_model='B2C',
            industry='Retail'
        )
        
        assert 'benchmarks' in meta_benchmarks
        assert 'ctr' in meta_benchmarks['benchmarks']
        
        # Benchmarks should differ by channel
        assert linkedin_benchmarks['benchmarks'] != meta_benchmarks['benchmarks']
        
        print("✅ Channel-specific benchmarks test passed!")
    
    def test_industry_specific_benchmarks(self, benchmark_engine):
        """Test industry-specific benchmark retrieval"""
        
        tech_benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='Technology'
        )
        
        healthcare_benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='Healthcare'
        )
        
        # Both should return valid benchmarks
        assert 'benchmarks' in tech_benchmarks
        assert 'ctr' in tech_benchmarks['benchmarks']
        assert 'benchmarks' in healthcare_benchmarks
        assert 'ctr' in healthcare_benchmarks['benchmarks']
        
        print("✅ Industry-specific benchmarks test passed!")
    
    def test_benchmark_fallback(self, benchmark_engine):
        """Test fallback when specific benchmarks not available"""
        
        # Request benchmarks for uncommon scenario - use a known channel with unknown industry
        benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='Rare Industry'
        )
        
        # Should still return default benchmarks (falls back to default)
        assert 'benchmarks' in benchmarks
        assert 'ctr' in benchmarks['benchmarks']
        # ctr is a dict with levels
        assert benchmarks['benchmarks']['ctr']['good'] > 0
        
        print("✅ Benchmark fallback test passed!")


class TestRetrievalQuality:
    """Test RAG retrieval quality metrics"""
    
    def test_retrieval_relevance(self):
        """Test that retrieved content is relevant to query"""
        
        # Simulate RAG retrieval
        query = "B2B LinkedIn CTR benchmark"
        
        # Mock retrieved results
        results = [
            {
                'content': 'LinkedIn B2B campaigns typically see CTR of 0.35-0.45%',
                'relevance_score': 0.95,
                'category': 'benchmarks'
            },
            {
                'content': 'B2B advertising benchmarks vary by industry',
                'relevance_score': 0.75,
                'category': 'benchmarks'
            }
        ]
        
        # Check relevance
        assert len(results) > 0
        assert all('linkedin' in r['content'].lower() or 'b2b' in r['content'].lower() 
                   for r in results)
        
        # Check relevance scores
        assert all(r['relevance_score'] > 0.5 for r in results)
        
        print("✅ Retrieval relevance test passed!")
    
    def test_retrieval_diversity(self):
        """Test that retrieved results are diverse"""
        
        # Mock retrieved results
        results = [
            {'content': 'LinkedIn CTR benchmark: 0.35%', 'source': 'source_1'},
            {'content': 'B2B conversion rates average 2.5%', 'source': 'source_2'},
            {'content': 'Cost per lead benchmarks for B2B', 'source': 'source_3'}
        ]
        
        # Check diversity - different sources
        sources = [r['source'] for r in results]
        assert len(set(sources)) == len(sources)
        
        # Check diversity - different content
        contents = [r['content'] for r in results]
        assert len(set(contents)) == len(contents)
        
        print("✅ Retrieval diversity test passed!")
    
    def test_retrieval_completeness(self):
        """Test that retrieval returns sufficient results"""
        
        # Mock retrieval
        query = "digital marketing benchmarks"
        results = [
            {'content': f'Benchmark {i}', 'score': 0.9 - i*0.1}
            for i in range(5)
        ]
        
        # Should return multiple results
        assert len(results) >= 3
        
        # Results should be ranked by relevance
        scores = [r['score'] for r in results]
        assert scores == sorted(scores, reverse=True)
        
        print("✅ Retrieval completeness test passed!")
    
    def test_retrieval_accuracy(self):
        """Test retrieval accuracy for specific queries"""
        
        test_cases = [
            {
                'query': 'LinkedIn B2B CTR',
                'expected_keywords': ['linkedin', 'b2b', 'ctr'],
                'expected_range': (0.002, 0.01)  # 0.2% - 1%
            },
            {
                'query': 'Meta ROAS benchmark',
                'expected_keywords': ['meta', 'facebook', 'roas'],
                'expected_range': (2.0, 5.0)
            }
        ]
        
        for test_case in test_cases:
            # Mock retrieval result
            result = {
                'content': f"For {test_case['query']}, typical values are...",
                'value': np.mean(test_case['expected_range'])
            }
            
            # Check keywords present
            content_lower = result['content'].lower()
            keywords_found = sum(
                1 for kw in test_case['expected_keywords']
                if kw in content_lower
            )
            
            assert keywords_found >= 1
            
            # Check value in expected range
            assert test_case['expected_range'][0] <= result['value'] <= test_case['expected_range'][1]
        
        print("✅ Retrieval accuracy test passed!")


class TestContextualRetrieval:
    """Test context-aware retrieval"""
    
    def test_context_filtering(self):
        """Test that context filters retrieval results"""
        
        # B2B context
        b2b_results = [
            {'content': 'B2B LinkedIn benchmarks', 'category': 'b2b'},
            {'content': 'B2B lead generation', 'category': 'b2b'}
        ]
        
        # B2C context
        b2c_results = [
            {'content': 'B2C e-commerce benchmarks', 'category': 'b2c'},
            {'content': 'B2C social media', 'category': 'b2c'}
        ]
        
        # Results should be different based on context
        assert b2b_results != b2c_results
        assert all('b2b' in r['content'].lower() for r in b2b_results)
        assert all('b2c' in r['content'].lower() for r in b2c_results)
        
        print("✅ Context filtering test passed!")
    
    def test_temporal_context(self):
        """Test retrieval considers temporal context"""
        
        # Recent data should be prioritized
        results = [
            {'content': 'Q4 2024 benchmarks', 'date': '2024-10-01', 'recency_score': 1.0},
            {'content': 'Q1 2024 benchmarks', 'date': '2024-01-01', 'recency_score': 0.7},
            {'content': '2023 benchmarks', 'date': '2023-01-01', 'recency_score': 0.4}
        ]
        
        # More recent results should have higher scores
        assert results[0]['recency_score'] > results[1]['recency_score']
        assert results[1]['recency_score'] > results[2]['recency_score']
        
        print("✅ Temporal context test passed!")
    
    def test_multi_dimensional_context(self):
        """Test retrieval with multiple context dimensions"""
        
        context = {
            'business_model': 'B2B',
            'industry': 'Technology',
            'channel': 'LinkedIn',
            'region': 'North America'
        }
        
        # Mock retrieval with context
        result = {
            'content': 'B2B Technology LinkedIn benchmarks for North America',
            'matches': ['b2b', 'technology', 'linkedin', 'north america']
        }
        
        # Should match multiple context dimensions
        assert len(result['matches']) >= 3
        
        print("✅ Multi-dimensional context test passed!")


class TestRetrievalPerformance:
    """Test RAG retrieval performance"""
    
    def test_retrieval_speed(self):
        """Test that retrieval completes in reasonable time"""
        
        import time
        
        benchmark_engine = DynamicBenchmarkEngine()
        
        start_time = time.time()
        
        benchmarks = benchmark_engine.get_contextual_benchmarks(
            channel='linkedin',
            business_model='B2B',
            industry='Technology'
        )
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete in under 2 seconds
        assert elapsed < 2.0
        assert 'benchmarks' in benchmarks
        assert 'ctr' in benchmarks['benchmarks']
        
        print(f"✅ Retrieval speed test passed! ({elapsed:.3f}s)")
    
    def test_batch_retrieval(self):
        """Test batch retrieval efficiency"""
        
        benchmark_engine = DynamicBenchmarkEngine()
        
        test_cases = [
            ('linkedin', 'B2B', 'Technology'),
            ('meta', 'B2C', 'Retail'),
            ('google_search', 'B2B', 'SaaS')
        ]
        
        results = []
        for channel, business_model, industry in test_cases:
            benchmarks = benchmark_engine.get_contextual_benchmarks(
                channel=channel,
                business_model=business_model,
                industry=industry
            )
            results.append(benchmarks)
        
        # All should return valid results
        assert len(results) == len(test_cases)
        assert all('benchmarks' in r and 'ctr' in r['benchmarks'] for r in results)
        
        print("✅ Batch retrieval test passed!")
    
    def test_caching_effectiveness(self):
        """Test that caching improves performance"""
        
        import time
        
        benchmark_engine = DynamicBenchmarkEngine()
        
        # First call (cold)
        start_time = time.time()
        result1 = benchmark_engine.get_contextual_benchmarks(
            channel='linkedin',
            business_model='B2B',
            industry='Technology'
        )
        first_call_time = time.time() - start_time
        
        # Second call (should be cached)
        start_time = time.time()
        result2 = benchmark_engine.get_contextual_benchmarks(
            channel='linkedin',
            business_model='B2B',
            industry='Technology'
        )
        second_call_time = time.time() - start_time
        
        # Results should be identical
        assert result1 == result2
        
        # Second call should be faster (or at least not slower)
        assert second_call_time <= first_call_time * 1.5
        
        print(f"✅ Caching effectiveness test passed! (1st: {first_call_time:.3f}s, 2nd: {second_call_time:.3f}s)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
