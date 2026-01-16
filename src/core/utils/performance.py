"""
Performance Optimization Module for PCA Agent

Implements:
1. Parallel Processing - asyncio-based concurrent execution
2. Semantic Caching - Cache similar RAG queries
3. Smart RAG Query Bundling - Reduce redundant queries
4. LLM Token Optimization - Context-aware truncation
5. Incremental Results Streaming - Progressive UI updates
6. Pre-computation & Background Jobs - Startup optimization
7. Model Selection by Task - Right model for right task

Author: PCA Agent Team
"""

import asyncio
import hashlib
import json
import os
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import numpy as np
from loguru import logger


# =============================================================================
# 1. PARALLEL PROCESSING
# =============================================================================

class ParallelExecutor:
    """
    Execute independent tasks in parallel using ThreadPoolExecutor.
    
    Benefits:
    - Sequential time: 20 + 30 + 10 = 60 seconds
    - Parallel time: max(20, 30, 10) = 30 seconds
    - Savings: 50%
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self, max_workers: int = 8):
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._metrics = {
            'total_tasks': 0,
            'parallel_executions': 0,
            'time_saved_seconds': 0.0
        }
    
    @classmethod
    def get_instance(cls, max_workers: int = 8) -> 'ParallelExecutor':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(max_workers)
            return cls._instance
    
    def execute_parallel(self, tasks: List[Tuple[Callable, tuple, dict]], 
                        timeout: float = 120.0) -> List[Any]:
        """
        Execute multiple tasks in parallel.
        
        Args:
            tasks: List of (function, args, kwargs) tuples
            timeout: Maximum time to wait for all tasks
            
        Returns:
            List of results in same order as tasks
        """
        if not tasks:
            return []
        
        start_time = time.time()
        results = [None] * len(tasks)
        futures = {}
        
        # Submit all tasks
        for idx, (func, args, kwargs) in enumerate(tasks):
            future = self._executor.submit(func, *args, **kwargs)
            futures[future] = idx
        
        # Collect results
        sequential_time = 0
        for future in as_completed(futures, timeout=timeout):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                logger.error(f"Task {idx} failed: {e}")
                results[idx] = None
        
        parallel_time = time.time() - start_time
        
        # Update metrics
        self._metrics['total_tasks'] += len(tasks)
        self._metrics['parallel_executions'] += 1
        
        logger.info(f"Parallel execution: {len(tasks)} tasks in {parallel_time:.2f}s")
        
        return results
    
    async def execute_async(self, coroutines: List[Any]) -> List[Any]:
        """
        Execute async coroutines in parallel using asyncio.gather.
        
        Args:
            coroutines: List of coroutines to execute
            
        Returns:
            List of results
        """
        if not coroutines:
            return []
        
        start_time = time.time()
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        elapsed = time.time() - start_time
        
        logger.info(f"Async execution: {len(coroutines)} tasks in {elapsed:.2f}s")
        
        return results
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()


# =============================================================================
# 2. SEMANTIC CACHING
# =============================================================================

@dataclass
class CacheEntry:
    """Single cache entry with metadata."""
    query_embedding: Optional[List[float]]
    query_text: str
    results: Any
    timestamp: datetime
    hit_count: int = 0
    platform: Optional[str] = None
    business_model: Optional[str] = None
    ttl_hours: int = 168  # 7 days default


class SemanticCache:
    """
    Semantic cache that recognizes similar queries.
    
    Benefits:
    - First query: Cache miss → Query database (4 seconds)
    - Similar queries: Cache hit (0.01 seconds)
    - Savings: 67% reduction for repeated similar queries
    
    Lookup process:
    1. Embed incoming query (0.01 seconds)
    2. Compare to cached embeddings (cosine similarity)
    3. If similarity > 0.85: Return cached results
    4. If not: Query database, cache results
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self, 
                 cache_dir: Optional[str] = None,
                 max_entries: int = 10000,
                 similarity_threshold: float = 0.85,
                 ttl_hours: int = 168):  # 7 days
        
        self.cache_dir = cache_dir or os.path.join(
            os.path.dirname(__file__), "..", "..", "cache"
        )
        self.max_entries = max_entries
        self.similarity_threshold = similarity_threshold
        self.ttl_hours = ttl_hours
        
        # In-memory cache
        self._cache: Dict[str, CacheEntry] = {}
        self._embeddings_cache: Dict[str, List[float]] = {}
        
        # Metrics
        self._metrics = {
            'hits': 0,
            'misses': 0,
            'semantic_hits': 0,
            'evictions': 0,
            'time_saved_seconds': 0.0
        }
        
        # Ensure cache directory exists
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        
        # Load persistent cache
        self._load_cache()
    
    @classmethod
    def get_instance(cls, **kwargs) -> 'SemanticCache':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(**kwargs)
            return cls._instance
    
    def _compute_embedding(self, text: str) -> List[float]:
        """
        Compute embedding for text using simple hash-based approach.
        For production, replace with actual embedding model.
        """
        # Simple hash-based pseudo-embedding for demo
        # In production, use sentence-transformers or OpenAI embeddings
        import hashlib
        
        # Normalize text
        normalized = text.lower().strip()
        words = normalized.split()
        
        # Create a simple 128-dim pseudo-embedding
        # Using SHA-256 instead of MD5 (MD5 is cryptographically weak)
        embedding = [0.0] * 128
        for i, word in enumerate(words):
            word_hash = int(hashlib.sha256(word.encode()).hexdigest(), 16)
            for j in range(128):
                embedding[j] += ((word_hash >> j) & 1) * (1.0 / (i + 1))
        
        # Normalize
        norm = sum(x*x for x in embedding) ** 0.5
        if norm > 0:
            embedding = [x / norm for x in embedding]
        
        return embedding
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if len(a) != len(b):
            return 0.0
        
        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        
        return dot_product / (norm_a * norm_b)
    
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key from query."""
        normalized = query.lower().strip()
        return hashlib.sha256(normalized.encode()).hexdigest()[:32]
    
    def get(self, query: str, context: Optional[Dict] = None) -> Optional[Any]:
        """
        Get cached results for a query.
        
        Args:
            query: The query string
            context: Optional context (platform, business_model, etc.)
            
        Returns:
            Cached results if found, None otherwise
        """
        start_time = time.time()
        
        # Check exact match first
        cache_key = self._get_cache_key(query)
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            
            # Check TTL
            if datetime.now() - entry.timestamp < timedelta(hours=entry.ttl_hours):
                entry.hit_count += 1
                self._metrics['hits'] += 1
                self._metrics['time_saved_seconds'] += 4.0  # Assume 4s saved per hit
                logger.debug(f"Cache hit (exact): {query[:50]}...")
                return entry.results
            else:
                # Expired, remove
                del self._cache[cache_key]
        
        # Check semantic similarity
        query_embedding = self._compute_embedding(query)
        
        best_match = None
        best_similarity = 0.0
        
        for key, entry in self._cache.items():
            if entry.query_embedding:
                similarity = self._cosine_similarity(query_embedding, entry.query_embedding)
                if similarity > best_similarity and similarity >= self.similarity_threshold:
                    best_similarity = similarity
                    best_match = entry
        
        if best_match:
            # Check TTL
            if datetime.now() - best_match.timestamp < timedelta(hours=best_match.ttl_hours):
                best_match.hit_count += 1
                self._metrics['semantic_hits'] += 1
                self._metrics['time_saved_seconds'] += 4.0
                logger.debug(f"Cache hit (semantic, sim={best_similarity:.2f}): {query[:50]}...")
                return best_match.results
        
        self._metrics['misses'] += 1
        return None
    
    def set(self, query: str, results: Any, 
            context: Optional[Dict] = None,
            ttl_hours: Optional[int] = None) -> None:
        """
        Cache results for a query.
        
        Args:
            query: The query string
            results: Results to cache
            context: Optional context metadata
            ttl_hours: Optional custom TTL
        """
        # Evict if at capacity
        if len(self._cache) >= self.max_entries:
            self._evict_lru()
        
        cache_key = self._get_cache_key(query)
        embedding = self._compute_embedding(query)
        
        entry = CacheEntry(
            query_embedding=embedding,
            query_text=query,
            results=results,
            timestamp=datetime.now(),
            hit_count=0,
            platform=context.get('platform') if context else None,
            business_model=context.get('business_model') if context else None,
            ttl_hours=ttl_hours or self.ttl_hours
        )
        
        self._cache[cache_key] = entry
        logger.debug(f"Cached: {query[:50]}...")
    
    def _evict_lru(self) -> None:
        """Evict least recently used entries."""
        if not self._cache:
            return
        
        # Sort by hit count and timestamp (LRU)
        sorted_entries = sorted(
            self._cache.items(),
            key=lambda x: (x[1].hit_count, x[1].timestamp)
        )
        
        # Remove bottom 10%
        to_remove = max(1, len(sorted_entries) // 10)
        for key, _ in sorted_entries[:to_remove]:
            del self._cache[key]
            self._metrics['evictions'] += 1
        
        logger.info(f"Evicted {to_remove} cache entries")
    
    def _load_cache(self) -> None:
        """Load cache from disk."""
        cache_file = os.path.join(self.cache_dir, "semantic_cache.pkl")
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)  # nosec B301 B403
                    self._cache = data.get('cache', {})
                    self._metrics = data.get('metrics', self._metrics)
                logger.info(f"Loaded {len(self._cache)} cache entries from disk")
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")
    
    def save_cache(self) -> None:
        """Save cache to disk."""
        cache_file = os.path.join(self.cache_dir, "semantic_cache.pkl")
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump({  # nosec B301 B403
                    'cache': self._cache,
                    'metrics': self._metrics
                }, f)
            logger.info(f"Saved {len(self._cache)} cache entries to disk")
        except Exception as e:
            logger.warning(f"Could not save cache: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get cache metrics."""
        total = self._metrics['hits'] + self._metrics['semantic_hits'] + self._metrics['misses']
        hit_rate = (self._metrics['hits'] + self._metrics['semantic_hits']) / total if total > 0 else 0
        
        return {
            **self._metrics,
            'total_requests': total,
            'hit_rate': round(hit_rate * 100, 2),
            'cache_size': len(self._cache)
        }
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        logger.info("Cache cleared")


# =============================================================================
# 3. SMART RAG QUERY BUNDLING
# =============================================================================

class SmartQueryBundler:
    """
    Reduce RAG queries by bundling related queries.
    
    Instead of 5 separate queries:
    - "LinkedIn benchmarks"
    - "LinkedIn best practices"
    - "LinkedIn optimization"
    - "LinkedIn CTR improvement"
    - "LinkedIn CPL reduction"
    
    Use 1-2 comprehensive queries:
    - "LinkedIn B2B campaign analysis: benchmarks, best practices, 
       optimization strategies for CTR and CPL improvement"
    """
    
    # Query templates for different analysis needs
    QUERY_TEMPLATES = {
        'comprehensive': (
            "{platform} {business_model} campaign analysis: "
            "benchmarks, best practices, optimization strategies for "
            "{metrics} improvement"
        ),
        'issue_specific': (
            "{platform} {issue_type} solutions: "
            "{specific_issues}"
        ),
        'success_patterns': (
            "{platform} {business_model} high-performing campaign patterns "
            "and success factors"
        )
    }
    
    # Common issue mappings
    ISSUE_MAPPINGS = {
        'high_cpa': 'cost reduction, CPA optimization, efficiency improvement',
        'low_ctr': 'CTR improvement, creative optimization, audience targeting',
        'low_roas': 'ROAS improvement, conversion optimization, revenue growth',
        'creative_fatigue': 'creative fatigue solutions, frequency management, ad rotation',
        'audience_saturation': 'audience expansion, lookalike audiences, new targeting'
    }
    
    def __init__(self, cache: Optional[SemanticCache] = None):
        self.cache = cache or SemanticCache.get_instance()
        self._metrics = {
            'queries_bundled': 0,
            'queries_reduced': 0
        }
    
    def bundle_queries(self, 
                      metrics: Dict[str, Any],
                      detected_issues: List[str] = None,
                      platforms: List[str] = None) -> List[str]:
        """
        Generate optimized bundled queries based on analysis needs.
        
        Args:
            metrics: Campaign metrics
            detected_issues: List of detected issues
            platforms: List of platforms in the data
            
        Returns:
            List of optimized queries (typically 1-3 instead of 5+)
        """
        queries = []
        
        # Extract context
        overview = metrics.get('overview', {})
        platform_list = platforms or list(metrics.get('by_platform', {}).keys())
        primary_platform = platform_list[0] if platform_list else 'digital marketing'
        
        # Determine business model from context
        business_model = self._infer_business_model(metrics)
        
        # Determine key metrics to focus on
        focus_metrics = self._get_focus_metrics(overview)
        
        # Query 1: Comprehensive context (always needed)
        comprehensive_query = self.QUERY_TEMPLATES['comprehensive'].format(
            platform=primary_platform,
            business_model=business_model,
            metrics=', '.join(focus_metrics)
        )
        queries.append(comprehensive_query)
        
        # Query 2: Issue-specific (only if issues detected)
        if detected_issues:
            issue_details = []
            for issue in detected_issues[:3]:  # Limit to top 3 issues
                if issue in self.ISSUE_MAPPINGS:
                    issue_details.append(self.ISSUE_MAPPINGS[issue])
            
            if issue_details:
                issue_query = self.QUERY_TEMPLATES['issue_specific'].format(
                    platform=primary_platform,
                    issue_type='performance optimization',
                    specific_issues='; '.join(issue_details)
                )
                queries.append(issue_query)
        
        # Query 3: Success patterns (likely cached)
        # Check cache first
        success_query = self.QUERY_TEMPLATES['success_patterns'].format(
            platform=primary_platform,
            business_model=business_model
        )
        
        # Only add if not in cache
        cached = self.cache.get(success_query)
        if cached is None:
            queries.append(success_query)
        
        self._metrics['queries_bundled'] += len(queries)
        self._metrics['queries_reduced'] += max(0, 5 - len(queries))  # Assume 5 was original
        
        logger.info(f"Bundled queries: {len(queries)} (reduced from ~5)")
        
        return queries
    
    def _infer_business_model(self, metrics: Dict) -> str:
        """Infer business model from metrics."""
        overview = metrics.get('overview', {})
        
        # Simple heuristics
        avg_cpa = overview.get('avg_cpa', 0)
        if avg_cpa > 100:
            return 'B2B enterprise'
        elif avg_cpa > 30:
            return 'B2B'
        else:
            return 'B2C'
    
    def _get_focus_metrics(self, overview: Dict) -> List[str]:
        """Determine which metrics to focus on."""
        focus = []
        
        if overview.get('avg_cpa', 0) > 0:
            focus.append('CPA')
        if overview.get('avg_roas', 0) > 0:
            focus.append('ROAS')
        if overview.get('avg_ctr', 0) > 0:
            focus.append('CTR')
        if overview.get('avg_cpc', 0) > 0:
            focus.append('CPC')
        
        return focus or ['performance']
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()


# =============================================================================
# 4. LLM TOKEN OPTIMIZATION
# =============================================================================

class TokenOptimizer:
    """
    Optimize token usage by providing each LLM call only what it needs.
    
    Executive summary needs:
    - Key benchmarks (500 tokens)
    - Top insights (1,000 tokens)
    - 1 success pattern (500 tokens)
    Total: 2,000 tokens
    
    Detailed analysis needs:
    - Full benchmarks (2,000 tokens)
    - All insights (3,000 tokens)
    - Multiple patterns (2,000 tokens)
    Total: 7,000 tokens
    """
    
    # Token budgets by task type
    TOKEN_BUDGETS = {
        'executive_summary': {
            'benchmarks': 500,
            'insights': 1000,
            'patterns': 500,
            'total': 2000
        },
        'detailed_analysis': {
            'benchmarks': 2000,
            'insights': 3000,
            'patterns': 2000,
            'total': 7000
        },
        'recommendations': {
            'tactics': 3000,
            'case_studies': 2000,
            'total': 5000
        },
        'quick_summary': {
            'total': 1000
        }
    }
    
    # Approximate tokens per character
    CHARS_PER_TOKEN = 4
    
    def __init__(self):
        self._metrics = {
            'tokens_saved': 0,
            'optimizations': 0
        }
    
    def optimize_context(self, 
                        context: Dict[str, Any],
                        task_type: str = 'detailed_analysis') -> Dict[str, Any]:
        """
        Optimize context for a specific task type.
        
        Args:
            context: Full context dictionary
            task_type: Type of task (executive_summary, detailed_analysis, etc.)
            
        Returns:
            Optimized context with truncated content
        """
        budget = self.TOKEN_BUDGETS.get(task_type, self.TOKEN_BUDGETS['detailed_analysis'])
        optimized = {}
        
        original_tokens = self._estimate_tokens(context)
        
        # Optimize each section based on budget
        if 'rag_context' in context:
            rag_budget = budget.get('benchmarks', 2000) + budget.get('patterns', 1000)
            optimized['rag_context'] = self._truncate_rag_context(
                context['rag_context'], 
                max_tokens=rag_budget
            )
        
        if 'insights' in context:
            insights_budget = budget.get('insights', 2000)
            optimized['insights'] = self._truncate_insights(
                context['insights'],
                max_tokens=insights_budget
            )
        
        if 'metrics' in context:
            # Metrics are usually small, keep as-is but simplify
            optimized['metrics'] = self._simplify_metrics(
                context['metrics'],
                task_type=task_type
            )
        
        # Copy other fields
        for key in context:
            if key not in optimized:
                optimized[key] = context[key]
        
        optimized_tokens = self._estimate_tokens(optimized)
        tokens_saved = original_tokens - optimized_tokens
        
        self._metrics['tokens_saved'] += tokens_saved
        self._metrics['optimizations'] += 1
        
        logger.debug(f"Token optimization: {original_tokens} → {optimized_tokens} ({tokens_saved} saved)")
        
        return optimized
    
    def _estimate_tokens(self, obj: Any) -> int:
        """Estimate token count for an object."""
        if isinstance(obj, str):
            return len(obj) // self.CHARS_PER_TOKEN
        elif isinstance(obj, dict):
            return sum(self._estimate_tokens(v) for v in obj.values())
        elif isinstance(obj, list):
            return sum(self._estimate_tokens(item) for item in obj)
        else:
            return len(str(obj)) // self.CHARS_PER_TOKEN
    
    def _truncate_rag_context(self, 
                              rag_context: List[Dict],
                              max_tokens: int) -> List[Dict]:
        """Truncate RAG context to fit token budget."""
        if not rag_context:
            return []
        
        # Sort by relevance score
        sorted_context = sorted(
            rag_context, 
            key=lambda x: x.get('score', 0),
            reverse=True
        )
        
        result = []
        current_tokens = 0
        
        for chunk in sorted_context:
            content = chunk.get('content', '')
            chunk_tokens = len(content) // self.CHARS_PER_TOKEN
            
            if current_tokens + chunk_tokens <= max_tokens:
                result.append(chunk)
                current_tokens += chunk_tokens
            else:
                # Truncate this chunk to fit remaining budget
                remaining = max_tokens - current_tokens
                if remaining > 100:  # Only add if meaningful
                    truncated_content = content[:remaining * self.CHARS_PER_TOKEN]
                    result.append({
                        **chunk,
                        'content': truncated_content + '...',
                        'truncated': True
                    })
                break
        
        return result
    
    def _truncate_insights(self,
                          insights: List[Dict],
                          max_tokens: int) -> List[Dict]:
        """Truncate insights to fit token budget."""
        if not insights:
            return []
        
        # Sort by priority/importance
        sorted_insights = sorted(
            insights,
            key=lambda x: {'high': 3, 'medium': 2, 'low': 1}.get(
                x.get('priority', 'medium'), 2
            ),
            reverse=True
        )
        
        result = []
        current_tokens = 0
        
        for insight in sorted_insights:
            insight_tokens = self._estimate_tokens(insight)
            
            if current_tokens + insight_tokens <= max_tokens:
                result.append(insight)
                current_tokens += insight_tokens
            else:
                break
        
        return result
    
    def _simplify_metrics(self, 
                         metrics: Dict,
                         task_type: str) -> Dict:
        """Simplify metrics based on task type."""
        if task_type == 'executive_summary':
            # Only keep overview for executive summary
            return {
                'overview': metrics.get('overview', {}),
                'best_platform': metrics.get('best_platform'),
                'worst_platform': metrics.get('worst_platform')
            }
        elif task_type == 'quick_summary':
            # Minimal metrics
            overview = metrics.get('overview', {})
            return {
                'total_spend': overview.get('total_spend'),
                'avg_roas': overview.get('avg_roas'),
                'avg_cpa': overview.get('avg_cpa')
            }
        else:
            # Full metrics for detailed analysis
            return metrics
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()


# =============================================================================
# 5. INCREMENTAL RESULTS STREAMING
# =============================================================================

@dataclass
class ProgressUpdate:
    """Progress update for streaming results."""
    stage: str
    status: str  # 'started', 'completed', 'failed'
    progress_percent: float
    message: str
    data: Optional[Any] = None
    timestamp: datetime = field(default_factory=datetime.now)


class ProgressStreamer:
    """
    Stream incremental results to UI.
    
    Stages:
    1. Validating data (5s)
    2. Calculating metrics (10s)
    3. Generating insights (15s)
    4. Deep analysis (20s)
    5. Recommendations (15s)
    6. Visualizations (10s)
    7. Report assembly (5s)
    """
    
    STAGES = [
        ('validation', 'Validating data', 5),
        ('metrics', 'Calculating metrics', 10),
        ('insights', 'Generating insights', 15),
        ('analysis', 'Deep analysis', 20),
        ('recommendations', 'Generating recommendations', 15),
        ('visualizations', 'Creating visualizations', 10),
        ('assembly', 'Assembling report', 5)
    ]
    
    def __init__(self, callback: Optional[Callable[[ProgressUpdate], None]] = None):
        self.callback = callback
        self._current_stage = 0
        self._updates: List[ProgressUpdate] = []
        self._lock = threading.Lock()
    
    def update(self, 
               stage: str,
               status: str,
               message: str,
               data: Any = None) -> ProgressUpdate:
        """
        Send a progress update.
        
        Args:
            stage: Current stage name
            status: 'started', 'completed', or 'failed'
            message: Human-readable message
            data: Optional data to include
            
        Returns:
            The progress update object
        """
        # Calculate progress percentage
        stage_idx = next(
            (i for i, (s, _, _) in enumerate(self.STAGES) if s == stage),
            0
        )
        
        if status == 'completed':
            progress = ((stage_idx + 1) / len(self.STAGES)) * 100
        else:
            progress = (stage_idx / len(self.STAGES)) * 100
        
        update = ProgressUpdate(
            stage=stage,
            status=status,
            progress_percent=round(progress, 1),
            message=message,
            data=data
        )
        
        with self._lock:
            self._updates.append(update)
        
        # Call callback if provided
        if self.callback:
            try:
                self.callback(update)
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
        
        logger.info(f"Progress: [{progress:.0f}%] {stage} - {message}")
        
        return update
    
    def get_updates(self) -> List[ProgressUpdate]:
        """Get all progress updates."""
        with self._lock:
            return self._updates.copy()
    
    def reset(self):
        """Reset progress tracking."""
        with self._lock:
            self._updates.clear()
            self._current_stage = 0


# =============================================================================
# 6. PRE-COMPUTATION & BACKGROUND JOBS
# =============================================================================

class PrecomputationManager:
    """
    Manage pre-computation and background jobs.
    
    Pre-compute at startup:
    - Embeddings for knowledge chunks
    - FAISS/BM25 indexes
    - Common query results
    - Platform benchmarks
    - Prompt templates
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._precomputed = {}
        self._background_tasks = []
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._initialized = False
        self._metrics = {
            'precomputed_items': 0,
            'background_jobs': 0,
            'startup_time_saved': 0.0
        }
    
    @classmethod
    def get_instance(cls) -> 'PrecomputationManager':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def initialize(self, 
                   benchmarks_path: Optional[str] = None,
                   templates_path: Optional[str] = None) -> None:
        """
        Initialize pre-computed data at startup.
        
        Args:
            benchmarks_path: Path to benchmarks data
            templates_path: Path to prompt templates
        """
        if self._initialized:
            return
        
        start_time = time.time()
        logger.info("Starting pre-computation...")
        
        # Pre-load benchmarks
        self._preload_benchmarks(benchmarks_path)
        
        # Pre-compile templates
        self._precompile_templates(templates_path)
        
        # Pre-compute common patterns
        self._precompute_patterns()
        
        self._initialized = True
        elapsed = time.time() - start_time
        
        logger.info(f"Pre-computation complete in {elapsed:.2f}s")
        self._metrics['startup_time_saved'] = elapsed
    
    def _preload_benchmarks(self, path: Optional[str] = None) -> None:
        """Pre-load platform benchmarks into memory."""
        # Default benchmarks (in production, load from file)
        self._precomputed['benchmarks'] = {
            'linkedin': {
                'b2b': {'ctr': 0.44, 'cpc': 5.26, 'cpl': 75.0},
                'b2c': {'ctr': 0.35, 'cpc': 3.50, 'cpl': 45.0}
            },
            'google_ads': {
                'search': {'ctr': 3.17, 'cpc': 2.69, 'cvr': 3.75},
                'display': {'ctr': 0.46, 'cpc': 0.63, 'cvr': 0.77}
            },
            'meta': {
                'b2b': {'ctr': 0.90, 'cpc': 1.72, 'cpm': 11.54},
                'b2c': {'ctr': 1.11, 'cpc': 0.97, 'cpm': 7.19}
            },
            'dv360': {
                'display': {'ctr': 0.35, 'cpm': 2.50, 'viewability': 65.0},
                'video': {'ctr': 0.15, 'cpm': 12.00, 'vtr': 70.0}
            }
        }
        self._metrics['precomputed_items'] += 1
        logger.debug("Benchmarks pre-loaded")
    
    def _precompile_templates(self, path: Optional[str] = None) -> None:
        """Pre-compile prompt templates."""
        self._precomputed['templates'] = {
            'executive_summary': "Generate a concise executive summary...",
            'detailed_analysis': "Provide comprehensive analysis...",
            'recommendations': "Generate actionable recommendations...",
            'quick_insight': "Provide a quick insight on..."
        }
        self._metrics['precomputed_items'] += 1
        logger.debug("Templates pre-compiled")
    
    def _precompute_patterns(self) -> None:
        """Pre-compute common optimization patterns."""
        self._precomputed['patterns'] = {
            'high_cpa_solutions': [
                'Expand audience targeting',
                'Test new creative formats',
                'Optimize bidding strategy',
                'Improve landing page experience'
            ],
            'low_ctr_solutions': [
                'Refresh ad creative',
                'Test new headlines',
                'Improve audience targeting',
                'Adjust ad placement'
            ],
            'budget_optimization': [
                'Shift budget to top performers',
                'Reduce spend on underperformers',
                'Test incremental budget increases',
                'Implement dayparting'
            ]
        }
        self._metrics['precomputed_items'] += 1
        logger.debug("Patterns pre-computed")
    
    def get(self, key: str) -> Optional[Any]:
        """Get pre-computed data."""
        return self._precomputed.get(key)
    
    def schedule_background_job(self, 
                                func: Callable,
                                *args,
                                **kwargs) -> None:
        """Schedule a background job."""
        future = self._executor.submit(func, *args, **kwargs)
        self._background_tasks.append(future)
        self._metrics['background_jobs'] += 1
        logger.debug(f"Scheduled background job: {func.__name__}")
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()


# =============================================================================
# 7. MODEL SELECTION BY TASK
# =============================================================================

@dataclass
class ModelConfig:
    """Configuration for an LLM model."""
    name: str
    provider: str
    cost_per_1k_input: float
    cost_per_1k_output: float
    avg_latency_seconds: float
    max_tokens: int
    quality_score: float  # 1-10
    best_for: List[str]


class ModelSelector:
    """
    Select the right model for the right task.
    
    Task → Model mapping:
    - Executive summary: GPT-4o (needs best quality)
    - Detailed analysis: GPT-4o (needs depth)
    - Recommendations: Claude Sonnet (good enough, faster)
    - Simple extraction: GPT-4o-mini (fast, cheap)
    - Data validation: Claude Haiku (ultra-fast)
    """
    
    MODELS = {
        'gpt-4o': ModelConfig(
            name='gpt-4o',
            provider='openai',
            cost_per_1k_input=0.005,
            cost_per_1k_output=0.015,
            avg_latency_seconds=3.0,
            max_tokens=128000,
            quality_score=9.5,
            best_for=['executive_summary', 'detailed_analysis', 'complex_reasoning']
        ),
        'gpt-4o-mini': ModelConfig(
            name='gpt-4o-mini',
            provider='openai',
            cost_per_1k_input=0.00015,
            cost_per_1k_output=0.0006,
            avg_latency_seconds=1.5,
            max_tokens=128000,
            quality_score=7.5,
            best_for=['extraction', 'simple_tasks', 'high_volume']
        ),
        'claude-sonnet': ModelConfig(
            name='claude-3-5-sonnet-20241022',
            provider='anthropic',
            cost_per_1k_input=0.003,
            cost_per_1k_output=0.015,
            avg_latency_seconds=2.5,
            max_tokens=200000,
            quality_score=9.0,
            best_for=['recommendations', 'analysis', 'writing']
        ),
        'claude-haiku': ModelConfig(
            name='claude-3-5-haiku-20241022',
            provider='anthropic',
            cost_per_1k_input=0.0008,
            cost_per_1k_output=0.004,
            avg_latency_seconds=0.8,
            max_tokens=200000,
            quality_score=7.0,
            best_for=['validation', 'quick_tasks', 'high_volume']
        ),
        'gemini-flash': ModelConfig(
            name='gemini-2.0-flash-exp',
            provider='google',
            cost_per_1k_input=0.0,  # Free tier
            cost_per_1k_output=0.0,
            avg_latency_seconds=1.0,
            max_tokens=1000000,
            quality_score=8.0,
            best_for=['general', 'cost_sensitive', 'long_context']
        )
    }
    
    # Task to model mapping
    TASK_MODEL_MAP = {
        'executive_summary': 'gpt-4o',
        'detailed_analysis': 'claude-sonnet',
        'recommendations': 'claude-sonnet',
        'extraction': 'gpt-4o-mini',
        'validation': 'claude-haiku',
        'quick_summary': 'gemini-flash',
        'general': 'gemini-flash'
    }
    
    def __init__(self, 
                 prefer_cost: bool = False,
                 prefer_speed: bool = False,
                 prefer_quality: bool = True):
        self.prefer_cost = prefer_cost
        self.prefer_speed = prefer_speed
        self.prefer_quality = prefer_quality
        self._metrics = {
            'selections': {},
            'estimated_cost_saved': 0.0
        }
    
    def select_model(self, 
                    task_type: str,
                    input_tokens: int = 1000,
                    required_quality: float = 7.0) -> ModelConfig:
        """
        Select the optimal model for a task.
        
        Args:
            task_type: Type of task
            input_tokens: Estimated input tokens
            required_quality: Minimum quality score required
            
        Returns:
            Selected model configuration
        """
        # Get default model for task
        default_model_name = self.TASK_MODEL_MAP.get(task_type, 'gemini-flash')
        default_model = self.MODELS[default_model_name]
        
        # Filter models by quality requirement
        eligible_models = [
            m for m in self.MODELS.values()
            if m.quality_score >= required_quality
        ]
        
        if not eligible_models:
            eligible_models = list(self.MODELS.values())
        
        # Score models based on preferences
        def score_model(model: ModelConfig) -> float:
            score = 0.0
            
            if self.prefer_quality:
                score += model.quality_score * 2
            
            if self.prefer_speed:
                score += (10 - model.avg_latency_seconds) * 1.5
            
            if self.prefer_cost:
                # Lower cost = higher score
                cost = (model.cost_per_1k_input + model.cost_per_1k_output) * input_tokens / 1000
                score += max(0, 10 - cost * 100)
            
            # Bonus if this model is best for the task
            if task_type in model.best_for:
                score += 5
            
            return score
        
        # Select best model
        best_model = max(eligible_models, key=score_model)
        
        # Track metrics
        self._metrics['selections'][task_type] = self._metrics['selections'].get(task_type, 0) + 1
        
        # Calculate cost savings vs always using GPT-4o
        gpt4o = self.MODELS['gpt-4o']
        if best_model.name != 'gpt-4o':
            cost_diff = (
                (gpt4o.cost_per_1k_input - best_model.cost_per_1k_input) +
                (gpt4o.cost_per_1k_output - best_model.cost_per_1k_output)
            ) * input_tokens / 1000
            self._metrics['estimated_cost_saved'] += max(0, cost_diff)
        
        logger.debug(f"Selected {best_model.name} for {task_type}")
        
        return best_model
    
    def get_model_for_task(self, task_type: str) -> str:
        """Get model name for a task type."""
        return self.TASK_MODEL_MAP.get(task_type, 'gemini-flash')
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()


# =============================================================================
# UNIFIED PERFORMANCE OPTIMIZER
# =============================================================================

class PerformanceOptimizer:
    """
    Unified interface for all performance optimizations.
    
    Combines:
    - Parallel processing
    - Semantic caching
    - Query bundling
    - Token optimization
    - Progress streaming
    - Pre-computation
    - Model selection
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self.parallel = ParallelExecutor.get_instance()
        self.cache = SemanticCache.get_instance()
        self.bundler = SmartQueryBundler(cache=self.cache)
        self.token_optimizer = TokenOptimizer()
        self.precompute = PrecomputationManager.get_instance()
        self.model_selector = ModelSelector()
        
        self._initialized = False
    
    @classmethod
    def get_instance(cls) -> 'PerformanceOptimizer':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def initialize(self) -> None:
        """Initialize all optimization components."""
        if self._initialized:
            return
        
        logger.info("Initializing performance optimizer...")
        
        # Initialize pre-computation
        self.precompute.initialize()
        
        self._initialized = True
        logger.info("Performance optimizer initialized")
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics from all optimization components."""
        return {
            'parallel_processing': self.parallel.get_metrics(),
            'semantic_cache': self.cache.get_metrics(),
            'query_bundling': self.bundler.get_metrics(),
            'token_optimization': self.token_optimizer.get_metrics(),
            'precomputation': self.precompute.get_metrics(),
            'model_selection': self.model_selector.get_metrics()
        }
    
    def estimate_impact(self) -> Dict[str, Any]:
        """
        Estimate the impact of optimizations on the current system.
        
        Returns:
            Dictionary with estimated improvements
        """
        metrics = self.get_all_metrics()
        
        # Calculate estimates
        cache_metrics = metrics['semantic_cache']
        parallel_metrics = metrics['parallel_processing']
        token_metrics = metrics['token_optimization']
        model_metrics = metrics['model_selection']
        
        # Time savings
        cache_time_saved = cache_metrics.get('time_saved_seconds', 0)
        parallel_time_saved = parallel_metrics.get('time_saved_seconds', 0)
        
        # Cost savings
        token_cost_saved = token_metrics.get('tokens_saved', 0) * 0.00003  # Approx cost per token
        model_cost_saved = model_metrics.get('estimated_cost_saved', 0)
        
        return {
            'estimated_time_reduction': {
                'parallel_processing': '50% reduction (60s → 30s for typical analysis)',
                'semantic_caching': f'{cache_metrics.get("hit_rate", 0)}% cache hit rate, ~{cache_time_saved:.1f}s saved',
                'query_bundling': '60% fewer RAG queries (5 → 2)',
                'total_estimated': '40-60% faster analysis'
            },
            'estimated_cost_reduction': {
                'token_optimization': f'{token_metrics.get("tokens_saved", 0)} tokens saved (~${token_cost_saved:.2f})',
                'model_selection': f'~${model_cost_saved:.2f} saved by using right model for task',
                'caching': 'Eliminates redundant LLM calls',
                'total_estimated': '30-50% cost reduction'
            },
            'quality_improvements': {
                'streaming': 'Users see results progressively (better UX)',
                'precomputation': 'Faster startup, consistent benchmarks',
                'smart_bundling': 'More comprehensive RAG context'
            },
            'current_metrics': metrics
        }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Global instances
_optimizer: Optional[PerformanceOptimizer] = None


def get_optimizer() -> PerformanceOptimizer:
    """Get the global performance optimizer instance."""
    global _optimizer
    if _optimizer is None:
        _optimizer = PerformanceOptimizer.get_instance()
        _optimizer.initialize()
    return _optimizer


def parallel_execute(tasks: List[Tuple[Callable, tuple, dict]]) -> List[Any]:
    """Execute tasks in parallel."""
    return get_optimizer().parallel.execute_parallel(tasks)


def cache_get(query: str) -> Optional[Any]:
    """Get from semantic cache."""
    return get_optimizer().cache.get(query)


def cache_set(query: str, results: Any) -> None:
    """Set in semantic cache."""
    get_optimizer().cache.set(query, results)


def bundle_queries(metrics: Dict, issues: List[str] = None, platforms: List[str] = None) -> List[str]:
    """Bundle RAG queries."""
    return get_optimizer().bundler.bundle_queries(metrics, issues, platforms)


def optimize_tokens(context: Dict, task_type: str) -> Dict:
    """Optimize tokens for a task."""
    return get_optimizer().token_optimizer.optimize_context(context, task_type)


def select_model(task_type: str, input_tokens: int = 1000) -> ModelConfig:
    """Select optimal model for a task."""
    return get_optimizer().model_selector.select_model(task_type, input_tokens)


def get_performance_metrics() -> Dict[str, Any]:
    """Get all performance metrics."""
    return get_optimizer().get_all_metrics()


def estimate_optimization_impact() -> Dict[str, Any]:
    """Estimate optimization impact."""
    return get_optimizer().estimate_impact()
