"""
Enhanced RAG Contextualization
Improved retrieval relevance with query understanding and context enhancement
"""
import re
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class QueryIntent(str, Enum):
    """Query intent classification"""
    PERFORMANCE = "performance"
    BUDGET = "budget"
    CREATIVE = "creative"
    AUDIENCE = "audience"
    COMPARISON = "comparison"
    TROUBLESHOOT = "troubleshoot"
    FORECAST = "forecast"
    STRATEGY = "strategy"
    GENERAL = "general"


@dataclass
class EnrichedQuery:
    """Query with extracted context and metadata"""
    original_query: str
    normalized_query: str
    intent: QueryIntent
    entities: Dict[str, List[str]]
    time_context: Optional[str]
    platform_context: Optional[str]
    metric_focus: List[str]
    expansion_terms: List[str]


@dataclass
class ContextualChunk:
    """Retrieved chunk with context scoring"""
    content: str
    base_score: float
    context_score: float
    final_score: float
    relevance_factors: Dict[str, float]
    source: str


class QueryUnderstanding:
    """
    Parse and understand user queries for better RAG retrieval
    """
    
    # Intent patterns
    INTENT_PATTERNS = {
        QueryIntent.PERFORMANCE: [
            r'\b(roas|cpa|cpc|ctr|cvr|conversion|performance|metric)\b',
            r'\b(how|what).*(perform|doing|result)\b'
        ],
        QueryIntent.BUDGET: [
            r'\b(budget|spend|cost|allocat|invest)\b',
            r'\b(how much|total|daily|monthly)\b.*\b(spend|cost)\b'
        ],
        QueryIntent.CREATIVE: [
            r'\b(creative|ad|copy|image|video|visual|banner)\b',
            r'\b(fatigue|refresh|new|test)\b.*\b(ad|creative)\b'
        ],
        QueryIntent.AUDIENCE: [
            r'\b(audience|segment|target|demographic|user)\b',
            r'\b(who|which).*(people|user|customer)\b'
        ],
        QueryIntent.COMPARISON: [
            r'\b(compare|vs|versus|between|difference)\b',
            r'\b(which|what).*(better|best|worse|top)\b'
        ],
        QueryIntent.TROUBLESHOOT: [
            r'\b(why|problem|issue|drop|decline|decrease)\b',
            r'\b(not|low|poor|bad)\b.*\b(performing|working)\b'
        ],
        QueryIntent.FORECAST: [
            r'\b(predict|forecast|expect|project|future)\b',
            r'\b(will|next|coming).*(week|month|quarter)\b'
        ],
        QueryIntent.STRATEGY: [
            r'\b(strategy|recommend|suggest|should|optimize)\b',
            r'\b(how|what).*(improve|increase|boost)\b'
        ]
    }
    
    # Entity patterns
    PLATFORM_PATTERNS = {
        "meta": r'\b(meta|facebook|instagram|fb|ig)\b',
        "google": r'\b(google|adwords|search|display|youtube)\b',
        "linkedin": r'\b(linkedin|li)\b',
        "tiktok": r'\b(tiktok|tt)\b',
        "twitter": r'\b(twitter|x)\b'
    }
    
    METRIC_PATTERNS = {
        "ROAS": r'\b(roas|return on ad spend)\b',
        "CPA": r'\b(cpa|cost per acquisition|cost per action)\b',
        "CPC": r'\b(cpc|cost per click)\b',
        "CTR": r'\b(ctr|click.through.rate|clickthrough)\b',
        "CVR": r'\b(cvr|conversion.rate)\b',
        "CPM": r'\b(cpm|cost per mille|cost per thousand)\b',
        "Impressions": r'\b(impression|impr)\b',
        "Clicks": r'\b(click)\b',
        "Spend": r'\b(spend|cost|budget)\b',
        "Conversions": r'\b(conversion|purchase|lead)\b'
    }
    
    TIME_PATTERNS = {
        "today": r'\b(today|now)\b',
        "yesterday": r'\b(yesterday)\b',
        "this_week": r'\b(this week|past 7 days|last 7 days)\b',
        "last_week": r'\b(last week|previous week)\b',
        "this_month": r'\b(this month|current month)\b',
        "last_month": r'\b(last month|previous month)\b',
        "this_quarter": r'\b(this quarter|q[1-4])\b',
        "ytd": r'\b(ytd|year to date)\b'
    }
    
    def understand(self, query: str) -> EnrichedQuery:
        """Parse and understand a query"""
        normalized = self._normalize(query)
        
        return EnrichedQuery(
            original_query=query,
            normalized_query=normalized,
            intent=self._classify_intent(normalized),
            entities=self._extract_entities(normalized),
            time_context=self._extract_time_context(normalized),
            platform_context=self._extract_platform(normalized),
            metric_focus=self._extract_metrics(normalized),
            expansion_terms=self._expand_query(normalized)
        )
    
    def _normalize(self, query: str) -> str:
        """Normalize query text"""
        query = query.lower().strip()
        query = re.sub(r'[^\w\s]', ' ', query)
        query = re.sub(r'\s+', ' ', query)
        return query
    
    def _classify_intent(self, query: str) -> QueryIntent:
        """Classify query intent"""
        scores = {}
        for intent, patterns in self.INTENT_PATTERNS.items():
            score = sum(1 for p in patterns if re.search(p, query, re.I))
            scores[intent] = score
        
        if max(scores.values()) > 0:
            return max(scores, key=scores.get)
        return QueryIntent.GENERAL
    
    def _extract_entities(self, query: str) -> Dict[str, List[str]]:
        """Extract named entities"""
        entities = {
            "platforms": [],
            "metrics": [],
            "campaigns": [],
            "time_periods": []
        }
        
        for platform, pattern in self.PLATFORM_PATTERNS.items():
            if re.search(pattern, query, re.I):
                entities["platforms"].append(platform)
        
        for metric, pattern in self.METRIC_PATTERNS.items():
            if re.search(pattern, query, re.I):
                entities["metrics"].append(metric)
        
        return entities
    
    def _extract_time_context(self, query: str) -> Optional[str]:
        """Extract time context"""
        for period, pattern in self.TIME_PATTERNS.items():
            if re.search(pattern, query, re.I):
                return period
        return None
    
    def _extract_platform(self, query: str) -> Optional[str]:
        """Extract primary platform"""
        for platform, pattern in self.PLATFORM_PATTERNS.items():
            if re.search(pattern, query, re.I):
                return platform
        return None
    
    def _extract_metrics(self, query: str) -> List[str]:
        """Extract metric focus"""
        metrics = []
        for metric, pattern in self.METRIC_PATTERNS.items():
            if re.search(pattern, query, re.I):
                metrics.append(metric)
        return metrics
    
    def _expand_query(self, query: str) -> List[str]:
        """Generate query expansion terms"""
        expansions = []
        
        # Add synonyms
        synonyms = {
            "roas": ["return on ad spend", "roi", "revenue"],
            "cpa": ["cost per acquisition", "cost per conversion"],
            "cpc": ["cost per click", "click cost"],
            "ctr": ["click through rate", "clickthrough"],
            "spend": ["budget", "cost", "investment"],
            "campaign": ["ad set", "ad group", "initiative"]
        }
        
        for term, syns in synonyms.items():
            if term in query:
                expansions.extend(syns)
        
        return list(set(expansions))


class ContextualRetriever:
    """
    Enhanced retrieval with context awareness
    """
    
    def __init__(self, base_retriever=None):
        self.base_retriever = base_retriever
        self.query_understanding = QueryUnderstanding()
    
    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        min_score: float = 0.3,
        use_cache: bool = True
    ) -> List[ContextualChunk]:
        """
        Retrieve with enhanced context scoring and caching
        """
        start_time = time.time()
        
        # Check cache
        cache_key = f"rag_context:{hash(query)}:{top_k}:{min_score}"
        if use_cache and self._check_cache(cache_key):
             logger.info(f"RAG Cache Hit for query: {query[:50]}...")
             return self._get_from_cache(cache_key)

        # Understand the query
        enriched = self.query_understanding.understand(query)
        
        # Get base retrieval (mock if no retriever)
        if self.base_retriever:
            base_results = self.base_retriever.retrieve(enriched.normalized_query, top_k=top_k*2)
        else:
            base_results = self._mock_retrieve(enriched, top_k*2)
        
        # Apply contextual scoring
        contextual_results = []
        for chunk, base_score in base_results:
            context_score, factors = self._calculate_context_score(chunk, enriched)
            final_score = 0.6 * base_score + 0.4 * context_score
            
            if final_score >= min_score:
                contextual_results.append(ContextualChunk(
                    content=chunk,
                    base_score=base_score,
                    context_score=context_score,
                    final_score=final_score,
                    relevance_factors=factors,
                    source="knowledge_base"
                ))
        
        # Sort by final score and return top_k
        contextual_results.sort(key=lambda x: x.final_score, reverse=True)
        results = contextual_results[:top_k]
        
        # Cache results
        if use_cache:
            self._set_cache(cache_key, results)
            
        execution_time = (time.time() - start_time) * 1000
        logger.info(f"RAG Retrieval Contextualization took {execution_time:.2f}ms")
        
        return results

    def _check_cache(self, key: str) -> bool:
        # Simple in-memory LRU for now, can swap with Redis
        return key in self._local_cache
        
    def _get_from_cache(self, key: str) -> List[ContextualChunk]:
        return self._local_cache[key]
        
    def _set_cache(self, key: str, value: Any):
        self._local_cache[key] = value
        # Limit cache size
        if len(self._local_cache) > 1000:
            self._local_cache.pop(next(iter(self._local_cache)))

    def __init__(self, base_retriever=None):
        self.base_retriever = base_retriever
        self.query_understanding = QueryUnderstanding()
        self._local_cache = {}

    
    def _calculate_context_score(
        self,
        chunk: str,
        enriched: EnrichedQuery
    ) -> Tuple[float, Dict[str, float]]:
        """Calculate contextual relevance score"""
        factors = {}
        
        # Intent match
        intent_keywords = {
            QueryIntent.PERFORMANCE: ["performance", "metric", "kpi"],
            QueryIntent.BUDGET: ["budget", "spend", "allocation"],
            QueryIntent.CREATIVE: ["creative", "ad", "fatigue"],
            QueryIntent.STRATEGY: ["strategy", "optimize", "improve"]
        }
        
        intent_score = 0
        if enriched.intent in intent_keywords:
            intent_score = sum(1 for kw in intent_keywords[enriched.intent] if kw in chunk.lower()) / len(intent_keywords[enriched.intent])
        factors["intent_match"] = intent_score
        
        # Platform match
        platform_score = 0
        if enriched.platform_context:
            platform_score = 1.0 if enriched.platform_context in chunk.lower() else 0.0
        factors["platform_match"] = platform_score
        
        # Metric match
        metric_score = 0
        if enriched.metric_focus:
            metric_score = sum(1 for m in enriched.metric_focus if m.lower() in chunk.lower()) / len(enriched.metric_focus)
        factors["metric_match"] = metric_score
        
        # Expansion match
        expansion_score = 0
        if enriched.expansion_terms:
            expansion_score = sum(1 for t in enriched.expansion_terms if t in chunk.lower()) / len(enriched.expansion_terms)
        factors["expansion_match"] = expansion_score
        
        # Calculate weighted score
        weights = {
            "intent_match": 0.3,
            "platform_match": 0.25,
            "metric_match": 0.25,
            "expansion_match": 0.2
        }
        
        total_score = sum(factors[k] * weights[k] for k in factors)
        return total_score, factors
    
    def _mock_retrieve(self, enriched: EnrichedQuery, top_k: int) -> List[Tuple[str, float]]:
        """Mock retrieval for testing"""
        mock_chunks = [
            ("ROAS optimization strategies: Focus on high-intent audiences and increase bids on converting keywords.", 0.8),
            ("Budget allocation best practices: Allocate 70% to proven channels, 30% to testing.", 0.7),
            ("Creative fatigue indicators: Monitor frequency, CTR decline, and engagement drops.", 0.6),
            ("Meta Ads performance benchmarks: Average CTR 1.2%, CPC $0.50, CPM $12.", 0.75),
            ("Google Ads quality score: Improve with relevant keywords, ad copy, and landing pages.", 0.65)
        ]
        return mock_chunks[:top_k]


# Global instances
_query_understanding: Optional[QueryUnderstanding] = None
_contextual_retriever: Optional[ContextualRetriever] = None

def get_query_understanding() -> QueryUnderstanding:
    global _query_understanding
    if _query_understanding is None:
        _query_understanding = QueryUnderstanding()
    return _query_understanding

def get_contextual_retriever() -> ContextualRetriever:
    global _contextual_retriever
    if _contextual_retriever is None:
        _contextual_retriever = ContextualRetriever()
    return _contextual_retriever
