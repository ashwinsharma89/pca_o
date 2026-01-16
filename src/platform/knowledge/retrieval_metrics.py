"""
RAG Retrieval Quality Metrics

Implements standard IR metrics for measuring retrieval quality:
- Precision@K
- Recall@K
- Mean Reciprocal Rank (MRR)
- Normalized Discounted Cumulative Gain (NDCG)
- Mean Average Precision (MAP)
"""

import json
from typing import List, Dict, Any, Set, Tuple
from pathlib import Path
import numpy as np
from dataclasses import dataclass, asdict


@dataclass
class RetrievalMetrics:
    """Container for retrieval quality metrics."""
    
    precision_at_1: float
    precision_at_3: float
    precision_at_5: float
    precision_at_10: float
    recall_at_1: float
    recall_at_3: float
    recall_at_5: float
    recall_at_10: float
    mrr: float  # Mean Reciprocal Rank
    ndcg_at_5: float  # Normalized Discounted Cumulative Gain
    ndcg_at_10: float
    map_score: float  # Mean Average Precision
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return asdict(self)
    
    def __str__(self) -> str:
        """Pretty print metrics."""
        return (
            f"Precision@1: {self.precision_at_1:.3f}\n"
            f"Precision@3: {self.precision_at_3:.3f}\n"
            f"Precision@5: {self.precision_at_5:.3f}\n"
            f"Precision@10: {self.precision_at_10:.3f}\n"
            f"Recall@1: {self.recall_at_1:.3f}\n"
            f"Recall@3: {self.recall_at_3:.3f}\n"
            f"Recall@5: {self.recall_at_5:.3f}\n"
            f"Recall@10: {self.recall_at_10:.3f}\n"
            f"MRR: {self.mrr:.3f}\n"
            f"NDCG@5: {self.ndcg_at_5:.3f}\n"
            f"NDCG@10: {self.ndcg_at_10:.3f}\n"
            f"MAP: {self.map_score:.3f}"
        )


class RetrievalQualityEvaluator:
    """
    Evaluates retrieval quality using standard IR metrics.
    
    Requires ground truth test set with queries and relevant documents.
    """
    
    def __init__(self):
        """Initialize evaluator."""
        pass
    
    def precision_at_k(
        self,
        retrieved: List[str],
        relevant: Set[str],
        k: int
    ) -> float:
        """
        Calculate Precision@K.
        
        Precision@K = (# relevant docs in top-K) / K
        
        Args:
            retrieved: List of retrieved document IDs (ordered by relevance)
            relevant: Set of relevant document IDs (ground truth)
            k: Number of top results to consider
            
        Returns:
            Precision@K score (0.0 to 1.0)
        """
        if k == 0 or not retrieved:
            return 0.0
        
        top_k = retrieved[:k]
        relevant_in_top_k = sum(1 for doc_id in top_k if doc_id in relevant)
        
        return relevant_in_top_k / k
    
    def recall_at_k(
        self,
        retrieved: List[str],
        relevant: Set[str],
        k: int
    ) -> float:
        """
        Calculate Recall@K.
        
        Recall@K = (# relevant docs in top-K) / (total # relevant docs)
        
        Args:
            retrieved: List of retrieved document IDs
            relevant: Set of relevant document IDs
            k: Number of top results to consider
            
        Returns:
            Recall@K score (0.0 to 1.0)
        """
        if not relevant:
            return 0.0
        
        top_k = retrieved[:k]
        relevant_in_top_k = sum(1 for doc_id in top_k if doc_id in relevant)
        
        return relevant_in_top_k / len(relevant)
    
    def mean_reciprocal_rank(
        self,
        retrieved: List[str],
        relevant: Set[str]
    ) -> float:
        """
        Calculate Mean Reciprocal Rank (MRR).
        
        MRR = 1 / (rank of first relevant document)
        
        Args:
            retrieved: List of retrieved document IDs
            relevant: Set of relevant document IDs
            
        Returns:
            MRR score (0.0 to 1.0)
        """
        for rank, doc_id in enumerate(retrieved, start=1):
            if doc_id in relevant:
                return 1.0 / rank
        
        return 0.0
    
    def dcg_at_k(
        self,
        retrieved: List[str],
        relevant: Set[str],
        k: int
    ) -> float:
        """
        Calculate Discounted Cumulative Gain at K.
        
        DCG@K = Σ (relevance / log2(rank + 1))
        
        Args:
            retrieved: List of retrieved document IDs
            relevant: Set of relevant document IDs
            k: Number of top results to consider
            
        Returns:
            DCG@K score
        """
        dcg = 0.0
        for rank, doc_id in enumerate(retrieved[:k], start=1):
            if doc_id in relevant:
                # Binary relevance: 1 if relevant, 0 otherwise
                dcg += 1.0 / np.log2(rank + 1)
        
        return dcg
    
    def ndcg_at_k(
        self,
        retrieved: List[str],
        relevant: Set[str],
        k: int
    ) -> float:
        """
        Calculate Normalized Discounted Cumulative Gain at K.
        
        NDCG@K = DCG@K / IDCG@K
        where IDCG is the ideal DCG (all relevant docs at top)
        
        Args:
            retrieved: List of retrieved document IDs
            relevant: Set of relevant document IDs
            k: Number of top results to consider
            
        Returns:
            NDCG@K score (0.0 to 1.0)
        """
        dcg = self.dcg_at_k(retrieved, relevant, k)
        
        # Ideal DCG: all relevant docs at the top
        ideal_retrieved = list(relevant) + [doc for doc in retrieved if doc not in relevant]
        idcg = self.dcg_at_k(ideal_retrieved, relevant, k)
        
        if idcg == 0:
            return 0.0
        
        return dcg / idcg
    
    def average_precision(
        self,
        retrieved: List[str],
        relevant: Set[str]
    ) -> float:
        """
        Calculate Average Precision (AP).
        
        AP = (Σ (Precision@k × relevance_k)) / (# relevant docs)
        
        Args:
            retrieved: List of retrieved document IDs
            relevant: Set of relevant document IDs
            
        Returns:
            AP score (0.0 to 1.0)
        """
        if not relevant:
            return 0.0
        
        relevant_count = 0
        precision_sum = 0.0
        
        for k, doc_id in enumerate(retrieved, start=1):
            if doc_id in relevant:
                relevant_count += 1
                precision_at_k = relevant_count / k
                precision_sum += precision_at_k
        
        return precision_sum / len(relevant)
    
    def evaluate_query(
        self,
        retrieved: List[str],
        relevant: Set[str]
    ) -> RetrievalMetrics:
        """
        Evaluate a single query with all metrics.
        
        Args:
            retrieved: List of retrieved document IDs (ordered by relevance)
            relevant: Set of relevant document IDs (ground truth)
            
        Returns:
            RetrievalMetrics with all computed scores
        """
        return RetrievalMetrics(
            precision_at_1=self.precision_at_k(retrieved, relevant, 1),
            precision_at_3=self.precision_at_k(retrieved, relevant, 3),
            precision_at_5=self.precision_at_k(retrieved, relevant, 5),
            precision_at_10=self.precision_at_k(retrieved, relevant, 10),
            recall_at_1=self.recall_at_k(retrieved, relevant, 1),
            recall_at_3=self.recall_at_k(retrieved, relevant, 3),
            recall_at_5=self.recall_at_k(retrieved, relevant, 5),
            recall_at_10=self.recall_at_k(retrieved, relevant, 10),
            mrr=self.mean_reciprocal_rank(retrieved, relevant),
            ndcg_at_5=self.ndcg_at_k(retrieved, relevant, 5),
            ndcg_at_10=self.ndcg_at_k(retrieved, relevant, 10),
            map_score=self.average_precision(retrieved, relevant)
        )
    
    def evaluate_test_set(
        self,
        test_queries: List[Dict[str, Any]]
    ) -> Tuple[RetrievalMetrics, List[Dict[str, Any]]]:
        """
        Evaluate entire test set and return average metrics.
        
        Args:
            test_queries: List of test queries, each with:
                - query: str
                - retrieved: List[str] (document IDs)
                - relevant: List[str] (ground truth document IDs)
                
        Returns:
            Tuple of (average metrics, per-query results)
        """
        per_query_metrics = []
        
        for test_case in test_queries:
            query = test_case['query']
            retrieved = test_case['retrieved']
            relevant = set(test_case['relevant'])
            
            metrics = self.evaluate_query(retrieved, relevant)
            
            per_query_metrics.append({
                'query': query,
                'metrics': metrics.to_dict(),
                'num_relevant': len(relevant),
                'num_retrieved': len(retrieved)
            })
        
        # Calculate average metrics
        avg_metrics = RetrievalMetrics(
            precision_at_1=np.mean([m['metrics']['precision_at_1'] for m in per_query_metrics]),
            precision_at_3=np.mean([m['metrics']['precision_at_3'] for m in per_query_metrics]),
            precision_at_5=np.mean([m['metrics']['precision_at_5'] for m in per_query_metrics]),
            precision_at_10=np.mean([m['metrics']['precision_at_10'] for m in per_query_metrics]),
            recall_at_1=np.mean([m['metrics']['recall_at_1'] for m in per_query_metrics]),
            recall_at_3=np.mean([m['metrics']['recall_at_3'] for m in per_query_metrics]),
            recall_at_5=np.mean([m['metrics']['recall_at_5'] for m in per_query_metrics]),
            recall_at_10=np.mean([m['metrics']['recall_at_10'] for m in per_query_metrics]),
            mrr=np.mean([m['metrics']['mrr'] for m in per_query_metrics]),
            ndcg_at_5=np.mean([m['metrics']['ndcg_at_5'] for m in per_query_metrics]),
            ndcg_at_10=np.mean([m['metrics']['ndcg_at_10'] for m in per_query_metrics]),
            map_score=np.mean([m['metrics']['map_score'] for m in per_query_metrics])
        )
        
        return avg_metrics, per_query_metrics
    
    def save_evaluation_report(
        self,
        avg_metrics: RetrievalMetrics,
        per_query_metrics: List[Dict[str, Any]],
        output_path: Path
    ) -> None:
        """
        Save evaluation report to JSON file.
        
        Args:
            avg_metrics: Average metrics across all queries
            per_query_metrics: Per-query detailed results
            output_path: Path to save report
        """
        report = {
            'summary': {
                'total_queries': len(per_query_metrics),
                'average_metrics': avg_metrics.to_dict()
            },
            'per_query_results': per_query_metrics
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
