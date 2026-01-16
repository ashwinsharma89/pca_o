"""Vector store utilities for semantic retrieval."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import faiss  # type: ignore
import numpy as np
from loguru import logger
from openai import OpenAI

try:
    from rank_bm25 import BM25Okapi  # type: ignore

    BM25_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    BM25_AVAILABLE = False

try:
    import cohere  # type: ignore

    COHERE_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    COHERE_AVAILABLE = False


def _matches_filters(metadata: Dict[str, Any], filters: Optional[Dict[str, Any]]) -> bool:
    if not filters:
        return True
    for key, value in filters.items():
        meta_value = metadata.get(key)
        if isinstance(value, (list, tuple, set)):
            if meta_value not in value:
                return False
        else:
            if meta_value != value:
                return False
    return True


@dataclass
class VectorStoreConfig:
    """Configuration for building and querying the vector store."""

    index_path: Path = Path("data/vector_store/faiss.index")
    metadata_path: Path = Path("data/vector_store/metadata.json")
    embedding_model: str = "text-embedding-3-small"
    batch_size: int = 64


class VectorStoreBuilder:
    """Builds a FAISS index from ingested knowledge chunks."""

    def __init__(
        self,
        config: VectorStoreConfig = VectorStoreConfig(),
        client: Optional[OpenAI] = None,
    ) -> None:
        self.config = config
        self.client = client or OpenAI()
        self.config.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    def build_from_documents(self, documents: List[Dict[str, Any]]) -> None:
        """Embed chunks and create FAISS index from ingested documents."""

        texts: List[str] = []
        metadata_records: List[Dict[str, Any]] = []

        for doc in documents:
            if not doc.get("success"):
                continue
            chunks = doc.get("chunks", [])
            if not chunks:
                continue

            base_metadata = {
                "source": doc.get("source"),
                "url": doc.get("url"),
                "title": doc.get("title"),
                "category": doc.get("category"),
                "priority": doc.get("priority"),
                "description": doc.get("description"),
            }

            for chunk in chunks:
                texts.append(chunk)
                metadata_records.append({**base_metadata, "text": chunk})

        if not texts:
            raise ValueError("No valid chunks found to build the vector store.")

        embeddings = self._embed_texts(texts)
        self._persist_index(embeddings)
        self._persist_metadata(metadata_records)

    def _embed_texts(self, texts: List[str]) -> np.ndarray:
        """Create embeddings for the provided texts."""

        vectors: List[np.ndarray] = []
        for start in range(0, len(texts), self.config.batch_size):
            batch = texts[start : start + self.config.batch_size]
            response = self.client.embeddings.create(
                model=self.config.embedding_model,
                input=batch,
            )
            batch_vectors = np.array(
                [data.embedding for data in response.data], dtype="float32"
            )
            vectors.append(batch_vectors)

        embeddings = np.vstack(vectors)
        faiss.normalize_L2(embeddings)
        return embeddings

    def _persist_index(self, embeddings: np.ndarray) -> None:
        """Write FAISS index to disk."""

        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)
        index.add(embeddings)
        faiss.write_index(index, str(self.config.index_path))

    def _persist_metadata(self, metadata: List[Dict[str, Any]]) -> None:
        """Write chunk metadata for retrieval lookups."""

        self.config.metadata_path.write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )

    def load_metadata(self) -> List[Dict[str, Any]]:
        """Load metadata if a downstream job needs to inspect it."""

        if not self.config.metadata_path.exists():
            raise FileNotFoundError(
                f"Metadata file not found at {self.config.metadata_path}. Build the store first."
            )
        metadata_text = self.config.metadata_path.read_text(encoding="utf-8")
        return json.loads(metadata_text)


class VectorRetriever:
    """Retrieves context chunks from the FAISS index."""

    def __init__(
        self,
        config: VectorStoreConfig = VectorStoreConfig(),
        client: Optional[OpenAI] = None,
    ) -> None:
        self.config = config
        self.client = client or OpenAI()
        self._load_index()
        self._load_metadata()

    def _load_index(self) -> None:
        if not self.config.index_path.exists():
            raise FileNotFoundError(
                f"Vector index not found at {self.config.index_path}. Build it first."
            )
        self.index = faiss.read_index(str(self.config.index_path))

    def _load_metadata(self) -> None:
        if not self.config.metadata_path.exists():
            raise FileNotFoundError(
                f"Metadata file not found at {self.config.metadata_path}."
            )
        metadata_text = self.config.metadata_path.read_text(encoding="utf-8")
        self.metadata = json.loads(metadata_text)

    def search(
        self,
        query: str,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Return top-k relevant chunks for the query."""

        response = self.client.embeddings.create(
            model=self.config.embedding_model,
            input=[query],
        )
        query_vector = np.array([response.data[0].embedding], dtype="float32")
        faiss.normalize_L2(query_vector)

        scores, indices = self.index.search(query_vector, top_k)
        results: List[Dict[str, Any]] = []

        for score, idx in zip(scores[0], indices[0]):
            if idx == -1 or idx >= len(self.metadata):
                continue
            record = self.metadata[idx]
            if not _matches_filters(record, metadata_filters):
                continue
            results.append(
                {
                    "score": float(score),
                    "text": record.get("text", ""),
                    "metadata": {k: v for k, v in record.items() if k != "text"},
                }
            )

        return results


class KeywordRetriever:
    """Keyword-based retriever (BM25) over stored metadata chunks."""

    def __init__(self, config: VectorStoreConfig = VectorStoreConfig()) -> None:
        if not BM25_AVAILABLE:
            raise ImportError(
                "rank_bm25 is not installed. Install with `pip install rank-bm25`."
            )

        if not config.metadata_path.exists():
            raise FileNotFoundError(
                f"Metadata file not found at {config.metadata_path}. Build the store first."
            )

        metadata_text = config.metadata_path.read_text(encoding="utf-8")
        self.metadata: List[Dict[str, Any]] = json.loads(metadata_text)
        self.config = config

        # Build BM25 corpus
        self.corpus_tokens: List[List[str]] = [self._tokenize(m.get("text", "")) for m in self.metadata]
        self.bm25 = BM25Okapi(self.corpus_tokens)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [token for token in text.lower().split() if token]

    def search(
        self,
        query: str,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        tokens = self._tokenize(query)
        if not tokens:
            return []

        scores = self.bm25.get_scores(tokens)
        scored_indices = sorted(
            enumerate(scores), key=lambda x: x[1], reverse=True
        )[:top_k]

        results: List[Dict[str, Any]] = []
        for idx, score in scored_indices:
            record = self.metadata[idx]
            if not _matches_filters(record, metadata_filters):
                continue
            results.append(
                {
                    "score": float(score),
                    "text": record.get("text", ""),
                    "metadata": {k: v for k, v in record.items() if k != "text"},
                }
            )

        return results


class CohereReranker:
    """Wrapper around Cohere's rerank API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "rerank-english-v3.0",
    ) -> None:
        if not COHERE_AVAILABLE:
            raise ImportError(
                "cohere package not installed. Install with `pip install cohere`."
            )

        key = api_key or os.getenv("COHERE_API_KEY")
        if not key:
            raise ValueError("COHERE_API_KEY not set. Cannot enable reranking.")

        self.client = cohere.Client(key)
        self.model = model

    def rerank(
        self, query: str, candidates: List[Dict[str, Any]], top_k: int
    ) -> List[Dict[str, Any]]:
        if not candidates:
            return []

        documents = [candidate.get("text", "") for candidate in candidates]
        response = self.client.rerank(
            model=self.model,
            query=query,
            documents=documents,
            top_n=min(top_k, len(documents)),
        )

        ranked: List[Dict[str, Any]] = []
        for result in response:
            candidate = candidates[result.index]
            ranked.append(
                {
                    **candidate,
                    "score": float(result.relevance_score),
                    "reranked": True,
                }
            )

        return ranked


class HybridRetriever:
    """Combines vector, keyword, and Cohere reranking for advanced RAG."""

    def __init__(
        self,
        config: VectorStoreConfig = VectorStoreConfig(),
        use_keyword: bool = True,
        use_rerank: bool = True,
        vector_weight: float = 0.7,
        keyword_weight: float = 0.3,
        rrf_k: float = 60.0,
    ) -> None:
        self.config = config
        self.vector_weight = vector_weight
        self.keyword_weight = keyword_weight
        self.rrf_k = rrf_k

        self.vector_retriever: Optional[VectorRetriever] = None
        self.keyword_retriever: Optional[KeywordRetriever] = None
        self.reranker: Optional[CohereReranker] = None

        try:
            self.vector_retriever = VectorRetriever(config=config)
        except Exception as exc:
            logger.error(f"Vector retriever unavailable: {exc}")

        if use_keyword:
            try:
                self.keyword_retriever = KeywordRetriever(config=config)
            except Exception as exc:
                logger.error(f"Keyword retriever unavailable: {exc}")

        if use_rerank:
            try:
                self.reranker = CohereReranker()
            except Exception as exc:
                logger.error(f"Cohere reranker unavailable: {exc}")

    def search(
        self,
        query: str,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        candidates: Dict[Tuple[str, str], Dict[str, Any]] = {}
        vector_results: List[Dict[str, Any]] = []
        keyword_results: List[Dict[str, Any]] = []

        def add_candidates(results: List[Dict[str, Any]], weight: float) -> None:
            if not results:
                return
            for rank, result in enumerate(results, start=1):
                key = (
                    result.get("text", ""),
                    result.get("metadata", {}).get("url", "unknown"),
                )
                rrf_score = weight * (1.0 / (self.rrf_k + rank))
                if key not in candidates:
                    candidates[key] = {
                        "text": result.get("text", ""),
                        "metadata": result.get("metadata", {}),
                        "score": 0.0,
                    }
                candidates[key]["score"] += rrf_score

        if self.vector_retriever:
            vector_results = self.vector_retriever.search(
                query, top_k=top_k * 2, metadata_filters=metadata_filters
            )
            add_candidates(vector_results, self.vector_weight)

        if self.keyword_retriever:
            keyword_results = self.keyword_retriever.search(
                query, top_k=top_k * 2, metadata_filters=metadata_filters
            )
            add_candidates(keyword_results, self.keyword_weight)

        merged = list(candidates.values())
        if not merged:
            return []

        merged.sort(key=lambda x: x["score"], reverse=True)

        final_results = merged

        if self.reranker:
            try:
                final_results = self.reranker.rerank(query, merged, top_k=top_k)
            except Exception as exc:
                logger.error(f"Cohere reranker failed: {exc}")
                final_results = merged[:top_k]
        else:
            final_results = merged[:top_k]

        self._log_metrics(
            query=query,
            vector_candidates=len(vector_results),
            keyword_candidates=len(keyword_results),
            final_count=len(final_results),
            used_rerank=bool(self.reranker),
            metadata_filters=metadata_filters,
        )

        return final_results

    def _log_metrics(
        self,
        query: str,
        vector_candidates: int,
        keyword_candidates: int,
        final_count: int,
        used_rerank: bool,
        metadata_filters: Optional[Dict[str, Any]],
    ) -> None:
        try:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "query": query,
                "vector_candidates": vector_candidates,
                "keyword_candidates": keyword_candidates,
                "final_results": final_count,
                "used_rerank": used_rerank,
                "filters": metadata_filters or {},
            }
            log_dir = self.config.metadata_path.parent
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / "retrieval_metrics.jsonl"
            with log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.error(f"Failed to log retrieval metrics: {exc}")
