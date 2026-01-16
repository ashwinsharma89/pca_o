"""
Chunk Size Optimization for RAG System.

Provides content-aware chunking strategies with performance optimization.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import tiktoken

from loguru import logger


class ChunkStrategy(Enum):
    """Chunking strategies."""
    
    FIXED_SIZE = "fixed_size"
    SENTENCE_BOUNDARY = "sentence_boundary"
    SEMANTIC = "semantic"
    HIERARCHICAL = "hierarchical"
    ADAPTIVE = "adaptive"


@dataclass
class ChunkConfig:
    """Configuration for chunking."""
    
    content_type: str
    chunk_size: int
    overlap: int
    strategy: ChunkStrategy
    preserve_sentences: bool = True
    min_chunk_size: int = 100
    max_chunk_size: int = 2000


class ChunkOptimizer:
    """
    Optimizes chunk sizes for different content types.
    
    Features:
    - Content-aware chunking
    - Multiple strategies
    - Sentence boundary preservation
    - Overlap optimization
    - Quality scoring
    """
    
    # Optimal configurations by content type
    OPTIMAL_CONFIGS = {
        "technical_docs": ChunkConfig(
            content_type="technical_docs",
            chunk_size=512,
            overlap=50,
            strategy=ChunkStrategy.SENTENCE_BOUNDARY,
            preserve_sentences=True
        ),
        "best_practices": ChunkConfig(
            content_type="best_practices",
            chunk_size=256,
            overlap=25,
            strategy=ChunkStrategy.FIXED_SIZE,
            preserve_sentences=True
        ),
        "case_studies": ChunkConfig(
            content_type="case_studies",
            chunk_size=1024,
            overlap=100,
            strategy=ChunkStrategy.SEMANTIC,
            preserve_sentences=True
        ),
        "benchmarks": ChunkConfig(
            content_type="benchmarks",
            chunk_size=128,
            overlap=0,
            strategy=ChunkStrategy.FIXED_SIZE,
            preserve_sentences=False
        ),
        "code_examples": ChunkConfig(
            content_type="code_examples",
            chunk_size=256,
            overlap=25,
            strategy=ChunkStrategy.FIXED_SIZE,
            preserve_sentences=False
        ),
        "api_docs": ChunkConfig(
            content_type="api_docs",
            chunk_size=384,
            overlap=40,
            strategy=ChunkStrategy.SENTENCE_BOUNDARY,
            preserve_sentences=True
        )
    }
    
    def __init__(self, encoding_name: str = "cl100k_base"):
        """
        Initialize chunk optimizer.
        
        Args:
            encoding_name: Tiktoken encoding name
        """
        try:
            self.encoding = tiktoken.get_encoding(encoding_name)
        except Exception:
            logger.warning(f"Failed to load encoding {encoding_name}, using fallback")
            self.encoding = None
        
        logger.info("Initialized ChunkOptimizer")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in text."""
        if self.encoding:
            return len(self.encoding.encode(text))
        else:
            # Fallback: rough estimate
            return len(text.split()) * 1.3
    
    def chunk_document(
        self,
        text: str,
        content_type: str = "technical_docs",
        config: Optional[ChunkConfig] = None
    ) -> List[str]:
        """
        Chunk document using optimal strategy for content type.
        
        Args:
            text: Document text
            content_type: Type of content
            config: Optional custom configuration
        
        Returns:
            List of text chunks
        """
        if config is None:
            config = self.OPTIMAL_CONFIGS.get(
                content_type,
                self.OPTIMAL_CONFIGS["technical_docs"]
            )
        
        logger.info(f"Chunking document ({content_type}) with {config.strategy.value} strategy")
        
        if config.strategy == ChunkStrategy.FIXED_SIZE:
            return self._fixed_size_chunking(text, config)
        elif config.strategy == ChunkStrategy.SENTENCE_BOUNDARY:
            return self._sentence_boundary_chunking(text, config)
        elif config.strategy == ChunkStrategy.SEMANTIC:
            return self._semantic_chunking(text, config)
        elif config.strategy == ChunkStrategy.HIERARCHICAL:
            return self._hierarchical_chunking(text, config)
        elif config.strategy == ChunkStrategy.ADAPTIVE:
            return self._adaptive_chunking(text, config)
        else:
            return self._fixed_size_chunking(text, config)
    
    def _fixed_size_chunking(
        self,
        text: str,
        config: ChunkConfig
    ) -> List[str]:
        """Fixed-size chunking with overlap."""
        chunks = []
        
        if self.encoding:
            tokens = self.encoding.encode(text)
            
            for i in range(0, len(tokens), config.chunk_size - config.overlap):
                chunk_tokens = tokens[i:i + config.chunk_size]
                chunk_text = self.encoding.decode(chunk_tokens)
                
                if len(chunk_tokens) >= config.min_chunk_size:
                    chunks.append(chunk_text)
        else:
            # Fallback: word-based chunking
            words = text.split()
            word_chunk_size = int(config.chunk_size / 1.3)
            word_overlap = int(config.overlap / 1.3)
            
            for i in range(0, len(words), word_chunk_size - word_overlap):
                chunk_words = words[i:i + word_chunk_size]
                chunk_text = " ".join(chunk_words)
                
                if len(chunk_words) >= config.min_chunk_size / 1.3:
                    chunks.append(chunk_text)
        
        logger.debug(f"Created {len(chunks)} fixed-size chunks")
        return chunks
    
    def _sentence_boundary_chunking(
        self,
        text: str,
        config: ChunkConfig
    ) -> List[str]:
        """Chunking that respects sentence boundaries."""
        # Split into sentences
        sentences = self._split_sentences(text)
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        
        for sentence in sentences:
            sentence_tokens = self.count_tokens(sentence)
            
            # If adding this sentence exceeds chunk size
            if current_tokens + sentence_tokens > config.chunk_size and current_chunk:
                # Save current chunk
                chunks.append(" ".join(current_chunk))
                
                # Start new chunk with overlap
                if config.overlap > 0:
                    # Keep last few sentences for overlap
                    overlap_text = " ".join(current_chunk)
                    overlap_tokens = self.count_tokens(overlap_text)
                    
                    while overlap_tokens > config.overlap and len(current_chunk) > 1:
                        current_chunk.pop(0)
                        overlap_text = " ".join(current_chunk)
                        overlap_tokens = self.count_tokens(overlap_text)
                else:
                    current_chunk = []
                
                current_tokens = self.count_tokens(" ".join(current_chunk))
            
            current_chunk.append(sentence)
            current_tokens += sentence_tokens
        
        # Add final chunk
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        logger.debug(f"Created {len(chunks)} sentence-boundary chunks")
        return chunks
    
    def _semantic_chunking(
        self,
        text: str,
        config: ChunkConfig
    ) -> List[str]:
        """
        Semantic chunking (groups related content).
        
        Note: This is a simplified version. For production,
        consider using sentence transformers for similarity.
        """
        # For now, use sentence boundary with paragraph detection
        paragraphs = text.split("\n\n")
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            para_tokens = self.count_tokens(para)
            
            if current_tokens + para_tokens > config.chunk_size and current_chunk:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = []
                current_tokens = 0
            
            current_chunk.append(para)
            current_tokens += para_tokens
        
        if current_chunk:
            chunks.append("\n\n".join(current_chunk))
        
        logger.debug(f"Created {len(chunks)} semantic chunks")
        return chunks
    
    def _hierarchical_chunking(
        self,
        text: str,
        config: ChunkConfig
    ) -> List[str]:
        """Hierarchical chunking (sections, paragraphs, sentences)."""
        # Detect sections (headers)
        sections = self._split_sections(text)
        
        chunks = []
        
        for section in sections:
            section_tokens = self.count_tokens(section)
            
            if section_tokens <= config.chunk_size:
                chunks.append(section)
            else:
                # Split large sections into paragraphs
                para_chunks = self._semantic_chunking(section, config)
                chunks.extend(para_chunks)
        
        logger.debug(f"Created {len(chunks)} hierarchical chunks")
        return chunks
    
    def _adaptive_chunking(
        self,
        text: str,
        config: ChunkConfig
    ) -> List[str]:
        """
        Adaptive chunking that chooses best strategy based on content.
        """
        # Analyze content
        has_code = bool(re.search(r'```|def |class |function ', text))
        has_headers = bool(re.search(r'^#+\s', text, re.MULTILINE))
        has_lists = bool(re.search(r'^\s*[-*]\s', text, re.MULTILINE))
        
        # Choose strategy
        if has_code:
            return self._fixed_size_chunking(text, config)
        elif has_headers:
            return self._hierarchical_chunking(text, config)
        elif has_lists:
            return self._semantic_chunking(text, config)
        else:
            return self._sentence_boundary_chunking(text, config)
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences."""
        # Simple sentence splitter
        sentences = re.split(r'(?<=[.!?])\s+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _split_sections(self, text: str) -> List[str]:
        """Split text into sections based on headers."""
        # Split on markdown headers
        sections = re.split(r'\n#{1,6}\s+', text)
        return [s.strip() for s in sections if s.strip()]
    
    def calculate_quality_score(
        self,
        chunks: List[str],
        config: ChunkConfig
    ) -> float:
        """
        Calculate quality score for chunks.
        
        Metrics:
        - Size consistency
        - Overlap effectiveness
        - Sentence preservation
        
        Returns:
            Quality score (0-1)
        """
        if not chunks:
            return 0.0
        
        scores = []
        
        # 1. Size consistency (chunks close to target size)
        target_size = config.chunk_size
        size_scores = []
        for chunk in chunks:
            chunk_size = self.count_tokens(chunk)
            size_diff = abs(chunk_size - target_size) / target_size
            size_score = max(0, 1 - size_diff)
            size_scores.append(size_score)
        
        scores.append(sum(size_scores) / len(size_scores))
        
        # 2. Sentence preservation (if enabled)
        if config.preserve_sentences:
            sentence_scores = []
            for chunk in chunks:
                # Check if chunk ends with sentence boundary
                ends_with_period = chunk.rstrip().endswith(('.', '!', '?'))
                sentence_scores.append(1.0 if ends_with_period else 0.5)
            
            scores.append(sum(sentence_scores) / len(sentence_scores))
        
        # 3. Overlap effectiveness
        if config.overlap > 0 and len(chunks) > 1:
            overlap_scores = []
            for i in range(len(chunks) - 1):
                # Check for actual overlap
                chunk1_end = chunks[i][-config.overlap:]
                chunk2_start = chunks[i + 1][:config.overlap]
                
                # Simple overlap check
                has_overlap = len(set(chunk1_end.split()) & set(chunk2_start.split())) > 0
                overlap_scores.append(1.0 if has_overlap else 0.0)
            
            if overlap_scores:
                scores.append(sum(overlap_scores) / len(overlap_scores))
        
        # Average all scores
        return sum(scores) / len(scores) if scores else 0.0
    
    def benchmark_strategies(
        self,
        text: str,
        content_type: str = "technical_docs"
    ) -> Dict[str, Any]:
        """
        Benchmark different chunking strategies.
        
        Args:
            text: Sample text
            content_type: Content type
        
        Returns:
            Benchmark results
        """
        results = {}
        
        for strategy in ChunkStrategy:
            config = ChunkConfig(
                content_type=content_type,
                chunk_size=512,
                overlap=50,
                strategy=strategy
            )
            
            try:
                chunks = self.chunk_document(text, content_type, config)
                quality = self.calculate_quality_score(chunks, config)
                
                results[strategy.value] = {
                    "chunk_count": len(chunks),
                    "avg_chunk_size": sum(self.count_tokens(c) for c in chunks) / len(chunks) if chunks else 0,
                    "quality_score": quality,
                    "strategy": strategy.value
                }
            except Exception as e:
                logger.error(f"Failed to benchmark {strategy.value}: {e}")
                results[strategy.value] = {"error": str(e)}
        
        # Find best strategy
        best_strategy = max(
            results.items(),
            key=lambda x: x[1].get("quality_score", 0)
        )[0]
        
        return {
            "results": results,
            "best_strategy": best_strategy,
            "text_length": len(text),
            "token_count": self.count_tokens(text)
        }


# Global instance
_optimizer: Optional[ChunkOptimizer] = None


def get_chunk_optimizer() -> ChunkOptimizer:
    """Get global chunk optimizer instance."""
    global _optimizer
    if _optimizer is None:
        _optimizer = ChunkOptimizer()
    return _optimizer
