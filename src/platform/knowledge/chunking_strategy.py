"""
Advanced Chunking Strategy with Overlap.

Implements intelligent text chunking with configurable overlap for better context preservation.
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import re

logger = logging.getLogger(__name__)


@dataclass
class ChunkingConfig:
    """
    Configuration for text chunking strategy.
    
    Attributes:
        chunk_size: Target size of each chunk in characters
        chunk_overlap: Number of characters to overlap between chunks
        min_chunk_size: Minimum chunk size (discard smaller chunks)
        max_chunk_size: Maximum chunk size (split larger chunks)
        respect_sentence_boundaries: Try to split at sentence boundaries
        respect_paragraph_boundaries: Try to split at paragraph boundaries
    """
    chunk_size: int = 1000
    chunk_overlap: int = 200
    min_chunk_size: int = 100
    max_chunk_size: int = 2000
    respect_sentence_boundaries: bool = True
    respect_paragraph_boundaries: bool = True


class OverlapChunker:
    """
    Intelligent text chunker with overlap strategy.
    
    Why Overlap Matters:
    - Preserves context across chunk boundaries
    - Prevents information loss at split points
    - Improves retrieval accuracy for queries spanning chunks
    - Maintains semantic coherence
    
    Overlap Strategy:
    1. Split text into chunks of target size
    2. Add overlap from previous chunk
    3. Respect natural boundaries (sentences, paragraphs)
    4. Ensure minimum and maximum chunk sizes
    
    Example:
        Text: "The campaign performed well. CTR was 5%. Budget was $10k."
        
        Without overlap:
        - Chunk 1: "The campaign performed well."
        - Chunk 2: "CTR was 5%."
        - Chunk 3: "Budget was $10k."
        
        With 20% overlap:
        - Chunk 1: "The campaign performed well. CTR was 5%."
        - Chunk 2: "CTR was 5%. Budget was $10k."
        
        Benefits: Query "What was the CTR?" matches both chunks!
    """
    
    def __init__(self, config: ChunkingConfig = ChunkingConfig()):
        """
        Initialize chunker with configuration.
        
        Args:
            config: Chunking configuration
        """
        self.config = config
        
        # Validate configuration
        if self.config.chunk_overlap >= self.config.chunk_size:
            raise ValueError(
                f"chunk_overlap ({self.config.chunk_overlap}) must be less than "
                f"chunk_size ({self.config.chunk_size})"
            )
        
        if self.config.min_chunk_size > self.config.chunk_size:
            raise ValueError(
                f"min_chunk_size ({self.config.min_chunk_size}) must be less than "
                f"chunk_size ({self.config.chunk_size})"
            )
        
        logger.info(f"Initialized OverlapChunker:")
        logger.info(f"  Chunk size: {self.config.chunk_size}")
        logger.info(f"  Overlap: {self.config.chunk_overlap} ({self._overlap_percentage():.1f}%)")
        logger.info(f"  Sentence boundaries: {self.config.respect_sentence_boundaries}")
        logger.info(f"  Paragraph boundaries: {self.config.respect_paragraph_boundaries}")
    
    def _overlap_percentage(self) -> float:
        """Calculate overlap as percentage of chunk size."""
        return (self.config.chunk_overlap / self.config.chunk_size) * 100
    
    def chunk_text(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Chunk text with overlap strategy.
        
        Args:
            text: Text to chunk
            metadata: Optional metadata for logging
            
        Returns:
            List of text chunks
        """
        if not text or not text.strip():
            return []
        
        # Normalize whitespace
        text = self._normalize_text(text)
        
        # If text is smaller than chunk size, return as single chunk
        if len(text) <= self.config.chunk_size:
            if len(text) >= self.config.min_chunk_size:
                return [text]
            else:
                return []
        
        chunks = []
        
        # Split by paragraphs first if enabled
        if self.config.respect_paragraph_boundaries:
            chunks = self._chunk_by_paragraphs(text)
        else:
            chunks = self._chunk_by_size(text)
        
        # Filter by size
        chunks = [c for c in chunks if len(c) >= self.config.min_chunk_size]
        
        # Log chunking stats
        if metadata:
            source = metadata.get('source', 'unknown')
            logger.debug(f"Chunked {source}: {len(text)} chars → {len(chunks)} chunks")
        
        return chunks
    
    def _normalize_text(self, text: str) -> str:
        """Normalize whitespace in text."""
        # Replace multiple spaces with single space
        text = re.sub(r' +', ' ', text)
        # Replace multiple newlines with double newline
        text = re.sub(r'\n\n+', '\n\n', text)
        return text.strip()
    
    def _chunk_by_paragraphs(self, text: str) -> List[str]:
        """
        Chunk text respecting paragraph boundaries.
        
        Strategy:
        1. Split by paragraphs
        2. Combine paragraphs until chunk size reached
        3. Add overlap from previous chunk
        """
        paragraphs = text.split('\n\n')
        chunks = []
        current_chunk = ""
        overlap_text = ""
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # If adding this paragraph exceeds chunk size
            if len(current_chunk) + len(para) > self.config.chunk_size:
                if current_chunk:
                    # Save current chunk
                    chunks.append(current_chunk.strip())
                    
                    # Calculate overlap for next chunk
                    overlap_text = self._get_overlap_text(current_chunk)
                    
                    # Start new chunk with overlap
                    current_chunk = overlap_text + " " + para if overlap_text else para
                else:
                    # Paragraph itself is too large, split it
                    para_chunks = self._chunk_by_sentences(para)
                    chunks.extend(para_chunks)
            else:
                # Add paragraph to current chunk
                if current_chunk:
                    current_chunk += "\n\n" + para
                else:
                    current_chunk = para
        
        # Add final chunk
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _chunk_by_sentences(self, text: str) -> List[str]:
        """
        Chunk text respecting sentence boundaries.
        
        Strategy:
        1. Split by sentences
        2. Combine sentences until chunk size reached
        3. Add overlap from previous chunk
        """
        # Simple sentence splitting (can be improved with NLTK)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        chunks = []
        current_chunk = ""
        overlap_text = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # If adding this sentence exceeds chunk size
            if len(current_chunk) + len(sentence) > self.config.chunk_size:
                if current_chunk:
                    # Save current chunk
                    chunks.append(current_chunk.strip())
                    
                    # Calculate overlap for next chunk
                    overlap_text = self._get_overlap_text(current_chunk)
                    
                    # Start new chunk with overlap
                    current_chunk = overlap_text + " " + sentence if overlap_text else sentence
                else:
                    # Sentence itself is too large, split by size
                    if len(sentence) > self.config.max_chunk_size:
                        size_chunks = self._chunk_by_size(sentence)
                        chunks.extend(size_chunks)
                    else:
                        chunks.append(sentence)
            else:
                # Add sentence to current chunk
                if current_chunk:
                    current_chunk += " " + sentence
                else:
                    current_chunk = sentence
        
        # Add final chunk
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _chunk_by_size(self, text: str) -> List[str]:
        """
        Chunk text by size with overlap (no boundary respect).
        
        Strategy:
        1. Split text into fixed-size chunks
        2. Add overlap from previous chunk
        """
        chunks = []
        start = 0
        
        while start < len(text):
            # Calculate end position
            end = start + self.config.chunk_size
            
            # Extract chunk
            chunk = text[start:end].strip()
            
            if chunk:
                chunks.append(chunk)
            
            # Move start position (accounting for overlap)
            start = end - self.config.chunk_overlap
        
        return chunks
    
    def _get_overlap_text(self, text: str) -> str:
        """
        Extract overlap text from end of chunk.
        
        Strategy:
        1. Take last N characters (overlap size)
        2. Try to start at sentence boundary
        3. Try to start at word boundary
        """
        if len(text) <= self.config.chunk_overlap:
            return text
        
        # Get last N characters
        overlap = text[-self.config.chunk_overlap:]
        
        if self.config.respect_sentence_boundaries:
            # Try to start at sentence boundary
            sentence_match = re.search(r'[.!?]\s+', overlap)
            if sentence_match:
                overlap = overlap[sentence_match.end():]
        
        # Try to start at word boundary
        space_idx = overlap.find(' ')
        if space_idx > 0:
            overlap = overlap[space_idx + 1:]
        
        return overlap.strip()
    
    def get_chunking_stats(self, chunks: List[str]) -> Dict[str, Any]:
        """
        Get statistics about chunked text.
        
        Args:
            chunks: List of text chunks
            
        Returns:
            Dictionary with chunking statistics
        """
        if not chunks:
            return {
                'total_chunks': 0,
                'total_characters': 0,
                'avg_chunk_size': 0,
                'min_chunk_size': 0,
                'max_chunk_size': 0,
                'overlap_percentage': self._overlap_percentage()
            }
        
        chunk_sizes = [len(c) for c in chunks]
        
        return {
            'total_chunks': len(chunks),
            'total_characters': sum(chunk_sizes),
            'avg_chunk_size': sum(chunk_sizes) / len(chunks),
            'min_chunk_size': min(chunk_sizes),
            'max_chunk_size': max(chunk_sizes),
            'overlap_percentage': self._overlap_percentage(),
            'config': {
                'chunk_size': self.config.chunk_size,
                'chunk_overlap': self.config.chunk_overlap,
                'respect_sentences': self.config.respect_sentence_boundaries,
                'respect_paragraphs': self.config.respect_paragraph_boundaries
            }
        }


def chunk_documents(
    documents: List[Dict[str, Any]],
    config: ChunkingConfig = ChunkingConfig()
) -> List[Dict[str, Any]]:
    """
    Chunk multiple documents with overlap strategy.
    
    Args:
        documents: List of documents with 'text' and 'metadata'
        config: Chunking configuration
        
    Returns:
        List of documents with 'chunks' added
    """
    chunker = OverlapChunker(config)
    
    for doc in documents:
        if not doc.get('success'):
            continue
        
        text = doc.get('text', '')
        metadata = doc.get('metadata', {})
        
        if text:
            chunks = chunker.chunk_text(text, metadata)
            doc['chunks'] = chunks
            doc['chunk_stats'] = chunker.get_chunking_stats(chunks)
    
    return documents
