"""
Knowledge Ingestion Module
Allows PCA Agent to learn from URLs, YouTube videos, and PDFs
Integrates with the reasoning layer for enhanced analysis
"""
import os
from typing import List, Dict, Any, Optional
from loguru import logger
import requests
from bs4 import BeautifulSoup
import re

# Optional dependencies - will gracefully degrade if not available
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    YOUTUBE_AVAILABLE = True
except ImportError:
    YOUTUBE_AVAILABLE = False
    logger.warning("youtube-transcript-api not installed. Install with: pip install youtube-transcript-api")

try:
    from PyPDF2 import PdfReader
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("PyPDF2 not installed. Install with: pip install PyPDF2")

try:
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    logger.warning("langchain not installed. Install with: pip install langchain")


class KnowledgeIngestion:
    """
    Ingest knowledge from various sources and prepare for reasoning layer.
    """
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200, min_content_length: int = 100):
        """
        Initialize knowledge ingestion.
        
        Args:
            chunk_size: Size of text chunks for processing
            chunk_overlap: Overlap between chunks
            min_content_length: Minimum content length to consider valid (default: 100 chars)
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_content_length = min_content_length
        self.knowledge_base = []
        
        if LANGCHAIN_AVAILABLE:
            self.text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                length_function=len,
            )
        else:
            self.text_splitter = None
        
        logger.info("Initialized KnowledgeIngestion with validation")
    
    def ingest_from_url(self, url: str) -> Dict[str, Any]:
        """
        Extract and process content from a URL.
        
        Args:
            url: Web URL to extract content from
            
        Returns:
            Dictionary with extracted content and metadata
        """
        try:
            logger.info(f"Ingesting content from URL: {url}")
            
            # Fetch the webpage
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Extract title
            title = soup.title.string if soup.title else url
            
            # Extract main content
            # Try to find main content area
            main_content = soup.find('main') or soup.find('article') or soup.find('body')
            
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
            else:
                text = soup.get_text(separator='\n', strip=True)
            
            # Clean up text
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            text = '\n'.join(lines)
            
            # Validate content quality
            validation = self._validate_content(text, source_type='url')
            if not validation['is_valid']:
                logger.warning(f"Content validation failed: {validation['reason']}")
                return {
                    'source': 'url',
                    'url': url,
                    'success': False,
                    'error': f"Content validation failed: {validation['reason']}",
                    'validation': validation
                }
            
            # Split into chunks
            chunks = self._split_text(text)
            
            result = {
                'source': 'url',
                'url': url,
                'title': title,
                'content': text,
                'chunks': chunks,
                'chunk_count': len(chunks),
                'validation': validation,
                'quality_score': validation.get('quality_score', 0),
                'success': True
            }
            
            # Add to knowledge base
            self.knowledge_base.append(result)
            
            logger.info(f"Successfully ingested {len(chunks)} chunks from URL")
            return result
            
        except Exception as e:
            logger.error(f"Error ingesting from URL: {e}")
            return {
                'source': 'url',
                'url': url,
                'success': False,
                'error': str(e)
            }
    
    def ingest_from_youtube(self, video_url: str, languages: List[str] = ['en']) -> Dict[str, Any]:
        """
        Extract transcript from YouTube video.
        
        Args:
            video_url: YouTube video URL
            languages: List of language codes to try (default: ['en'])
            
        Returns:
            Dictionary with transcript and metadata
        """
        if not YOUTUBE_AVAILABLE:
            return {
                'source': 'youtube',
                'url': video_url,
                'success': False,
                'error': 'youtube-transcript-api not installed'
            }
        
        try:
            logger.info(f"Ingesting transcript from YouTube: {video_url}")
            
            # Extract video ID
            video_id = self._extract_youtube_id(video_url)
            if not video_id:
                raise ValueError("Could not extract video ID from URL")
            
            # Get transcript
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)
            
            # Combine transcript entries
            full_text = " ".join([entry['text'] for entry in transcript_list])
            
            # Also create timestamped version
            timestamped_text = "\n".join([
                f"[{self._format_timestamp(entry['start'])}] {entry['text']}"
                for entry in transcript_list
            ])
            
            # Split into chunks
            chunks = self._split_text(full_text)
            
            result = {
                'source': 'youtube',
                'url': video_url,
                'video_id': video_id,
                'content': full_text,
                'timestamped_content': timestamped_text,
                'chunks': chunks,
                'chunk_count': len(chunks),
                'duration': transcript_list[-1]['start'] + transcript_list[-1]['duration'] if transcript_list else 0,
                'success': True
            }
            
            # Add to knowledge base
            self.knowledge_base.append(result)
            
            logger.info(f"Successfully ingested {len(chunks)} chunks from YouTube video")
            return result
            
        except Exception as e:
            logger.error(f"Error ingesting from YouTube: {e}")
            return {
                'source': 'youtube',
                'url': video_url,
                'success': False,
                'error': str(e)
            }
    
    def ingest_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extract text from PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Dictionary with extracted text and metadata
        """
        if not PDF_AVAILABLE:
            return {
                'source': 'pdf',
                'path': pdf_path,
                'success': False,
                'error': 'PyPDF2 not installed'
            }
        
        try:
            logger.info(f"Ingesting content from PDF: {pdf_path}")
            
            # Read PDF
            reader = PdfReader(pdf_path)
            
            # Extract text from all pages
            pages_text = []
            for i, page in enumerate(reader.pages):
                text = page.extract_text()
                pages_text.append({
                    'page_number': i + 1,
                    'text': text
                })
            
            # Combine all text
            full_text = "\n\n".join([p['text'] for p in pages_text])
            
            # Split into chunks
            chunks = self._split_text(full_text)
            
            result = {
                'source': 'pdf',
                'path': pdf_path,
                'filename': os.path.basename(pdf_path),
                'content': full_text,
                'pages': pages_text,
                'page_count': len(reader.pages),
                'chunks': chunks,
                'chunk_count': len(chunks),
                'success': True
            }
            
            # Add to knowledge base
            self.knowledge_base.append(result)
            
            logger.info(f"Successfully ingested {len(chunks)} chunks from PDF ({len(reader.pages)} pages)")
            return result
            
        except Exception as e:
            logger.error(f"Error ingesting from PDF: {e}")
            return {
                'source': 'pdf',
                'path': pdf_path,
                'success': False,
                'error': str(e)
            }
    
    def get_context_for_query(self, query: str, max_chunks: int = 5) -> str:
        """
        Get relevant context from knowledge base for a query.
        Simple keyword-based retrieval (can be enhanced with embeddings).
        
        Args:
            query: User query
            max_chunks: Maximum number of chunks to return
            
        Returns:
            Combined context string
        """
        if not self.knowledge_base:
            return ""
        
        # Simple keyword matching (can be enhanced with semantic search)
        query_words = set(query.lower().split())
        
        scored_chunks = []
        for doc in self.knowledge_base:
            if doc.get('success') and doc.get('chunks'):
                for chunk in doc['chunks']:
                    chunk_words = set(chunk.lower().split())
                    # Calculate overlap score
                    score = len(query_words & chunk_words)
                    if score > 0:
                        scored_chunks.append({
                            'chunk': chunk,
                            'score': score,
                            'source': doc['source']
                        })
        
        # Sort by score and get top chunks
        scored_chunks.sort(key=lambda x: x['score'], reverse=True)
        top_chunks = scored_chunks[:max_chunks]
        
        if not top_chunks:
            return ""
        
        # Combine chunks with source attribution
        context_parts = []
        for item in top_chunks:
            context_parts.append(f"[Source: {item['source']}]\n{item['chunk']}")
        
        return "\n\n---\n\n".join(context_parts)
    
    def get_knowledge_summary(self) -> Dict[str, Any]:
        """
        Get summary of ingested knowledge.
        
        Returns:
            Dictionary with knowledge base statistics
        """
        summary = {
            'total_documents': len(self.knowledge_base),
            'by_source': {},
            'total_chunks': 0
        }
        
        for doc in self.knowledge_base:
            source = doc.get('source', 'unknown')
            if source not in summary['by_source']:
                summary['by_source'][source] = 0
            summary['by_source'][source] += 1
            summary['total_chunks'] += doc.get('chunk_count', 0)
        
        return summary
    
    def clear_knowledge_base(self):
        """Clear all ingested knowledge."""
        self.knowledge_base = []
        logger.info("Knowledge base cleared")
    
    def _split_text(self, text: str) -> List[str]:
        """
        Split text into chunks.
        
        Args:
            text: Text to split
            
        Returns:
            List of text chunks
        """
        if self.text_splitter:
            # Use LangChain splitter if available
            return self.text_splitter.split_text(text)
        else:
            # Simple chunking by character count
            chunks = []
            for i in range(0, len(text), self.chunk_size - self.chunk_overlap):
                chunk = text[i:i + self.chunk_size]
                if chunk.strip():
                    chunks.append(chunk)
            return chunks
    
    def _extract_youtube_id(self, url: str) -> Optional[str]:
        """
        Extract YouTube video ID from URL.
        
        Args:
            url: YouTube URL
            
        Returns:
            Video ID or None
        """
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/)([^&\n?#]+)',
            r'youtube\.com\/embed\/([^&\n?#]+)',
            r'youtube\.com\/v\/([^&\n?#]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    def _format_timestamp(self, seconds: float) -> str:
        """
        Format seconds into MM:SS or HH:MM:SS.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted timestamp string
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"
    
    def _validate_content(self, text: str, source_type: str = 'unknown') -> Dict[str, Any]:
        """
        Validate content quality and reliability.
        
        Args:
            text: Extracted text content
            source_type: Type of source (url, youtube, pdf)
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            'is_valid': True,
            'reason': '',
            'quality_score': 0,
            'warnings': []
        }
        
        # Check minimum length
        if len(text) < self.min_content_length:
            validation['is_valid'] = False
            validation['reason'] = f"Content too short ({len(text)} chars, minimum: {self.min_content_length})"
            return validation
        
        # Check for excessive repetition (spam indicator)
        words = text.lower().split()
        if len(words) > 10:
            unique_words = len(set(words))
            word_diversity = unique_words / len(words)
            if word_diversity < 0.3:  # Less than 30% unique words
                validation['warnings'].append("Low word diversity - possible spam or low-quality content")
                validation['quality_score'] -= 20
        
        # Check for meaningful content (not just navigation/menu items)
        avg_word_length = sum(len(word) for word in words) / len(words) if words else 0
        if avg_word_length < 3:  # Very short average word length
            validation['warnings'].append("Very short average word length - may be navigation/menu text")
            validation['quality_score'] -= 15
        
        # Check for excessive special characters (garbled text indicator)
        special_char_count = sum(1 for char in text if not char.isalnum() and not char.isspace())
        special_char_ratio = special_char_count / len(text) if text else 0
        if special_char_ratio > 0.3:  # More than 30% special characters
            validation['warnings'].append("High special character ratio - possible garbled text")
            validation['quality_score'] -= 25
        
        # Check for reasonable sentence structure
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        if len(sentences) > 0:
            avg_sentence_length = len(words) / len(sentences)
            if avg_sentence_length < 3:  # Very short sentences
                validation['warnings'].append("Very short sentences - may not be substantive content")
                validation['quality_score'] -= 10
            elif avg_sentence_length > 100:  # Extremely long sentences
                validation['warnings'].append("Extremely long sentences - possible formatting issues")
                validation['quality_score'] -= 10
        
        # Check for common spam/error indicators
        spam_indicators = [
            'error 404', '404 not found', 'page not found',
            'access denied', 'forbidden', 'unauthorized',
            'javascript required', 'enable javascript',
            'cookies required', 'enable cookies'
        ]
        text_lower = text.lower()
        for indicator in spam_indicators:
            if indicator in text_lower:
                validation['is_valid'] = False
                validation['reason'] = f"Content contains error indicator: '{indicator}'"
                return validation
        
        # Calculate base quality score (0-100)
        base_score = 100
        base_score += validation['quality_score']  # Apply penalties
        
        # Bonus for longer, substantive content
        if len(text) > 1000:
            base_score += 10
        if len(text) > 5000:
            base_score += 10
        
        # Bonus for good sentence structure
        if len(sentences) > 5 and 5 < avg_sentence_length < 50:
            base_score += 10
        
        validation['quality_score'] = max(0, min(100, base_score))
        
        # Final validation check
        if validation['quality_score'] < 30:
            validation['is_valid'] = False
            validation['reason'] = f"Quality score too low ({validation['quality_score']}/100)"
        
        return validation


# Example usage
if __name__ == "__main__":
    # Initialize
    ki = KnowledgeIngestion()
    
    # Ingest from URL
    result = ki.ingest_from_url("https://example.com/article")
    print(f"URL ingestion: {result['success']}")
    
    # Ingest from YouTube
    result = ki.ingest_from_youtube("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    print(f"YouTube ingestion: {result['success']}")
    
    # Ingest from PDF
    result = ki.ingest_from_pdf("document.pdf")
    print(f"PDF ingestion: {result['success']}")
    
    # Get context for query
    context = ki.get_context_for_query("campaign optimization")
    print(f"Context: {context[:200]}...")
    
    # Get summary
    summary = ki.get_knowledge_summary()
    print(f"Knowledge base: {summary}")
