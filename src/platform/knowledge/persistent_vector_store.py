"""
Persistent Vector Store using ChromaDB.
Replaces in-memory FAISS with persistent, scalable vector database.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import json

try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False

from openai import OpenAI

logger = logging.getLogger(__name__)


class PersistentVectorStore:
    """
    Persistent vector store using ChromaDB.
    
    Features:
    - Persistent storage (survives restarts)
    - Scalable (handles large datasets)
    - Metadata filtering
    - Automatic embeddings
    - Collection management
    """
    
    def __init__(
        self,
        collection_name: str = "pca_knowledge_base",
        persist_directory: str = "./data/chroma_db",
        embedding_model: str = "text-embedding-3-small",
        openai_client: Optional[OpenAI] = None
    ):
        """
        Initialize persistent vector store.
        
        Args:
            collection_name: Name of the collection
            persist_directory: Directory to persist data
            embedding_model: OpenAI embedding model
            openai_client: Optional OpenAI client
        """
        if not CHROMADB_AVAILABLE:
            raise ImportError(
                "chromadb not installed. Install with: pip install chromadb"
            )
        
        self.collection_name = collection_name
        self.persist_directory = Path(persist_directory)
        self.embedding_model = embedding_model
        self.client_openai = openai_client or OpenAI()
        
        # Create persist directory
        self.persist_directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB client with persistence
        self.client = chromadb.PersistentClient(
            path=str(self.persist_directory),
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )
        
        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        
        logger.info(f"✅ Initialized persistent vector store: {collection_name}")
        logger.info(f"   Location: {self.persist_directory}")
        logger.info(f"   Documents: {self.collection.count()}")
    
    def add_documents(
        self,
        documents: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Add documents to the vector store.
        
        Args:
            documents: List of document dicts with 'chunks', 'metadata'
            batch_size: Batch size for processing
            
        Returns:
            Dictionary with add results
        """
        try:
            texts = []
            metadatas = []
            ids = []
            
            doc_count = 0
            chunk_count = 0
            
            for doc in documents:
                if not doc.get("success"):
                    continue
                
                chunks = doc.get("chunks", [])
                if not chunks:
                    continue
                
                base_metadata = {
                    "source": doc.get("source", "unknown"),
                    "url": doc.get("url", ""),
                    "title": doc.get("title", ""),
                    "category": doc.get("category", ""),
                    "priority": str(doc.get("priority", 0)),
                    "description": doc.get("description", ""),
                    "ingested_at": datetime.now().isoformat()
                }
                
                for idx, chunk in enumerate(chunks):
                    chunk_id = f"{doc.get('source', 'unknown')}_{doc_count}_{idx}"
                    texts.append(chunk)
                    metadatas.append(base_metadata)
                    ids.append(chunk_id)
                    chunk_count += 1
                
                doc_count += 1
            
            if not texts:
                return {
                    'success': False,
                    'message': 'No valid chunks to add',
                    'documents_processed': 0,
                    'chunks_added': 0
                }
            
            # Generate embeddings
            logger.info(f"Generating embeddings for {len(texts)} chunks...")
            embeddings = self._generate_embeddings(texts, batch_size)
            
            # Add to collection in batches
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i + batch_size]
                batch_metadatas = metadatas[i:i + batch_size]
                batch_ids = ids[i:i + batch_size]
                batch_embeddings = embeddings[i:i + batch_size]
                
                self.collection.add(
                    documents=batch_texts,
                    metadatas=batch_metadatas,
                    ids=batch_ids,
                    embeddings=batch_embeddings
                )
            
            logger.info(f"✅ Added {chunk_count} chunks from {doc_count} documents")
            
            return {
                'success': True,
                'message': f'Added {chunk_count} chunks',
                'documents_processed': doc_count,
                'chunks_added': chunk_count,
                'total_documents': self.collection.count()
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to add documents: {e}")
            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'documents_processed': 0,
                'chunks_added': 0
            }
    
    def _generate_embeddings(
        self,
        texts: List[str],
        batch_size: int = 100
    ) -> List[List[float]]:
        """Generate embeddings for texts."""
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            response = self.client_openai.embeddings.create(
                model=self.embedding_model,
                input=batch
            )
            
            batch_embeddings = [data.embedding for data in response.data]
            all_embeddings.extend(batch_embeddings)
        
        return all_embeddings
    
    def search(
        self,
        query: str,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant documents.
        
        Args:
            query: Search query
            top_k: Number of results to return
            metadata_filters: Optional metadata filters
            
        Returns:
            List of search results with scores and metadata
        """
        try:
            # Generate query embedding
            response = self.client_openai.embeddings.create(
                model=self.embedding_model,
                input=[query]
            )
            query_embedding = response.data[0].embedding
            
            # Build where clause for metadata filtering
            where = None
            if metadata_filters:
                where = {}
                for key, value in metadata_filters.items():
                    if isinstance(value, (list, tuple)):
                        where[key] = {"$in": list(value)}
                    else:
                        where[key] = value
            
            # Query collection
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k,
                where=where
            )
            
            # Format results
            formatted_results = []
            
            if results['documents'] and results['documents'][0]:
                for i, doc in enumerate(results['documents'][0]):
                    formatted_results.append({
                        'text': doc,
                        'score': 1.0 - results['distances'][0][i],  # Convert distance to similarity
                        'metadata': results['metadatas'][0][i] if results['metadatas'] else {},
                        'id': results['ids'][0][i] if results['ids'] else None
                    })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []
    
    def delete_collection(self):
        """Delete the entire collection."""
        try:
            self.client.delete_collection(name=self.collection_name)
            logger.info(f"✅ Deleted collection: {self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete collection: {e}")
            return False
    
    def reset_collection(self):
        """Reset collection (delete and recreate)."""
        try:
            self.delete_collection()
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            logger.info(f"✅ Reset collection: {self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to reset collection: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        try:
            count = self.collection.count()
            
            # Get sample metadata
            sample = self.collection.get(limit=1)
            sample_metadata = sample['metadatas'][0] if sample['metadatas'] else {}
            
            return {
                'collection_name': self.collection_name,
                'total_documents': count,
                'persist_directory': str(self.persist_directory),
                'embedding_model': self.embedding_model,
                'sample_metadata_keys': list(sample_metadata.keys()) if sample_metadata else []
            }
        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")
            return {}
    
    def export_metadata(self, output_path: str):
        """Export all metadata to JSON file."""
        try:
            all_data = self.collection.get()
            
            export_data = {
                'collection_name': self.collection_name,
                'total_documents': len(all_data['ids']),
                'exported_at': datetime.now().isoformat(),
                'documents': [
                    {
                        'id': all_data['ids'][i],
                        'text': all_data['documents'][i] if all_data['documents'] else None,
                        'metadata': all_data['metadatas'][i] if all_data['metadatas'] else {}
                    }
                    for i in range(len(all_data['ids']))
                ]
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Exported metadata to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to export metadata: {e}")
            return False


# Global instance
_vector_store: Optional[PersistentVectorStore] = None


def get_vector_store(
    collection_name: str = "pca_knowledge_base",
    persist_directory: str = "./data/chroma_db"
) -> PersistentVectorStore:
    """Get or create global vector store instance."""
    global _vector_store
    if _vector_store is None:
        _vector_store = PersistentVectorStore(
            collection_name=collection_name,
            persist_directory=persist_directory
        )
    return _vector_store
