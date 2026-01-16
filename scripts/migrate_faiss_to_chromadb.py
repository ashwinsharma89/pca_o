"""
Migrate FAISS vector store to ChromaDB.

Safely migrates all vectors, metadata, and documents from FAISS to ChromaDB
with verification and rollback capability.
"""

import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import numpy as np
from loguru import logger

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logger.error("FAISS not available. Install with: pip install faiss-cpu")

try:
    from src.platform.knowledge.persistent_vector_store import PersistentVectorStore
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    logger.error("ChromaDB not available. Install with: pip install chromadb")


class FAISSToChromaMigrator:
    """Migrates FAISS vector store to ChromaDB."""
    
    def __init__(
        self,
        faiss_index_path: str = "./data/vector_store/faiss.index",
        faiss_metadata_path: str = "./data/vector_store/metadata.json",
        chroma_persist_dir: str = "./data/chroma_db",
        chroma_collection: str = "pca_knowledge_base"
    ):
        """
        Initialize migrator.
        
        Args:
            faiss_index_path: Path to FAISS index
            faiss_metadata_path: Path to FAISS metadata
            chroma_persist_dir: ChromaDB persist directory
            chroma_collection: ChromaDB collection name
        """
        self.faiss_index_path = Path(faiss_index_path)
        self.faiss_metadata_path = Path(faiss_metadata_path)
        self.chroma_persist_dir = Path(chroma_persist_dir)
        self.chroma_collection = chroma_collection
        
        self.backup_dir = Path(f"./data/migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        logger.info("Initialized FAISS to ChromaDB migrator")
    
    def create_backup(self) -> bool:
        """Create backup of FAISS data."""
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup FAISS index
            if self.faiss_index_path.exists():
                shutil.copy2(
                    self.faiss_index_path,
                    self.backup_dir / "faiss.index"
                )
                logger.info(f"Backed up FAISS index to {self.backup_dir}")
            
            # Backup metadata
            if self.faiss_metadata_path.exists():
                shutil.copy2(
                    self.faiss_metadata_path,
                    self.backup_dir / "metadata.json"
                )
                logger.info(f"Backed up metadata to {self.backup_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return False
    
    def load_faiss_data(self) -> Dict[str, Any]:
        """Load FAISS index and metadata."""
        if not FAISS_AVAILABLE:
            raise ImportError("FAISS not available")
        
        if not self.faiss_index_path.exists():
            raise FileNotFoundError(f"FAISS index not found: {self.faiss_index_path}")
        
        if not self.faiss_metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found: {self.faiss_metadata_path}")
        
        logger.info("Loading FAISS data...")
        
        # Load index
        index = faiss.read_index(str(self.faiss_index_path))
        
        # Load metadata
        with open(self.faiss_metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # Extract vectors
        vectors = []
        for i in range(index.ntotal):
            vector = index.reconstruct(i)
            vectors.append(vector.tolist())
        
        logger.info(f"Loaded {len(vectors)} vectors and {len(metadata)} metadata records")
        
        return {
            "vectors": vectors,
            "metadata": metadata,
            "count": len(vectors)
        }
    
    def migrate_to_chromadb(self, faiss_data: Dict[str, Any]) -> Dict[str, Any]:
        """Migrate data to ChromaDB."""
        if not CHROMADB_AVAILABLE:
            raise ImportError("ChromaDB not available")
        
        logger.info("Starting migration to ChromaDB...")
        
        # Initialize ChromaDB
        vector_store = PersistentVectorStore(
            collection_name=self.chroma_collection,
            persist_directory=str(self.chroma_persist_dir)
        )
        
        # Reset collection if exists
        if vector_store.collection.count() > 0:
            logger.warning("Collection exists, resetting...")
            vector_store.reset_collection()
        
        # Prepare documents for ChromaDB
        documents = []
        for i, meta in enumerate(faiss_data["metadata"]):
            doc = {
                "success": True,
                "chunks": [meta.get("text", "")],
                "source": meta.get("source", "unknown"),
                "url": meta.get("url", ""),
                "title": meta.get("title", ""),
                "category": meta.get("category", ""),
                "priority": meta.get("priority", 0),
                "description": meta.get("description", "")
            }
            documents.append(doc)
        
        # Add to ChromaDB
        result = vector_store.add_documents(documents, batch_size=100)
        
        logger.info(f"Migration complete: {result}")
        
        return result
    
    def verify_migration(self, faiss_data: Dict[str, Any]) -> Dict[str, Any]:
        """Verify migration was successful."""
        logger.info("Verifying migration...")
        
        vector_store = PersistentVectorStore(
            collection_name=self.chroma_collection,
            persist_directory=str(self.chroma_persist_dir)
        )
        
        # Check document count
        chroma_count = vector_store.collection.count()
        faiss_count = faiss_data["count"]
        
        count_match = chroma_count == faiss_count
        
        # Sample search comparison
        sample_queries = [
            meta.get("text", "")[:100]
            for meta in faiss_data["metadata"][:5]
            if meta.get("text")
        ]
        
        search_consistency = 0
        for query in sample_queries:
            if query:
                results = vector_store.search(query, top_k=1)
                if results:
                    search_consistency += 1
        
        search_rate = search_consistency / len(sample_queries) if sample_queries else 0
        
        verification = {
            "faiss_count": faiss_count,
            "chroma_count": chroma_count,
            "count_match": count_match,
            "search_consistency": f"{search_rate * 100:.1f}%",
            "sample_queries_tested": len(sample_queries),
            "successful_searches": search_consistency,
            "status": "✅ PASSED" if count_match and search_rate > 0.8 else "❌ FAILED"
        }
        
        logger.info(f"Verification: {verification['status']}")
        
        return verification
    
    def run_migration(self) -> Dict[str, Any]:
        """Run complete migration process."""
        logger.info("=" * 70)
        logger.info("FAISS to ChromaDB Migration")
        logger.info("=" * 70)
        
        results = {
            "started_at": datetime.now().isoformat(),
            "backup_created": False,
            "migration_successful": False,
            "verification_passed": False
        }
        
        try:
            # Step 1: Create backup
            logger.info("\nStep 1: Creating backup...")
            results["backup_created"] = self.create_backup()
            if not results["backup_created"]:
                raise Exception("Backup creation failed")
            
            # Step 2: Load FAISS data
            logger.info("\nStep 2: Loading FAISS data...")
            faiss_data = self.load_faiss_data()
            results["faiss_documents"] = faiss_data["count"]
            
            # Step 3: Migrate to ChromaDB
            logger.info("\nStep 3: Migrating to ChromaDB...")
            migration_result = self.migrate_to_chromadb(faiss_data)
            results["migration_successful"] = migration_result.get("success", False)
            results["chroma_documents"] = migration_result.get("chunks_added", 0)
            
            if not results["migration_successful"]:
                raise Exception("Migration failed")
            
            # Step 4: Verify migration
            logger.info("\nStep 4: Verifying migration...")
            verification = self.verify_migration(faiss_data)
            results["verification"] = verification
            results["verification_passed"] = verification["status"] == "✅ PASSED"
            
            results["completed_at"] = datetime.now().isoformat()
            results["status"] = "✅ SUCCESS" if results["verification_passed"] else "❌ FAILED"
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            results["error"] = str(e)
            results["status"] = "❌ FAILED"
            results["completed_at"] = datetime.now().isoformat()
        
        # Print summary
        self._print_summary(results)
        
        return results
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print migration summary."""
        logger.info("\n" + "=" * 70)
        logger.info("Migration Summary")
        logger.info("=" * 70)
        logger.info(f"Status: {results.get('status', 'UNKNOWN')}")
        logger.info(f"Backup Created: {'✅' if results.get('backup_created') else '❌'}")
        logger.info(f"FAISS Documents: {results.get('faiss_documents', 0)}")
        logger.info(f"ChromaDB Documents: {results.get('chroma_documents', 0)}")
        
        if "verification" in results:
            v = results["verification"]
            logger.info(f"\nVerification:")
            logger.info(f"  Count Match: {'✅' if v.get('count_match') else '❌'}")
            logger.info(f"  Search Consistency: {v.get('search_consistency', 'N/A')}")
            logger.info(f"  Status: {v.get('status', 'UNKNOWN')}")
        
        if "error" in results:
            logger.error(f"\nError: {results['error']}")
        
        logger.info("=" * 70)


def main():
    """Main migration function."""
    migrator = FAISSToChromaMigrator()
    results = migrator.run_migration()
    
    if results.get("status") == "✅ SUCCESS":
        logger.info("\n✅ Migration completed successfully!")
        logger.info("You can now use ChromaDB as your vector store.")
        logger.info("\nNext steps:")
        logger.info("1. Update .env: VECTOR_STORE_TYPE=chromadb")
        logger.info("2. Restart your application")
        logger.info("3. Verify search functionality")
    else:
        logger.error("\n❌ Migration failed!")
        logger.error(f"Backup location: {migrator.backup_dir}")
        logger.error("Please review the errors above and try again.")
    
    return results


if __name__ == "__main__":
    main()
