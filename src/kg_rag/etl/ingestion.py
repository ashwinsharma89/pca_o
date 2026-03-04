"""
KG-RAG ETL Ingestion Pipeline

Orchestrates data extraction, transformation, and loading.
"""

import logging
from typing import Dict, Any, List, Optional

from src.kg_rag.etl.loaders.kuzu_loader import KuzuLoader
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Orchestrate ETL operations for the Knowledge Graph.
    
    Usage:
        pipeline = ETLPipeline()
        pipeline.ingest_campaigns(campaign_data)
        pipeline.ingest_metrics(metric_data)
    """

    def __init__(self, loader: Optional[KuzuLoader] = None):
        self._loader = loader or KuzuLoader()
        self._settings = get_kg_rag_settings()

    def ingest_campaigns(self, campaigns: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest campaigns."""
        return self._loader.load_campaigns(campaigns)

    def ingest_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest metrics."""
        return self._loader.load_metrics(metrics)

    def ingest_targeting(self, targeting: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest targeting."""
        return self._loader.load_targeting(targeting)

    def ingest_entity_groups(self, entity_groups: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest entity groups."""
        return self._loader.load_entity_groups(entity_groups)

    def ingest_keywords(self, keywords: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest keywords."""
        return self._loader.load_keywords(keywords)

    def ingest_placements(self, placements: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ingest placements."""
        return self._loader.load_placements(placements)
