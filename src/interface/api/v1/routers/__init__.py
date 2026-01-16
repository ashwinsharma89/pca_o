"""
API v1 Routers Package

Domain-specific routers split from campaigns.py monolith.
Each router handles a specific domain:
- ingestion: Upload, filters, schema
- analytics: Charts, visualizations, dashboard
- chat: NL-to-SQL, RAG queries
- reports: AI analysis, report generation
- campaigns: CRUD operations
"""

from fastapi import APIRouter

# Import all routers for registration in main.py
from .ingestion import router as ingestion_router

# Master router that includes all domain routers
router = APIRouter()
router.include_router(ingestion_router)

__all__ = [
    "router",
    "ingestion_router",
]
