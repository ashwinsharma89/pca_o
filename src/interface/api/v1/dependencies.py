"""
FastAPI Dependency Injection for Services.

Provides clean factories for service layer instances,
ensuring proper wiring of repositories and dependencies.
"""

from fastapi import Depends
from sqlalchemy.orm import Session

from src.core.database.connection import get_db
from src.core.database.repositories import CampaignRepository, \
    AnalysisRepository, \
    CampaignContextRepository
from src.core.database.duckdb_repository import get_duckdb_repository
from src.engine.services.campaign_service import CampaignService


def get_campaign_service(db: Session = Depends(get_db)) -> CampaignService:
    """
    Factory for CampaignService with all dependencies injected.
    
    Usage in endpoints:
        @router.get("/stats")
        async def get_stats(svc: CampaignService = Depends(get_campaign_service)):
            return svc.get_dashboard_stats()
    """
    return CampaignService(
        campaign_repo=CampaignRepository(db),
        analysis_repo=AnalysisRepository(db),
        context_repo=CampaignContextRepository(db),
        duckdb_repo=get_duckdb_repository()
    )
