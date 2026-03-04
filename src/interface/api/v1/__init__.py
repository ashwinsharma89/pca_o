"""
API v1 Router.
"""

from fastapi import APIRouter
from loguru import logger

# Create v1 router
router_v1 = APIRouter(prefix="/api/v1", tags=["v1"])

# Import and include sub-routers
from .auth import router as auth_router
from .routers.campaigns import router as campaigns_router
from .user_management import router as user_management_router
# from .api_keys import router as api_keys_router
# from .webhooks import router as webhooks_router
from .intelligence import router as intelligence_router
from .dashboards import router as dashboards_router
from .connectors import router as connectors_router
from .health_check import router as health_check_router

# Import new domain-specific routers
from .routers.ingestion import router as ingestion_router
from .routers.analytics import router as analytics_router
from .routers.chat import router as chat_router
from .routers.reports import router as reports_router
from .routers.analysis import router as analysis_router
from .routers.pacing import router as pacing_router
from .routers.feedback import router as feedback_router
from .routers.system import router as system_router

router_v1.include_router(auth_router)
router_v1.include_router(user_management_router)
# router_v1.include_router(api_keys_router)
# router_v1.include_router(webhooks_router)
router_v1.include_router(intelligence_router)
router_v1.include_router(dashboards_router)
router_v1.include_router(connectors_router)
router_v1.include_router(health_check_router)

# New domain routers (Phase 2 refactoring)
router_v1.include_router(ingestion_router)
router_v1.include_router(reports_router)  # MUST be before analytics_router to handle x_axis/y_axis params
router_v1.include_router(analytics_router)
router_v1.include_router(chat_router)
router_v1.include_router(analysis_router)
router_v1.include_router(pacing_router)
router_v1.include_router(feedback_router)
router_v1.include_router(system_router)

# Legacy/General campaigns router (must be last due to /{campaign_id} catch-all)
router_v1.include_router(campaigns_router)

# from .databases import router as databases_router
# router_v1.include_router(databases_router)

from .routers.kg_summary import router as kg_summary_router
router_v1.include_router(kg_summary_router)

from .upload import router as upload_router
router_v1.include_router(upload_router)

# from .orchestrator import router as orchestrator_router
# router_v1.include_router(orchestrator_router)

# KG-RAG (Knowledge Graph RAG) router
try:
    from src.kg_rag.api.router import router as kg_rag_router
    router_v1.include_router(kg_rag_router)
    logger.info("KG-RAG router mounted at /api/v1/kg")
except ImportError as e:
    logger.warning(f"KG-RAG router not available: {e}")


__all__ = ['router_v1']

