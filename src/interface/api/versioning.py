"""
API Versioning - URL-based versioning with deprecation support
Provides clean API versioning strategy
"""
from fastapi import APIRouter, Header, HTTPException, Response
from typing import Optional
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class APIVersion(str, Enum):
    """Supported API versions"""
    V1 = "v1"
    V2 = "v2"  # Future version


class VersionedRouter:
    """Manage API versions with deprecation support"""
    
    def __init__(self):
        self.routers = {}
        self.deprecated_versions = set()
        self.sunset_dates = {}
    
    def create_router(
        self,
        version: APIVersion,
        deprecated: bool = False,
        sunset_date: str = None
    ) -> APIRouter:
        """
        Create versioned router
        
        Args:
            version: API version
            deprecated: Whether version is deprecated
            sunset_date: Sunset date (ISO format: YYYY-MM-DD)
            
        Returns:
            FastAPI router for this version
        """
        router = APIRouter(
            prefix=f"/api/{version.value}",
            tags=[f"API {version.value}"]
        )
        
        self.routers[version] = router
        
        if deprecated:
            self.deprecated_versions.add(version)
            if sunset_date:
                self.sunset_dates[version] = sunset_date
            
            # Add deprecation middleware
            @router.middleware("http")
            async def deprecation_warning(request, call_next):
                response = await call_next(request)
                response.headers["X-API-Deprecated"] = "true"
                response.headers["X-API-Version"] = version.value
                if sunset_date:
                    response.headers["X-API-Sunset"] = sunset_date
                response.headers["Warning"] = (
                    f'299 - "API version {version.value} is deprecated. '
                    f'Please migrate to the latest version."'
                )
                return response
        
        logger.info(f"Created router for API {version.value} (deprecated: {deprecated})")
        return router
    
    def get_router(self, version: APIVersion) -> APIRouter:
        """Get router for version"""
        return self.routers.get(version)
    
    def is_deprecated(self, version: APIVersion) -> bool:
        """Check if version is deprecated"""
        return version in self.deprecated_versions


# Global versioning instance
versioning = VersionedRouter()

# Create V1 router (current)
router_v1 = versioning.create_router(APIVersion.V1, deprecated=False)

# V2 router (future - when ready)
# router_v2 = versioning.create_router(APIVersion.V2, deprecated=False)
