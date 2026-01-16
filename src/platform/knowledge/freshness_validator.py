"""
Knowledge Source Freshness Validator.

Automatically validates knowledge source freshness and triggers refresh when needed.
"""

import json
import hashlib
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class SourceMetadata:
    """Metadata for a knowledge source."""
    
    source_id: str
    url: str
    source_type: str  # web_content, youtube, documentation, benchmark
    last_updated: datetime
    last_checked: datetime
    content_hash: Optional[str] = None
    ttl_days: int = 7
    auto_refresh: bool = True
    is_stale: bool = False
    check_count: int = 0
    refresh_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_id": self.source_id,
            "url": self.url,
            "source_type": self.source_type,
            "last_updated": self.last_updated.isoformat(),
            "last_checked": self.last_checked.isoformat(),
            "content_hash": self.content_hash,
            "ttl_days": self.ttl_days,
            "auto_refresh": self.auto_refresh,
            "is_stale": self.is_stale,
            "check_count": self.check_count,
            "refresh_count": self.refresh_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SourceMetadata':
        """Create from dictionary."""
        return cls(
            source_id=data["source_id"],
            url=data["url"],
            source_type=data["source_type"],
            last_updated=datetime.fromisoformat(data["last_updated"]),
            last_checked=datetime.fromisoformat(data["last_checked"]),
            content_hash=data.get("content_hash"),
            ttl_days=data.get("ttl_days", 7),
            auto_refresh=data.get("auto_refresh", True),
            is_stale=data.get("is_stale", False),
            check_count=data.get("check_count", 0),
            refresh_count=data.get("refresh_count", 0)
        )


class FreshnessValidator:
    """
    Validates knowledge source freshness and triggers refresh.
    
    Features:
    - TTL-based staleness detection
    - HTTP HEAD request validation
    - Content hash comparison
    - Automatic refresh triggers
    - Configurable rules per source type
    """
    
    # Default TTL rules by source type (in days)
    DEFAULT_TTL_RULES = {
        "web_content": 7,
        "youtube_videos": 30,
        "documentation": 14,
        "benchmarks": 90,
        "best_practices": 180,
        "case_studies": 30,
        "api_docs": 7
    }
    
    def __init__(
        self,
        metadata_path: str = "./data/knowledge_freshness.json",
        ttl_rules: Optional[Dict[str, int]] = None
    ):
        """
        Initialize freshness validator.
        
        Args:
            metadata_path: Path to store freshness metadata
            ttl_rules: Custom TTL rules by source type
        """
        self.metadata_path = Path(metadata_path)
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.ttl_rules = ttl_rules or self.DEFAULT_TTL_RULES
        self.sources: Dict[str, SourceMetadata] = {}
        
        # Load existing metadata
        self._load_metadata()
        
        logger.info(f"Initialized FreshnessValidator with {len(self.sources)} sources")
    
    def _load_metadata(self):
        """Load metadata from disk."""
        if not self.metadata_path.exists():
            return
        
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.sources = {
                source_id: SourceMetadata.from_dict(source_data)
                for source_id, source_data in data.items()
            }
            
            logger.info(f"Loaded {len(self.sources)} source metadata records")
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
    
    def _save_metadata(self):
        """Save metadata to disk."""
        try:
            data = {
                source_id: source.to_dict()
                for source_id, source in self.sources.items()
            }
            
            with open(self.metadata_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved {len(self.sources)} source metadata records")
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def register_source(
        self,
        source_id: str,
        url: str,
        source_type: str,
        content_hash: Optional[str] = None,
        ttl_days: Optional[int] = None,
        auto_refresh: bool = True
    ) -> SourceMetadata:
        """
        Register a new knowledge source.
        
        Args:
            source_id: Unique source identifier
            url: Source URL
            source_type: Type of source
            content_hash: Optional content hash
            ttl_days: Optional custom TTL
            auto_refresh: Whether to auto-refresh when stale
        
        Returns:
            Source metadata
        """
        if ttl_days is None:
            ttl_days = self.ttl_rules.get(source_type, 7)
        
        now = datetime.now()
        
        source = SourceMetadata(
            source_id=source_id,
            url=url,
            source_type=source_type,
            last_updated=now,
            last_checked=now,
            content_hash=content_hash,
            ttl_days=ttl_days,
            auto_refresh=auto_refresh
        )
        
        self.sources[source_id] = source
        self._save_metadata()
        
        logger.info(f"Registered source: {source_id} ({source_type}, TTL: {ttl_days} days)")
        return source
    
    def check_freshness(self, source_id: str) -> Dict[str, Any]:
        """
        Check if a source is fresh.
        
        Args:
            source_id: Source identifier
        
        Returns:
            Freshness status dictionary
        """
        if source_id not in self.sources:
            return {
                "source_id": source_id,
                "exists": False,
                "is_fresh": False,
                "error": "Source not registered"
            }
        
        source = self.sources[source_id]
        source.last_checked = datetime.now()
        source.check_count += 1
        
        # Calculate age
        age = datetime.now() - source.last_updated
        age_days = age.total_seconds() / 86400
        
        # Check if stale
        is_stale = age_days > source.ttl_days
        source.is_stale = is_stale
        
        self._save_metadata()
        
        result = {
            "source_id": source_id,
            "exists": True,
            "is_fresh": not is_stale,
            "is_stale": is_stale,
            "age_days": round(age_days, 2),
            "ttl_days": source.ttl_days,
            "last_updated": source.last_updated.isoformat(),
            "last_checked": source.last_checked.isoformat(),
            "auto_refresh": source.auto_refresh,
            "needs_refresh": is_stale and source.auto_refresh
        }
        
        if is_stale:
            logger.warning(f"Source {source_id} is stale (age: {age_days:.1f} days, TTL: {source.ttl_days} days)")
        
        return result
    
    def validate_url(self, source_id: str) -> Dict[str, Any]:
        """
        Validate source URL is still accessible.
        
        Args:
            source_id: Source identifier
        
        Returns:
            Validation result
        """
        if source_id not in self.sources:
            return {"valid": False, "error": "Source not registered"}
        
        source = self.sources[source_id]
        
        try:
            # Use HEAD request to check URL
            response = requests.head(source.url, timeout=10, allow_redirects=True)
            
            is_valid = response.status_code < 400
            
            result = {
                "source_id": source_id,
                "url": source.url,
                "valid": is_valid,
                "status_code": response.status_code,
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag")
            }
            
            if not is_valid:
                logger.warning(f"Source {source_id} URL validation failed: {response.status_code}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to validate URL for {source_id}: {e}")
            return {
                "source_id": source_id,
                "url": source.url,
                "valid": False,
                "error": str(e)
            }
    
    def get_stale_sources(self) -> List[SourceMetadata]:
        """Get all stale sources."""
        stale = []
        
        for source in self.sources.values():
            status = self.check_freshness(source.source_id)
            if status["is_stale"]:
                stale.append(source)
        
        return stale
    
    def get_sources_needing_refresh(self) -> List[SourceMetadata]:
        """Get sources that need refresh (stale + auto_refresh enabled)."""
        return [
            source for source in self.get_stale_sources()
            if source.auto_refresh
        ]
    
    def mark_refreshed(self, source_id: str):
        """Mark a source as refreshed."""
        if source_id not in self.sources:
            logger.warning(f"Cannot mark unknown source as refreshed: {source_id}")
            return
        
        source = self.sources[source_id]
        source.last_updated = datetime.now()
        source.is_stale = False
        source.refresh_count += 1
        
        self._save_metadata()
        
        logger.info(f"Marked source {source_id} as refreshed")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get freshness statistics."""
        total = len(self.sources)
        stale = len(self.get_stale_sources())
        fresh = total - stale
        needs_refresh = len(self.get_sources_needing_refresh())
        
        # Group by source type
        by_type = {}
        for source in self.sources.values():
            if source.source_type not in by_type:
                by_type[source.source_type] = {"total": 0, "stale": 0}
            by_type[source.source_type]["total"] += 1
            if source.is_stale:
                by_type[source.source_type]["stale"] += 1
        
        return {
            "total_sources": total,
            "fresh_sources": fresh,
            "stale_sources": stale,
            "needs_refresh": needs_refresh,
            "freshness_rate": round(fresh / total * 100, 2) if total > 0 else 0,
            "by_type": by_type,
            "last_check": datetime.now().isoformat()
        }
    
    def get_report(self) -> str:
        """Generate a text report."""
        stats = self.get_statistics()
        
        lines = [
            "=" * 70,
            "Knowledge Source Freshness Report",
            "=" * 70,
            "",
            f"Total Sources: {stats['total_sources']}",
            f"Fresh: {stats['fresh_sources']} ({stats['freshness_rate']:.1f}%)",
            f"Stale: {stats['stale_sources']}",
            f"Needs Refresh: {stats['needs_refresh']}",
            "",
            "By Source Type:",
            ""
        ]
        
        for source_type, counts in stats['by_type'].items():
            freshness = (counts['total'] - counts['stale']) / counts['total'] * 100 if counts['total'] > 0 else 0
            status_icon = "✅" if freshness >= 90 else "⚠️" if freshness >= 70 else "❌"
            lines.append(f"  {status_icon} {source_type}: {counts['total']} total, {counts['stale']} stale ({freshness:.1f}% fresh)")
        
        lines.extend([
            "",
            "=" * 70
        ])
        
        return "\n".join(lines)


# Global instance
_validator: Optional[FreshnessValidator] = None


def get_freshness_validator() -> FreshnessValidator:
    """Get global freshness validator instance."""
    global _validator
    if _validator is None:
        _validator = FreshnessValidator()
    return _validator
