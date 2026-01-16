"""
Enhanced Knowledge Base Auto-Refresh System
Automated source freshness checking, version tracking, and updates
"""

import os
import hashlib
import json
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import threading
import time
import requests
from loguru import logger

@dataclass
class SourceVersion:
    """Version information for a knowledge source."""
    version_number: int
    content_hash: str
    timestamp: str
    size_bytes: int
    change_summary: str

@dataclass
class EnhancedSourceMetadata:
    """Enhanced metadata for a knowledge source."""
    source_id: str
    source_type: str  # 'url', 'file', 'directory', 'api'
    location: str
    current_version: int
    versions: List[Dict[str, Any]]  # Version history
    last_checked: str
    last_refreshed: str
    refresh_count: int = 0
    error_count: int = 0
    enabled: bool = True
    priority: int = 1  # 1=high, 2=medium, 3=low
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

class EnhancedKnowledgeBaseRefresher:
    """
    Enhanced automatic knowledge base refresh manager.
    
    Features:
    - Source change detection with content hashing
    - Version tracking and history
    - Automatic re-ingestion on changes
    - Configurable refresh intervals per source
    - Priority-based refresh scheduling
    - Change impact analysis
    - Rollback capability
    - Detailed refresh reporting
    """
    
    def __init__(
        self,
        metadata_path: str = "./data/enhanced_refresh_metadata.json",
        on_refresh_callback: Optional[Callable] = None,
        check_interval_seconds: int = 3600
    ):
        """
        Initialize enhanced auto-refresh manager.
        
        Args:
            metadata_path: Path to store refresh metadata
            on_refresh_callback: Callback function to execute on refresh
            check_interval_seconds: How often to check for changes
        """
        self.metadata_path = Path(metadata_path)
        self.on_refresh_callback = on_refresh_callback
        self.check_interval_seconds = check_interval_seconds
        
        # Create metadata directory
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing metadata
        self.sources: Dict[str, EnhancedSourceMetadata] = self._load_metadata()
        
        # Refresh control
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_refresh_time: Optional[datetime] = None
        
        # Statistics
        self.stats = {
            "total_checks": 0,
            "total_changes_detected": 0,
            "total_refreshes": 0,
            "total_errors": 0
        }
        
        logger.info("✅ Enhanced Knowledge Base Refresher initialized")
        logger.info(f"   Check interval: {check_interval_seconds}s")
        logger.info(f"   Tracked sources: {len(self.sources)}")
    
    def register_source(
        self,
        source_id: str,
        source_type: str,
        location: str,
        priority: int = 1,
        tags: List[str] = None
    ) -> bool:
        """
        Register a knowledge source for monitoring.
        
        Args:
            source_id: Unique identifier for source
            source_type: Type of source ('url', 'file', 'directory', 'api')
            location: Location of source
            priority: Priority level (1=high, 2=medium, 3=low)
            tags: Optional tags for categorization
            
        Returns:
            True if registered successfully
        """
        try:
            # Calculate initial hash and size
            content_hash, size_bytes = self._calculate_hash_and_size(source_type, location)
            
            # Create initial version
            initial_version = SourceVersion(
                version_number=1,
                content_hash=content_hash,
                timestamp=datetime.utcnow().isoformat(),
                size_bytes=size_bytes,
                change_summary="Initial version"
            )
            
            metadata = EnhancedSourceMetadata(
                source_id=source_id,
                source_type=source_type,
                location=location,
                current_version=1,
                versions=[asdict(initial_version)],
                last_checked=datetime.utcnow().isoformat(),
                last_refreshed=datetime.utcnow().isoformat(),
                priority=priority,
                tags=tags or []
            )
            
            self.sources[source_id] = metadata
            self._save_metadata()
            
            logger.info(f"✅ Registered source: {source_id} ({source_type}, priority={priority})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to register source {source_id}: {e}")
            return False
    
    def check_for_changes(
        self,
        source_ids: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Check sources for changes.
        
        Args:
            source_ids: Optional list of specific sources to check
            
        Returns:
            Dictionary mapping source_id to change information
        """
        self.stats["total_checks"] += 1
        changes = {}
        
        # Determine which sources to check
        sources_to_check = (
            {sid: self.sources[sid] for sid in source_ids if sid in self.sources}
            if source_ids
            else self.sources
        )
        
        for source_id, metadata in sources_to_check.items():
            if not metadata.enabled:
                continue
            
            try:
                # Calculate current hash and size
                current_hash, current_size = self._calculate_hash_and_size(
                    metadata.source_type,
                    metadata.location
                )
                
                # Get last version
                last_version = metadata.versions[-1]
                last_hash = last_version["content_hash"]
                last_size = last_version["size_bytes"]
                
                # Check if changed
                has_changed = current_hash != last_hash
                
                if has_changed:
                    self.stats["total_changes_detected"] += 1
                    
                    # Analyze change
                    size_change = current_size - last_size
                    size_change_pct = (size_change / last_size * 100) if last_size > 0 else 0
                    
                    change_info = {
                        "changed": True,
                        "old_hash": last_hash,
                        "new_hash": current_hash,
                        "old_size": last_size,
                        "new_size": current_size,
                        "size_change": size_change,
                        "size_change_percent": size_change_pct,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    logger.info(
                        f"🔄 Change detected in {source_id}: "
                        f"size {size_change:+d} bytes ({size_change_pct:+.1f}%)"
                    )
                else:
                    change_info = {
                        "changed": False,
                        "hash": current_hash,
                        "size": current_size
                    }
                
                changes[source_id] = change_info
                
                # Update last checked time
                metadata.last_checked = datetime.utcnow().isoformat()
                
            except Exception as e:
                logger.error(f"❌ Error checking source {source_id}: {e}")
                metadata.error_count += 1
                self.stats["total_errors"] += 1
                changes[source_id] = {
                    "changed": False,
                    "error": str(e)
                }
        
        self._save_metadata()
        return changes
    
    def refresh_source(
        self,
        source_id: str,
        change_info: Dict[str, Any]
    ) -> bool:
        """
        Refresh a specific source.
        
        Args:
            source_id: Source to refresh
            change_info: Information about the change
            
        Returns:
            True if refresh succeeded
        """
        if source_id not in self.sources:
            logger.error(f"❌ Source not found: {source_id}")
            return False
        
        metadata = self.sources[source_id]
        
        try:
            logger.info(f"🔄 Refreshing source: {source_id}")
            
            # Execute refresh callback
            if self.on_refresh_callback:
                self.on_refresh_callback([source_id])
            
            # Create new version
            new_version = SourceVersion(
                version_number=metadata.current_version + 1,
                content_hash=change_info["new_hash"],
                timestamp=datetime.utcnow().isoformat(),
                size_bytes=change_info["new_size"],
                change_summary=self._generate_change_summary(change_info)
            )
            
            # Update metadata
            metadata.current_version += 1
            metadata.versions.append(asdict(new_version))
            metadata.last_refreshed = datetime.utcnow().isoformat()
            metadata.refresh_count += 1
            
            # Keep only last 10 versions
            if len(metadata.versions) > 10:
                metadata.versions = metadata.versions[-10:]
            
            self._save_metadata()
            self.stats["total_refreshes"] += 1
            
            logger.info(f"✅ Refreshed source: {source_id} (v{metadata.current_version})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh source {source_id}: {e}")
            metadata.error_count += 1
            self.stats["total_errors"] += 1
            return False
    
    def refresh_all_changed(self) -> Dict[str, Any]:
        """
        Check all sources and refresh those that changed.
        
        Returns:
            Dictionary with refresh results
        """
        logger.info("🔄 Checking all sources for changes...")
        
        # Check for changes
        changes = self.check_for_changes()
        
        # Refresh changed sources
        changed_sources = [
            sid for sid, info in changes.items()
            if info.get("changed", False)
        ]
        
        if not changed_sources:
            logger.info("✅ No changes detected")
            return {
                "success": True,
                "message": "No changes detected",
                "sources_checked": len(changes),
                "sources_refreshed": 0
            }
        
        # Refresh in priority order
        sources_by_priority = sorted(
            changed_sources,
            key=lambda sid: self.sources[sid].priority
        )
        
        refreshed = []
        failed = []
        
        for source_id in sources_by_priority:
            change_info = changes[source_id]
            if self.refresh_source(source_id, change_info):
                refreshed.append(source_id)
            else:
                failed.append(source_id)
        
        logger.info(
            f"✅ Refresh complete: {len(refreshed)} refreshed, "
            f"{len(failed)} failed"
        )
        
        return {
            "success": True,
            "message": f"Refreshed {len(refreshed)} sources",
            "sources_checked": len(changes),
            "sources_refreshed": len(refreshed),
            "sources_failed": len(failed),
            "refreshed": refreshed,
            "failed": failed,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def rollback_source(
        self,
        source_id: str,
        target_version: Optional[int] = None
    ) -> bool:
        """
        Rollback a source to a previous version.
        
        Args:
            source_id: Source to rollback
            target_version: Version to rollback to (None = previous version)
            
        Returns:
            True if rollback succeeded
        """
        if source_id not in self.sources:
            logger.error(f"❌ Source not found: {source_id}")
            return False
        
        metadata = self.sources[source_id]
        
        if len(metadata.versions) < 2:
            logger.error(f"❌ No previous versions available for {source_id}")
            return False
        
        try:
            # Determine target version
            if target_version is None:
                target_version = metadata.current_version - 1
            
            # Find target version in history
            target_version_data = None
            for version in metadata.versions:
                if version["version_number"] == target_version:
                    target_version_data = version
                    break
            
            if not target_version_data:
                logger.error(f"❌ Version {target_version} not found for {source_id}")
                return False
            
            logger.info(f"🔄 Rolling back {source_id} to v{target_version}")
            
            # Execute rollback via refresh callback
            if self.on_refresh_callback:
                self.on_refresh_callback([source_id])
            
            # Update metadata
            metadata.current_version = target_version
            metadata.last_refreshed = datetime.utcnow().isoformat()
            
            self._save_metadata()
            
            logger.info(f"✅ Rolled back {source_id} to v{target_version}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to rollback {source_id}: {e}")
            return False
    
    def start_auto_refresh(self):
        """Start automatic refresh monitoring."""
        if self._running:
            logger.warning("Auto-refresh already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()
        
        logger.info("✅ Started auto-refresh monitoring")
    
    def stop_auto_refresh(self):
        """Stop automatic refresh monitoring."""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        
        logger.info("✅ Stopped auto-refresh monitoring")
    
    def _refresh_loop(self):
        """Background loop for automatic refresh."""
        logger.info("🔄 Auto-refresh loop started")
        
        while self._running:
            try:
                # Refresh all changed sources
                self.refresh_all_changed()
                
                # Sleep until next check
                time.sleep(self.check_interval_seconds)
                
            except Exception as e:
                logger.error(f"❌ Error in refresh loop: {e}")
                time.sleep(60)  # Wait 1 minute on error
        
        logger.info("🔄 Auto-refresh loop stopped")
    
    def _calculate_hash_and_size(
        self,
        source_type: str,
        location: str
    ) -> tuple[str, int]:
        """
        Calculate content hash and size for a source.
        
        Returns:
            Tuple of (hash, size_in_bytes)
        """
        if source_type == 'file':
            return self._hash_file(location)
        elif source_type == 'directory':
            return self._hash_directory(location)
        elif source_type == 'url':
            return self._hash_url(location)
        elif source_type == 'api':
            return self._hash_api(location)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
    
    def _hash_file(self, file_path: str) -> tuple[str, int]:
        """Calculate hash and size of file."""
        path = Path(file_path)
        if not path.exists():
            return "", 0
        
        size = path.stat().st_size
        
        hasher = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hasher.update(chunk)
        
        return hasher.hexdigest(), size
    
    def _hash_directory(self, dir_path: str) -> tuple[str, int]:
        """Calculate hash and total size of directory."""
        path = Path(dir_path)
        if not path.exists():
            return "", 0
        
        hasher = hashlib.sha256()
        total_size = 0
        
        # Sort files for consistent hashing
        files = sorted(path.rglob('*'))
        
        for file_path in files:
            if file_path.is_file():
                file_hash, file_size = self._hash_file(str(file_path))
                hasher.update(str(file_path).encode())
                hasher.update(file_hash.encode())
                total_size += file_size
        
        return hasher.hexdigest(), total_size
    
    def _hash_url(self, url: str) -> tuple[str, int]:
        """Calculate hash and size of URL content."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            content = response.content
            size = len(content)
            
            hasher = hashlib.sha256()
            hasher.update(content)
            
            return hasher.hexdigest(), size
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch URL {url}: {e}")
            return "", 0
    
    def _hash_api(self, api_endpoint: str) -> tuple[str, int]:
        """Calculate hash and size of API response."""
        # Same as URL for now, but could be extended for API-specific logic
        return self._hash_url(api_endpoint)
    
    def _generate_change_summary(self, change_info: Dict[str, Any]) -> str:
        """Generate human-readable change summary."""
        size_change = change_info.get("size_change", 0)
        size_change_pct = change_info.get("size_change_percent", 0)
        
        if size_change > 0:
            return f"Content added: +{size_change} bytes (+{size_change_pct:.1f}%)"
        elif size_change < 0:
            return f"Content removed: {size_change} bytes ({size_change_pct:.1f}%)"
        else:
            return "Content modified (same size)"
    
    def _load_metadata(self) -> Dict[str, EnhancedSourceMetadata]:
        """Load source metadata from disk."""
        if not self.metadata_path.exists():
            return {}
        
        try:
            with open(self.metadata_path, 'r') as f:
                data = json.load(f)
            
            return {
                source_id: EnhancedSourceMetadata(**meta)
                for source_id, meta in data.items()
            }
        except Exception as e:
            logger.error(f"❌ Failed to load metadata: {e}")
            return {}
    
    def _save_metadata(self):
        """Save source metadata to disk."""
        try:
            data = {
                source_id: asdict(meta)
                for source_id, meta in self.sources.items()
            }
            
            with open(self.metadata_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"❌ Failed to save metadata: {e}")
    
    def get_source_info(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a source."""
        if source_id not in self.sources:
            return None
        
        metadata = self.sources[source_id]
        
        return {
            "source_id": metadata.source_id,
            "source_type": metadata.source_type,
            "location": metadata.location,
            "current_version": metadata.current_version,
            "version_history": metadata.versions,
            "last_checked": metadata.last_checked,
            "last_refreshed": metadata.last_refreshed,
            "refresh_count": metadata.refresh_count,
            "error_count": metadata.error_count,
            "enabled": metadata.enabled,
            "priority": metadata.priority,
            "tags": metadata.tags
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get refresh statistics."""
        return {
            "total_sources": len(self.sources),
            "enabled_sources": sum(1 for m in self.sources.values() if m.enabled),
            "auto_refresh_running": self._running,
            "check_interval_seconds": self.check_interval_seconds,
            "last_refresh": self._last_refresh_time.isoformat() if self._last_refresh_time else None,
            **self.stats,
            "sources_by_priority": {
                "high": sum(1 for m in self.sources.values() if m.priority == 1),
                "medium": sum(1 for m in self.sources.values() if m.priority == 2),
                "low": sum(1 for m in self.sources.values() if m.priority == 3)
            }
        }
    
    def generate_report(self) -> str:
        """Generate detailed refresh report."""
        stats = self.get_stats()
        
        lines = [
            "=" * 70,
            "Knowledge Base Auto-Refresh Report",
            "=" * 70,
            f"Generated: {datetime.utcnow().isoformat()}",
            "",
            "Statistics:",
            f"  Total Sources: {stats['total_sources']}",
            f"  Enabled Sources: {stats['enabled_sources']}",
            f"  Auto-Refresh: {'Running' if stats['auto_refresh_running'] else 'Stopped'}",
            f"  Check Interval: {stats['check_interval_seconds']}s",
            "",
            "Activity:",
            f"  Total Checks: {stats['total_checks']}",
            f"  Changes Detected: {stats['total_changes_detected']}",
            f"  Total Refreshes: {stats['total_refreshes']}",
            f"  Total Errors: {stats['total_errors']}",
            "",
            "Sources by Priority:",
            f"  High (1): {stats['sources_by_priority']['high']}",
            f"  Medium (2): {stats['sources_by_priority']['medium']}",
            f"  Low (3): {stats['sources_by_priority']['low']}",
            "",
            "Sources:",
            ""
        ]
        
        for source_id, metadata in self.sources.items():
            status = "✅" if metadata.enabled else "❌"
            lines.append(f"{status} {source_id}")
            lines.append(f"   Type: {metadata.source_type}")
            lines.append(f"   Version: {metadata.current_version}")
            lines.append(f"   Refreshes: {metadata.refresh_count}")
            lines.append(f"   Errors: {metadata.error_count}")
            lines.append(f"   Last Refreshed: {metadata.last_refreshed}")
            lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)


# Global instance
_enhanced_refresher: Optional[EnhancedKnowledgeBaseRefresher] = None


def get_enhanced_refresher(
    on_refresh_callback: Optional[Callable] = None,
    check_interval_seconds: int = 3600
) -> EnhancedKnowledgeBaseRefresher:
    """Get or create global enhanced refresher instance."""
    global _enhanced_refresher
    if _enhanced_refresher is None:
        _enhanced_refresher = EnhancedKnowledgeBaseRefresher(
            on_refresh_callback=on_refresh_callback,
            check_interval_seconds=check_interval_seconds
        )
    return _enhanced_refresher
