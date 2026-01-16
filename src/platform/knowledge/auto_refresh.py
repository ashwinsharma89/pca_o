"""
Automatic Knowledge Base Refresh Mechanism.

Monitors knowledge sources and automatically refreshes the vector store when changes are detected.
"""

import os
import logging
import hashlib
import json
from typing import List, Dict, Any, Optional, Callable
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import threading
import time

logger = logging.getLogger(__name__)


@dataclass
class RefreshConfig:
    """
    Configuration for automatic refresh.
    
    Attributes:
        check_interval_seconds: How often to check for changes
        auto_refresh_enabled: Whether auto-refresh is enabled
        refresh_on_startup: Refresh on application startup
        max_refresh_attempts: Maximum retry attempts on failure
        refresh_cooldown_seconds: Minimum time between refreshes
    """
    check_interval_seconds: int = 3600  # 1 hour
    auto_refresh_enabled: bool = True
    refresh_on_startup: bool = True
    max_refresh_attempts: int = 3
    refresh_cooldown_seconds: int = 300  # 5 minutes


@dataclass
class SourceMetadata:
    """Metadata for a knowledge source."""
    source_id: str
    source_type: str  # 'url', 'file', 'directory'
    location: str
    last_hash: str
    last_checked: str
    last_refreshed: str
    refresh_count: int = 0
    error_count: int = 0


class KnowledgeBaseRefresher:
    """
    Automatic knowledge base refresh manager.
    
    Features:
    - Monitors knowledge sources for changes
    - Detects changes via content hashing
    - Automatically triggers refresh on changes
    - Configurable refresh intervals
    - Error handling and retry logic
    - Refresh history tracking
    """
    
    def __init__(
        self,
        config: RefreshConfig = RefreshConfig(),
        metadata_path: str = "./data/refresh_metadata.json",
        on_refresh_callback: Optional[Callable] = None
    ):
        """
        Initialize auto-refresh manager.
        
        Args:
            config: Refresh configuration
            metadata_path: Path to store refresh metadata
            on_refresh_callback: Callback function to execute on refresh
        """
        self.config = config
        self.metadata_path = Path(metadata_path)
        self.on_refresh_callback = on_refresh_callback
        
        # Create metadata directory
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing metadata
        self.sources: Dict[str, SourceMetadata] = self._load_metadata()
        
        # Refresh control
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_refresh_time: Optional[datetime] = None
        
        logger.info("✅ Initialized KnowledgeBaseRefresher")
        logger.info(f"   Check interval: {config.check_interval_seconds}s")
        logger.info(f"   Auto-refresh: {config.auto_refresh_enabled}")
        logger.info(f"   Tracked sources: {len(self.sources)}")
    
    def register_source(
        self,
        source_id: str,
        source_type: str,
        location: str
    ) -> bool:
        """
        Register a knowledge source for monitoring.
        
        Args:
            source_id: Unique identifier for source
            source_type: Type of source ('url', 'file', 'directory')
            location: Location of source (URL, file path, etc.)
            
        Returns:
            True if registered successfully
        """
        try:
            # Calculate initial hash
            content_hash = self._calculate_hash(source_type, location)
            
            metadata = SourceMetadata(
                source_id=source_id,
                source_type=source_type,
                location=location,
                last_hash=content_hash,
                last_checked=datetime.now().isoformat(),
                last_refreshed=datetime.now().isoformat()
            )
            
            self.sources[source_id] = metadata
            self._save_metadata()
            
            logger.info(f"✅ Registered source: {source_id} ({source_type})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to register source {source_id}: {e}")
            return False
    
    def unregister_source(self, source_id: str) -> bool:
        """Unregister a knowledge source."""
        if source_id in self.sources:
            del self.sources[source_id]
            self._save_metadata()
            logger.info(f"✅ Unregistered source: {source_id}")
            return True
        return False
    
    def check_for_changes(self) -> Dict[str, bool]:
        """
        Check all registered sources for changes.
        
        Returns:
            Dictionary mapping source_id to changed status
        """
        changes = {}
        
        for source_id, metadata in self.sources.items():
            try:
                # Calculate current hash
                current_hash = self._calculate_hash(
                    metadata.source_type,
                    metadata.location
                )
                
                # Check if changed
                has_changed = current_hash != metadata.last_hash
                changes[source_id] = has_changed
                
                # Update metadata
                metadata.last_checked = datetime.now().isoformat()
                
                if has_changed:
                    logger.info(f"🔄 Change detected in source: {source_id}")
                    metadata.last_hash = current_hash
                
            except Exception as e:
                logger.error(f"❌ Error checking source {source_id}: {e}")
                metadata.error_count += 1
                changes[source_id] = False
        
        self._save_metadata()
        return changes
    
    def _calculate_hash(self, source_type: str, location: str) -> str:
        """
        Calculate content hash for a source.
        
        Args:
            source_type: Type of source
            location: Location of source
            
        Returns:
            SHA256 hash of content
        """
        if source_type == 'file':
            return self._hash_file(location)
        elif source_type == 'directory':
            return self._hash_directory(location)
        elif source_type == 'url':
            return self._hash_url(location)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
    
    def _hash_file(self, file_path: str) -> str:
        """Calculate hash of file content."""
        path = Path(file_path)
        if not path.exists():
            return ""
        
        hasher = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def _hash_directory(self, dir_path: str) -> str:
        """Calculate hash of all files in directory."""
        path = Path(dir_path)
        if not path.exists():
            return ""
        
        hasher = hashlib.sha256()
        
        # Sort files for consistent hashing
        files = sorted(path.rglob('*'))
        
        for file_path in files:
            if file_path.is_file():
                # Hash file path and content
                hasher.update(str(file_path).encode())
                hasher.update(self._hash_file(str(file_path)).encode())
        
        return hasher.hexdigest()
    
    def _hash_url(self, url: str) -> str:
        """Calculate hash of URL content."""
        try:
            import requests
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            hasher = hashlib.sha256()
            hasher.update(response.content)
            return hasher.hexdigest()
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch URL {url}: {e}")
            return ""
    
    def trigger_refresh(self, source_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Manually trigger a refresh.
        
        Args:
            source_ids: Optional list of specific sources to refresh (None = all)
            
        Returns:
            Dictionary with refresh results
        """
        # Check cooldown
        if self._last_refresh_time:
            elapsed = (datetime.now() - self._last_refresh_time).total_seconds()
            if elapsed < self.config.refresh_cooldown_seconds:
                return {
                    'success': False,
                    'message': f'Refresh on cooldown ({int(self.config.refresh_cooldown_seconds - elapsed)}s remaining)',
                    'sources_refreshed': []
                }
        
        try:
            logger.info("🔄 Starting knowledge base refresh...")
            
            # Determine which sources to refresh
            if source_ids:
                sources_to_refresh = {
                    sid: meta for sid, meta in self.sources.items()
                    if sid in source_ids
                }
            else:
                sources_to_refresh = self.sources
            
            # Execute refresh callback
            if self.on_refresh_callback:
                result = self.on_refresh_callback(list(sources_to_refresh.keys()))
                
                # Update metadata
                for source_id in sources_to_refresh:
                    self.sources[source_id].last_refreshed = datetime.now().isoformat()
                    self.sources[source_id].refresh_count += 1
                
                self._save_metadata()
                self._last_refresh_time = datetime.now()
                
                logger.info(f"✅ Refresh complete: {len(sources_to_refresh)} sources")
                
                return {
                    'success': True,
                    'message': 'Refresh completed successfully',
                    'sources_refreshed': list(sources_to_refresh.keys()),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'success': False,
                    'message': 'No refresh callback configured',
                    'sources_refreshed': []
                }
                
        except Exception as e:
            logger.error(f"❌ Refresh failed: {e}")
            return {
                'success': False,
                'message': f'Refresh failed: {str(e)}',
                'sources_refreshed': []
            }
    
    def start_auto_refresh(self):
        """Start automatic refresh monitoring in background thread."""
        if not self.config.auto_refresh_enabled:
            logger.warning("Auto-refresh is disabled in configuration")
            return
        
        if self._running:
            logger.warning("Auto-refresh already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()
        
        logger.info("✅ Started auto-refresh monitoring")
        
        # Refresh on startup if configured
        if self.config.refresh_on_startup:
            self.trigger_refresh()
    
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
                # Check for changes
                changes = self.check_for_changes()
                
                # Trigger refresh if any changes detected
                changed_sources = [sid for sid, changed in changes.items() if changed]
                
                if changed_sources:
                    logger.info(f"🔄 Changes detected in {len(changed_sources)} sources")
                    self.trigger_refresh(changed_sources)
                
                # Sleep until next check
                time.sleep(self.config.check_interval_seconds)
                
            except Exception as e:
                logger.error(f"❌ Error in refresh loop: {e}")
                time.sleep(60)  # Wait 1 minute on error
        
        logger.info("🔄 Auto-refresh loop stopped")
    
    def _load_metadata(self) -> Dict[str, SourceMetadata]:
        """Load source metadata from disk."""
        if not self.metadata_path.exists():
            return {}
        
        try:
            with open(self.metadata_path, 'r') as f:
                data = json.load(f)
            
            return {
                source_id: SourceMetadata(**meta)
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
    
    def get_refresh_stats(self) -> Dict[str, Any]:
        """Get refresh statistics."""
        total_refreshes = sum(meta.refresh_count for meta in self.sources.values())
        total_errors = sum(meta.error_count for meta in self.sources.values())
        
        return {
            'total_sources': len(self.sources),
            'total_refreshes': total_refreshes,
            'total_errors': total_errors,
            'auto_refresh_enabled': self.config.auto_refresh_enabled,
            'auto_refresh_running': self._running,
            'last_refresh': self._last_refresh_time.isoformat() if self._last_refresh_time else None,
            'sources': [
                {
                    'source_id': meta.source_id,
                    'source_type': meta.source_type,
                    'refresh_count': meta.refresh_count,
                    'error_count': meta.error_count,
                    'last_refreshed': meta.last_refreshed
                }
                for meta in self.sources.values()
            ]
        }


# Global instance
_refresher: Optional[KnowledgeBaseRefresher] = None


def get_refresher(
    config: RefreshConfig = RefreshConfig(),
    on_refresh_callback: Optional[Callable] = None
) -> KnowledgeBaseRefresher:
    """Get or create global refresher instance."""
    global _refresher
    if _refresher is None:
        _refresher = KnowledgeBaseRefresher(
            config=config,
            on_refresh_callback=on_refresh_callback
        )
    return _refresher
