# Knowledge Base Auto-Refresh Guide

## Overview

The Knowledge Base Auto-Refresh system automatically monitors knowledge sources for changes and triggers re-ingestion when updates are detected. This ensures your knowledge base always has the latest information.

---

## Features

### 1. **Source Change Detection** ✅
- Content hashing for change detection
- File, directory, URL, and API monitoring
- Efficient change detection (no unnecessary refreshes)

### 2. **Version Tracking** ✅
- Full version history (last 10 versions)
- Change summaries
- Rollback capability

### 3. **Automatic Re-ingestion** ✅
- Automatic refresh on change detection
- Priority-based refresh scheduling
- Background refresh processing

### 4. **Freshness Monitoring** ✅
- Last checked timestamps
- Last refreshed timestamps
- Refresh statistics

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│         Knowledge Base Auto-Refresh System              │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │         Enhanced Refresher                         │ │
│  │  - Source Registration                             │ │
│  │  - Change Detection                                │ │
│  │  - Version Tracking                                │ │
│  │  - Auto-Refresh Loop                               │ │
│  └────────────────────────────────────────────────────┘ │
│                          │                               │
└──────────────────────────┼───────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
    ┌──────────────────┐      ┌──────────────────┐
    │  Knowledge       │      │  Vector Store    │
    │  Sources         │      │  (ChromaDB)      │
    │  - Files         │      │                  │
    │  - URLs          │      │  - Documents     │
    │  - Directories   │      │  - Embeddings    │
    │  - APIs          │      │  - Metadata      │
    └──────────────────┘      └──────────────────┘
```

---

## Setup

### 1. Initialize Refresher

```python
from src.knowledge.enhanced_auto_refresh import get_enhanced_refresher

# Define refresh callback
def on_refresh(source_ids):
    """Called when sources need to be refreshed."""
    for source_id in source_ids:
        # Re-ingest source
        ingest_source(source_id)

# Initialize refresher
refresher = get_enhanced_refresher(
    on_refresh_callback=on_refresh,
    check_interval_seconds=3600  # Check every hour
)
```

### 2. Register Sources

```python
# Register a file
refresher.register_source(
    source_id="campaign_guide",
    source_type="file",
    location="/path/to/campaign_guide.pdf",
    priority=1,  # High priority
    tags=["documentation", "campaigns"]
)

# Register a directory
refresher.register_source(
    source_id="knowledge_docs",
    source_type="directory",
    location="/path/to/docs/",
    priority=2,  # Medium priority
    tags=["documentation"]
)

# Register a URL
refresher.register_source(
    source_id="api_docs",
    source_type="url",
    location="https://api.example.com/docs",
    priority=1,
    tags=["api", "documentation"]
)

# Register an API endpoint
refresher.register_source(
    source_id="benchmark_data",
    source_type="api",
    location="https://api.example.com/benchmarks",
    priority=2,
    tags=["benchmarks", "data"]
)
```

### 3. Start Auto-Refresh

```python
# Start automatic monitoring
refresher.start_auto_refresh()

# The refresher will now:
# 1. Check sources every hour
# 2. Detect changes via content hashing
# 3. Automatically refresh changed sources
# 4. Track versions and statistics
```

---

## Usage

### Manual Change Check

```python
# Check all sources
changes = refresher.check_for_changes()

# Check specific sources
changes = refresher.check_for_changes(["campaign_guide", "api_docs"])

# Result:
# {
#     "campaign_guide": {
#         "changed": True,
#         "old_hash": "abc123...",
#         "new_hash": "def456...",
#         "size_change": 1024,
#         "size_change_percent": 5.2
#     },
#     "api_docs": {
#         "changed": False,
#         "hash": "xyz789...",
#         "size": 10240
#     }
# }
```

### Manual Refresh

```python
# Refresh all changed sources
result = refresher.refresh_all_changed()

# Result:
# {
#     "success": True,
#     "message": "Refreshed 2 sources",
#     "sources_checked": 4,
#     "sources_refreshed": 2,
#     "refreshed": ["campaign_guide", "benchmark_data"],
#     "failed": []
# }
```

### Version Rollback

```python
# Rollback to previous version
refresher.rollback_source("campaign_guide")

# Rollback to specific version
refresher.rollback_source("campaign_guide", target_version=3)
```

### Get Source Info

```python
# Get detailed source information
info = refresher.get_source_info("campaign_guide")

# Result:
# {
#     "source_id": "campaign_guide",
#     "source_type": "file",
#     "location": "/path/to/campaign_guide.pdf",
#     "current_version": 5,
#     "version_history": [
#         {
#             "version_number": 1,
#             "content_hash": "abc123...",
#             "timestamp": "2024-01-01T00:00:00",
#             "size_bytes": 10240,
#             "change_summary": "Initial version"
#         },
#         # ... more versions
#     ],
#     "last_checked": "2024-01-15T10:30:00",
#     "last_refreshed": "2024-01-15T09:00:00",
#     "refresh_count": 4,
#     "error_count": 0,
#     "enabled": True,
#     "priority": 1,
#     "tags": ["documentation", "campaigns"]
# }
```

### Get Statistics

```python
# Get refresh statistics
stats = refresher.get_stats()

# Result:
# {
#     "total_sources": 10,
#     "enabled_sources": 9,
#     "auto_refresh_running": True,
#     "check_interval_seconds": 3600,
#     "total_checks": 150,
#     "total_changes_detected": 25,
#     "total_refreshes": 23,
#     "total_errors": 2,
#     "sources_by_priority": {
#         "high": 4,
#         "medium": 5,
#         "low": 1
#     }
# }
```

### Generate Report

```python
# Generate detailed report
report = refresher.generate_report()
print(report)

# Output:
# ======================================================================
# Knowledge Base Auto-Refresh Report
# ======================================================================
# Generated: 2024-01-15T10:30:00
#
# Statistics:
#   Total Sources: 10
#   Enabled Sources: 9
#   Auto-Refresh: Running
#   Check Interval: 3600s
#
# Activity:
#   Total Checks: 150
#   Changes Detected: 25
#   Total Refreshes: 23
#   Total Errors: 2
#
# Sources:
# ✅ campaign_guide
#    Type: file
#    Version: 5
#    Refreshes: 4
#    Last Refreshed: 2024-01-15T09:00:00
# ...
```

---

## API Endpoints

### Register Source

```bash
POST /api/knowledge/refresh/register
Content-Type: application/json

{
  "source_id": "campaign_guide",
  "source_type": "file",
  "location": "/path/to/file.pdf",
  "priority": 1,
  "tags": ["documentation"]
}
```

### Check for Changes

```bash
GET /api/knowledge/refresh/check?source_ids=source1,source2
```

### Trigger Refresh

```bash
POST /api/knowledge/refresh/refresh
Content-Type: application/json

{
  "source_ids": ["source1", "source2"]  # Optional
}
```

### Rollback Source

```bash
POST /api/knowledge/refresh/rollback
Content-Type: application/json

{
  "source_id": "campaign_guide",
  "target_version": 3  # Optional
}
```

### Get Source Info

```bash
GET /api/knowledge/refresh/source/{source_id}
```

### List All Sources

```bash
GET /api/knowledge/refresh/sources
```

### Get Statistics

```bash
GET /api/knowledge/refresh/stats
```

### Start/Stop Auto-Refresh

```bash
POST /api/knowledge/refresh/start
POST /api/knowledge/refresh/stop
```

### Enable/Disable Source

```bash
PATCH /api/knowledge/refresh/source/{source_id}/enable
PATCH /api/knowledge/refresh/source/{source_id}/disable
```

---

## Change Detection

### How It Works

1. **Content Hashing**: Each source is hashed using SHA-256
2. **Comparison**: Current hash is compared with last known hash
3. **Change Detection**: If hashes differ, change is detected
4. **Refresh Trigger**: Changed sources are queued for refresh

### Supported Source Types

| Type | Description | Hash Method |
|------|-------------|-------------|
| `file` | Single file | File content hash |
| `directory` | Directory of files | Combined hash of all files |
| `url` | Web URL | HTTP response content hash |
| `api` | API endpoint | API response content hash |

---

## Version Tracking

### Version History

Each source maintains a history of up to 10 versions:

```python
{
  "version_number": 5,
  "content_hash": "abc123...",
  "timestamp": "2024-01-15T09:00:00",
  "size_bytes": 10240,
  "change_summary": "Content added: +1024 bytes (+11.1%)"
}
```

### Change Summaries

Automatic change summaries:
- "Content added: +1024 bytes (+10.0%)"
- "Content removed: -512 bytes (-5.0%)"
- "Content modified (same size)"

---

## Priority-Based Refresh

Sources are refreshed in priority order:

1. **Priority 1 (High)**: Critical sources (refreshed first)
2. **Priority 2 (Medium)**: Important sources
3. **Priority 3 (Low)**: Nice-to-have sources (refreshed last)

```python
# High priority source
refresher.register_source(
    source_id="critical_docs",
    source_type="file",
    location="/path/to/critical.pdf",
    priority=1  # Refreshed first
)

# Low priority source
refresher.register_source(
    source_id="optional_docs",
    source_type="file",
    location="/path/to/optional.pdf",
    priority=3  # Refreshed last
)
```

---

## Best Practices

### 1. Set Appropriate Check Intervals

```python
# For frequently updated sources
refresher = get_enhanced_refresher(check_interval_seconds=900)  # 15 minutes

# For rarely updated sources
refresher = get_enhanced_refresher(check_interval_seconds=86400)  # 24 hours
```

### 2. Use Priority Levels

```python
# Critical documentation: Priority 1
refresher.register_source("api_docs", "url", "...", priority=1)

# Reference material: Priority 3
refresher.register_source("archive", "directory", "...", priority=3)
```

### 3. Tag Your Sources

```python
refresher.register_source(
    "campaign_guide",
    "file",
    "/path/to/file.pdf",
    tags=["documentation", "campaigns", "v2024"]
)
```

### 4. Monitor Statistics

```python
# Regularly check stats
stats = refresher.get_stats()

if stats["total_errors"] > 10:
    logger.warning("High error rate in knowledge refresh")

if stats["total_changes_detected"] == 0:
    logger.info("No changes detected recently")
```

### 5. Handle Refresh Callback Properly

```python
def on_refresh(source_ids):
    """Proper refresh callback."""
    for source_id in source_ids:
        try:
            # Get source info
            info = refresher.get_source_info(source_id)
            
            # Re-ingest based on type
            if info["source_type"] == "file":
                ingest_file(info["location"])
            elif info["source_type"] == "url":
                ingest_url(info["location"])
            
            logger.info(f"✅ Refreshed {source_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh {source_id}: {e}")
```

---

## Troubleshooting

### Sources Not Being Checked

1. **Check if auto-refresh is running**:
```python
stats = refresher.get_stats()
print(stats["auto_refresh_running"])  # Should be True
```

2. **Verify source is enabled**:
```python
info = refresher.get_source_info("source_id")
print(info["enabled"])  # Should be True
```

### Changes Not Detected

1. **Manually check for changes**:
```python
changes = refresher.check_for_changes(["source_id"])
print(changes)
```

2. **Verify source location**:
```python
info = refresher.get_source_info("source_id")
print(info["location"])  # Verify path/URL is correct
```

### High Error Count

1. **Check error count**:
```python
info = refresher.get_source_info("source_id")
print(info["error_count"])
```

2. **Review logs** for error details

3. **Disable problematic source**:
```python
refresher.sources["source_id"].enabled = False
refresher._save_metadata()
```

---

## Performance Considerations

### Optimization Tips

1. **Adjust check intervals** based on update frequency
2. **Use priority levels** to refresh critical sources first
3. **Disable unused sources** to reduce overhead
4. **Monitor statistics** to identify bottlenecks

### Resource Usage

- **CPU**: Minimal (hashing only during checks)
- **Memory**: ~1MB per 100 sources
- **Disk**: ~10KB per source (metadata)
- **Network**: Only when checking URLs/APIs

---

## Conclusion

The Knowledge Base Auto-Refresh system provides:

- ✅ Automatic change detection
- ✅ Version tracking and history
- ✅ Automatic re-ingestion
- ✅ Priority-based refresh
- ✅ Rollback capability
- ✅ Comprehensive monitoring

**Status**: Production-ready knowledge base auto-refresh!
