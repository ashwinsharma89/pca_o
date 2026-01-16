#!/usr/bin/env python3
"""
Step 2 Import Refactoring Script - Karpathy + Dean Style
=========================================================
Reliable, predictable, zero-copy-paste-errors import migration.

Rules:
1. Use explicit mappings, no regex guessing.
2. Preserve file history (sed in-place).
3. Log every change for debugging.
"""

import os
import re
import sys
from pathlib import Path

# Explicit import path mappings: old -> new
IMPORT_MAPPINGS = {
    # Platform layer
    'src.ingestion': 'src.platform.ingestion',
    'src.query_engine': 'src.platform.query_engine',
    'src.knowledge': 'src.platform.knowledge',
    'src.connectors': 'src.platform.connectors',
    'src.models': 'src.platform.models',
    'src.data': 'src.platform.data',
    'src.data_processing': 'src.platform.data_processing',
    
    # Engine layer
    'src.agents': 'src.engine.agents',
    'src.analytics': 'src.engine.analytics',
    'src.predictive': 'src.engine.predictive',
    'src.orchestration': 'src.engine.orchestration',
    'src.intelligence': 'src.engine.intelligence',
    'src.visualization': 'src.engine.visualization',
    'src.voice': 'src.engine.voice',
    'src.services': 'src.engine.services',
    
    # Interface layer
    'src.api': 'src.interface.api',
    'src.gateway': 'src.interface.gateway',
    'src.mcp': 'src.interface.mcp',
    'src.mcp_server': 'src.interface.mcp_server',
    'src.di': 'src.interface.di',
    
    # Ops layer
    'src.monitoring': 'src.ops.monitoring',
    'src.observability': 'src.ops.observability',
    'src.chaos': 'src.ops.chaos',
    
    # Quality layer
    'src.evaluation': 'src.quality.evaluation',
    'src.feedback': 'src.quality.feedback',
    'src.testing': 'src.quality.testing',
    
    # Core consolidation (already done in Step 1, but verify)
    'src.backup': 'src.core.backup',
    'src.events': 'src.core.events',
    'src.interfaces': 'src.core.interfaces',
}

def refactor_file(filepath: Path, dry_run: bool = False) -> int:
    """Refactor imports in a single file. Returns count of changes."""
    try:
        content = filepath.read_text(encoding='utf-8')
    except Exception as e:
        print(f"  ⚠️  Could not read {filepath}: {e}")
        return 0
    
    original = content
    changes = 0
    
    # Sort by length descending to avoid partial replacements
    # e.g., 'src.api' before 'src.a'
    sorted_mappings = sorted(IMPORT_MAPPINGS.items(), key=lambda x: -len(x[0]))
    
    for old_path, new_path in sorted_mappings:
        # Match import statements
        # Pattern: from src.xxx or import src.xxx
        pattern = rf'\b{re.escape(old_path)}\b'
        if re.search(pattern, content):
            content = re.sub(pattern, new_path, content)
            changes += 1
    
    if changes > 0 and content != original:
        if not dry_run:
            filepath.write_text(content, encoding='utf-8')
        print(f"  ✅ {filepath.name}: {changes} import(s) updated")
        return changes
    
    return 0

def main():
    dry_run = '--dry-run' in sys.argv
    
    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    src_dir = project_root / 'src'
    tests_dir = project_root / 'tests'
    
    if not src_dir.exists():
        print(f"❌ Source directory not found: {src_dir}")
        sys.exit(1)
    
    print("=" * 60)
    print("🔧 Step 2: Import Refactoring (Platform & Engine)")
    print("=" * 60)
    if dry_run:
        print("⚠️  DRY RUN MODE - No files will be modified")
    print()
    
    total_changes = 0
    files_modified = 0
    
    # Process src directory
    print("📁 Processing src/...")
    for py_file in src_dir.rglob('*.py'):
        changes = refactor_file(py_file, dry_run)
        if changes > 0:
            total_changes += changes
            files_modified += 1
    
    # Process tests directory
    print("\n📁 Processing tests/...")
    for py_file in tests_dir.rglob('*.py'):
        changes = refactor_file(py_file, dry_run)
        if changes > 0:
            total_changes += changes
            files_modified += 1
    
    # Process scripts directory
    scripts_dir = project_root / 'scripts'
    if scripts_dir.exists():
        print("\n📁 Processing scripts/...")
        for py_file in scripts_dir.rglob('*.py'):
            if py_file.name != 'refactor_step2_imports.py':  # Don't modify self
                changes = refactor_file(py_file, dry_run)
                if changes > 0:
                    total_changes += changes
                    files_modified += 1
    
    print()
    print("=" * 60)
    print(f"📊 Summary: {total_changes} imports updated across {files_modified} files")
    if dry_run:
        print("   Run without --dry-run to apply changes")
    print("=" * 60)

if __name__ == '__main__':
    main()
