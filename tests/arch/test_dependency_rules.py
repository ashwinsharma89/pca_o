
import os
import ast
import logging
from typing import Dict, List, Set, Tuple

# Architecture Definition
# Layer rules: key = layer, value = set of allowed layers to import from
LAYERS = {
    'core': set(),
    'platform': {'core'},
    'engine': {'core', 'platform'},
    'interface': {'core', 'platform', 'engine', 'ops', 'enterprise'}, # Interface wires everything
    'ops': {'core', 'platform'},  # Ops observes Platform/Core. Ideally not Interface (cycle).
    'quality': {'core', 'platform', 'engine', 'interface'},
    'enterprise': {'core', 'platform', 'engine'}, # Enterprise extends base logic
    'workers': {'core', 'platform', 'engine'},  # Workers are background jobs using core infra
}

# Post-Migration Mapping: Current nested directories -> Layer
# Now that files are in src/platform/ingestion, src/engine/agents, etc.
CURRENT_TO_FUTURE_LAYER = {
    # Core layer (src/core/*)
    'core': 'core',
    
    # Platform layer (src/platform/*)
    'platform': 'platform',
    
    # Engine layer (src/engine/*)
    'engine': 'engine',
    
    # Interface layer (src/interface/*)
    'interface': 'interface',
    
    # Ops layer (src/ops/*)
    'ops': 'ops',
    
    # Quality layer (src/quality/*)
    'quality': 'quality',
    
    # Enterprise layer (src/enterprise/*)
    'enterprise': 'enterprise',
    
    # Workers (standalone)
    'workers': 'workers',
    
    # Legacy unmoved (should be cleaned up)
    'events': 'core',
    'interfaces': 'core',
    'utils': 'core',
}

class ImportVisitor(ast.NodeVisitor):
    def __init__(self):
        self.imports = set()

    def visit_Import(self, node):
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module:
            self.imports.add(node.module)
        self.generic_visit(node)

def get_file_imports(filepath: str) -> Set[str]:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        tree = ast.parse(content)
        visitor = ImportVisitor()
        visitor.visit(tree)
        return visitor.imports
    except Exception as e:
        # print(f"Error parsing {filepath}: {e}")
        return set()

def analyze_dependencies(src_root: str) -> List[str]:
    violations = []
    
    for root, dirs, files in os.walk(src_root):
        # Determine current layer based on top-level folder
        rel_path = os.path.relpath(root, src_root)
        top_folder = rel_path.split(os.sep)[0]
        
        if top_folder == '.':
            continue
            
        current_layer = CURRENT_TO_FUTURE_LAYER.get(top_folder)

        # Special case: core/di is wiring (Interface layer)
        if rel_path.startswith(os.path.join('core', 'di')):
            current_layer = 'interface'
        
        # If folder not mapped, skip (or legacy/unknown)
        if not current_layer:
            continue

        allowed_deps = LAYERS.get(current_layer, set())
        
        for file in files:
            if not file.endswith('.py'):
                continue
                
            filepath = os.path.join(root, file)
            imports = get_file_imports(filepath)
            
            for imp in imports:
                # Check for src.* imports
                if imp.startswith('src.'):
                    parts = imp.split('.')
                    if len(parts) > 1:
                        imported_module = parts[1]
                        target_layer = CURRENT_TO_FUTURE_LAYER.get(imported_module)
                        
                        if target_layer and target_layer != current_layer:
                            # Check rule
                            if target_layer not in allowed_deps:
                                violations.append(
                                    f"[VIOLATION] {current_layer.upper()} ({top_folder}) imports {target_layer.upper()} ({imported_module}) "
                                    f"in {os.path.relpath(filepath, src_root)} -> import {imp}"
                                )
                # Also check relative imports? (Harder to track without full resolution)
                # For now, strict 'src.' check is good baseline

    return violations

if __name__ == "__main__":
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src"))
    print(f"Scanning dependencies in: {src_path}")
    
    violations = analyze_dependencies(src_path)
    
    if violations:
        print(f"Found {len(violations)} architectural violations:")
        for v in violations:
            print(v)
        print("\n\u274c FAILED: Architectural integrity validation failed.")
        exit(1)
    else:
        print("\u2705 PASSED: No architectural violations found.")
        exit(0)
