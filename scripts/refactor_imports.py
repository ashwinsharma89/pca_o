import os
import re

def refactor_imports():
    root_dirs = ['src', 'tests']
    
    # Mapping of core modules
    core_modules = ['config', 'database', 'utils', 'cache', 'di']
    
    count = 0
    for root_dir in root_dirs:
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                if file.endswith('.py'):
                    path = os.path.join(root, file)
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        new_content = content
                        
                        # 1. Fix absolute imports: src.xxx -> src.core.xxx
                        for mod in core_modules:
                            # Matches 'from src.mod' or 'import src.mod' or 'patch("src.mod'
                            # Captures the keyword/prefix and the mandatory whitespace
                            pattern = re.compile(rf'((?:from|import|patch\("?|patch\(\'?)(\s+))src\.{mod}\b')
                            new_content = pattern.sub(rf'\1src.core.{mod}', new_content)
                        
                        # 2. Fix relative imports: ..xxx -> src.core.xxx
                        for mod in core_modules:
                            # from ..mod -> from src.core.mod
                            pattern = re.compile(rf'(from\s+)\.\.{mod}\b')
                            new_content = pattern.sub(rf'\1src.core.{mod}', new_content)
                            
                            pattern = re.compile(rf'(from\s+)\.\.{mod}\.')
                            new_content = pattern.sub(rf'\1src.core.{mod}.', new_content)

                        # 3. Fix deeper relative imports: ...xxx -> src.core.xxx
                        for mod in core_modules:
                            pattern = re.compile(rf'(from\s+)\.\.\.{mod}\b')
                            new_content = pattern.sub(rf'\1src.core.{mod}', new_content)
                            
                            pattern = re.compile(rf'(from\s+)\.\.\.{mod}\.')
                            new_content = pattern.sub(rf'\1src.core.{mod}.', new_content)
                        
                        if new_content != content:
                            with open(path, 'w', encoding='utf-8') as f:
                                f.write(new_content)
                            print(f"Updated {path}")
                            count += 1
                    except Exception as e:
                        print(f"Skipping {path}: {e}")
    print(f"Updated {count} files.")

if __name__ == '__main__':
    refactor_imports()
