import pathlib
import re
import json

base = pathlib.Path('knowledge_sources')
pattern = re.compile(r'https?://[^\s)>"]+')
all_urls = set()
by_file = {}

for path in base.rglob('*.md'):
    text = path.read_text(encoding='utf-8')
    matches = pattern.findall(text)
    if matches:
        by_file[str(path)] = matches
        all_urls.update(matches)

result = {
    "total_unique_urls": len(all_urls),
    "files_scanned": len(by_file)
}
print(json.dumps(result, indent=2))
