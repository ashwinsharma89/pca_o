"""Generate pending URL list for ingestion.
Uses knowledge_sources markdown files as source of truth and removes
URLs already ingested or already flagged as failed.
"""
import json
import pathlib
import re

BASE_DIR = pathlib.Path('knowledge_sources')
SUCCESS_FILE = pathlib.Path('SUCCESSFUL_INGESTIONS.md')
FAILED_FILE = pathlib.Path('FAILED_INGESTIONS.md')
OUTPUT_FILE = pathlib.Path('data/pending_urls.json')

URL_PATTERN = re.compile(r'https?://[^\s)>"]+')

def extract_urls_from_markdown(markdown_path: pathlib.Path):
    text = markdown_path.read_text(encoding='utf-8')
    return URL_PATTERN.findall(text)

def extract_urls_from_list_file(path: pathlib.Path):
    urls = set()
    if not path.exists():
        return urls
    text = path.read_text(encoding='utf-8')
    urls.update(URL_PATTERN.findall(text))
    return urls

def main():
    # Gather URLs from success and failure lists
    success_urls = extract_urls_from_list_file(SUCCESS_FILE)
    failed_urls = extract_urls_from_list_file(FAILED_FILE)

    seen = set()
    pending_entries = []

    for md_path in sorted(BASE_DIR.rglob('*.md')):
        urls = extract_urls_from_markdown(md_path)
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            if url in success_urls or url in failed_urls:
                continue
            pending_entries.append({
                "url": url,
                "source_file": str(md_path),
                "category": md_path.stem,
                "description": f"{md_path.stem} source"
            })

    OUTPUT_FILE.write_text(json.dumps(pending_entries, indent=2), encoding='utf-8')
    summary = {
        "total_unique_urls": len(seen),
        "success_urls": len(success_urls),
        "failed_urls": len(failed_urls),
        "pending_urls": len(pending_entries),
        "output": str(OUTPUT_FILE)
    }
    print(json.dumps(summary, indent=2))

if __name__ == '__main__':
    main()
