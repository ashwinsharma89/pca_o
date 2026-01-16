#!/usr/bin/env python3
"""
HTML Documentation Generator for PCA Agent
Creates professional, styled HTML documentation that looks like a PDF
"""

import subprocess
from pathlib import Path

# Configuration
GUIDE_DIR = Path("guide")
OUTPUT_DIR = Path("guide/html_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Custom CSS for professional styling with page numbers
CSS_TEMPLATE = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
    
    :root {
        --primary-color: #2563eb;
        --text-color: #1f2937;
        --bg-color: #ffffff;
        --border-color: #e5e7eb;
        --code-bg: #f3f4f6;
        --heading-color: #111827;
    }
    
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    
    body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-size: 11pt;
        line-height: 1.7;
        color: var(--text-color);
        background: var(--bg-color);
        max-width: 8.5in;
        margin: 0 auto;
        padding: 0.75in;
        counter-reset: page;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        font-weight: 700;
        color: var(--heading-color);
        margin-top: 1.5em;
        margin-bottom: 0.75em;
        line-height: 1.3;
    }
    
    h1 {
        font-size: 28pt;
        font-weight: 900;
        border-bottom: 3px solid var(--primary-color);
        padding-bottom: 0.3em;
        margin-top: 0;
        page-break-before: always;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* First h1 shouldn't have page break */
    h1:first-of-type {
        page-break-before: avoid;
    }
    
    h2 {
        font-size: 20pt;
        font-weight: 700;
        border-bottom: 2px solid var(--border-color);
        padding-bottom: 0.3em;
    }
    
    h3 {
        font-size: 16pt;
        font-weight: 700;
        color: var(--primary-color);
    }
    
    h4 {
        font-size: 13pt;
        font-weight: 600;
    }
    
    /* Paragraphs */
    p {
        margin-bottom: 1em;
        text-align: justify;
    }
    
    /* Links */
    a {
        color: var(--primary-color);
        text-decoration: none;
        border-bottom: 1px solid transparent;
        transition: border-color 0.2s;
    }
    
    a:hover {
        border-bottom-color: var(--primary-color);
    }
    
    /* Lists */
    ul, ol {
        margin-left: 1.5em;
        margin-bottom: 1em;
    }
    
    li {
        margin-bottom: 0.5em;
    }
    
    /* Tables */
    table {
        width: 100%;
        border-collapse: collapse;
        margin: 1.5em 0;
        font-size: 10pt;
    }
    
    th {
        background: var(--primary-color);
        color: white;
        padding: 12px;
        text-align: left;
        font-weight: 600;
    }
    
    td {
        padding: 10px 12px;
        border-bottom: 1px solid var(--border-color);
    }
    
    tr:hover {
        background: #f9fafb;
    }
    
    /* Code blocks */
    code {
        font-family: 'JetBrains Mono', 'Courier New', monospace;
        background: var(--code-bg);
        padding: 2px 6px;
        border-radius: 3px;
        font-size: 10pt;
    }
    
    pre {
        background: var(--code-bg);
        padding: 1em;
        border-radius: 6px;
        overflow-x: auto;
        margin: 1em 0;
        border-left: 4px solid var(--primary-color);
    }
    
    pre code {
        background: none;
        padding: 0;
    }
    
    /* Blockquotes */
    blockquote {
        border-left: 4px solid var(--primary-color);
        padding-left: 1em;
        margin: 1em 0;
        font-style: italic;
        color: #4b5563;
        background: #eff6ff;
        padding: 1em;
        border-radius: 4px;
    }
    
    /* Horizontal rules */
    hr {
        border: none;
        border-top: 2px solid var(--border-color);
        margin: 2em 0;
    }
    
    /* Navigation */
    .navigation {
        background: #f9fafb;
        padding: 1em;
        border-radius: 6px;
        margin: 2em 0;
        text-align: center;
        border: 1px solid var(--border-color);
    }
    
    .navigation a {
        margin: 0 1em;
        font-weight: 600;
    }
    
    /* Table of Contents */
    #TOC {
        background: #f9fafb;
        padding: 1.5em;
        border-radius: 6px;
        margin: 2em 0;
        border: 1px solid var(--border-color);
    }
    
    #TOC ul {
        list-style: none;
        margin-left: 0;
    }
    
    #TOC li {
        margin: 0.3em 0;
    }
    
    #TOC a {
        color: var(--text-color);
    }
    
    #TOC > ul > li > a {
        font-weight: 600;
        font-size: 12pt;
    }
    
    /* Terminology boxes */
    .terminology {
        background: #eff6ff;
        border-left: 4px solid var(--primary-color);
        padding: 1em;
        margin: 1em 0;
        border-radius: 4px;
    }
    
    .terminology strong {
        color: var(--primary-color);
        font-size: 11pt;
    }
    
    /* Mermaid diagrams */
    .mermaid {
        background: white;
        padding: 1.5em;
        margin: 1.5em 0;
        border-radius: 6px;
        border: 1px solid var(--border-color);
        text-align: center;
    }
    
    /* Print styles with page numbers */
    @media print {
        @page {
            margin: 0.75in;
            @bottom-right {
                content: "Page " counter(page);
                font-family: 'Inter', sans-serif;
                font-size: 9pt;
                color: #6b7280;
            }
            @bottom-left {
                content: "PCA Agent Documentation";
                font-family: 'Inter', sans-serif;
                font-size: 9pt;
                color: #6b7280;
            }
        }
        
        body {
            max-width: none;
            padding: 0;
            counter-increment: page;
        }
        
        .navigation {
            display: none;
        }
        
        a {
            color: var(--text-color);
            border-bottom: none;
        }
        
        a[href^="http"]:after {
            content: " (" attr(href) ")";
            font-size: 8pt;
            color: #6b7280;
        }
        
        h1, h2, h3 {
            page-break-after: avoid;
        }
        
        h1 {
            page-break-before: always;
        }
        
        h1:first-of-type {
            page-break-before: avoid;
        }
        
        pre, table, .terminology, .mermaid {
            page-break-inside: avoid;
        }
        
        /* Page numbers in TOC */
        #TOC a::after {
            content: leader('.') target-counter(attr(href), page);
            float: right;
        }
    }
</style>

<!-- Mermaid.js for diagram rendering -->
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
  mermaid.initialize({ 
    startOnLoad: true,
    theme: 'default',
    themeVariables: {
      primaryColor: '#2563eb',
      primaryTextColor: '#fff',
      primaryBorderColor: '#1e40af',
      lineColor: '#6b7280',
      secondaryColor: '#f3f4f6',
      tertiaryColor: '#eff6ff'
    }
  });
</script>
"""

def generate_html_docs():
    """Generate styled HTML documentation"""
    
    # Copy diagrams to output directory
    import shutil
    diagrams_src = GUIDE_DIR / "diagrams"
    diagrams_dst = OUTPUT_DIR / "diagrams"
    
    if diagrams_src.exists():
        if diagrams_dst.exists():
            shutil.rmtree(diagrams_dst)
        shutil.copytree(diagrams_src, diagrams_dst)
        print(f"📁 Copied diagrams to output")
    
    # Order of chapters
    chapters = [
        ("chapters/01_introduction.md", "Chapter 1: Introduction & Overview"),
        ("chapters/02_architecture_overview.md", "Chapter 2: The Six-Layer Architecture"),
        ("chapters/03_layer1_api_gateway.md", "Chapter 3: Layer 1 - API Gateway"),
        ("chapters/04_layer2_query_understanding.md", "Chapter 4: Layer 2 - Query Understanding"),
        ("chapters/05_layer3_data_retrieval.md", "Chapter 5: Layer 3 - Data Retrieval"),
        ("chapters/06_layer4_analysis.md", "Chapter 6: Layer 4 - Analysis"),
        ("chapters/07_layer5_visualization.md", "Chapter 7: Layer 5 - Visualization"),
        ("chapters/08_layer6_response.md", "Chapter 8: Layer 6 - Response Formatting"),
    ]
    
    appendices = [
        ("appendix_a_glossary.md", "Appendix A: Glossary"),
    ]
    
    all_docs = chapters + appendices
    
    print("Generating HTML documentation...")
    print("=" * 60)
    
    # Generate individual HTML files
    for md_file, title in all_docs:
        input_path = GUIDE_DIR / md_file
        if not input_path.exists():
            continue
            
        output_name = input_path.stem + ".html"
        output_path = OUTPUT_DIR / output_name
        
        cmd = [
            "pandoc",
            str(input_path),
            "-o", str(output_path),
            "-s",  # Standalone HTML
            "--toc",  # Table of contents
            "--toc-depth=3",
            "--metadata", f"title={title}",
            "--metadata", "date=January 2026",
            "-H", "/dev/stdin",  # Custom CSS via stdin
        ]
        
        try:
            result = subprocess.run(
                cmd,
                input=CSS_TEMPLATE,
                text=True,
                capture_output=True,
                check=True
            )
            
            # Post-process to fix Mermaid diagrams
            fix_mermaid_in_html(output_path)
            
            # Fix diagram paths in individual files
            fix_image_paths(output_path)
            
            print(f"✅ Generated: {output_name}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error generating {md_file}:")
            print(e.stderr)
    
    # Generate combined HTML
    print("\nGenerating combined documentation...")
    all_input_files = [str(GUIDE_DIR / f) for f, _ in all_docs if (GUIDE_DIR / f).exists()]
    combined_output = OUTPUT_DIR / "PCA_Agent_Complete_Guide.html"
    
    cmd = [
        "pandoc",
        *all_input_files,
        "-o", str(combined_output),
        "-s",
        "--toc",
        "--toc-depth=3",
        "--metadata", "title=PCA Agent: Complete Technical Guide",
        "--metadata", "author=PCA Agent Development Team",
        "--metadata", "date=January 2026",
        "-H", "/dev/stdin",
    ]
    
    try:
        subprocess.run(
            cmd,
            input=CSS_TEMPLATE,
            text=True,
            capture_output=True,
            check=True
        )
        
        # Post-process to fix Mermaid diagrams
        fix_mermaid_in_html(combined_output)
        
        # Fix image paths in combined HTML
        fix_image_paths(combined_output)
        
        print(f"✅ Generated: PCA_Agent_Complete_Guide.html")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error generating combined HTML:")
        print(e.stderr)
    
    print("\n" + "=" * 60)
    print("HTML Generation Complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    print("\nTo view:")
    print(f"  open {combined_output}")
    print("\nTo print to PDF:")
    print(f"  Open in browser → Print → Save as PDF")
    print("=" * 60)

def fix_image_paths(html_file):
    """Fix image paths to be relative to HTML file"""
    import re
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace ../diagrams/ with diagrams/
    content = content.replace('src="../diagrams/', 'src="diagrams/')
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(content)

def fix_mermaid_in_html(html_file):
    """Post-process HTML to convert Mermaid code blocks to divs"""
    import re
    
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace <pre><code class="language-mermaid">...</code></pre> with <div class="mermaid">...</div>
    # This regex finds mermaid code blocks and converts them
    pattern = r'<pre[^>]*><code[^>]*class="[^"]*\blanguage-mermaid\b[^"]*"[^>]*>(.*?)</code></pre>'
    
    def replace_mermaid(match):
        mermaid_code = match.group(1)
        # Unescape HTML entities
        mermaid_code = mermaid_code.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
        return f'<div class="mermaid">\n{mermaid_code}\n</div>'
    
    content = re.sub(pattern, replace_mermaid, content, flags=re.DOTALL)
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    generate_html_docs()
