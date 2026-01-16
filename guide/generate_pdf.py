#!/usr/bin/env python3
"""
PDF Generator for PCA Agent Documentation
Converts markdown documentation to professional PDF with styling
"""

import os
import subprocess
from pathlib import Path

# Configuration
GUIDE_DIR = Path("guide")
OUTPUT_DIR = Path("guide/pdf_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Pandoc template for professional styling
PANDOC_OPTIONS = [
    "--pdf-engine=xelatex",
    "--toc",  # Table of contents
    "--toc-depth=3",
    "--number-sections",
    "-V", "geometry:margin=1in",
    "-V", "fontsize=11pt",
    "-V", "documentclass=report",
    "-V", "colorlinks=true",
    "-V", "linkcolor=blue",
    "-V", "urlcolor=blue",
    "-V", "toccolor=blue",
]

def generate_combined_pdf():
    """Generate a single PDF from all chapters"""
    
    # Order of chapters
    chapters = [
        "chapters/01_introduction.md",
        "chapters/02_architecture_overview.md",
        "chapters/03_layer1_api_gateway.md",
        "chapters/04_layer2_query_understanding.md",
        # Add more as they're created
    ]
    
    # Appendices
    appendices = [
        "appendix_a_glossary.md",
    ]
    
    # Combine all markdown files
    all_files = chapters + appendices
    input_files = [str(GUIDE_DIR / f) for f in all_files if (GUIDE_DIR / f).exists()]
    
    output_file = OUTPUT_DIR / "PCA_Agent_Complete_Guide.pdf"
    
    print(f"Generating PDF: {output_file}")
    print(f"Input files: {len(input_files)}")
    
    cmd = [
        "pandoc",
        *input_files,
        "-o", str(output_file),
        *PANDOC_OPTIONS,
        "--metadata", "title=PCA Agent: Complete Technical Guide",
        "--metadata", "author=PCA Agent Development Team",
        "--metadata", "date=January 2026",
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✅ PDF generated successfully: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"❌ Error generating PDF:")
        print(e.stderr)
        return None

def generate_individual_pdfs():
    """Generate individual PDFs for each chapter"""
    
    chapters = list((GUIDE_DIR / "chapters").glob("*.md"))
    
    for chapter in chapters:
        output_file = OUTPUT_DIR / f"{chapter.stem}.pdf"
        
        cmd = [
            "pandoc",
            str(chapter),
            "-o", str(output_file),
            *PANDOC_OPTIONS,
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"✅ Generated: {output_file.name}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error generating {chapter.name}:")
            print(e.stderr)

if __name__ == "__main__":
    print("=" * 60)
    print("PCA Agent Documentation PDF Generator")
    print("=" * 60)
    
    print("\n1. Generating combined PDF...")
    combined_pdf = generate_combined_pdf()
    
    print("\n2. Generating individual chapter PDFs...")
    generate_individual_pdfs()
    
    print("\n" + "=" * 60)
    print("PDF Generation Complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)
