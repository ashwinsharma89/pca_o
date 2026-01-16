#!/usr/bin/env python3
"""
PDF Generation Helper for PCA Agent Documentation
Opens the HTML in browser for easy PDF export
"""

import subprocess
from pathlib import Path
import webbrowser

# Configuration
HTML_DIR = Path("guide/html_output")
combined_html = HTML_DIR / "PCA_Agent_Complete_Guide.html"

print("=" * 70)
print("PCA Agent Documentation - PDF Export Helper")
print("=" * 70)

if combined_html.exists():
    print(f"\n✅ Opening documentation in your browser...")
    print(f"   File: {combined_html}")
    
    # Open in default browser
    webbrowser.open(f"file://{combined_html.absolute()}")
    
    print("\n📄 To save as PDF:")
    print("   1. Press Cmd+P (or Ctrl+P on Windows)")
    print("   2. Select 'Save as PDF' as the destination")
    print("   3. Click 'Save'")
    print("\n✨ Features in the PDF:")
    print("   • Page numbers in bottom-right corner")
    print("   • Document title in bottom-left")
    print("   • Each chapter starts on a new page")
    print("   • All diagrams and charts included")
    print("   • Clickable table of contents")
    print("   • Professional formatting")
    
    print(f"\n💡 Recommended PDF filename:")
    print(f"   PCA_Agent_Complete_Guide.pdf")
    print(f"\n   Save to: guide/pdf_output/")
    
else:
    print(f"\n❌ HTML file not found: {combined_html}")
    print("   Please run: python3 guide/generate_html.py")

print("\n" + "=" * 70)
