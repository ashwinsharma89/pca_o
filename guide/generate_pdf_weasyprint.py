#!/usr/bin/env python3
"""
Professional PDF Generator for PCA Agent Documentation
Uses WeasyPrint for high-quality PDF generation with proper page breaks and styling
"""

import subprocess
from pathlib import Path
import sys

# Configuration
GUIDE_DIR = Path("guide")
HTML_DIR = Path("guide/html_output")
PDF_DIR = Path("guide/pdf_output")
PDF_DIR.mkdir(exist_ok=True)

def install_weasyprint():
    """Install WeasyPrint if not already installed"""
    try:
        import weasyprint
        print("✅ WeasyPrint already installed")
        return True
    except ImportError:
        print("📦 Installing WeasyPrint...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "weasyprint"], check=True)
            print("✅ WeasyPrint installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install WeasyPrint")
            return False

def generate_pdf_from_html(html_file, pdf_file):
    """Generate PDF from HTML using WeasyPrint"""
    try:
        from weasyprint import HTML, CSS
        
        print(f"📄 Generating PDF: {pdf_file.name}")
        
        # Custom CSS for PDF-specific styling
        pdf_css = CSS(string='''
            @page {
                size: letter;
                margin: 0.75in;
                @bottom-right {
                    content: "Page " counter(page);
                    font-size: 9pt;
                    color: #6b7280;
                }
                @bottom-left {
                    content: "PCA Agent Documentation";
                    font-size: 9pt;
                    color: #6b7280;
                }
            }
            
            h1 {
                page-break-before: always;
            }
            
            h1:first-of-type {
                page-break-before: avoid;
            }
            
            .mermaid, pre, table {
                page-break-inside: avoid;
            }
        ''')
        
        HTML(filename=str(html_file)).write_pdf(
            str(pdf_file),
            stylesheets=[pdf_css]
        )
        
        print(f"✅ PDF generated: {pdf_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error generating PDF: {e}")
        return False

def main():
    print("=" * 60)
    print("PCA Agent Documentation - PDF Generator")
    print("=" * 60)
    
    # Install WeasyPrint if needed
    if not install_weasyprint():
        print("\n⚠️  WeasyPrint installation failed.")
        print("Falling back to browser-based PDF generation.")
        print("Please open the HTML file and use Print → Save as PDF")
        return
    
    # Generate PDF from combined HTML
    combined_html = HTML_DIR / "PCA_Agent_Complete_Guide.html"
    combined_pdf = PDF_DIR / "PCA_Agent_Complete_Guide.pdf"
    
    if combined_html.exists():
        print(f"\n📖 Processing: {combined_html.name}")
        if generate_pdf_from_html(combined_html, combined_pdf):
            print(f"\n🎉 Success! PDF created at:")
            print(f"   {combined_pdf}")
            print(f"\nTo open:")
            print(f"   open {combined_pdf}")
    else:
        print(f"❌ HTML file not found: {combined_html}")
        print("Please run generate_html.py first")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
