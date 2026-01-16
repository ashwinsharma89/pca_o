#!/usr/bin/env python3
"""
Direct PDF Generator for PCA Agent Documentation
Creates a professional PDF directly from markdown with embedded diagrams
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT
from pathlib import Path
import markdown
from bs4 import BeautifulSoup
import re

# Configuration
GUIDE_DIR = Path("guide")
PDF_OUTPUT = Path("guide/pdf_output")
PDF_OUTPUT.mkdir(exist_ok=True)
OUTPUT_FILE = PDF_OUTPUT / "PCA_Agent_Complete_Guide.pdf"

def create_pdf():
    """Generate PDF with proper formatting"""
    
    # Create PDF
    doc = SimpleDocTemplate(
        str(OUTPUT_FILE),
        pagesize=letter,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72,
    )
    
    # Container for the 'Flowable' objects
    elements = []
    
    # Define styles
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#111827'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    chapter_style = ParagraphStyle(
        'ChapterTitle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=colors.HexColor('#2563eb'),
        spaceAfter=20,
        spaceBefore=20,
        fontName='Helvetica-Bold',
        borderWidth=2,
        borderColor=colors.HexColor('#2563eb'),
        borderPadding=10
    )
    
    heading2_style = ParagraphStyle(
        'CustomHeading2',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#1f2937'),
        spaceAfter=12,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        alignment=TA_JUSTIFY,
        spaceAfter=12,
        leading=14
    )
    
    # Title Page
    elements.append(Spacer(1, 2*inch))
    elements.append(Paragraph("PCA AGENT", title_style))
    elements.append(Paragraph("Complete Technical Guide", title_style))
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph("A Comprehensive Documentation for Laymen", body_style))
    elements.append(Spacer(1, 0.3*inch))
    elements.append(Paragraph("January 2026", body_style))
    elements.append(PageBreak())
    
    # Chapter 1
    elements.append(Paragraph("CHAPTER 1: INTRODUCTION & OVERVIEW", chapter_style))
    elements.append(Spacer(1, 0.2*inch))
    
    # Add content
    intro_text = """
    The Performance Campaign Analytics (PCA) Agent represents a paradigm shift in marketing analytics, 
    enabling natural language interaction with complex campaign data. This system eliminates the traditional 
    dependency on data analysts for routine queries by leveraging artificial intelligence.
    """
    elements.append(Paragraph(intro_text, body_style))
    elements.append(Spacer(1, 0.2*inch))
    
    # Add diagram
    diagram_path = GUIDE_DIR / "diagrams" / "six_layer_architecture.png"
    if diagram_path.exists():
        img = Image(str(diagram_path), width=6*inch, height=5*inch)
        elements.append(img)
        elements.append(Spacer(1, 0.2*inch))
    
    elements.append(PageBreak())
    
    # Chapter 2
    elements.append(Paragraph("CHAPTER 2: THE SIX-LAYER ARCHITECTURE", chapter_style))
    elements.append(Spacer(1, 0.2*inch))
    
    arch_text = """
    The PCA Agent employs a six-layer architecture where each layer has a single, well-defined responsibility. 
    This modular design ensures maintainability, testability, and scalability.
    """
    elements.append(Paragraph(arch_text, body_style))
    elements.append(Spacer(1, 0.2*inch))
    
    # Add layer communication diagram
    comm_diagram = GUIDE_DIR / "diagrams" / "layer_communication.png"
    if comm_diagram.exists():
        img = Image(str(comm_diagram), width=6*inch, height=3*inch)
        elements.append(img)
        elements.append(Spacer(1, 0.2*inch))
    
    # Add request flow diagram
    flow_diagram = GUIDE_DIR / "diagrams" / "request_flow.png"
    if flow_diagram.exists():
        img = Image(str(flow_diagram), width=6.5*inch, height=4*inch)
        elements.append(img)
        elements.append(Spacer(1, 0.2*inch))
    
    elements.append(PageBreak())
    
    # Chapter 3
    elements.append(Paragraph("CHAPTER 3: QUERY UNDERSTANDING", chapter_style))
    elements.append(Spacer(1, 0.2*inch))
    
    query_text = """
    Layer 2 implements two processing paths: the Bulletproof Path for common queries (fast, 5ms) 
    and the LLM Path for complex queries (flexible, 800ms). This dual approach optimizes for both 
    speed and capability.
    """
    elements.append(Paragraph(query_text, body_style))
    elements.append(Spacer(1, 0.2*inch))
    
    # Add bulletproof vs LLM diagram
    bp_diagram = GUIDE_DIR / "diagrams" / "bulletproof_vs_llm.png"
    if bp_diagram.exists():
        img = Image(str(bp_diagram), width=6*inch, height=4*inch)
        elements.append(img)
    
    # Build PDF
    doc.build(elements, onFirstPage=add_page_number, onLaterPages=add_page_number)
    
    print(f"✅ PDF created: {OUTPUT_FILE}")
    print(f"   File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")
    return OUTPUT_FILE

def add_page_number(canvas, doc):
    """Add page numbers to each page"""
    page_num = canvas.getPageNumber()
    text = f"Page {page_num}"
    canvas.saveState()
    canvas.setFont('Helvetica', 9)
    canvas.setFillColor(colors.grey)
    canvas.drawRightString(7.5*inch, 0.5*inch, text)
    canvas.drawString(inch, 0.5*inch, "PCA Agent Documentation")
    canvas.restoreState()

if __name__ == "__main__":
    print("=" * 60)
    print("Generating PDF Documentation")
    print("=" * 60)
    pdf_file = create_pdf()
    print("=" * 60)
    print(f"\n📄 To open PDF:")
    print(f"   open {pdf_file}")
