#!/usr/bin/env python3
"""
Comprehensive PDF Generator - Converts ALL markdown content to PDF
Includes all text, analogies, diagrams, and proper formatting
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle, KeepTogether
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT
from reportlab.pdfgen import canvas
from pathlib import Path
import re

# Configuration
GUIDE_DIR = Path("guide")
PDF_OUTPUT = Path("guide/pdf_output")
PDF_OUTPUT.mkdir(exist_ok=True)
OUTPUT_FILE = PDF_OUTPUT / "PCA_Agent_Complete_Guide.pdf"

class NumberedCanvas(canvas.Canvas):
    """Custom canvas with page numbers"""
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        self.setFont("Helvetica", 9)
        self.setFillColor(colors.grey)
        self.drawRightString(7.5*inch, 0.5*inch, f"Page {self._pageNumber} of {page_count}")
        self.drawString(inch, 0.5*inch, "PCA Agent Documentation")

def parse_markdown_to_flowables(md_file, styles):
    """Parse markdown file and convert to ReportLab flowables"""
    flowables = []
    
    if not md_file.exists():
        return flowables
    
    with open(md_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split into lines
    lines = content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip navigation links
        if 'Navigation:' in line or '← ' in line or ' →' in line:
            i += 1
            continue
        
        # Chapter titles (bold text)
        if line.startswith('**CHAPTER') and line.endswith('**'):
            text = line.replace('**', '')
            flowables.append(Paragraph(text, styles['ChapterTitle']))
            flowables.append(Spacer(1, 0.2*inch))
            i += 1
            continue
        
        # H2 headers (##)
        if line.startswith('## '):
            text = line.replace('## ', '')
            flowables.append(Paragraph(text, styles['CustomH2']))
            flowables.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # H3 headers (###)
        if line.startswith('### '):
            text = line.replace('### ', '')
            flowables.append(Paragraph(text, styles['CustomH3']))
            flowables.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # H4 headers (####)
        if line.startswith('#### '):
            text = line.replace('#### ', '')
            flowables.append(Paragraph(text, styles['CustomH4']))
            flowables.append(Spacer(1, 0.05*inch))
            i += 1
            continue
        
        # Blockquotes (office analogies)
        if line.startswith('> '):
            quote_lines = []
            while i < len(lines) and (lines[i].strip().startswith('> ') or lines[i].strip() == '>'):
                quote_lines.append(lines[i].strip().replace('> ', ''))
                i += 1
            quote_text = ' '.join(quote_lines)
            # Clean up markdown formatting
            quote_text = quote_text.replace('**', '<b>').replace('**', '</b>')
            flowables.append(Paragraph(quote_text, styles['Blockquote']))
            flowables.append(Spacer(1, 0.1*inch))
            continue
        
        # Images
        if line.startswith('![') and '](' in line:
            # Extract image path
            match = re.search(r'!\[.*?\]\((.*?)\)', line)
            if match:
                img_path = match.group(1)
                # Convert relative path to absolute
                if img_path.startswith('../'):
                    img_path = GUIDE_DIR / img_path.replace('../', '')
                else:
                    img_path = GUIDE_DIR / img_path
                
                if img_path.exists():
                    try:
                        img = Image(str(img_path), width=6*inch, height=4*inch)
                        flowables.append(img)
                        flowables.append(Spacer(1, 0.2*inch))
                    except:
                        pass
            i += 1
            continue
        
        # Tables
        if line.startswith('|') and '|' in line:
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                table_lines.append(lines[i].strip())
                i += 1
            
            if len(table_lines) > 2:  # Header + separator + data
                # Parse table
                rows = []
                for tline in table_lines:
                    if '---' not in tline:  # Skip separator
                        cells = [cell.strip() for cell in tline.split('|')[1:-1]]
                        rows.append(cells)
                
                if rows:
                    table = Table(rows)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2563eb')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                    ]))
                    flowables.append(table)
                    flowables.append(Spacer(1, 0.2*inch))
            continue
        
        # Horizontal rules
        if line.startswith('---'):
            flowables.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # Bullet lists
        if line.startswith('- ') or line.startswith('* '):
            list_items = []
            while i < len(lines) and (lines[i].strip().startswith('- ') or lines[i].strip().startswith('* ')):
                item = lines[i].strip()[2:]
                # Clean markdown
                item = item.replace('**', '<b>').replace('**', '</b>')
                item = item.replace('*', '<i>').replace('*', '</i>')
                list_items.append(item)
                i += 1
            
            for item in list_items:
                flowables.append(Paragraph(f"• {item}", styles['CustomBody']))
            flowables.append(Spacer(1, 0.1*inch))
            continue
        
        # Numbered lists
        if re.match(r'^\d+\. ', line):
            list_items = []
            while i < len(lines) and re.match(r'^\d+\. ', lines[i].strip()):
                item = re.sub(r'^\d+\. ', '', lines[i].strip())
                item = item.replace('**', '<b>').replace('**', '</b>')
                list_items.append(item)
                i += 1
            
            for idx, item in enumerate(list_items, 1):
                flowables.append(Paragraph(f"{idx}. {item}", styles['CustomBody']))
            flowables.append(Spacer(1, 0.1*inch))
            continue
        
        # Regular paragraphs
        if line and not line.startswith('#'):
            # Clean markdown formatting
            text = line.replace('**', '<b>').replace('**', '</b>')
            text = text.replace('*', '<i>').replace('*', '</i>')
            text = text.replace('`', '<font name="Courier">')
            text = text.replace('`', '</font>')
            
            if text:
                flowables.append(Paragraph(text, styles['CustomBody']))
                flowables.append(Spacer(1, 0.05*inch))
        
        i += 1
    
    return flowables

def create_comprehensive_pdf():
    """Generate comprehensive PDF from all markdown files"""
    
    # Create PDF
    doc = SimpleDocTemplate(
        str(OUTPUT_FILE),
        pagesize=letter,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72,
    )
    
    # Define styles
    styles = getSampleStyleSheet()
    
    styles.add(ParagraphStyle(
        name='ChapterTitle',
        parent=styles['Heading1'],
        fontSize=22,
        textColor=colors.HexColor('#2563eb'),
        spaceAfter=20,
        spaceBefore=20,
        fontName='Helvetica-Bold',
        alignment=TA_CENTER,
        borderWidth=2,
        borderColor=colors.HexColor('#2563eb'),
        borderPadding=10,
        backColor=colors.HexColor('#eff6ff')
    ))
    
    styles.add(ParagraphStyle(
        name='CustomH2',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#1f2937'),
        spaceAfter=12,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CustomH3',
        parent=styles['Heading3'],
        fontSize=14,
        textColor=colors.HexColor('#2563eb'),
        spaceAfter=10,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CustomH4',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#374151'),
        spaceAfter=8,
        spaceBefore=8,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        alignment=TA_JUSTIFY,
        spaceAfter=12,
        leading=16,
        textColor=colors.HexColor('#1f2937')
    ))
    
    styles.add(ParagraphStyle(
        name='Blockquote',
        parent=styles['BodyText'],
        fontSize=10,
        leftIndent=20,
        rightIndent=20,
        spaceAfter=12,
        spaceBefore=12,
        textColor=colors.HexColor('#4b5563'),
        backColor=colors.HexColor('#eff6ff'),
        borderColor=colors.HexColor('#2563eb'),
        borderWidth=1,
        borderPadding=10,
        fontName='Helvetica-Oblique'
    ))
    
    # Container for flowables
    elements = []
    
    # Title Page
    elements.append(Spacer(1, 2*inch))
    title_style = ParagraphStyle(
        'Title',
        parent=styles['Heading1'],
        fontSize=28,
        textColor=colors.HexColor('#2563eb'),
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
        spaceAfter=20
    )
    elements.append(Paragraph("PCA AGENT", title_style))
    elements.append(Paragraph("Complete Technical Guide", title_style))
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph("A Comprehensive Documentation for Laymen and Experts", styles['BodyText']))
    elements.append(Spacer(1, 0.2*inch))
    elements.append(Paragraph("With Office Analogies, Technical Deep-Dives, and Visual Diagrams", styles['BodyText']))
    elements.append(Spacer(1, 0.3*inch))
    elements.append(Paragraph("January 2026", styles['BodyText']))
    elements.append(PageBreak())
    
    # Process each chapter
    chapters = [
        GUIDE_DIR / "chapters" / "01_introduction.md",
        GUIDE_DIR / "chapters" / "02_architecture_overview.md",
        GUIDE_DIR / "chapters" / "03_layer1_api_gateway.md",
        GUIDE_DIR / "chapters" / "04_layer2_query_understanding.md",
    ]
    
    for chapter_file in chapters:
        print(f"Processing: {chapter_file.name}")
        chapter_elements = parse_markdown_to_flowables(chapter_file, styles)
        elements.extend(chapter_elements)
        elements.append(PageBreak())
    
    # Appendix
    appendix_file = GUIDE_DIR / "appendix_a_glossary.md"
    if appendix_file.exists():
        print(f"Processing: {appendix_file.name}")
        appendix_elements = parse_markdown_to_flowables(appendix_file, styles)
        elements.extend(appendix_elements)
    
    # Build PDF with custom canvas for page numbers
    doc.build(elements, canvasmaker=NumberedCanvas)
    
    print(f"\n✅ PDF created: {OUTPUT_FILE}")
    print(f"   File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")
    print(f"   Pages: Estimated {len(elements) // 20}")
    return OUTPUT_FILE

if __name__ == "__main__":
    print("=" * 70)
    print("Generating Comprehensive PDF Documentation")
    print("=" * 70)
    print()
    pdf_file = create_comprehensive_pdf()
    print()
    print("=" * 70)
    print(f"✅ Complete! Opening PDF...")
    print("=" * 70)
    
    import subprocess
    subprocess.run(["open", str(pdf_file)])
