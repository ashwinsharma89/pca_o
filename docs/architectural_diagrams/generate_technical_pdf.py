from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib import colors
# SimpleDocTemplate allows better flow control, but Canvas is fine for absolute positioning of full page headers
from reportlab.platypus import SimpleDocTemplate, Image as ReportLabImage, PageBreak, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import os

def generate_technical_pdf():
    base_dir = "docs/architectural_diagrams"
    output_path = os.path.join(base_dir, "PCA_Agent_Technical_Schematics.pdf")
    
    # Define Content
    charts = [
        {
            "title": "Chart A: Architecture Swimlanes",
            "subtitle": "Functional Topology & Component Flow",
            "file": "plantuml_chart_a_arch.png"
        },
        {
            "title": "Chart B: Agent Swarm (21 Agents)",
            "subtitle": "Management, Perception, Analysis, Channel & Synthesis Layers",
            "file": "plantuml_chart_b_agents.png"
        },
        {
            "title": "Chart C: Data Physics",
            "subtitle": "Ingestion Pipeline, Storage Mechanics & Unified Retrieval",
            "file": "plantuml_chart_c_data.png"
        },
        {
            "title": "Chart D: Layer Latency Topology",
            "subtitle": "Time Costs: Perception -> Deep Reasoning -> Data Fetch",
            "file": "plantuml_chart_d_latency.png"
        },
        {
            "title": "Chart E: Library Stack",
            "subtitle": "Core Dependency Flow (Next.js -> FastAPI -> LangChain)",
            "file": "plantuml_chart_e_libs.png"
        },
        {
            "title": "Chart F: The Full Tech Universe",
            "subtitle": "Comprehensive Stack including Data Processing & Observability",
            "file": "plantuml_chart_f_full_libs.png"
        },
        {
            "title": "Chart G: Observability Pipeline",
            "subtitle": "Logs (Loguru) + Metrics (Prometheus) + Traces (OTel) + Errors (Sentry)",
            "file": "plantuml_chart_g_observability.png"
        },
        {
            "title": "Chart H: Trace Lifecycle",
            "subtitle": "Distributed Trace Flow (User -> API -> Orch -> Loguru)",
            "file": "plantuml_chart_h_tracing.png"
        }
    ]
    
    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(letter),
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=30
    )
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'MainTitle',
        parent=styles['Title'],
        fontSize=28,
        spaceAfter=30,
        textColor=colors.darkblue
    )
    chart_title_style = ParagraphStyle(
        'ChartTitle',
        parent=styles['Heading1'],
        fontSize=20,
        alignment=1, # Center
        spaceAfter=10,
        textColor=colors.black
    )
    chart_subtitle_style = ParagraphStyle(
        'ChartSubtitle',
        parent=styles['Normal'],
        fontSize=14,
        alignment=1, # Center
        spaceAfter=20,
        textColor=colors.grey
    )
    
    story = []
    
    # Title Page
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph("PCA Agent", title_style))
    story.append(Paragraph("Technical Schematics Report", title_style))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("Generated via PlantUML & Antigravity", chart_subtitle_style))
    story.append(PageBreak())
    
    # Charts
    for chart in charts:
        img_path = os.path.join(base_dir, chart["file"])
        if os.path.exists(img_path):
            story.append(Paragraph(chart["title"], chart_title_style))
            story.append(Paragraph(chart["subtitle"], chart_subtitle_style))
            
            # Dynamic scaling
            try:
                # Max width/height for landscape letter
                max_w = 9.5 * inch
                max_h = 6 * inch
                
                img = ReportLabImage(img_path)
                
                # Get original size
                img_width = img.drawWidth
                img_height = img.drawHeight
                
                # Calculate scale
                scale = min(max_w/img_width, max_h/img_height)
                
                # Apply scale if needed, but don't upscale too much
                if scale < 1.0:
                    img.drawWidth = img_width * scale
                    img.drawHeight = img_height * scale
                elif scale > 1.0 and scale < 2.0: # Slight upscale allowed
                    img.drawWidth = img_width * scale
                    img.drawHeight = img_height * scale
                    
                story.append(img)
            except Exception as e:
                story.append(Paragraph(f"Error loading image: {str(e)}", styles["Normal"]))
        else:
            story.append(Paragraph(f"Image not found: {chart['file']}", styles["Normal"]))
            
        story.append(PageBreak())
        
    doc.build(story)
    print(f"Generated PDF at {output_path}")

if __name__ == "__main__":
    generate_technical_pdf()
