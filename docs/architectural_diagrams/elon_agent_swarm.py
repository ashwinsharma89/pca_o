from diagrams import Diagram, Cluster, Edge
from diagrams.programming.flowchart import Action, Inspection, Document
from diagrams.onprem.client import User

# Elon Style: High Density, No Fluff
graph_attr = {
    "fontsize": "20",
    "bgcolor": "#FAFAFA",
    "compound": "true",
    "splines": "spline"
}

with Diagram("Chart B - Agent Swarm (21 Agents)", show=False, filename="chart_b_agent_swarm", direction="TB", graph_attr=graph_attr):
    
    # Input
    trigger = User("Input Trigger")

    # 1. Management Layer (Orchestrators)
    with Cluster("Management Layer"):
        router = Inspection("Router Agent")
        monitor = Inspection("Monitor Agent")
        
    # 2. Perception Layer (Input Processing)
    with Cluster("Perception Layer"):
        vision = Action("Vision Agent")
        ocr = Action("OCR Specialist")
        extract = Action("Extraction Agent")
        validate = Action("Validation Agent")
    
    # 3. Analysis Layer (The Crunchers)
    with Cluster("Analysis Layer"):
        budget = Action("Budget Analyzer")
        creative = Action("Creative Analyzer")
        audience = Action("Audience Analyzer")
        trend = Action("Trend Analyzer")
        general = Action("General Analyzer")
        b2b = Action("B2B Specialist")
    
    # 4. Channel Layer (Platform Specialists)
    with Cluster("Channel Layer"):
        google = Action("Google Specialist")
        meta = Action("Meta Specialist")
        linkedin = Action("LinkedIn Specialist")
        tiktok = Action("TikTok Specialist")
        dv360 = Action("Programmatic Agent")
    
    # 5. Synthesis Layer (Output)
    with Cluster("Synthesis Layer"):
        recommender = Action("Recommender")
        chart_gen = Action("Chart Generator")
        report = Action("Report Agent")
        synthesizer = Action("Synthesizer")
        
    # Flow
    trigger >> router
    router >> vision >> ocr >> extract >> validate
    validate >> router
    
    router >> [budget, creative, audience, trend, general, b2b]
    router >> [google, meta, linkedin, tiktok, dv360]
    
    # Cross communication
    google >> recommender
    meta >> recommender
    budget >> recommender
    
    recommender >> chart_gen
    chart_gen >> report
    report >> synthesizer
