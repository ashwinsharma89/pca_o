from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import FastAPI, React
from diagrams.programming.flowchart import Action, Inspection, Database
from diagrams.onprem.database import PostgreSQL, Neo4J
from diagrams.onprem.client import User
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import SQS

# Elon Style: Minimalist, Functional, Dense
graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.0",
    "ranksep": "1.2",
    "fontname": "Sans-Serif"
}

with Diagram("Chart A - Architecture Swimlanes", show=False, filename="chart_a_architecture", direction="LR", graph_attr=graph_attr):
    user = User("User / Director")

    # Swimlane 1: Interface (The Skin)
    with Cluster("1. Interface (The Skin)"):
        ui = React("Next.js App\n(Visual Cortex)")
        api = FastAPI("API Gateway\n(Nervous System)")
    
    # Swimlane 2: Orchestration (The Brain Stem)
    with Cluster("2. Orchestration (The Brain Stem)"):
        router = Inspection("Router Agent\n(Gatekeeper)")
        graph = Lambda("LangGraph\n(State Machine)")
        synth = Inspection("Synthesizer\n(Compression)")

    # Swimlane 3: Intelligence (The Neural Net)
    with Cluster("3. Intelligence (The Neural Net)"):
        # Grouping for visual density
        with Cluster("Analyzers"):
            analyzers = [Action("Budget"), Action("Creative"), Action("Trend")]
        
        with Cluster("Specialists"):
            specialists = [Action("Google"), Action("Meta"), Action("LinkedIn")]

    # Swimlane 4: Memory (The Hippocampus)
    with Cluster("4. Memory (The Hippocampus)"):
        kg = Neo4J("Knowledge Graph\n(Neo4j)")
        db = PostgreSQL("Analytics DB\n(Postgres)")
        vec = Database("Vector Store")

    # Connection Logic (The Axons)
    user >> Edge(label="Query/Input", color="black") >> ui
    ui >> Edge(color="black") >> api
    api >> Edge(label="Request", color="darkblue") >> router
    
    router >> Edge(color="darkblue") >> graph
    graph >> Edge(color="darkgreen") >> analyzers
    graph >> Edge(color="darkgreen") >> specialists
    
    analyzers >> Edge(style="dashed") >> db
    specialists >> Edge(style="dashed") >> kg
    
    specialists >> vec
    
    analyzers >> synth
    specialists >> synth
    
    synth >> Edge(label="Response", color="black") >> api
