from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import React, FastAPI
from diagrams.programming.language import Python
from diagrams.onprem.database import PostgreSQL as Database

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "splines": "ortho"
}

with Diagram("Chart D - Layer Latency", show=False, filename="chart_d_layer_latency", direction="TB", graph_attr=graph_attr):
    
    with Cluster("L1: User Perception (<100ms)"):
        ui = React("Frontend UI")
    
    with Cluster("L2: Gateway (<50ms)"):
        api = FastAPI("API Processing")
        
    with Cluster("L3: Orchestration (100-300ms)"):
        orch = Python("Routing Logic")
        
    with Cluster("L4: Deep Reasoning (2s - 15s)"):
        agents = [Python("Step 1: Plan"), Python("Step 2: Tool Use"), Python("Step 3: Synth")]
        
    with Cluster("L5: Data Fetch (50ms - 500ms)"):
        data = Database("DB / Graph")

    # Time costs
    ui >> Edge(label="30ms (Net)", color="red") >> api
    api >> Edge(label="10ms", color="orange") >> orch
    orch >> Edge(label="Setup", color="orange") >> agents[0]
    
    agents[0] >> Edge(label="Thinking...", color="blue") >> agents[1]
    agents[1] >> Edge(label="Thinking...", color="blue") >> agents[2]
    
    agents[1] >> Edge(label="Query (200ms)", color="green") >> data
    
    agents[2] >> Edge(label="Stream (Total 4s)", color="red") >> ui
