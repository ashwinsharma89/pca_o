# Architecture Diagram using Python 'diagrams' library
# To run this: pip install diagrams && python architecture_diagrams.py

from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import FastAPI, React
from diagrams.onprem.database import Neo4J, PostgreSQL
from diagrams.onprem.client import User
from diagrams.programming.flowchart import Action, Inspection

with Diagram("PCA Agent Architecture", show=False, direction="LR"):
    user = User("User")
    
    with Cluster("Frontend"):
        ui = React("Next.js UI")
    
    with Cluster("Backend API"):
        api = FastAPI("API Gateway")
        
    with Cluster("Orchestration Engine"):
        orchestrator = Action("Query Orchestrator")
        router = Inspection("Multi-Agent Router")
        
    with Cluster("Agent Layer"):
        reasoning = Action("Reasoning Agent")
        viz = Action("Viz Agent")
        b2b = Action("B2B Agent")
        
        agents = [reasoning, viz, b2b]
        
    with Cluster("Data Layer"):
        kg = Neo4J("Knowledge Graph")
        db = PostgreSQL("Analytics DB")

    # Flow
    user >> ui >> api >> orchestrator
    
    orchestrator >> Edge(label="Ambiguous?") >> router
    router >> agents
    
    agents >> kg
    agents >> db
