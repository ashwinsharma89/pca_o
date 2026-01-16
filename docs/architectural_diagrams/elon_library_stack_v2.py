from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import React, FastAPI
from diagrams.programming.language import Python, Javascript, Rust
from diagrams.onprem.database import PostgreSQL, Neo4J
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.container import Docker
from diagrams.onprem.monitoring import Prometheus, Sentry, Grafana
from diagrams.onprem.queue import Kafka
from diagrams.saas.chat import Slack

# Custom nodes (Generic) for missing icons
from diagrams.programming.flowchart import Database, Action, Inspection

graph_attr = {
    "fontsize": "26",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "0.6",
    "ranksep": "1.2",
    "pad": "0.5"
}

with Diagram("Chart F - The Full Tech Stack Universe", show=False, filename="chart_f_full_stack", direction="TB", graph_attr=graph_attr):
    
    with Cluster("1. User Experience"):
        nextjs = React("Next.js 14")
        tail = Javascript("TailwindCSS")
        charts = Javascript("Recharts")
        motion = Javascript("Framer Motion")
        
    with Cluster("2. Gateway & Security"):
        api = FastAPI("FastAPI")
        pyd = Python("Pydantic")
        rate = Action("SlowAPI")
        auth = Action("PyJWT/Bcrypt")
        vault = Action("Vault (hvac)")
        
        [rate, auth, vault] >> api

    with Cluster("3. Agent Orchestration"):
        l_graph = Python("LangGraph")
        l_chain = Python("LangChain")
        groq = Action("Groq (Inference)")
        
        l_graph - l_chain - groq

    with Cluster("4. Data Processing (The Heavy Lifting)"):
        polars = Rust("Polars")
        duck = Database("DuckDB (OLAP)")
        pandas = Python("Pandas 2.0")
        pandera = Python("Pandera (Validation)")
        sqlglot = Python("SQLGlot")
        
        api >> pandera >> [polars, pandas] >> duck
        duck >> sqlglot

    with Cluster("5. Semantic Intelligence"):
        chroma = Database("ChromaDB")
        neo = Neo4J("Neo4j")
        llms = [Action("OpenAI GPT-4"), Action("Anthropic Claude")]
        
    with Cluster("6. Observability & Ops"):
        otel = Action("OpenTelemetry")
        prom = Prometheus("Prometheus")
        sentry = Sentry("Sentry")
        celery = Python("Celery")
        flower = Action("Flower")
        redis = Redis("Redis")
        
        celery >> redis
        flower >> celery
        api >> otel >> [prom, sentry]
        
    # Main Flows
    nextjs >> api
    api >> l_graph
    l_graph >> llms
    l_graph >> [chroma, neo]
    l_graph >> duck
