from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import React, FastAPI
from diagrams.programming.language import Python, Javascript
from diagrams.onprem.database import PostgreSQL, Neo4J
from diagrams.onprem.container import Docker

graph_attr = {
    "fontsize": "22",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "0.8",
    "ranksep": "1.0"
}

with Diagram("Chart E - Library Stack & Flow", show=False, filename="chart_e_libraries", direction="TB", graph_attr=graph_attr):
    
    with Cluster("1. Frontend Stack"):
        fe_core = React("Next.js 14\n(Framework)")
        fe_style = Javascript("TailwindCSS\n(Styling)")
        fe_viz = Javascript("Recharts\n(Viz)")
        
        [fe_style, fe_viz] >> fe_core

    with Cluster("2. API Stack"):
        api_core = FastAPI("FastAPI\n(Gateway)")
        api_valid = Python("Pydantic\n(Validation)")
        api_server = Python("Uvicorn\n(ASGI)")
        
        [api_valid, api_server] >> api_core

    with Cluster("3. Orchestration Stack"):
        llm_frame = Python("LangChain\n(LLM Interface)")
        dag_frame = Python("LangGraph\n(State Machine)")
        log = Python("Loguru\n(Logging)")
        
        fe_core >> Edge(color="black") >> api_core
        api_core >> Edge(color="darkblue") >> dag_frame
        
        dag_frame >> llm_frame
        dag_frame >> log

    with Cluster("4. Intelligence Stack"):
        gpt = Python("OpenAI SDK\n(GPT-4)")
        claude = Python("Anthropic SDK\n(Claude 3.5)")
        
        llm_frame >> gpt
        llm_frame >> claude

    with Cluster("5. Data Access Stack"):
        g_driver = Python("Neo4j Driver")
        sql_orm = Python("SQLAlchemy")
        
        dag_frame >> g_driver
        dag_frame >> sql_orm

    with Cluster("6. Infrastructure"):
        dock = Docker("Docker")
        
    # Visual grouping
    api_core - Edge(style="dashed", color="transparent") - dock
