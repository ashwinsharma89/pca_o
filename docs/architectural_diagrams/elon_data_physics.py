from diagrams import Diagram, Cluster, Edge
from diagrams.programming.flowchart import Database, InputOutput, ManualInput
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white"
}

with Diagram("Chart C - Data Physics", show=False, filename="chart_c_data_physics", direction="LR", graph_attr=graph_attr):
    
    raw = ManualInput("Raw Input\n(Text/Image)")
    
    with Cluster("Ingestion Pipeline"):
        norm = InputOutput("Normalization")
        clean = InputOutput("Sanitization")
        embed = InputOutput("Embedding (Vector)")
    
    with Cluster("State (Hot Storage)"):
        redis = Database("Session State")
        
    with Cluster("Knowledge (Cold Storage)"):
        graph = Database("Graph Nodes\n(Neo4j)")
        vector = Database("Vector Index")
        sql = Database("Structured Metrics")

    with Cluster("Processing Physics"):
        compute = Spark("Model Inference")
        agg = InputOutput("Aggregation")

    # The Flow
    raw >> Edge(label="Ingest") >> norm >> clean
    clean >> Edge(label="Vectorize") >> embed
    
    embed >> vector
    clean >> graph
    clean >> sql
    
    # Query Data Physics
    query = ManualInput("User Query")
    query >> compute
    compute >> Edge(label="Fetch Cxt") >> graph
    compute >> Edge(label="Fetch Sim") >> vector
    compute >> Edge(label="Agg Stats") >> sql
    
    graph >> agg
    vector >> agg
    sql >> agg
    
    agg >> redis >> InputOutput("Final Response")
