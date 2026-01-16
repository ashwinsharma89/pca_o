---
markmap:
  colorFreezeLevel: 2
  maxWidth: 300
---

# 🏗️ PCA Agent Architecture

## 1. Interface Layer (The Skin)
- **Frontend**
  - Next.js (React Framework)
  - TailwindCSS (Styling)
  - Recharts (Data Viz)
- **API Gateway**
  - FastAPI (Python)
  - Pydantic (Validation)
  - Uvicorn (Server)

## 2. Orchestration Layer (The Brain Stem)
- **Router Agent**
  - Intent Classification
  - Query Routing
- **LangGraph**
  - State Management
  - Workflow DAGs
  - Cyclic Execution

## 3. Intelligence Layer (The Neural Net)
- **Core Agents**
  - Vision Agent (GPT-4V)
  - Reasoning Agent (Claude 3.5)
- **Specialist Swarm**
  - *Analyzers*: Budget, Creative, Trend
  - *Channel Experts*: Google, Meta, LinkedIn
  - *Utility*: Extractor, Validator

## 4. Data Layer (The Memory)
- **Knowledge Graph**
  - Neo4j (Graph DB)
  - Cypher Queries
- **Vector Store**
  - OpenAI Embeddings
  - Semantic Search
- **Analytics DB**
  - DuckDB/Postgres
  - SQL Metrics
