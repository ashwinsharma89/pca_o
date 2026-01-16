# PCA Agent: The Machine
> *“The best part is no part. The best process is no process.”*

## 1. The Neural Network (Agents)
### Capabilities
- **Core Intelligence**
    - `RouterAgent` (The Gatekeeper)
        - *Logic*: Keywords → Specialist
    - `EnhancedReasoningAgent` (The Cortex)
        - *Logic*: Pattern Detection
    - `SynthesizerAgent` (The Voice)
        - *Logic*: Optimization & Compression
- **Specialized Neurons**
    - `VisionAgent` (Visual Cortex)
        - *Input*: Images/Screenshots
        - *Ops*: OCR, Platform Detection
    - `ExtractionAgent` (Data Ingestion)
    - `VisualizationAgent` (Visual Output)
- **Channel Specialists**
    - `SearchChannelAgent` (Google/Bing)
    - `SocialChannelAgent` (Meta/LinkedIn)
    - `ProgrammaticAgent` (DV360)

## 2. The Algorithm (Workflow)
- **Input Vector**: User Query + Campaign Data
- **Step 1: Routing**
    - If `budget/spend` → `BudgetAnalyzer`
    - If `creative` → `CreativeAnalyzer`
    - Else → `GeneralAnalyzer`
- **Step 2: Analysis (Compute)**
    - Calculate Metrics (CTR, CPC, ROAS)
    - Identify Anomalies
- **Step 3: Recommendation (Optimization)**
    - Generate Actionable Insights
    - *Goal*: Maximizing Utility
- **Step 4: Synthesis**
    - Aggregate Results + Recommendations
    - Filter by Confidence (>0.85)

## 3. Responsibility Matrix (Swimlane Logic)
- **Orchestrator Lane**
    - `Router`: Directs traffic
    - `Monitor`: Tracks state & errors
- **Analysis Lane**
    - `Specialists`: Execute platform logic
    - `Analyzers`: Crunch numbers
- **Output Lane**
    - `Recommender`: Strategy generation
    - `Synthesizer`: Final report assembly
    - `Reporter`: PPTX Generation

## 4. System Architecture (The Hardware)
- **Visual Cortex (Frontend)**
    - Next.js (React)
    - `ChatInterface`
- **Nervous System (API)**
    - FastAPI Gateway
    - Rate Limiting (Protection)
    - Auth (Security)
- **Brain Stem (Engine)**
    - Multi-Agent Orchestrator
    - LangGraph (State Mgmt)
- **Hippocampus (Memory)**
    - Neo4j (Graph Knowledge)
    - Postgres (Analytics Data)
    - Vector Store (Embeddings)

## 5. Data Vector Flow
- **Input**: User Query
- **Transform**: 
    - `API` → `Orchestrator`
    - `Orchestrator` → `Agent`
    - `Agent` → `Tool` (DB/Graph)
- **Output**: 
    - `Tool` → `Agent` → `Synthesizer` → `JSON Response`
