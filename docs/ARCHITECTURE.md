# PCA Agent - System Architecture

This document describes the technical architecture of the PCA Agent system.

---

## System Overview

PCA Agent is a multi-tier, AI-powered campaign analysis system built with modern web technologies and agentic AI patterns.

```mermaid
graph TB
    subgraph "Client Layer"
        UI[Next.js Frontend<br/>React + TypeScript + TailwindCSS]
    end
    
    subgraph "API Layer"
        API[FastAPI Gateway<br/>Python 3.11+]
        Auth[JWT Authentication]
        CORS[CORS Middleware]
    end
    
    subgraph "Business Logic Layer"
        Orchestrator[Multi-Agent Orchestrator<br/>LangGraph]
        
        subgraph "AI Agents"
            Vision[Vision Agent<br/>GPT-4V/Claude]
            Reasoning[Reasoning Agent<br/>GPT-4/Claude]
            Extraction[Data Extraction Agent]
            Viz[Visualization Agent]
            Report[Report Agent]
            Pacing[Pacing Report Agent]
        end
        
        subgraph "Services"
            CampaignSvc[Campaign Service]
            AuthSvc[Auth Service]
            QuerySvc[Query Service]
            ReportSvc[Report Service]
        end
    end
    
    subgraph "Data Layer"
        DuckDB[(DuckDB<br/>Analytics DB)]
        Parquet[(Parquet Files<br/>Columnar Storage)]
        Vector[(FAISS<br/>Vector Store)]
        Redis[(Redis<br/>Cache - Optional)]
    end
    
    subgraph "External Services"
        OpenAI[OpenAI API<br/>GPT-4, Embeddings]
        Anthropic[Anthropic API<br/>Claude 3.5]
        Cohere[Cohere API<br/>Reranking]
    end
    
    UI --> API
    API --> Auth
    API --> CORS
    API --> Orchestrator
    Orchestrator --> Vision
    Orchestrator --> Reasoning
    Orchestrator --> Extraction
    Orchestrator --> Viz
    Orchestrator --> Report
    Orchestrator --> Pacing
    
    Orchestrator --> CampaignSvc
    Orchestrator --> AuthSvc
    Orchestrator --> QuerySvc
    Orchestrator --> ReportSvc
    
    CampaignSvc --> DuckDB
    CampaignSvc --> Parquet
    QuerySvc --> DuckDB
    QuerySvc --> Vector
    ReportSvc --> DuckDB
    
    Vision --> OpenAI
    Vision --> Anthropic
    Reasoning --> OpenAI
    Reasoning --> Anthropic
    QuerySvc --> Cohere
    
    style UI fill:#4CAF50
    style API fill:#2196F3
    style Orchestrator fill:#FF9800
    style DuckDB fill:#9C27B0
    style OpenAI fill:#00BCD4
```

---

## Architecture Layers

### 1. Client Layer (Frontend)

**Technology**: Next.js 14, React 18, TypeScript, TailwindCSS

**Components**:
- **Pages**: Campaign management, analytics, reports, chat
- **Components**: Reusable UI components (charts, tables, forms)
- **State Management**: React hooks, context API
- **API Client**: Axios for HTTP requests
- **Visualizations**: Recharts library

**Key Features**:
- Server-side rendering (SSR)
- Static site generation (SSG)
- API routes for backend proxy
- Responsive design
- Real-time updates

---

### 2. API Layer (Gateway)

**Technology**: FastAPI, Python 3.11+

**Responsibilities**:
- Request routing
- Authentication/authorization (JWT)
- Input validation (Pydantic)
- CORS handling
- Rate limiting
- Error handling
- API documentation (OpenAPI/Swagger)

**Endpoints**:
```
/api/v1/
├── auth/           # Authentication
├── campaigns/      # Campaign management
├── pacing-reports/ # Pacing reports
├── regression/     # Regression analysis
├── chat/           # Natural language Q&A
├── knowledge/      # RAG knowledge base
├── visualizations/ # Data visualizations
└── intelligence/   # AI insights
```

---

### 3. Business Logic Layer

#### Multi-Agent Orchestrator (LangGraph)

**Purpose**: Coordinate AI agents for complex workflows

**Workflow Example** (Campaign Analysis):
```mermaid
graph LR
    A[Upload Data] --> B[Data Extraction Agent]
    B --> C[Validation]
    C --> D[Reasoning Agent]
    D --> E[Visualization Agent]
    E --> F[Report Agent]
    F --> G[Generate Report]
```

#### AI Agents

1. **Vision Agent**
   - Extract data from screenshots
   - OCR for text recognition
   - Chart/graph interpretation
   - Models: GPT-4V, Claude 3.5 Sonnet

2. **Reasoning Agent**
   - Generate insights
   - Detect achievements
   - Provide recommendations
   - Cross-channel analysis

3. **Data Extraction Agent**
   - Normalize multi-platform data
   - Validate data quality
   - Handle missing values
   - Data type conversion

4. **Visualization Agent**
   - Create charts and graphs
   - Generate infographics
   - Comparison visuals

5. **Report Agent**
   - Generate PowerPoint reports
   - Apply branding
   - Insert visualizations
   - Format data tables

6. **Pacing Report Agent**
   - Adaptive sheet population
   - Dynamic pivot analysis
   - Formula preservation
   - Excel template processing

#### Services

1. **Campaign Service**
   - CRUD operations
   - Data upload/import
   - Filtering and aggregation
   - Export functionality

2. **Auth Service**
   - User registration/login
   - Password hashing (bcrypt)
   - JWT token generation
   - Session management

3. **Query Service**
   - Natural language to SQL
   - Query optimization
   - Result formatting
   - Query caching

4. **Report Service**
   - Report generation
   - Template management
   - Scheduling
   - Distribution

---

### 4. Data Layer

#### DuckDB (Analytics Database)

**Purpose**: Fast analytics queries on campaign data

**Features**:
- Columnar storage
- Parallel query execution
- Performance indexes
- SQL interface
- ACID compliance

**Schema**:
```sql
CREATE TABLE campaigns (
    Date DATE,
    Platform VARCHAR,
    Campaign VARCHAR,
    Spend DECIMAL(10,2),
    Impressions INTEGER,
    Clicks INTEGER,
    Conversions INTEGER,
    -- Additional metrics
);
```

#### Parquet Files (Data Storage)

**Purpose**: Compressed columnar storage for large datasets

**Advantages**:
- 10x compression vs CSV
- Fast column-based queries
- Type preservation
- Partitioning support

**Location**: `data/campaigns.parquet`

#### FAISS (Vector Store)

**Purpose**: Semantic search for RAG knowledge base

**Features**:
- Fast approximate nearest neighbor search
- OpenAI embeddings (text-embedding-3-small)
- Metadata filtering
- Hybrid retrieval (vector + keyword)

**Location**: `data/vector_store/faiss.index`

#### Redis (Cache - Optional)

**Purpose**: Caching for improved performance

**Use Cases**:
- Query result caching
- Session storage
- Rate limiting
- Celery task queue

---

### 5. External Services

#### OpenAI API

**Models Used**:
- GPT-4o (reasoning, chat)
- GPT-4-turbo (analysis)
- GPT-4V (vision)
- text-embedding-3-small (embeddings)

**Use Cases**:
- Natural language understanding
- Image analysis
- Embeddings for RAG
- Text generation

#### Anthropic API

**Models Used**:
- Claude 3.5 Sonnet (reasoning, vision)

**Use Cases**:
- Alternative to GPT-4
- Vision analysis
- Long-context reasoning

#### Cohere API

**Models Used**:
- rerank-english-v3.0

**Use Cases**:
- Reranking search results
- Improving RAG relevance

---

## Data Flow

### Campaign Upload Flow

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant API
    participant CampaignService
    participant DuckDB
    participant Parquet
    
    User->>Frontend: Upload CSV file
    Frontend->>API: POST /api/v1/campaigns/upload
    API->>CampaignService: Process file
    CampaignService->>CampaignService: Validate data
    CampaignService->>Parquet: Save to parquet
    CampaignService->>DuckDB: Create indexes
    DuckDB-->>CampaignService: Success
    CampaignService-->>API: Return status
    API-->>Frontend: 200 OK
    Frontend-->>User: Show success message
```

### Natural Language Query Flow

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant API
    participant QueryService
    participant RAG
    participant LLM
    participant DuckDB
    
    User->>Frontend: Ask question
    Frontend->>API: POST /api/v1/chat
    API->>QueryService: Process query
    QueryService->>RAG: Retrieve context
    RAG-->>QueryService: Return snippets
    QueryService->>LLM: Generate SQL
    LLM-->>QueryService: Return SQL
    QueryService->>DuckDB: Execute SQL
    DuckDB-->>QueryService: Return results
    QueryService-->>API: Format response
    API-->>Frontend: Return data
    Frontend-->>User: Display results
```

### Pacing Report Generation Flow

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant API
    participant PacingAgent
    participant DuckDB
    participant Excel
    
    User->>Frontend: Request report
    Frontend->>API: POST /api/v1/pacing-reports/generate
    API->>PacingAgent: Generate report
    PacingAgent->>DuckDB: Query campaign data
    DuckDB-->>PacingAgent: Return data
    PacingAgent->>PacingAgent: Discover channels
    PacingAgent->>Excel: Load template
    PacingAgent->>Excel: Populate data
    PacingAgent->>Excel: Insert formulas
    Excel-->>PacingAgent: Return file
    PacingAgent-->>API: Return report
    API-->>Frontend: Download link
    Frontend-->>User: Download Excel
```

---

## Security Architecture

### Authentication Flow

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant API
    participant AuthService
    participant Database
    
    User->>Frontend: Enter credentials
    Frontend->>API: POST /api/v1/auth/login
    API->>AuthService: Validate credentials
    AuthService->>Database: Query user
    Database-->>AuthService: Return user
    AuthService->>AuthService: Verify password (bcrypt)
    AuthService->>AuthService: Generate JWT token
    AuthService-->>API: Return token
    API-->>Frontend: Return token
    Frontend->>Frontend: Store token (localStorage)
    Frontend-->>User: Redirect to dashboard
```

### Authorization Flow

```mermaid
graph LR
    A[Request] --> B{Has Token?}
    B -->|No| C[401 Unauthorized]
    B -->|Yes| D{Valid Token?}
    D -->|No| E[401 Unauthorized]
    D -->|Yes| F{Has Permission?}
    F -->|No| G[403 Forbidden]
    F -->|Yes| H[Process Request]
```

---

## Deployment Architecture

### Development

```
┌─────────────────────────────────────┐
│  Local Machine                       │
│  ┌──────────┐      ┌──────────┐    │
│  │ Frontend │      │ Backend  │    │
│  │ :3000    │◄────►│ :8000    │    │
│  └──────────┘      └──────────┘    │
│                          │          │
│                    ┌─────▼─────┐   │
│                    │  DuckDB   │   │
│                    │  Parquet  │   │
│                    └───────────┘   │
└─────────────────────────────────────┘
```

### Production

```
┌─────────────────────────────────────────────┐
│  Cloud Infrastructure (AWS/GCP/Azure)        │
│                                              │
│  ┌────────────────────────────────────┐    │
│  │  Load Balancer                      │    │
│  └──────────┬─────────────────────────┘    │
│             │                                │
│  ┌──────────▼──────────┐  ┌──────────────┐ │
│  │  Frontend (Vercel)  │  │  CDN         │ │
│  │  Next.js SSR        │  │  (CloudFlare)│ │
│  └──────────┬──────────┘  └──────────────┘ │
│             │                                │
│  ┌──────────▼──────────────────────────┐   │
│  │  API Gateway (AWS API Gateway)      │   │
│  └──────────┬──────────────────────────┘   │
│             │                                │
│  ┌──────────▼──────────┐                    │
│  │  Backend (ECS/K8s)  │                    │
│  │  FastAPI + Gunicorn │                    │
│  │  Auto-scaling       │                    │
│  └──────────┬──────────┘                    │
│             │                                │
│  ┌──────────▼──────────┐  ┌──────────────┐ │
│  │  PostgreSQL (RDS)   │  │  Redis       │ │
│  │  Primary Database   │  │  (ElastiCache│ │
│  └─────────────────────┘  └──────────────┘ │
│                                              │
│  ┌──────────────────────┐                   │
│  │  S3 (File Storage)   │                   │
│  │  - Reports           │                   │
│  │  - Uploads           │                   │
│  └──────────────────────┘                   │
└─────────────────────────────────────────────┘
```

---

## Technology Stack Summary

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | Next.js 14, React 18, TypeScript | User interface |
| **Styling** | TailwindCSS | UI styling |
| **Visualizations** | Recharts | Charts and graphs |
| **API** | FastAPI | REST API gateway |
| **Language** | Python 3.11+ | Backend logic |
| **AI Framework** | LangChain, LangGraph | Agent orchestration |
| **LLMs** | OpenAI GPT-4, Anthropic Claude | AI reasoning |
| **Database** | DuckDB | Analytics database |
| **Storage** | Parquet | Columnar data storage |
| **Vector DB** | FAISS | Semantic search |
| **Cache** | Redis (optional) | Caching layer |
| **Auth** | JWT, bcrypt | Authentication |
| **Testing** | Pytest, Playwright | Testing framework |
| **Deployment** | Docker, K8s | Containerization |

---

## Performance Considerations

### Optimization Strategies

1. **Database Indexing**
   - Indexes on Date, Platform, Channel, Region
   - Composite indexes for common filter combinations

2. **Query Optimization**
   - Parameterized queries
   - Query result caching (Redis)
   - Parallel query execution (DuckDB)

3. **Data Storage**
   - Parquet columnar format (10x compression)
   - Partitioning by date/platform
   - Snappy compression

4. **API Performance**
   - Response caching
   - Pagination for large datasets
   - Async request handling

5. **Frontend Optimization**
   - Code splitting
   - Lazy loading
   - Image optimization
   - SSR/SSG for faster initial load

---

## Scalability

### Horizontal Scaling

- **API**: Multiple FastAPI instances behind load balancer
- **Database**: Read replicas for PostgreSQL
- **Cache**: Redis cluster
- **Frontend**: CDN distribution

### Vertical Scaling

- **DuckDB**: Increase memory allocation
- **API**: Increase worker processes
- **Database**: Larger instance sizes

---

## Monitoring & Observability

### Metrics

- API response times
- Database query performance
- Error rates
- User activity
- Resource utilization

### Logging

- Application logs (JSON format)
- Access logs
- Error logs
- Audit logs

### Tools

- Prometheus (metrics)
- Grafana (dashboards)
- Sentry (error tracking)
- ELK Stack (log aggregation)

---

## References

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Next.js Documentation](https://nextjs.org/docs)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [LangChain Documentation](https://python.langchain.com/)
- [OpenAI API Reference](https://platform.openai.com/docs/api-reference)

---

**Last Updated**: 2025-12-28  
**Version**: 1.0.0
