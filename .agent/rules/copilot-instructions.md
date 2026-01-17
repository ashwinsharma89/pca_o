---
trigger: manual
---

# PCA Agent - AI Coding Assistant Instructions

## Architecture Overview

This is a **multi-agent marketing analytics platform** with a FastAPI backend and Next.js frontend. The system uses AI agents to analyze campaign data across multiple advertising platforms (Google Ads, Meta, LinkedIn, etc.).

### Core Agent System
- **Vision Agent** (`src/engine/agents/vision_agent.py`): Extracts data from dashboard screenshots using GPT-4V/Claude Sonnet
- **Data Extraction Agent** (`src/engine/agents/extraction_agent.py`): Normalizes multi-platform data
- **Enhanced Reasoning Agent** (`src/engine/agents/enhanced_reasoning_agent.py`): Generates insights, detects achievements, provides recommendations
- **Visualization Agent** (`src/engine/agents/enhanced_visualization_agent.py`): Creates charts and infographics
- **Channel Specialists** (`src/engine/agents/channel_specialists/`): Platform-specific analysis (Search, Social, Programmatic)

### Tech Stack
- **Backend**: FastAPI (Python 3.11+), LangGraph orchestration, Pydantic schemas
- **Databases**: DuckDB (primary analytics), PostgreSQL (optional), Redis (caching), Neo4j (knowledge graph)
- **Frontend**: Next.js 16, React 19, TypeScript, TailwindCSS 4, Recharts
- **AI/ML**: OpenAI GPT-4o, Anthropic Claude, Google Gemini, LangChain, FAISS/ChromaDB vectors
- **Data Processing**: Polars (fast DataFrames), Pandera (schema validation), SQLGlot (SQL parsing)

## Critical Conventions

### 🔐 Authentication (FROZEN - DO NOT MODIFY)
**PERMANENT LOGIN CREDENTIALS**:
- Username: `ashwin`
- Password: `Pca12345!`
- Role: `admin`
- Tier: `enterprise`
- Database: `data/campaigns.duckdb`

These credentials are **FROZEN** and must work in all code changes. The user record (id=3) is immutable.

### Enterprise Robustness Principles
1. **Stateless Agents**: Agents must be isolated. Use `StateExchange` for context sharing, never internal side-effects
2. **Contract-Based Development**: All agent inputs/outputs use strict **Pydantic schemas**. Interface changes must not break other agents
3. **Golden Set Testing**: All changes must pass 35+ marketing scenario tests before deployment
4. **Multi-Tier Caching**: Semantic (Tier 1), SQL (Tier 2), Result (Tier 3) caching for determinism
5. **LLM Priority**: `gpt-4o` is production default. Use `gemini-1.5-flash` or `deepseek-chat` as fallbacks only

## Developer Workflows

### Starting Services
```bash
# Start all services (API + Frontend)
./start_all.sh
# API: http://localhost:8000
# Web: http://localhost:3000

# Check service health
python ops/check_health.py

# Stop all services
./stop_all.sh
```

### Database Architecture
- **DuckDB** (`data/campaigns.duckdb`): Primary analytics database, embedded, no server required
- **PostgreSQL**: Optional for production deployments (connection pooling via SQLAlchemy)
- **Neo4j**: Knowledge graph for KG-RAG system (`src/kg_rag/`)
- **Redis**: Distributed caching and Celery task queue

**Database Connection Pattern**:
```python
from src.core.database.connection import get_db_manager, get_db

# Context manager (recommended)
with db_manager.get_session() as session:
    # Auto-commit/rollback
    pass

# FastAPI dependency injection
@app.get("/endpoint")
def endpoint(db: Session = Depends(get_db)):
    # Session auto-managed
    pass
```

### Testing
```bash
# Run all tests (856 tests)
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=src --cov-report=html

# Frontend E2E tests (Playwright)
cd frontend && npm run test
```

### Code Quality
```bash
# Format code (Black, line length 100)
black src/

# Lint (Flake8)
flake8 src/

# Type checking (MyPy)
mypy src/
```

## Project-Specific Patterns

### Agent Communication via StateExchange
Agents share context through a typed state exchange, not direct calls:
```python
from src.engine.agents.shared_context import StateExchange

# Agent writes to state
state_exchange.set("query_results", data)

# Another agent reads from state
results = state_exchange.get("query_results")
```

### Pydantic Schema Validation
All data models use Pydantic v2 with strict validation:
```python
from pydantic import BaseModel, Field

class CampaignMetrics(BaseModel):
    impressions: int = Field(ge=0)
    clicks: int = Field(ge=0)
    spend: float = Field(ge=0.0)
    
    class Config:
        validate_assignment = True
```

### Multi-Model LLM Pattern
The system supports multiple LLM providers with fallback logic:
```python
from src.core.config.settings import get_settings

settings = get_settings()
# Primary: settings.openai_api_key (gpt-4o)
# Fallback: settings.google_api_key (gemini-1.5-flash)
# Fallback: settings.deepseek_api_key (deepseek-chat)
```

### KG-RAG (Knowledge Graph + RAG)
The system uses hybrid retrieval (vector + keyword + graph):
- **Vector Store**: FAISS index at `data/vector_store/faiss.index`
- **Knowledge Base**: `data/knowledge_base.json` (curated marketing knowledge)
- **Ingestion**: `python scripts/auto_ingest_knowledge.py --source-file knowledge_sources_priority1.txt`
- **Retrieval**: Combines FAISS (semantic) + BM25 (keyword) + Cohere reranking

### Frontend API Integration
The Next.js frontend uses React Query for API calls:
```typescript
// Pattern: API calls via fetch with error handling
const response = await fetch(`${API_BASE_URL}/api/v1/endpoint`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(data)
});
```

## Key File Locations

### Backend Entry Points
- `src/interface/api/main.py`: FastAPI application with JWT auth, rate limiting, CORS
- `src/engine/orchestration/`: LangGraph workflows for agent coordination
- `src/core/database/connection.py`: Database connection pooling and session management

### Frontend Structure
- `frontend/src/app/`: Next.js App Router pages
- `frontend/src/components/`: Reusable React components (shadcn/ui + custom)
- `frontend/src/lib/`: Utilities and API clients

### Configuration
- `.env`: Environment variables (API keys, database URLs)
- `pyproject.toml`: Python project config (Black, MyPy, Pytest)
- `requirements.txt`: Python dependencies with security pins

### Data & Knowledge
- `data/campaigns.duckdb`: Primary analytics database
- `data/knowledge_base.json`: RAG knowledge base
- `knowledge_sources/`: Curated marketing knowledge (channels, KPIs, best practices)

## Integration Points

### API Gateway Pattern
All external requests go through `src/interface/gateway/api_gateway.py` which handles:
- Request validation
- Rate limiting
- Circuit breaker pattern (via `pybreaker`)
- Retry logic (via `tenacity`)

### Observability Stack
- **Prometheus**: Metrics at `:9090`
- **Grafana**: Dashboards at `:3001`
- **Flower**: Celery monitoring at `:5540`
- **OpenTelemetry**: Distributed tracing (optional)

### Report Generation
- **Pacing Reports**: Excel with dynamic pivot tables (`src/engine/reports/pacing_report.py`)
- **PowerPoint Reports**: Branded presentations (`src/engine/reports/`)
- **PDF Exports**: Frontend-based (jsPDF + html2canvas)

## Common Gotchas

1. **DuckDB vs PostgreSQL**: Most features work with DuckDB only. PostgreSQL is optional for production
2. **Agent Isolation**: Never import one agent directly into another. Use `StateExchange` or orchestration layer
3. **Pydantic v2**: Use `model_validate()` not `parse_obj()`, `model_dump()` not `dict()`
4. **Frontend API URLs**: Use `process.env.NEXT_PUBLIC_API_URL` for client-side, `API_URL` for server-side
5. **Caching Keys**: Semantic cache uses query embeddings, SQL cache uses normalized SQL AST
6. **Date Filtering**: Backend uses `start_date`/`end_date` (snake_case), frontend uses `startDate`/`endDate` (camelCase)

## When Making Changes

1. **Backup First**: For major refactors, create `.bak` file: `cp file.py file.py.bak`
2. **Update Tests**: Add/update tests in `tests/unit/` for new functionality
3. **Check Golden Set**: Run `pytest tests/unit/test_golden_set.py` for SQL generation changes
4. **Update Schemas**: If changing data models, update Pydantic schemas and migration scripts
5. **Preserve Auth**: Never modify authentication logic without preserving frozen credentials
