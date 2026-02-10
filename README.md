# Post Campaign Analysis (PCA) Agentic System

An AI-powered system for automated campaign performance analysis across multiple advertising platforms using Vision Language Models (VLMs), Large Language Models (LLMs), and agentic reasoning.

[![Tests](https://img.shields.io/badge/tests-856%20passing-brightgreen)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-35.3%25-yellow)](tests/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](requirements.txt)

## Features

### 🎯 Core Capabilities
- **Multi-Platform Support**: Google Ads, CM360, DV360, Meta Ads, Snapchat Ads, LinkedIn Ads
- **Vision-Based Extraction**: Extract metrics, graphs, and tables from dashboard screenshots
- **Agentic Reasoning**: Cross-channel analysis, attribution modeling, achievement detection
- **Natural Language Q&A**: Ask questions about your campaign data in plain English
- **RAG-Powered Insights**: Knowledge base with industry benchmarks and best practices
- **Pacing Reports**: Dynamic Excel reports with pivot analysis and budget tracking
- **Regression Analysis**: SHAP explainability, channel contributions, and budget recommendations
- **Automated Report Generation**: PowerPoint reports with data, visuals, and insights
- **API-First Design**: RESTful API for programmatic access

### 🤖 AI Agents
1. **Vision Agent**: Extract data from dashboard screenshots using GPT-4V/Claude Sonnet 4
2. **Data Extraction Agent**: Normalize and validate multi-platform data
3. **Reasoning Agent**: Generate insights, detect achievements, provide recommendations
4. **Channel Specialists**: Platform-specific analysis (Search, Social, Programmatic)
5. **Visualization Agent**: Create charts, infographics, and comparison visuals
6. **Report Assembly Agent**: Generate branded PowerPoint reports

### 📊 Supported Platforms
- **Google Ads**: Search, Display, Video campaigns
- **Campaign Manager 360 (CM360)**: Display campaign metrics
- **Display & Video 360 (DV360)**: Programmatic buying
- **Meta Ads**: Facebook, Instagram campaigns
- **Snapchat Ads**: Story ads, lens engagement
- **LinkedIn Ads**: B2B lead generation

### 🧠 LLM Support (Multi-Model)
- **OpenAI**: GPT-4o, GPT-4-turbo
- **Anthropic**: Claude 3.5 Sonnet
- **Google**: Gemini 2.5 Flash (Free tier)
- **DeepSeek**: DeepSeek Chat (Free, coding specialist)
- **Groq**: Llama 3.1 (Free, fast inference)

## Architecture

```
API Gateway (FastAPI)
    ↓
Orchestration Layer (LangGraph)
    ↓
┌─────────┬──────────┬──────────┬──────────┬────────────┐
│ Vision  │ Data     │ Reasoning│ Visual   │ Report Gen │
│ Agent   │ Extract  │ Agent    │ Gen      │ Agent      │
└─────────┴──────────┴──────────┴──────────┴────────────┘
    ↓
Storage (PostgreSQL + Redis + S3)
```

## Tech Stack

- **AI/ML**: OpenAI GPT-4V, Anthropic Claude Sonnet 4, LangGraph, LangChain
- **Backend**: FastAPI, Python 3.11+, Celery, Redis
- **Database**: PostgreSQL, Redis
- **Visualization**: Plotly, Matplotlib, Seaborn
- **Report Generation**: python-pptx, ReportLab
- **Image Processing**: OpenCV, Pillow, Tesseract OCR
- **Frontend**: Next.js (Production Interface)

## Quick Start

### 1. Installation & Setup
The system includes a unified management tool that works on both macOS and Windows.

**Standard (macOS / Linux / Windows):**
```bash
python manage.py setup
```

**Windows (Alternative):**
You can also simply double-click **`setup.bat`** in the project root to initialize the environment.

This command will:
- Create a virtual environment (if missing)
- Install all Python backend dependencies
- Create a default `.env` from the template
- Install all NPM frontend dependencies

### 2. Configuration
Open the newly created `.env` and add your API keys:
```env
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
```

### 3. Start the Application
Launch both the backend and frontend simultaneously with one command:

```bash
python manage.py start
```

Open [http://localhost:3000](http://localhost:3000) to access the dashboard.
To stop the services, press **Ctrl+C** in the terminal or run `python manage.py stop`.

## Knowledge Base & Advanced RAG

The PCA Agent now ships with an ingestion + retrieval pipeline that powers hybrid RAG across every reasoning surface (EnhancedReasoningEngine, query clarifier, NL-to-SQL, Next.js frontend).

### 1. Prepare sources
- Curate URLs/YouTube/PDFs in `knowledge_sources/` (see `knowledge_sources/README.md`)
- Prioritized URL lists live in `knowledge_sources_priority1.txt` (text) and `.json` (structured)
- Track ingestion outcomes in `SUCCESSFUL_INGESTIONS.md` / `FAILED_INGESTIONS.md`

### 2. Auto-ingest & rebuild vectors

```bash
python scripts/auto_ingest_knowledge.py --source-file knowledge_sources_priority1.txt
```

This script:
1. Fetches all sources, storing normalized chunks + metadata to `data/knowledge_base.json`
2. Installs embeddings via OpenAI `text-embedding-3-small`
3. Rebuilds the FAISS index at `data/vector_store/faiss.index`
4. Persists companion metadata at `data/vector_store/metadata.json`

Manual rebuilds (e.g., after editing `knowledge_base.json`):

```python
from src.knowledge.vector_store import VectorStoreBuilder
builder = VectorStoreBuilder()
docs = json.load(open("data/knowledge_base.json", "r", encoding="utf-8"))
builder.build_from_documents(docs)
```

### 3. Dependencies & API keys
- `pip install rank-bm25 cohere` (keyword retrieval + Cohere rerank)
- `OPENAI_API_KEY` (embeddings + GPT reasoning)
- `COHERE_API_KEY` (optional reranking)
- Optional LLMs: `GOOGLE_API_KEY`, `DEEPSEEK_API_KEY`, `ANTHROPIC_API_KEY`, `GROQ_API_KEY`

### 4. Retrieval stack
- **VectorRetriever**: FAISS + OpenAI embeddings
- **KeywordRetriever**: BM25 (rank-bm25) over chunk text
- **HybridRetriever**: Combines both via Reciprocal Rank Fusion (RRF)
- **CohereReranker**: Optional rerank of merged candidates (`rerank-english-v3.0`)
- **Metadata filters**: Limit retrieval by `category`, `priority`, etc.
- **SQLKnowledgeHelper**: Bundles best practices + schema snapshot + retrieved snippets for NL-to-SQL & query clarification

### 5. Monitoring
- Every retrieval call writes metrics to `data/vector_store/retrieval_metrics.jsonl`
- Metrics include:
  - Query volume over time
  - Vector vs keyword candidate counts
  - Cohere rerank usage
  - Filter/category/priority distributions
  - Raw log for debugging

### 6. Q&A Context Panel
- On the "💬 Chat" page in the Next.js frontend, each answer displays the SQL Knowledge Context (best practices, schema, top retrieved snippets) so analysts can validate reasoning.

### 7. Troubleshooting tips
- Missing FAISS index → rerun ingestion or delete `data/vector_store/*` and rebuild
- Cohere errors → confirm `COHERE_API_KEY` and rerank model availability
- Empty retrieval responses → inspect `FAILED_INGESTIONS.md` and monitoring dashboard filters

## Usage

### API Workflow

```python
import requests

# 1. Create campaign analysis job
response = requests.post("http://localhost:8001/api/campaigns", json={
    "campaign_name": "Q4 2024 Holiday Campaign",
    "objectives": ["awareness", "conversion"],
    "date_range": {"start": "2024-10-01", "end": "2024-12-31"}
})
campaign_id = response.json()["campaign_id"]

# 2. Upload dashboard snapshots
files = [
    ("files", open("google_ads_dashboard.png", "rb")),
    ("files", open("meta_ads_dashboard.png", "rb")),
    ("files", open("linkedin_ads_dashboard.png", "rb"))
]
requests.post(f"http://localhost:8001/api/campaigns/{campaign_id}/snapshots", files=files)

# 3. Check status
status = requests.get(f"http://localhost:8001/api/campaigns/{campaign_id}/status")
print(status.json())

# 4. Download report
report = requests.get(f"http://localhost:8001/api/campaigns/{campaign_id}/report")
with open("campaign_report.pptx", "wb") as f:
    f.write(report.content)
```

### Dashboard (Next.js)

1. Secure Login with username/password
2. Upload dashboard screenshots or CSV/Parquet data
3. Select campaign objectives and date range
4. Click "Analyze Campaign"
5. View extracted data, insights, and interactive visualizations (Recharts)
6. Download structured reports

## Project Structure

```
PCA_Agent/
├── src/
│   ├── agents/              # AI agents
│   │   ├── vision_agent.py
│   │   ├── extraction_agent.py
│   │   ├── reasoning_agent.py
│   │   ├── visualization_agent.py
│   │   └── report_agent.py
│   ├── api/                 # FastAPI endpoints
│   │   ├── main.py
│   │   └── routes/
│   ├── models/              # Data models
│   │   ├── campaign.py
│   │   ├── platform.py
│   │   └── report.py
│   ├── orchestration/       # LangGraph workflows
│   │   └── workflow.py
│   ├── utils/               # Utilities
│   │   ├── image_processor.py
│   │   ├── chart_generator.py
│   │   └── logger.py
│   └── config/              # Configuration
│       └── settings.py
├── templates/               # Report templates
│   └── ppt_templates/
├── tests/                   # Unit tests
├── data/                    # Sample data
│   └── sample_dashboards/
├── frontend/           # Next.js Production Frontend (React, TypeScript, TailwindCSS)
├── src/                # Backend source code (FastAPI, Python 3.11+)
├── tests/              # Unit and integration tests (856+ tests)
├── archived/           # Deprecated components (legacy frontends)
└── README.md
```

## API Endpoints

### Campaign Management
- `POST /api/campaigns` - Create new campaign analysis
- `GET /api/campaigns/{id}` - Get campaign details
- `DELETE /api/campaigns/{id}` - Delete campaign

### Snapshot Upload
- `POST /api/campaigns/{id}/snapshots` - Upload dashboard screenshots
- `GET /api/campaigns/{id}/snapshots` - List uploaded snapshots

### Analysis & Reports
- `GET /api/campaigns/{id}/status` - Check processing status
- `GET /api/campaigns/{id}/data` - Get extracted data (JSON)
- `GET /api/campaigns/{id}/insights` - Get AI-generated insights
- `GET /api/campaigns/{id}/report` - Download PowerPoint report
- `POST /api/campaigns/{id}/regenerate` - Regenerate report with new template

## Examples

### Supported Dashboard Formats
- PNG, JPG, JPEG (screenshots)
- PDF (exported reports)
- Multi-page PDFs (automatically split)

### Extracted Metrics
- **Performance**: Impressions, Clicks, CTR, Conversions, CPA, ROAS
- **Engagement**: Likes, Shares, Comments, Video Views, Completion Rate
- **Audience**: Demographics, Interests, Device, Location
- **Creative**: Ad format performance, A/B test results

### Generated Insights
- Channel performance comparison
- Budget efficiency analysis
- Attribution modeling (first-touch, last-touch, multi-touch)
- Audience segment analysis
- Creative performance ranking
- Trend analysis and forecasting
- Actionable recommendations

## Development

### Run Tests
```bash
# Run all unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_agents_vision.py -v
```

### Test Coverage Summary (856 tests passing)

| Module | Coverage | Status |
|--------|----------|--------|
| models | 94.9% | ✅ Excellent |
| di | 92.1% | ✅ Excellent |
| visualization | 78.9% | ✅ Good |
| orchestration | 65.7% | ✅ Good |
| nextjs_frontend | 85.0% | ✅ Excellent |
| data_processing | 59.2% | 🟡 Moderate |
| utils | 48.6% | 🟡 Moderate |
| database | 48.6% | 🟡 Moderate |
| query_engine | 43.6% | 🟡 Moderate |
| knowledge | 32.0% | 🟡 Needs more |

### Code Quality
```bash
# Format code
black src/

# Lint
flake8 src/

# Type checking
mypy src/
```

## Documentation

See the `docs/` folder for detailed documentation:
- [Deployment Guide](DEPLOYMENT_GUIDE.md) - Production deployment
- [Docker Setup](DOCKER_SETUP.md) - Container deployment
- [Database Setup](DATABASE_SETUP.md) - PostgreSQL configuration
- [Testing Guide](TESTING_GUIDE.md) - Test writing guidelines
- [Knowledge Base Setup](KNOWLEDGE_BASE_SETUP.md) - RAG configuration

## License

MIT License

## Support

For issues and questions, please open a GitHub issue.
