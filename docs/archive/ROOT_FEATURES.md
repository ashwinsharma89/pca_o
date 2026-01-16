# PCA Agent - Features Overview

This document provides a comprehensive overview of all features available in the PCA Agent system.

---

## 🎯 Core Features

### 1. Campaign Management

**Description**: Upload, manage, and analyze campaign data from multiple advertising platforms.

**Capabilities**:
- Multi-format upload (CSV, Excel, Parquet)
- Multi-platform support (Google Ads, Meta, DV360, LinkedIn, Snapchat, CM360, TradeDesk)
- Automatic data validation and normalization
- Campaign metadata management
- Bulk upload and processing

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@data/samples/csv/google_ads_sample.csv"

# Via Frontend
1. Login at http://localhost:3000
2. Navigate to "Upload" page
3. Drag and drop CSV file
4. View uploaded data in "Campaigns" page
```

**Sample Data**: `data/samples/csv/*.csv`

---

### 2. Pacing Reports

**Description**: Generate dynamic Excel reports with pivot analysis, budget tracking, and pacing indicators.

**Capabilities**:
- Dynamic pivot tables by channel, platform, region
- SUMIF formulas for automatic aggregation
- Budget pacing calculations (on-track, over-pacing, under-pacing)
- Multiple report templates (Executive Summary, Campaign Tracker, Platform Comparison)
- Customizable date ranges (daily, weekly, monthly)
- Conditional formatting and visualizations

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/pacing-reports/generate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": "1",
    "date_range": "daily",
    "filters": {"platform": "Google Ads"}
  }'

# Via Frontend
1. Go to "Pacing Reports" page
2. Select template
3. Choose date range and filters
4. Click "Generate Report"
5. Download Excel file
```

**Features**:
- ✅ Adaptive sheet population (preserves template structure)
- ✅ Dynamic channel discovery
- ✅ Persistent formulas in Excel output
- ✅ Multiple template support

**Sample Template**: `data/samples/excel/pacing_template_sample.xlsx`

---

### 3. Regression Analysis

**Description**: Advanced regression modeling with SHAP explainability and budget recommendations.

**Capabilities**:
- Multiple regression models (Linear, Ridge, Random Forest, XGBoost)
- SHAP (SHapley Additive exPlanations) for feature importance
- Channel contribution analysis
- Auto-recommendations (Scale, Hold, Cut)
- Budget optimization suggestions
- Model performance metrics (R², RMSE, MAE)

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/regression/analyze \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "conversions",
    "features": ["spend", "impressions", "clicks"],
    "model_type": "xgboost"
  }'

# Via Frontend
1. Go to "Regression" page
2. Select target metric (Conversions, Revenue, etc.)
3. Choose features and model type
4. Click "Run Analysis"
5. View SHAP plots and recommendations
```

**Features**:
- ✅ SHAP summary plots
- ✅ Channel contribution breakdown
- ✅ Budget recommendations with confidence scores
- ✅ Model comparison

---

### 4. Analytics Studio

**Description**: Interactive dashboards with real-time visualizations and insights.

**Capabilities**:
- Performance metrics (Spend, Impressions, Clicks, Conversions, CTR, CPC, CPA, ROAS)
- Trend analysis over time
- Platform/channel comparison
- Geographic performance
- Device performance
- Funnel analysis
- Custom date ranges and filters

**How to Use**:
```bash
# Via Frontend
1. Go to "Analytics Studio" or "Visualizations" page
2. Select filters (platform, channel, date range)
3. View interactive charts:
   - Line charts for trends
   - Bar charts for comparisons
   - Pie charts for distribution
   - Tables for detailed data
```

**Visualizations**:
- 📊 Spend trends over time
- 📈 Performance by platform
- 🥧 Channel distribution
- 📉 CTR/CPC/CPA trends
- 🗺️ Geographic heatmaps

---

### 5. Natural Language Q&A

**Description**: Ask questions about your campaign data in plain English and get SQL-powered answers.

**Capabilities**:
- Natural language to SQL conversion
- Context-aware query understanding
- Multi-table joins
- Aggregations and calculations
- Query clarification for ambiguous questions
- SQL query display for transparency

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is the total spend by platform?"
  }'

# Via Frontend
1. Go to "Chat" page
2. Type your question in natural language
3. View results in table format
4. See generated SQL query
```

**Example Questions**:
- "What's the total spend by platform?"
- "Show me campaigns with CTR > 2%"
- "Which channel has the best ROAS?"
- "Compare Google Ads vs Meta Ads performance"
- "What's the average CPA for mobile campaigns?"

---

### 6. RAG Knowledge Base

**Description**: Hybrid retrieval-augmented generation system with industry benchmarks and best practices.

**Capabilities**:
- Vector retrieval (FAISS + OpenAI embeddings)
- Keyword retrieval (BM25)
- Hybrid retrieval (Reciprocal Rank Fusion)
- Cohere reranking (optional)
- Metadata filtering (category, priority)
- Knowledge base ingestion from URLs, PDFs, YouTube

**How to Use**:
```bash
# Ingest knowledge sources
python scripts/auto_ingest_knowledge.py --source-file knowledge_sources_priority1.txt

# Query via API
curl -X POST http://localhost:8000/api/v1/knowledge/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What are best practices for Google Ads CTR?",
    "top_k": 5
  }'
```

**Features**:
- ✅ Hybrid retrieval (vector + keyword)
- ✅ Cohere reranking for better relevance
- ✅ Metadata filtering
- ✅ Retrieval metrics monitoring
- ✅ Context injection for LLM reasoning

**Knowledge Sources**: `knowledge_sources/` directory

---

### 7. Vision-Based Extraction

**Description**: Extract metrics, graphs, and tables from dashboard screenshots using GPT-4V/Claude.

**Capabilities**:
- Screenshot analysis
- Table extraction
- Chart/graph interpretation
- Multi-page PDF processing
- OCR for text extraction

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/vision/extract \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@dashboard_screenshot.png"
```

**Supported Formats**: PNG, JPG, JPEG, PDF

---

### 8. Automated Report Generation

**Description**: Generate branded PowerPoint reports with data, visuals, and AI-generated insights.

**Capabilities**:
- PowerPoint report generation
- Custom templates
- Data visualizations (charts, tables)
- AI-generated insights and recommendations
- Brand customization (colors, logos)

**How to Use**:
```bash
# Via API
curl -X POST http://localhost:8000/api/v1/reports/generate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "campaign_id": "123",
    "template": "corporate"
  }'
```

---

## 🔒 Security Features

### Authentication & Authorization

**Features**:
- JWT-based authentication
- Bcrypt password hashing (cost factor: 12)
- Token expiration (24 hours default)
- Role-based access control (RBAC)
- Secure session management

**How to Use**:
```bash
# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"auditor","password":"audit123"}'

# Use token
curl -X GET http://localhost:8000/api/v1/campaigns \
  -H "Authorization: Bearer $TOKEN"
```

### Input Validation

**Features**:
- SQL injection prevention (parameterized queries)
- XSS protection (input sanitization)
- File upload validation (type, size, content)
- Request validation (Pydantic models)

### API Security

**Features**:
- CORS configuration (whitelist)
- Rate limiting on sensitive endpoints
- Security headers (CSP, X-Frame-Options)
- HTTPS enforcement (production)

---

## 🧪 Testing Features

### Test Coverage

**Statistics**:
- **Total Tests**: 856+
- **Unit Tests**: 207+ files
- **Integration Tests**: Available
- **E2E Tests**: Playwright (frontend)
- **Security Tests**: Available

**How to Run**:
```bash
# All tests
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=src --cov-report=html

# Security tests
pytest tests/security/ -v

# E2E tests
cd frontend && npx playwright test
```

---

## 📊 Data Processing Features

### DuckDB Analytics

**Features**:
- Fast columnar analytics
- Parquet storage (10x compression)
- Performance indexes
- Parallel query execution
- SQL query optimization

**Database Files**:
- `data/analytics.duckdb` - DuckDB database
- `data/campaigns.parquet` - Campaign data (columnar)

### Multi-Platform Support

**Supported Platforms**:
- Google Ads (Search, Display, Video)
- Meta Ads (Facebook, Instagram)
- DV360 (Programmatic)
- LinkedIn Ads (B2B)
- Snapchat Ads (Stories)
- CM360 (Display)
- TradeDesk (Programmatic)

---

## 🚀 API Features

### RESTful API

**Endpoints**:
- `/api/v1/campaigns` - Campaign management
- `/api/v1/pacing-reports` - Pacing reports
- `/api/v1/regression` - Regression analysis
- `/api/v1/chat` - Natural language Q&A
- `/api/v1/knowledge` - RAG knowledge base
- `/api/v1/auth` - Authentication
- `/api/v1/visualizations` - Data visualizations

**Documentation**: http://localhost:8000/docs (when running)

---

## 🎨 Frontend Features

### Next.js Production Interface

**Technology Stack**:
- Next.js 14
- React 18
- TypeScript
- TailwindCSS
- Recharts (visualizations)

**Pages**:
- `/` - Dashboard home
- `/campaigns` - Campaign list
- `/upload` - Data upload
- `/pacing-reports` - Pacing reports
- `/regression` - Regression analysis
- `/visualizations-2` - Analytics Studio
- `/chat` - Natural language Q&A
- `/login` - Authentication

---

## 📈 Performance Features

### Optimization

**Features**:
- DuckDB indexes for fast queries
- Parquet columnar storage
- Redis caching (optional)
- Parallel processing
- Query optimization

**Performance Metrics**:
- Query response time: < 100ms (indexed queries)
- Data upload: Handles 100K+ rows
- Report generation: < 5 seconds

---

## 🔧 Developer Features

### Code Quality

**Tools**:
- Black (code formatting)
- Flake8 (linting)
- MyPy (type checking)
- Bandit (security scanning)
- Pre-commit hooks

**How to Use**:
```bash
# Format code
black src/

# Lint
flake8 src/

# Type check
mypy src/

# Security scan
bandit -r src/
```

---

## 📚 Documentation

**Available Docs**:
- `README.md` - Project overview
- `SETUP.md` - Setup guide
- `FEATURES.md` - This file
- `docs/SECURITY_TESTING.md` - Security testing
- `docs/DATA_REQUIREMENTS.md` - Data schemas
- `TESTING_GUIDE.md` - Testing procedures
- `DEPLOYMENT_GUIDE.md` - Deployment instructions

---

## 🎯 Quick Start

1. **Clone repository**:
   ```bash
   git clone https://github.com/ashwinsharma89/pca_with_pacing_28_Dec
   cd pca_with_pacing_28_Dec
   ```

2. **Setup backend**:
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env
   # Edit .env with API keys
   ```

3. **Seed database**:
   ```bash
   python scripts/seed_database.py
   ```

4. **Start application**:
   ```bash
   # Terminal 1: Backend
   uvicorn src.api.main:app --reload

   # Terminal 2: Frontend
   cd frontend && npm install && npm run dev
   ```

5. **Access application**:
   - Frontend: http://localhost:3000
   - API Docs: http://localhost:8000/docs
   - Login: username=`auditor`, password=`audit123`

---

## 💡 Tips & Best Practices

1. **Use sample data** for testing: `data/samples/`
2. **Check API docs** for endpoint details: http://localhost:8000/docs
3. **Run tests** before deploying: `pytest tests/unit/ -v`
4. **Monitor logs** for debugging: `logs/pca_agent.json`
5. **Use DuckDB** for fast analytics (no PostgreSQL needed for dev)

---

## 🆘 Support

- **Setup Issues**: See `SETUP.md`
- **Security Questions**: See `docs/SECURITY_TESTING.md`
- **Data Format Questions**: See `docs/DATA_REQUIREMENTS.md`
- **Testing Help**: See `TESTING_GUIDE.md`

---

**Last Updated**: 2025-12-28  
**Version**: 1.0.0
