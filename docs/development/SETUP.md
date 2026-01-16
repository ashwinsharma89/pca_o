# PCA Agent - Setup Guide for Auditors

This guide will help you set up the PCA Agent system from a fresh git clone and explore all features including security, testing, and data processing.

## Prerequisites

- **Python 3.11+** (Python 3.12 recommended)
- **Node.js 18+** and npm
- **Git**
- **Optional**: PostgreSQL 14+ (for production mode; DuckDB works for development)

## Quick Start (15 minutes)

### 1. Clone Repository

```bash
git clone https://github.com/ashwinsharma89/pca_with_pacing_28_Dec
cd pca_with_pacing_28_Dec
```

### 2. Backend Setup

```bash
# Create Python virtual environment
python3.11 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env and add your API keys (see Configuration section below)
```

### 3. Database Setup (DuckDB - Development Mode)

```bash
# Seed database with sample data
python scripts/seed_database.py

# This creates:
# - data/analytics.duckdb (DuckDB database)
# - data/campaigns.parquet (campaign data)
# - Sample user: username=auditor, password=audit123
```

### 4. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

### 5. Start Backend API

```bash
# In a new terminal, from project root
source venv/bin/activate
uvicorn src.api.main:app --reload --port 8000
```

### 6. Access Application

- **Frontend**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs
- **Login**: username=`auditor`, password=`audit123`

---

## Configuration

### Required Environment Variables

Edit `.env` file with the following:

#### API Keys (Required for AI features)
```bash
# At least one LLM provider is required
OPENAI_API_KEY=sk-your-key-here              # GPT-4, GPT-4V
ANTHROPIC_API_KEY=sk-ant-your-key-here       # Claude 3.5 Sonnet (optional)
GOOGLE_API_KEY=your-key-here                 # Gemini (optional, free tier)
```

#### Database Configuration
```bash
# DuckDB (Development - Default)
# No configuration needed, uses data/analytics.duckdb

# PostgreSQL (Production - Optional)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pca_agent
DB_USER=postgres
DB_PASSWORD=your_password
```

#### Security Settings
```bash
# JWT Secret (IMPORTANT: Change this!)
JWT_SECRET_KEY=your-secure-random-secret-here
# Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"

# CORS (Frontend URL)
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

#### Optional Services
```bash
# Redis (for caching - optional)
REDIS_ENABLED=false
REDIS_URL=redis://localhost:6379/0

# Celery (for background tasks - optional)
CELERY_BROKER_URL=redis://localhost:6379/1
```

---

## Database Options

### Option 1: DuckDB (Recommended for Audit)

**Pros**: No setup required, fast analytics, works out of the box
**Cons**: Single-user, file-based

```bash
# Already configured in .env.example
# Data stored in: data/analytics.duckdb
# Campaign data: data/campaigns.parquet
```

### Option 2: PostgreSQL (Production)

**Pros**: Multi-user, production-ready, ACID compliant
**Cons**: Requires PostgreSQL installation

```bash
# Install PostgreSQL
# macOS: brew install postgresql@14
# Ubuntu: sudo apt-get install postgresql-14

# Create database
createdb pca_agent

# Run migrations
alembic upgrade head

# Seed data
python scripts/seed_database.py --use-postgres
```

---

## Sample Data

Sample data is provided in `data/samples/` for testing:

```
data/samples/
├── csv/
│   ├── google_ads_sample.csv          # 50 rows
│   ├── meta_ads_sample.csv            # 50 rows
│   ├── dv360_sample.csv               # 50 rows
│   ├── linkedin_ads_sample.csv        # 50 rows
│   └── snapchat_ads_sample.csv        # 50 rows
├── excel/
│   └── pacing_template_sample.xlsx    # Pacing report template
└── parquet/
    └── campaigns_sample.parquet        # 50 rows
```

### Loading Sample Data

```bash
# Via API (after starting backend)
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -F "file=@data/samples/csv/google_ads_sample.csv"

# Via Frontend
# 1. Login at http://localhost:3000
# 2. Go to "Upload" page
# 3. Drag and drop CSV file
```

---

## Testing

### Run Unit Tests

```bash
# All tests (856 tests)
pytest tests/unit/ -v

# With coverage report
pytest tests/unit/ --cov=src --cov-report=html

# View coverage
open htmlcov/index.html  # macOS
# or
xdg-open htmlcov/index.html  # Linux
```

### Run Integration Tests

```bash
pytest tests/integration/ -v
```

### Run E2E Tests (Frontend)

```bash
cd frontend

# Install Playwright browsers (first time only)
npx playwright install

# Run E2E tests
npx playwright test

# Run with UI
npx playwright test --ui
```

### Security Testing

See [docs/SECURITY_TESTING.md](docs/SECURITY_TESTING.md) for detailed security testing procedures.

```bash
# Run security tests
pytest tests/security/ -v

# Security scan with Bandit
bandit -r src/ -f json -o bandit_report.json
```

---

## Features to Explore

### 1. Campaign Upload & Analysis

**Via Frontend:**
1. Login at http://localhost:3000
2. Navigate to "Upload" page
3. Upload CSV file from `data/samples/csv/`
4. View extracted data and insights

**Via API:**
```bash
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -F "file=@data/samples/csv/google_ads_sample.csv"
```

### 2. Pacing Reports

**Generate pacing report:**
1. Go to "Pacing Reports" page
2. Select template
3. Choose date range and filters
4. Click "Generate Report"
5. Download Excel file

**Via API:**
```bash
curl -X POST http://localhost:8000/api/v1/pacing-reports/generate \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": "1",
    "date_range": "daily",
    "filters": {}
  }'
```

### 3. Analytics & Visualizations

**Explore dashboards:**
- **Analytics Studio**: http://localhost:3000/visualizations-2
- **Regression Analysis**: http://localhost:3000/regression
- **Campaign Dashboard**: http://localhost:3000/campaigns

### 4. Q&A with Natural Language

**Ask questions about your data:**
1. Go to "Chat" or Q&A page
2. Ask questions like:
   - "What's the total spend by platform?"
   - "Show me campaigns with CTR > 2%"
   - "Which channel has the best ROAS?"

### 5. Security Features

**Authentication:**
- JWT-based authentication
- Secure password hashing (bcrypt)
- Token expiration (24 hours default)

**Authorization:**
- Role-based access control
- User-specific data isolation

**Input Validation:**
- SQL injection prevention
- XSS protection
- CORS restrictions

---

## Troubleshooting

### Backend won't start

**Error**: `ModuleNotFoundError: No module named 'X'`
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

**Error**: `Database connection failed`
```bash
# Check DuckDB file exists
ls -la data/analytics.duckdb

# Recreate database
python scripts/seed_database.py
```

### Frontend won't start

**Error**: `npm ERR! missing script: dev`
```bash
# Ensure you're in frontend directory
cd frontend

# Reinstall dependencies
rm -rf node_modules package-lock.json
npm install
```

**Error**: `CORS error when calling API`
```bash
# Check CORS_ALLOWED_ORIGINS in .env includes http://localhost:3000
# Restart backend after changing .env
```

### Tests failing

**Error**: `No module named 'pytest'`
```bash
# Install test dependencies
pip install -r requirements-test.txt
```

**Error**: `Database not found in tests`
```bash
# Tests use separate test database
# Ensure data/test_pca.db can be created
chmod 755 data/
```

### Sample data not loading

**Error**: `File not found: data/samples/csv/...`
```bash
# Create sample data
python scripts/create_sample_data.py

# Or use existing data
cp data/google_ads_dataset.csv data/samples/csv/google_ads_sample.csv
```

---

## Development Workflow

### Code Quality

```bash
# Format code
black src/

# Lint
flake8 src/

# Type checking
mypy src/

# Security scan
bandit -r src/
```

### Running in Production Mode

```bash
# Set production environment
export PRODUCTION_MODE=true
export DEBUG=false

# Use PostgreSQL instead of DuckDB
export DB_HOST=your-db-host
export DB_NAME=pca_agent_prod

# Use Gunicorn for production
gunicorn src.api.main:app -w 4 -k uvicorn.workers.UvicornWorker
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend (Next.js)                    │
│  - React Components                                      │
│  - Recharts Visualizations                              │
│  - TailwindCSS Styling                                   │
└─────────────────────┬───────────────────────────────────┘
                      │ HTTP/REST
┌─────────────────────▼───────────────────────────────────┐
│                  API Layer (FastAPI)                     │
│  - Authentication (JWT)                                  │
│  - Campaign Management                                   │
│  - Pacing Reports                                        │
│  - Analytics Endpoints                                   │
└─────────────────────┬───────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
┌───────▼──────┐ ┌───▼────┐ ┌─────▼──────┐
│   DuckDB     │ │ Parquet│ │  AI Agents │
│  Analytics   │ │Campaign│ │  (GPT-4)   │
│   Database   │ │  Data  │ │            │
└──────────────┘ └────────┘ └────────────┘
```

### Tech Stack

- **Backend**: FastAPI, Python 3.11+
- **Database**: DuckDB (analytics), Parquet (data storage)
- **Frontend**: Next.js 14, React, TypeScript
- **AI/ML**: OpenAI GPT-4, Anthropic Claude, LangChain
- **Testing**: Pytest, Playwright
- **Visualization**: Recharts, Plotly

---

## Data Schema

See [docs/DATA_REQUIREMENTS.md](docs/DATA_REQUIREMENTS.md) for detailed schema documentation.

### Campaign Data Format

```csv
Date,Platform,Campaign,Spend,Impressions,Clicks,Conversions,CTR,CPC,CPA
2024-01-01,Google Ads,Holiday Campaign,1000,50000,1500,50,3.0,0.67,20.0
```

**Required Columns:**
- `Date`: YYYY-MM-DD format
- `Platform`: Google Ads, Meta Ads, DV360, etc.
- `Spend`: Numeric (USD)
- `Impressions`: Integer
- `Clicks`: Integer

**Optional Columns:**
- `Conversions`, `CTR`, `CPC`, `CPA`, `ROAS`
- `Channel`, `Region`, `Device`, `Objective`

---

## Support & Documentation

- **API Documentation**: http://localhost:8000/docs (when running)
- **Testing Guide**: [TESTING_GUIDE.md](TESTING_GUIDE.md)
- **Security Testing**: [docs/SECURITY_TESTING.md](docs/SECURITY_TESTING.md)
- **Data Requirements**: [docs/DATA_REQUIREMENTS.md](docs/DATA_REQUIREMENTS.md)
- **Deployment Guide**: [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

---

## Next Steps

1. ✅ Complete setup following this guide
2. ✅ Login with sample credentials
3. ✅ Upload sample CSV data
4. ✅ Generate a pacing report
5. ✅ Run test suite
6. ✅ Explore security features
7. ✅ Review API documentation
8. ✅ Test E2E workflows

**Estimated time to full functionality**: 15-20 minutes

---

## License

MIT License - See LICENSE file for details
