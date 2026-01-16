# Sample Data for PCA Agent

This directory contains small sample datasets for testing and auditing the PCA Agent system.

## Directory Structure

```
data/samples/
├── csv/                    # Sample CSV files (50 rows each)
│   ├── google_ads_sample.csv
│   ├── meta_ads_sample.csv
│   ├── dv360_sample.csv
│   ├── linkedin_ads_sample.csv
│   └── snapchat_ads_sample.csv
├── excel/                  # Sample Excel templates
│   └── pacing_template_sample.xlsx
└── parquet/                # Sample Parquet files
    └── campaigns_sample.parquet
```

## Usage

### 1. Upload via Frontend

1. Start the application (see [SETUP.md](../../SETUP.md))
2. Login at http://localhost:3000
3. Navigate to "Upload" page
4. Drag and drop any CSV file from `csv/` directory
5. View extracted data and insights

### 2. Upload via API

```bash
# Get JWT token first
TOKEN=$(curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"auditor","password":"audit123"}' \
  | jq -r '.access_token')

# Upload sample data
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@data/samples/csv/google_ads_sample.csv"
```

### 3. Load Directly into DuckDB

```python
import pandas as pd
from src.database.duckdb_manager import get_duckdb_manager

# Load sample CSV
df = pd.read_csv("data/samples/csv/google_ads_sample.csv")

# Save to DuckDB
db = get_duckdb_manager()
db.save_campaigns(df)
```

## Sample Data Details

### CSV Files

Each CSV file contains **50 rows** of sample campaign data for different platforms:

- **google_ads_sample.csv**: Google Ads campaigns (Search, Display, Video)
- **meta_ads_sample.csv**: Facebook/Instagram campaigns
- **dv360_sample.csv**: Display & Video 360 programmatic campaigns
- **linkedin_ads_sample.csv**: LinkedIn B2B campaigns
- **snapchat_ads_sample.csv**: Snapchat Story ads

**Common Columns:**
- `Date`: Campaign date (YYYY-MM-DD)
- `Platform`: Advertising platform
- `Campaign`: Campaign name
- `Spend`: Total spend (USD)
- `Impressions`: Number of impressions
- `Clicks`: Number of clicks
- `Conversions`: Number of conversions
- `CTR`: Click-through rate (%)
- `CPC`: Cost per click (USD)
- `CPA`: Cost per acquisition (USD)

### Excel Template

**pacing_template_sample.xlsx**: Sample pacing report template with:
- Executive Summary sheet
- Pivot Analysis sheet
- Campaign Tracker sheet
- Pre-configured formulas and formatting

### Parquet File

**campaigns_sample.parquet**: Compressed columnar format with 50 rows
- Same schema as CSV files
- Optimized for analytics queries
- Used by DuckDB for fast processing

## Data Schema

See [docs/DATA_REQUIREMENTS.md](../../docs/DATA_REQUIREMENTS.md) for complete schema documentation.

### Required Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| Date | date | Campaign date | 2024-01-01 |
| Platform | string | Ad platform | Google Ads |
| Campaign | string | Campaign name | Holiday Sale 2024 |
| Spend | float | Total spend (USD) | 1000.50 |
| Impressions | integer | Number of impressions | 50000 |
| Clicks | integer | Number of clicks | 1500 |

### Optional Columns

- `Conversions`: Number of conversions
- `CTR`: Click-through rate (calculated if missing)
- `CPC`: Cost per click (calculated if missing)
- `CPA`: Cost per acquisition (calculated if missing)
- `Channel`: Marketing channel (Search, Social, Display)
- `Region`: Geographic region
- `Device`: Device type (Desktop, Mobile, Tablet)
- `Objective`: Campaign objective (Awareness, Consideration, Conversion)

## Generating More Sample Data

If you need to create additional sample data:

```bash
# Create sample data from existing datasets
python scripts/create_sample_data.py --rows 100 --output data/samples/csv/custom_sample.csv

# Or extract from existing files
head -n 101 data/google_ads_dataset.csv > data/samples/csv/google_ads_large_sample.csv
```

## Testing with Sample Data

### Unit Tests

```bash
# Test sample data integrity
pytest tests/unit/test_sample_data.py -v
```

### Integration Tests

```bash
# Test upload and processing
pytest tests/integration/test_campaign_upload.py -v --sample-data
```

### E2E Tests

```bash
# Test full workflow with sample data
cd frontend
npx playwright test tests/e2e/upload-sample-data.spec.ts
```

## Notes

- All sample files are **< 100 KB** to avoid bloating the git repository
- Data is anonymized and does not contain real campaign information
- Files are safe to commit to version control
- Sample data is automatically loaded by `scripts/seed_database.py`

## Support

For questions about sample data or data formats, see:
- [SETUP.md](../../SETUP.md) - Setup guide
- [docs/DATA_REQUIREMENTS.md](../../docs/DATA_REQUIREMENTS.md) - Data schema
- [TESTING_GUIDE.md](../../TESTING_GUIDE.md) - Testing procedures
