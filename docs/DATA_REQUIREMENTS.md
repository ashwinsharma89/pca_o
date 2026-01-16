# Data Requirements & Schema Documentation

This document describes the data formats, schemas, and requirements for the PCA Agent system.

## Overview

PCA Agent supports multiple data formats and advertising platforms. Data can be uploaded via CSV, Excel, Parquet, or directly from API integrations.

---

## Supported Platforms

- **Google Ads**: Search, Display, Video campaigns
- **Meta Ads**: Facebook, Instagram campaigns  
- **DV360**: Display & Video 360 programmatic campaigns
- **LinkedIn Ads**: B2B lead generation campaigns
- **Snapchat Ads**: Story ads and lens engagement
- **CM360**: Campaign Manager 360 display campaigns
- **TradeDesk**: Programmatic advertising platform

---

## CSV File Format

### Required Columns

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `Date` | date | Campaign date (YYYY-MM-DD) | 2024-01-15 |
| `Platform` | string | Advertising platform | Google Ads |
| `Campaign` | string | Campaign name | Holiday Sale 2024 |
| `Spend` | float | Total spend in USD | 1250.50 |
| `Impressions` | integer | Number of impressions | 125000 |
| `Clicks` | integer | Number of clicks | 3500 |

### Optional Columns

| Column Name | Data Type | Description | Calculated If Missing |
|------------|-----------|-------------|----------------------|
| `Conversions` | integer | Number of conversions | No |
| `CTR` | float | Click-through rate (%) | Yes: (Clicks/Impressions)*100 |
| `CPC` | float | Cost per click (USD) | Yes: Spend/Clicks |
| `CPA` | float | Cost per acquisition (USD) | Yes: Spend/Conversions |
| `ROAS` | float | Return on ad spend | No |
| `CPM` | float | Cost per 1000 impressions | Yes: (Spend/Impressions)*1000 |

### Dimensional Columns (Optional)

| Column Name | Description | Example Values |
|------------|-------------|----------------|
| `Channel` | Marketing channel | Search, Social, Display, Video |
| `Region` | Geographic region | North America, EMEA, APAC |
| `State` | State/Province | California, Texas, Ontario |
| `Device` | Device type | Desktop, Mobile, Tablet |
| `Objective` | Campaign objective | Awareness, Consideration, Conversion |
| `Ad_Group` | Ad group name | Brand Keywords, Retargeting |
| `Placement` | Ad placement | Facebook Feed, Google Search |
| `Funnel` | Funnel stage | Top, Middle, Bottom |

### Example CSV

```csv
Date,Platform,Campaign,Channel,Spend,Impressions,Clicks,Conversions,CTR,CPC,CPA
2024-01-01,Google Ads,Holiday Sale,Search,1000.00,50000,1500,50,3.0,0.67,20.00
2024-01-01,Meta Ads,Holiday Sale,Social,800.00,75000,2250,60,3.0,0.36,13.33
2024-01-02,Google Ads,Holiday Sale,Search,1100.00,52000,1560,52,3.0,0.71,21.15
```

---

## Database Schema

### DuckDB / Parquet Schema

The system uses DuckDB with Parquet files for analytics. Data is stored in columnar format for fast querying.

#### Campaigns Table

```sql
CREATE TABLE campaigns (
    Date DATE,
    Platform VARCHAR,
    Campaign VARCHAR,
    Channel VARCHAR,
    Spend DECIMAL(10,2),
    Impressions INTEGER,
    Clicks INTEGER,
    Conversions INTEGER,
    CTR DECIMAL(5,2),
    CPC DECIMAL(10,2),
    CPA DECIMAL(10,2),
    ROAS DECIMAL(10,2),
    CPM DECIMAL(10,2),
    Region VARCHAR,
    Device VARCHAR,
    Objective VARCHAR,
    Ad_Group VARCHAR,
    Placement VARCHAR,
    Funnel VARCHAR
);
```

#### Users Table (Authentication)

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR UNIQUE NOT NULL,
    email VARCHAR UNIQUE NOT NULL,
    hashed_password VARCHAR NOT NULL,
    full_name VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## API Request/Response Formats

### Upload Campaign Data

**Endpoint**: `POST /api/v1/campaigns/upload`

**Request** (multipart/form-data):
```bash
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@campaigns.csv"
```

**Response**:
```json
{
  "status": "success",
  "rows_imported": 150,
  "message": "Campaign data uploaded successfully"
}
```

### Get Campaign Data

**Endpoint**: `GET /api/v1/campaigns`

**Query Parameters**:
- `platform`: Filter by platform (e.g., "Google Ads")
- `channel`: Filter by channel (e.g., "Search")
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)
- `limit`: Max rows to return (default: 100)

**Response**:
```json
{
  "data": [
    {
      "date": "2024-01-01",
      "platform": "Google Ads",
      "campaign": "Holiday Sale",
      "spend": 1000.00,
      "impressions": 50000,
      "clicks": 1500,
      "conversions": 50,
      "ctr": 3.0,
      "cpc": 0.67,
      "cpa": 20.00
    }
  ],
  "total": 150,
  "filters_applied": {
    "platform": "Google Ads"
  }
}
```

### Generate Pacing Report

**Endpoint**: `POST /api/v1/pacing-reports/generate`

**Request**:
```json
{
  "template_id": "1",
  "date_range": "daily",
  "filters": {
    "platform": "Google Ads",
    "channel": "Search"
  }
}
```

**Response**:
```json
{
  "status": "success",
  "report_id": "pacing_report_daily_20240115_120000",
  "download_url": "/api/v1/pacing-reports/download/pacing_report_daily_20240115_120000.xlsx"
}
```

---

## Data Validation Rules

### Field Validation

1. **Date**: Must be valid date in YYYY-MM-DD format
2. **Spend**: Must be non-negative number
3. **Impressions**: Must be non-negative integer
4. **Clicks**: Must be non-negative integer, ≤ Impressions
5. **Conversions**: Must be non-negative integer, ≤ Clicks
6. **CTR**: If provided, must be between 0-100
7. **CPC**: If provided, must be non-negative
8. **CPA**: If provided, must be non-negative

### Data Quality Checks

```python
# Example validation
def validate_campaign_data(df):
    errors = []
    
    # Check required columns
    required = ['Date', 'Platform', 'Campaign', 'Spend', 'Impressions', 'Clicks']
    missing = set(required) - set(df.columns)
    if missing:
        errors.append(f"Missing required columns: {missing}")
    
    # Check data types
    if df['Spend'].dtype not in ['float64', 'int64']:
        errors.append("Spend must be numeric")
    
    # Check logical constraints
    if (df['Clicks'] > df['Impressions']).any():
        errors.append("Clicks cannot exceed Impressions")
    
    # Check for negative values
    if (df['Spend'] < 0).any():
        errors.append("Spend cannot be negative")
    
    return errors
```

---

## Platform-Specific Schemas

### Google Ads

**Additional Columns**:
- `Ad_Network`: Search, Display, Video
- `Match_Type`: Exact, Phrase, Broad
- `Quality_Score`: 1-10

### Meta Ads

**Additional Columns**:
- `Placement`: Facebook Feed, Instagram Stories, etc.
- `Audience`: Custom, Lookalike, Interest-based
- `Ad_Format`: Image, Video, Carousel

### DV360

**Additional Columns**:
- `Exchange`: Google Ad Manager, AppNexus, etc.
- `Creative_Type`: Banner, Video, Native
- `Viewability`: Percentage

---

## Excel Template Format

### Pacing Report Template

**Sheet 1: Executive Summary**
- Campaign overview
- Key metrics (Spend, Impressions, Clicks, Conversions)
- Performance vs. goals

**Sheet 2: Pivot Analysis**
- Dynamic pivot table by channel
- SUMIF formulas for aggregation
- Conditional formatting

**Sheet 3: Campaign Tracker**
- Detailed campaign-level data
- Trend analysis
- Pacing indicators

**Required Named Ranges**:
- `DataStartRow`: First row of data
- `ChannelColumn`: Column containing channel data
- `SpendColumn`: Column containing spend data

---

## Parquet File Format

### Advantages
- **Columnar storage**: Fast analytics queries
- **Compression**: 10x smaller than CSV
- **Type safety**: Preserves data types
- **Partitioning**: Efficient filtering

### Creating Parquet Files

```python
import pandas as pd

# Load CSV
df = pd.read_csv('campaigns.csv')

# Save as Parquet
df.to_parquet('campaigns.parquet', 
              index=False, 
              compression='snappy')
```

### Reading Parquet Files

```python
# With pandas
df = pd.read_parquet('campaigns.parquet')

# With DuckDB (faster for large files)
import duckdb
conn = duckdb.connect()
df = conn.execute("SELECT * FROM 'campaigns.parquet'").df()
```

---

## Common Data Issues & Solutions

### Issue: Missing Required Columns

**Error**: `Missing required columns: {'Date', 'Spend'}`

**Solution**: Ensure CSV has all required columns. Check for typos in column names.

### Issue: Invalid Date Format

**Error**: `Invalid date format in row 5`

**Solution**: Use YYYY-MM-DD format. Convert Excel dates:
```python
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
```

### Issue: Clicks > Impressions

**Error**: `Data validation failed: Clicks cannot exceed Impressions`

**Solution**: Check data source for errors. Clicks should always be ≤ Impressions.

### Issue: Negative Spend

**Error**: `Spend cannot be negative`

**Solution**: Check for data entry errors or currency conversion issues.

---

## Sample Data

Sample data files are provided in `data/samples/`:

- `google_ads_sample.csv`: 50 rows of Google Ads data
- `meta_ads_sample.csv`: 50 rows of Meta Ads data
- `dv360_sample.csv`: 50 rows of DV360 data
- `linkedin_ads_sample.csv`: 50 rows of LinkedIn data
- `snapchat_ads_sample.csv`: 50 rows of Snapchat data
- `campaigns_sample.parquet`: 50 rows in Parquet format

See [data/samples/README.md](../data/samples/README.md) for usage instructions.

---

## Data Import Best Practices

1. **Validate before upload**: Check data quality locally first
2. **Use consistent naming**: Standardize column names across platforms
3. **Include metadata**: Add campaign objective, channel, region when available
4. **Aggregate appropriately**: Daily or weekly granularity recommended
5. **Handle missing data**: Use NULL or 0 appropriately
6. **Test with samples**: Use sample data to verify format before bulk upload

---

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Parquet Format Specification](https://parquet.apache.org/docs/)
- [Pandas Data Types](https://pandas.pydata.org/docs/user_guide/basics.html#dtypes)
- [FastAPI Request Files](https://fastapi.tiangolo.com/tutorial/request-files/)
