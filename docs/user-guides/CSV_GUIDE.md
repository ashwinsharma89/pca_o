# CSV Input Guide for PCA Agent

## Overview

PCA Agent now supports **CSV files as input** in addition to dashboard screenshots. This allows you to:
- Skip vision extraction if you already have data
- Faster processing (no AI vision calls needed)
- Use data from custom sources or exports
- Combine multiple data sources easily

## CSV Format Requirements

### Required Columns

1. **Platform** (Required)
   - Platform identifier
   - Valid values: `google_ads`, `meta_ads`, `linkedin_ads`, `dv360`, `cm360`, `snapchat_ads`
   - Case-insensitive

2. **At least 3 metrics** (Required)
   - Choose from supported metrics below
   - More metrics = better analysis

### Supported Metrics

#### Performance Metrics
- `Impressions` - Total ad impressions
- `Clicks` - Total clicks
- `CTR` - Click-through rate (%)
- `Conversions` - Total conversions
- `Conversion_Rate` - Conversion rate (%)

#### Cost Metrics
- `Spend` - Total spend (currency)
- `CPC` - Cost per click
- `CPM` - Cost per thousand impressions
- `CPA` - Cost per acquisition
- `ROAS` - Return on ad spend (ratio)

#### Engagement Metrics
- `Likes` - Social likes/reactions
- `Shares` - Social shares
- `Comments` - Social comments
- `Video_Views` - Video view count
- `Video_Completion_Rate` - Video completion (%)

#### Reach Metrics
- `Reach` - Unique users reached
- `Frequency` - Average frequency
- `Unique_Users` - Unique user count

#### Quality Metrics
- `Quality_Score` - Quality score (1-10)
- `Relevance_Score` - Relevance score (1-10)
- `Viewability` - Viewability percentage

## Sample CSV Templates

### Basic Template (Minimum Required)

```csv
Platform,Impressions,Clicks,Spend
google_ads,1250000,25000,45000
meta_ads,980000,18500,32000
linkedin_ads,450000,12000,18000
```

### Standard Template (Recommended)

```csv
Platform,Impressions,Clicks,CTR,Conversions,Spend,CPC,CPA,ROAS
google_ads,1250000,25000,2.0,850,45000,1.80,52.94,4.2
meta_ads,980000,18500,1.89,620,32000,1.73,51.61,3.8
linkedin_ads,450000,12000,2.67,380,18000,1.50,47.37,3.5
dv360,2100000,31500,1.5,720,38000,1.21,52.78,3.2
```

### Complete Template (All Metrics)

```csv
Platform,Impressions,Clicks,CTR,Conversions,Spend,CPC,CPM,CPA,ROAS,Reach,Frequency,Likes,Shares,Comments
google_ads,1250000,25000,2.0,850,45000,1.80,36.00,52.94,4.2,980000,1.28,0,0,0
meta_ads,980000,18500,1.89,620,32000,1.73,32.65,51.61,3.8,750000,1.31,12500,3200,1800
linkedin_ads,450000,12000,2.67,380,18000,1.50,40.00,47.37,3.5,380000,1.18,4500,890,420
```

## How to Use CSV Input

### In Streamlit Dashboard

1. **Navigate to "New Analysis" tab**
2. **Select "📊 CSV Data Files"** radio button
3. **Download sample template** (optional)
4. **Prepare your CSV file(s)**:
   - One CSV per platform, OR
   - One CSV with all platforms (multiple rows)
5. **Upload CSV file(s)**
6. **Review preview** to verify data
7. **Click "Analyze Campaign"**

### CSV File Naming

Recommended naming convention:
- `google_ads_campaign.csv`
- `meta_ads_q4_2024.csv`
- `all_platforms_data.csv`

### Multiple CSV Files

You can upload multiple CSV files:
- **Option 1**: One file per platform
  - `google_ads.csv`
  - `meta_ads.csv`
  - `linkedin_ads.csv`

- **Option 2**: One combined file
  - `all_campaigns.csv` (with multiple rows)

## Data Validation

The system will automatically:
- ✅ Detect metric columns
- ✅ Validate platform names
- ✅ Check for required columns
- ✅ Show preview of detected data
- ✅ Highlight any issues

### Common Issues

**Issue**: "Platform column not found"
- **Solution**: Add a column named "Platform" with platform identifiers

**Issue**: "Not enough metrics"
- **Solution**: Include at least 3 metric columns (Impressions, Clicks, Spend, etc.)

**Issue**: "Invalid platform name"
- **Solution**: Use valid platform names: google_ads, meta_ads, linkedin_ads, dv360, cm360, snapchat_ads

**Issue**: "CSV parsing error"
- **Solution**: Ensure proper CSV format (comma-separated, no extra quotes)

## Exporting Data from Platforms

### Google Ads
1. Go to Reports → Predefined Reports → Campaign Performance
2. Select date range
3. Click Download → CSV
4. Add "Platform" column with value "google_ads"

### Meta Ads Manager
1. Go to Ads Manager → Campaigns
2. Select date range
3. Click Export → Export Table Data → CSV
4. Add "Platform" column with value "meta_ads"

### LinkedIn Campaign Manager
1. Go to Campaign Manager → Campaigns
2. Select date range
3. Click Export → CSV
4. Add "Platform" column with value "linkedin_ads"

### DV360
1. Go to Reporting
2. Create custom report with required metrics
3. Export as CSV
4. Add "Platform" column with value "dv360"

## CSV vs Screenshots

| Feature | CSV Input | Screenshot Input |
|---------|-----------|------------------|
| **Speed** | ⚡ Fast (no vision AI) | 🐌 Slower (vision processing) |
| **Accuracy** | ✅ 100% (direct data) | 📊 90%+ (depends on image quality) |
| **Setup** | 📝 Manual CSV prep | 📸 Just screenshot |
| **Flexibility** | 🔧 Custom data sources | 🎯 Any dashboard |
| **Cost** | 💰 No vision API costs | 💸 Vision API costs |

## Best Practices

1. **Use consistent column names**: Match the supported metric names
2. **Include currency info**: Add currency column if using multiple currencies
3. **Date ranges**: Ensure all platforms use the same date range
4. **Clean data**: Remove any summary rows or headers
5. **Validate first**: Upload and preview before analyzing
6. **Backup**: Keep original CSV files

## Advanced Usage

### Custom Metrics

If you have custom metrics not in the standard list:
1. Add them as additional columns
2. System will attempt to categorize them
3. They'll appear in "Other Metrics" section

### Multi-Period Analysis

For time-series data:
```csv
Platform,Date,Impressions,Clicks,Spend
google_ads,2024-10-01,125000,2500,4500
google_ads,2024-10-02,130000,2600,4600
google_ads,2024-10-03,128000,2550,4550
```

### Calculated Metrics

You can include pre-calculated metrics:
- CTR = (Clicks / Impressions) × 100
- CPC = Spend / Clicks
- CPA = Spend / Conversions
- ROAS = Revenue / Spend

Or let the system calculate them from base metrics.

## Example Workflow

### Scenario: Quarterly Campaign Review

1. **Export data from each platform**:
   - Google Ads → `google_ads_q4.csv`
   - Meta Ads → `meta_ads_q4.csv`
   - LinkedIn → `linkedin_ads_q4.csv`

2. **Add Platform column** to each CSV

3. **Upload to PCA Agent**:
   - Select CSV input method
   - Upload all 3 files
   - Review preview

4. **Analyze**:
   - System processes data
   - Generates insights
   - Creates PowerPoint report

5. **Result**:
   - Complete analysis in 1-2 minutes
   - No vision API costs
   - 100% data accuracy

## Download Sample Template

In the Streamlit dashboard:
1. Select "📊 CSV Data Files"
2. Click "📥 Download Sample CSV Template"
3. Edit with your campaign data
4. Upload and analyze

## Support

For issues with CSV input:
- Check CSV format matches examples
- Verify column names are correct
- Ensure Platform column exists
- Review preview before analyzing

---

**CSV input makes PCA Agent faster and more flexible. Use it when you have data exports or want to skip vision extraction!**
