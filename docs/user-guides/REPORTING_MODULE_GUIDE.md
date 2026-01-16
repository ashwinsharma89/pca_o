# PCA Agent - Reporting Module Guide

## How It Works: Complete Flow

### Overview
The Reporting Module automatically populates report templates with your campaign data by:
1. **Detecting placeholders** in your template
2. **Mapping placeholders** to data columns
3. **Replacing placeholders** with aggregated values
4. **Preserving all formatting** and structure

---

## Step-by-Step Example

### Example 1: Excel Report Template

#### 1. Create Your Template (`campaign_report_template.xlsx`)

**Sheet: Summary**
```
Campaign Performance Report
===========================

Total Spend: {{Total_Spend}}
Total Clicks: {{Total_Clicks}}
Total Conversions: {{Total_Conversions}}

Average CPC: {{Avg_CPC}}
Conversion Rate: {{Conv_Rate}}%

Platform Breakdown:
- Google Ads: {{Google_Spend}}
- Facebook: {{Facebook_Spend}}
- LinkedIn: {{LinkedIn_Spend}}
```

#### 2. Prepare Your Data (`campaign_data.csv`)

```csv
Date,Platform,Spend,Clicks,Conversions,CPC
2024-01-01,Google Ads,1500.50,450,25,3.33
2024-01-02,Google Ads,1800.75,520,30,3.46
2024-01-03,Facebook,950.25,380,18,2.50
2024-01-04,Facebook,1100.00,420,22,2.62
2024-01-05,LinkedIn,2200.00,180,15,12.22
```

#### 3. Upload & Map

**In the App:**
- Upload `campaign_report_template.xlsx` → System detects 7 placeholders
- Upload `campaign_data.csv` → System loads 5 rows
- Auto-mapping suggests:
  - `{{Total_Spend}}` → `Spend` column (Sum)
  - `{{Total_Clicks}}` → `Clicks` column (Sum)
  - `{{Total_Conversions}}` → `Conversions` column (Sum)
  - `{{Avg_CPC}}` → `CPC` column (Average)
  - `{{Google_Spend}}` → Filter Platform='Google Ads', sum Spend
  - etc.

#### 4. Generate Report

**Output (`campaign_report_2024.xlsx`):**
```
Campaign Performance Report
===========================

Total Spend: $7,550.50
Total Clicks: 1,950
Total Conversions: 110

Average CPC: $4.83
Conversion Rate: 5.64%

Platform Breakdown:
- Google Ads: $3,301.25
- Facebook: $2,050.25
- LinkedIn: $2,200.00
```

**Plus a new sheet "Campaign_Data"** with all raw data formatted nicely.

---

### Example 2: PowerPoint Template

#### Template (`monthly_report.pptx`)

**Slide 1: Title**
```
Monthly Campaign Report
{{Month}} {{Year}}
```

**Slide 2: Key Metrics**
```
Total Investment: {{Total_Spend}}
Total Conversions: {{Total_Conversions}}
ROAS: {{ROAS}}x
```

**Slide 3: Table**
| Platform | Spend | Conversions |
|----------|-------|-------------|
| Google   | {{Google_Spend}} | {{Google_Conv}} |
| Facebook | {{Facebook_Spend}} | {{Facebook_Conv}} |

#### After Generation

**Slide 2 becomes:**
```
Total Investment: $45.2K
Total Conversions: 234
ROAS: 3.5x
```

**Slide 3 table:**
| Platform | Spend | Conversions |
|----------|-------|-------------|
| Google   | $25,450.00 | 145 |
| Facebook | $19,750.00 | 89 |

---

## Supported Placeholder Formats

### Text Patterns
- `{{field_name}}` - Double braces (recommended)
- `{field_name}` - Single braces
- `[field_name]` - Square brackets
- `<field_name>` - Angle brackets

### Naming Conventions
- Use descriptive names: `{{Total_Campaign_Spend}}`
- Match your data columns: If column is "Spend", use `{{Spend}}`
- Use underscores: `{{Avg_CPC}}` not `{{Avg CPC}}`

---

## Aggregation Methods

### Sum (Default)
```
Data: [100, 200, 300]
Result: 600
Use for: Spend, Clicks, Conversions, Impressions
```

### Average
```
Data: [100, 200, 300]
Result: 200
Use for: CPC, CPA, CTR, ROAS
```

### Latest
```
Data: [100, 200, 300]
Result: 300
Use for: Current status, most recent value
```

### All Rows
```
Data: [100, 200, 300]
Result: "100, 200, 300"
Use for: Lists, multiple values
```

---

## Advanced Features

### 1. Conditional Mapping
Filter data before aggregation:
```
Placeholder: {{Google_Spend}}
Mapping: Spend column WHERE Platform = 'Google Ads'
```

### 2. Calculated Fields
Use formulas in template:
```
Template cell A1: {{Total_Spend}}
Template cell A2: {{Total_Revenue}}
Template cell A3: =A2/A1  (ROAS formula preserved)
```

### 3. Multiple Sheets
Each sheet in Excel template is processed:
```
Sheet1: Executive Summary (placeholders)
Sheet2: Detailed Metrics (placeholders)
Sheet3: Charts (preserved as-is)
Sheet4: Campaign_Data (auto-generated)
```

### 4. Formatting Preservation
- **Fonts, colors, borders** → Preserved
- **Charts and graphs** → Preserved
- **Formulas** → Preserved (unless cell has placeholder)
- **Images and logos** → Preserved

---

## Real-World Use Cases

### Use Case 1: Weekly Client Report
**Template:** `weekly_client_report.xlsx`
- Executive summary with KPIs
- Platform breakdown charts
- Budget vs. actual table

**Data:** Weekly campaign exports from ad platforms
**Output:** Branded report ready to send to client

### Use Case 2: Monthly Performance Deck
**Template:** `monthly_deck.pptx`
- Title slide with month/year
- Key metrics slide
- Platform comparison slides
- Recommendations slide (manual)

**Data:** Monthly aggregated campaign data
**Output:** Presentation ready for stakeholder meeting

### Use Case 3: Campaign Analysis
**Template:** `campaign_analysis.xlsx`
- Multiple tabs for different analyses
- Pivot tables and charts
- Executive summary

**Data:** Campaign-level granular data
**Output:** Comprehensive analysis workbook

---

## Tips & Best Practices

### Template Design
1. **Use clear placeholder names** that match your data columns
2. **Add formatting** to template (colors, fonts, borders)
3. **Include formulas** for calculated metrics
4. **Test with sample data** first

### Data Preparation
1. **Clean column names** (no special characters)
2. **Consistent data types** (numbers as numbers, dates as dates)
3. **Remove duplicates** if needed
4. **Validate data** before upload

### Mapping Configuration
1. **Review auto-suggestions** - they're usually correct
2. **Use appropriate aggregation** for each metric
3. **Save mapping** for reuse
4. **Test with small dataset** first

### Report Generation
1. **Choose right aggregation method** for your use case
2. **Include summary sheet** for raw data reference
3. **Download and verify** output before sharing
4. **Keep template and data** for future updates

---

## Troubleshooting

### Placeholder Not Replaced
**Issue:** Placeholder still shows as `{{field}}` in output
**Solution:** 
- Check mapping configuration
- Verify placeholder name matches exactly
- Ensure data column has values

### Wrong Values
**Issue:** Numbers don't match expectations
**Solution:**
- Check aggregation method (Sum vs. Average)
- Verify data column selection
- Check for null/empty values in data

### Formatting Lost
**Issue:** Template formatting disappeared
**Solution:**
- Use .xlsx format (not .xls)
- Avoid complex conditional formatting
- Test with simpler template first

### Error During Generation
**Issue:** "Error generating report"
**Solution:**
- Check data types match expectations
- Verify all mapped columns exist in data
- Try with smaller dataset first

---

## API Integration (Future)

```python
# Future: Programmatic report generation
from pca_reporting import ReportGenerator

generator = ReportGenerator()
generator.load_template('template.xlsx')
generator.load_data('data.csv')
generator.auto_map()
report = generator.generate(aggregation='sum')
report.save('output.xlsx')
```

---

## Support

For issues or questions:
1. Check this guide first
2. Review example templates
3. Test with sample data
4. Contact support team

---

**Last Updated:** December 2024
**Version:** 1.0
