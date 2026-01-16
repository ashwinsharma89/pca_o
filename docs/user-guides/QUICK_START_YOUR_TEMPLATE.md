# Quick Start: Use Your Template in 3 Steps

## Step 1: Locate Your Template

Find your Excel template file. For example:
```
C:\Users\asharm08\Documents\my_report_template.xlsx
```

## Step 2: Run This Command

```bash
python scripts/testing/test_reporting_with_template.py --template "C:\Users\asharm08\Documents\my_report_template.xlsx"
```

**That's it!** The script will:
- ✅ Analyze your template
- ✅ Generate sample campaign data
- ✅ Calculate all KPIs (CTR, ROAS, CPC, CPM, CPA)
- ✅ Track budget utilization
- ✅ Populate your template
- ✅ Save the final report

## Step 3: Open the Generated Report

Look for output like:
```
📁 Output: reports/automated/my_report_template_populated_20241204_160530.xlsx
```

Open that file and see your template populated with real metrics!

---

## What Gets Calculated Automatically

### KPIs
- **CTR** (Click-Through Rate) = (Clicks / Impressions) × 100
- **CPC** (Cost Per Click) = Spend / Clicks  
- **CPM** (Cost Per Mille) = (Spend / Impressions) × 1,000
- **CPA** (Cost Per Acquisition) = Spend / Conversions
- **ROAS** (Return on Ad Spend) = Revenue / Spend
- **Conversion Rate** = (Conversions / Clicks) × 100

### Budget Tracking
- **Budget Spent %** = (Spent / Total Budget) × 100
- **Budget Remaining %** = 100 - Budget Spent %
- **Days Remaining** = End Date - Today
- **Daily Budget** = Total Budget / Total Days

---

## Add Placeholders to Your Template

Open your Excel template and add these anywhere:

### Simple Example
```
Campaign Report - {{Report_Date}}

Total Spend: {{Total_Spend}}
Total Revenue: {{Total_Revenue}}
ROAS: {{Overall_ROAS}}x

Average CPC: {{Avg_CPC}}
Average CTR: {{Avg_CTR}}%
```

### With Budget Tracking
```
Budget Status:
Total: {{Budget_Total}}
Spent: {{Budget_Spent}} ({{Budget_Spent_Pct}}%)
Remaining: {{Budget_Remaining}}
```

### Top Campaigns
```
Top 3 Campaigns:

1. {{Campaign1_Name}}
   Spend: {{Campaign1_Spend}}
   ROAS: {{Campaign1_ROAS}}x

2. {{Campaign2_Name}}
   Spend: {{Campaign2_Spend}}
   ROAS: {{Campaign2_ROAS}}x

3. {{Campaign3_Name}}
   Spend: {{Campaign3_Spend}}
   ROAS: {{Campaign3_ROAS}}x
```

---

## Use Your Own Data

If you have campaign data:

```bash
python scripts/testing/test_reporting_with_template.py \
  --template "C:\Users\asharm08\Documents\my_template.xlsx" \
  --data "C:\Users\asharm08\Documents\campaign_data.csv"
```

Your CSV should look like:
```csv
Date,Campaign_ID,Campaign_Name,Spend,Impressions,Clicks,Conversions,Revenue
2024-12-04,CAMP001,Brand Campaign,1500,45000,450,25,7500
2024-12-04,CAMP002,Sales Push,2800,68000,820,45,14000
```

---

## Example Output

### Before (Your Template)
```
Campaign Performance Report

Total Spend: {{Total_Spend}}
Total Clicks: {{Total_Clicks}}
Overall ROAS: {{Overall_ROAS}}
```

### After (Populated Report)
```
Campaign Performance Report

Total Spend: $6,901.50
Total Clicks: 2,150
Overall ROAS: 5.64x
```

**Plus:** A new "Campaign_Data" sheet with all detailed metrics!

---

## Full Command Options

```bash
# Basic usage
python scripts/testing/test_reporting_with_template.py --template "path/to/template.xlsx"

# With your data
python scripts/testing/test_reporting_with_template.py \
  --template "path/to/template.xlsx" \
  --data "path/to/data.csv"

# With budget tracking
python scripts/testing/test_reporting_with_template.py \
  --template "path/to/template.xlsx" \
  --budgets "config/campaign_budgets.csv"

# Custom output location
python scripts/testing/test_reporting_with_template.py \
  --template "path/to/template.xlsx" \
  --output "reports/my_report.xlsx"

# All options combined
python scripts/testing/test_reporting_with_template.py \
  --template "path/to/template.xlsx" \
  --data "path/to/data.csv" \
  --budgets "config/campaign_budgets.csv" \
  --output "reports/weekly_report.xlsx"
```

---

## What You'll See

```
======================================================================
PCA Agent - Automated Reporting with Your Template
======================================================================

1️⃣ Initializing automated reporter...
   ✓ Loaded 5 campaign budgets

2️⃣ Loading campaign data...
   ✓ Loaded 5 rows

3️⃣ Calculating KPIs...
   ✓ Processed 5 campaigns
   
   Campaign Metrics:
   - Q4 Brand Campaign
     Spend: $1,500.50 | ROAS: 5.00x | CTR: 1.00%
     CPC: $3.33 | CPA: $60.00 | Conv Rate: 5.56%
     Budget: $35,426.20 / $50,000.00 (70.9%)

4️⃣ Creating template mapping...
🔍 Analyzing your template...
   Template: my_template.xlsx
   Sheets: Summary, Details
   Found: {{Total_Spend}} at Summary!B5
   Found: {{Overall_ROAS}} at Summary!B8
   ...

5️⃣ Generating report...
📝 Populating template...
   ✓ Made 23 replacements
   ✓ Adding Campaign_Data sheet...
   ✓ Saved to: reports/my_template_populated_20241204_160530.xlsx

======================================================================
✅ Report Generated Successfully!
======================================================================

📊 Summary:
   Template Used: my_template.xlsx
   Campaigns Processed: 5
   Total Spend: $6,901.50
   Total Revenue: $38,900.00
   Overall ROAS: 5.64x

📁 Output: reports/automated/my_template_populated_20241204_160530.xlsx
```

---

## Try It Now!

1. Open your terminal/command prompt
2. Navigate to the PCA_Agent folder
3. Run the command with your template path
4. Open the generated report
5. Enjoy automated reporting! 🎉

**No manual calculations. No copy-paste. Just automated, accurate reports!**
