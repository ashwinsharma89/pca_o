# 🚀 Advanced Data Processing Features - Complete Guide

## ✅ All Requirements Implemented

Your 6 requirements have been fully implemented with enterprise-grade data processing capabilities.

---

## 1️⃣ **Avoid Data Type Issues** ✅

### **Auto-Detection System**
The system automatically detects and converts data types:

**Features:**
- ✅ **Numeric Detection**: Automatically converts strings to numbers
- ✅ **Currency Handling**: Removes $, commas from currency values
- ✅ **Percentage Handling**: Removes % symbols and converts
- ✅ **Date Parsing**: Detects and parses date columns in any format
- ✅ **Type Validation**: 80% threshold for conversion confidence
- ✅ **Error Handling**: Gracefully handles mixed types

**Example:**
```python
# Input CSV with mixed types
"$1,234.56"  → 1234.56  (float)
"45%"        → 45.0     (float)
"2024-11-14" → datetime
"1,000"      → 1000     (int)
```

**Implementation:**
```python
processor = MediaDataProcessor()
df = processor.load_data(your_df, auto_detect=True)
# All types automatically fixed!
```

---

## 2️⃣ **Time Granularity & Seasonality** ✅

### **Multi-Granularity Support**
Handles daily, weekly, monthly, quarterly, and yearly data automatically.

**Features:**
- ✅ **Auto-Detection**: Identifies data granularity (daily/weekly/monthly)
- ✅ **Time Aggregation**: Aggregate to any period
- ✅ **Period Comparison**: Month-over-month, week-over-week, year-over-year
- ✅ **Seasonality Detection**: Identifies patterns by month, week, day
- ✅ **Derived Columns**: Auto-creates Year, Month, Week, Quarter, Day_of_Week

**Supported Granularities:**
- **Hourly**: Hour-by-hour analysis
- **Daily**: Day-by-day performance
- **Weekly**: Week-by-week trends
- **Monthly**: Month-by-month comparison
- **Quarterly**: Q1, Q2, Q3, Q4 analysis
- **Yearly**: Year-over-year growth

**Time Comparisons:**
```python
# Month-over-Month
comparison = processor.compare_periods(period_type="month", num_periods=2)
# Returns: Latest vs Previous month with % change

# Last 2 Months vs Previous 2 Months
comparison = processor.compare_periods(period_type="month", num_periods=4)

# Week-over-Week
comparison = processor.compare_periods(period_type="week", num_periods=2)

# Last 2 Weeks
comparison = processor.compare_periods(period_type="week", num_periods=2)
```

**Seasonality Analysis:**
```python
# Detect seasonal patterns
seasonality = processor.detect_seasonality(metric="Spend")
# Returns:
# - Peak month (e.g., November for Black Friday)
# - Low month (e.g., January post-holiday)
# - Peak day of week (e.g., Tuesday)
# - Variation percentage
```

**Output Example:**
```json
{
  "peak_month": 11,  // November
  "low_month": 1,    // January
  "peak_day": "Tuesday",
  "low_day": "Sunday",
  "variation_pct": 45.2,
  "by_month": {
    "1": 50000,
    "11": 150000,  // Black Friday spike
    "12": 120000   // Holiday season
  }
}
```

---

## 3️⃣ **Calculate KPIs at Overall Level** ✅

### **Automatic KPI Calculation**
Calculates all standard KPIs automatically at portfolio level.

**KPIs Calculated:**
- ✅ **CTR** (Click-Through Rate): (Clicks / Impressions) × 100
- ✅ **CPC** (Cost Per Click): Spend / Clicks
- ✅ **CPM** (Cost Per Mille): (Spend / Impressions) × 1000
- ✅ **CPA** (Cost Per Acquisition): Spend / Conversions
- ✅ **ROAS** (Return on Ad Spend): Revenue / Spend
- ✅ **Conversion Rate**: (Conversions / Clicks) × 100
- ✅ **Frequency**: Impressions / Reach
- ✅ **Engagement Rate**: (Engagements / Impressions) × 100

**Usage:**
```python
# Calculate all KPIs at overall level
kpis = processor.calculate_overall_kpis()

# Returns:
{
  "Total_Spend": 641000.0,
  "Total_Impressions": 12500000,
  "Total_Clicks": 250000,
  "Total_Conversions": 17280,
  "Overall_CTR": 2.0,
  "Overall_CPC": 2.56,
  "Overall_CPM": 51.28,
  "Overall_ROAS": 4.2,
  "Overall_CPA": 37.11,
  "Overall_Conversion_Rate": 6.91
}
```

**Missing Metrics Auto-Calculated:**
If your CSV doesn't have CTR, CPC, etc., they're calculated automatically from base metrics (Spend, Clicks, Impressions, Conversions).

---

## 4️⃣ **Group Data at Any Level** ✅

### **Multi-Dimensional Grouping**
Group data by any combination of dimensions.

**Supported Groupings:**
- ✅ Campaign level
- ✅ Campaign + Placement
- ✅ Campaign + Device
- ✅ Platform + Placement
- ✅ Ad Group + Keyword
- ✅ Creative + Placement
- ✅ Any custom combination

**Usage:**
```python
# Group by Campaign and Placement
grouped = processor.group_by_dimensions(
    dimensions=["Campaign", "Placement"],
    metrics=["Spend", "Conversions", "ROAS"],
    top_n=10  # Optional: Get top 10 only
)

# Group by Platform and Device
grouped = processor.group_by_dimensions(
    dimensions=["Platform", "Device"],
    metrics=["Impressions", "Clicks", "CTR"]
)

# Group by Ad Group and Keyword (Google Ads)
grouped = processor.group_by_dimensions(
    dimensions=["Ad_Group", "Keyword"],
    metrics=["Clicks", "CPC", "Quality_Score"]
)
```

**Output Example:**
```
Campaign          | Placement | Spend    | Conversions | ROAS
------------------|-----------|----------|-------------|------
Cyber Monday 2024 | Feed      | $45,000  | 850         | 5.5x
Cyber Monday 2024 | Stories   | $12,000  | 180         | 4.2x
Black Friday 2024 | Feed      | $85,000  | 1,800       | 5.1x
```

---

## 5️⃣ **Understand All Media Dimensions & Terminologies** ✅

### **Comprehensive Media Knowledge**
Built-in understanding of all major platforms and their terminologies.

**Platforms Supported:**
- ✅ Google Ads
- ✅ Meta Ads (Facebook/Instagram)
- ✅ LinkedIn Ads
- ✅ DV360 (Display & Video 360)
- ✅ CM360 (Campaign Manager 360)
- ✅ Snapchat Ads
- ✅ TikTok Ads
- ✅ Twitter Ads
- ✅ Pinterest Ads

**Dimension Mappings:**
The system automatically recognizes variations:
- **Campaign**: campaign, campaign_name, campaign_id, campaignname
- **Ad Group**: ad_group, adgroup, ad_set, adset, ad_squad
- **Ad**: ad, ad_name, ad_id, adname
- **Creative**: creative, creative_name, creative_id
- **Placement**: placement, placement_name, site
- **Keyword**: keyword, keyword_text, search_term
- **Device**: device, device_type, device_category
- **Location**: location, geo, region, country, state, city
- **Age**: age, age_range, age_group
- **Gender**: gender
- **Audience**: audience, audience_name, segment
- **Platform**: platform, channel, source

**Auto-Standardization:**
```python
# Your CSV columns (any variation)
"campaignname" → "Campaign"
"adset"        → "Ad_Group"
"creativeid"   → "Creative"
"geo"          → "Location"
```

---

## 6️⃣ **Understand Data Hierarchies** ✅

### **Platform-Specific Hierarchies**
Built-in knowledge of each platform's data structure.

**Google Ads Hierarchy:**
```
Account → Campaign → Ad Group → Ad → Keyword
```

**Meta Ads Hierarchy:**
```
Account → Campaign → Ad Set → Ad → Creative
```

**LinkedIn Ads Hierarchy:**
```
Account → Campaign Group → Campaign → Ad → Creative
```

**DV360 Hierarchy:**
```
Advertiser → Insertion Order → Line Item → Creative → Placement
```

**CM360 Hierarchy:**
```
Advertiser → Campaign → Placement → Creative → Site
```

**Snapchat Ads Hierarchy:**
```
Account → Campaign → Ad Squad → Ad → Creative
```

**Usage:**
```python
# Get data at all hierarchy levels for a platform
hierarchy_data = processor.get_hierarchy_data(
    platform="google_ads",
    levels=["Campaign", "Ad_Group", "Ad"]  # Optional: specific levels
)

# Returns:
{
  "Campaign": DataFrame with campaign-level data,
  "Ad_Group": DataFrame with ad group-level data,
  "Ad": DataFrame with ad-level data
}
```

**Multi-Level Analysis:**
```python
# Analyze at campaign level
campaign_data = processor.group_by_dimensions(["Campaign"])

# Drill down to ad group level
adgroup_data = processor.group_by_dimensions(["Campaign", "Ad_Group"])

# Drill down to ad level
ad_data = processor.group_by_dimensions(["Campaign", "Ad_Group", "Ad"])

# Drill down to keyword level (Google Ads)
keyword_data = processor.group_by_dimensions(["Campaign", "Ad_Group", "Keyword"])
```

---

## 🎯 Complete Usage Example

```python
from src.data_processing import MediaDataProcessor
import pandas as pd

# Initialize processor
processor = MediaDataProcessor()

# Load your CSV (any format, any platform)
df = pd.read_csv("your_campaign_data.csv")

# 1. Auto-fix data types
df = processor.load_data(df, auto_detect=True)

# 2. Get data summary
summary = processor.get_data_summary()
print(f"Granularity: {summary['time_granularity']}")
print(f"Date range: {summary['date_range']}")

# 3. Calculate overall KPIs
kpis = processor.calculate_overall_kpis()
print(f"Overall ROAS: {kpis['Overall_ROAS']:.2f}x")
print(f"Overall CTR: {kpis['Overall_CTR']:.2f}%")

# 4. Compare last 2 months
comparison = processor.compare_periods("month", 2)
print(f"Spend change: {comparison['comparisons']['Spend']['change_pct']:.1f}%")

# 5. Detect seasonality
seasonality = processor.detect_seasonality("Spend")
print(f"Peak month: {seasonality['peak_month']}")
print(f"Peak day: {seasonality['peak_day']}")

# 6. Group by campaign and placement
grouped = processor.group_by_dimensions(
    dimensions=["Campaign", "Placement"],
    metrics=["Spend", "ROAS"],
    top_n=10
)

# 7. Get hierarchy data
hierarchy = processor.get_hierarchy_data("google_ads")
campaign_level = hierarchy["Campaign"]
adgroup_level = hierarchy["Ad_Group"]
```

---

## 📊 Real-World Scenarios

### **Scenario 1: Monthly Performance Review**
```python
# Compare this month vs last month
comparison = processor.compare_periods("month", 2)

# Detect if there's a seasonal pattern
seasonality = processor.detect_seasonality("Conversions")

# Group by campaign to find winners/losers
campaigns = processor.group_by_dimensions(["Campaign"], top_n=10)
```

### **Scenario 2: Platform Analysis**
```python
# Group by platform
platforms = processor.group_by_dimensions(["Platform"])

# Drill down to platform + placement
platform_placement = processor.group_by_dimensions(["Platform", "Placement"])

# Calculate overall KPIs per platform
for platform in df['Platform'].unique():
    platform_df = df[df['Platform'] == platform]
    processor_temp = MediaDataProcessor()
    processor_temp.load_data(platform_df)
    kpis = processor_temp.calculate_overall_kpis()
    print(f"{platform}: ROAS = {kpis['Overall_ROAS']:.2f}x")
```

### **Scenario 3: Weekly Trend Analysis**
```python
# Aggregate by week
weekly = processor.aggregate_by_time("week")

# Compare last 2 weeks
comparison = processor.compare_periods("week", 2)

# Check day-of-week performance
seasonality = processor.detect_seasonality("Clicks")
print(f"Best day: {seasonality['peak_day']}")
```

### **Scenario 4: Multi-Level Campaign Analysis**
```python
# Campaign level
campaigns = processor.group_by_dimensions(["Campaign"])

# Ad group level within campaigns
adgroups = processor.group_by_dimensions(["Campaign", "Ad_Group"])

# Ad level within ad groups
ads = processor.group_by_dimensions(["Campaign", "Ad_Group", "Ad"])

# Keyword level (for search campaigns)
keywords = processor.group_by_dimensions(["Campaign", "Ad_Group", "Keyword"])
```

---

## 🎉 Summary

**All 6 Requirements Fully Implemented:**

1. ✅ **Data Type Handling**: Auto-detection, conversion, validation
2. ✅ **Time Granularity**: Daily/weekly/monthly, MoM, WoW, YoY, seasonality
3. ✅ **Overall KPIs**: CTR, CPC, CPM, ROAS, CPA, Conversion Rate
4. ✅ **Multi-Dimensional Grouping**: Any combination of dimensions
5. ✅ **Media Terminology**: All platforms, dimensions, metrics understood
6. ✅ **Data Hierarchies**: Platform-specific structures, multi-level analysis

**Files Created:**
- `src/data_processing/advanced_processor.py` (600+ lines)
- `src/data_processing/__init__.py`
- `MEDIA_TERMINOLOGY_GUIDE.md` (Complete reference)
- `ADVANCED_DATA_FEATURES.md` (This guide)

**Integration:**
- ✅ Integrated into `MediaAnalyticsExpert`
- ✅ Works with existing Streamlit app
- ✅ Compatible with all analysis features

**The system now handles ANY media data format, from ANY platform, at ANY granularity!** 🚀
