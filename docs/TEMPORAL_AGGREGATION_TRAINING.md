# Temporal Comparison & Aggregation Training Guide

## Overview

This guide explains how the PCA Agent Q&A system has been trained to handle temporal comparisons and calculate metrics correctly using proper aggregation rules.

## 🔴 Critical Aggregation Rules

### Never Average Rate Metrics

**WRONG ❌:**
```sql
SELECT AVG(CTR) as Average_CTR FROM campaigns
SELECT AVG(ROAS) as Average_ROAS FROM campaigns
SELECT AVG(CPC) as Average_CPC FROM campaigns
```

**CORRECT ✅:**
```sql
-- CTR: Click-Through Rate
SELECT ROUND((SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100, 2) as CTR
FROM campaigns

-- ROAS: Return on Ad Spend
SELECT ROUND(SUM(Revenue) / NULLIF(SUM(Spend), 0), 2) as ROAS
FROM campaigns

-- CPC: Cost Per Click
SELECT ROUND(SUM(Spend) / NULLIF(SUM(Clicks), 0), 2) as CPC
FROM campaigns

-- CPM: Cost Per Mille (1000 impressions)
SELECT ROUND((SUM(Spend) / NULLIF(SUM(Impressions), 0)) * 1000, 2) as CPM
FROM campaigns

-- CPA: Cost Per Acquisition
SELECT ROUND(SUM(Spend) / NULLIF(SUM(Conversions), 0), 2) as CPA
FROM campaigns

-- Conversion Rate
SELECT ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) as Conversion_Rate
FROM campaigns
```

### Why This Matters

**Example showing the problem:**

| Week | Clicks | Impressions | Pre-calculated CTR |
|------|--------|-------------|-------------------|
| Week 1 | 100 | 5,000 | 2.0% |
| Week 2 | 200 | 5,000 | 4.0% |

**Wrong approach (averaging):**
```
Average CTR = (2.0% + 4.0%) / 2 = 3.0%
```

**Correct approach (aggregate then calculate):**
```
Total Clicks = 100 + 200 = 300
Total Impressions = 5,000 + 5,000 = 10,000
CTR = 300 / 10,000 = 3.0%
```

In this case they match, but consider this:

| Week | Clicks | Impressions | Pre-calculated CTR |
|------|--------|-------------|-------------------|
| Week 1 | 100 | 5,000 | 2.0% |
| Week 2 | 200 | 20,000 | 1.0% |

**Wrong:**
```
Average CTR = (2.0% + 1.0%) / 2 = 1.5%
```

**Correct:**
```
Total Clicks = 100 + 200 = 300
Total Impressions = 5,000 + 20,000 = 25,000
CTR = 300 / 25,000 = 1.2%
```

The difference is significant!

## ⏰ Temporal Comparison Patterns

### Understanding Time Phrases

The system has been trained to understand these natural language time expressions:

#### Recent Periods

| Phrase | SQL Pattern | Example |
|--------|-------------|---------|
| "last 2 weeks" | `WHERE Date >= CURRENT_DATE - INTERVAL 14 DAY` | Last 14 days |
| "previous 2 weeks" | `WHERE Date >= CURRENT_DATE - INTERVAL 28 DAY AND Date < CURRENT_DATE - INTERVAL 14 DAY` | Days 15-28 ago |
| "last month" | `WHERE Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)` | Previous calendar month |
| "last 2 months" | `WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY` | Last 60 days |
| "couple of months" | `WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY` | Last 60 days |
| "last 6 months" | `WHERE Date >= CURRENT_DATE - INTERVAL 180 DAY` | Last 180 days |
| "last 2 years" | `WHERE Date >= CURRENT_DATE - INTERVAL 730 DAY` | Last 730 days |

#### Trend Analysis

| Phrase | SQL Pattern | Use Case |
|--------|-------------|----------|
| "week-over-week" | `GROUP BY DATE_TRUNC('week', Date)` | Weekly trends |
| "month-over-month" | `GROUP BY DATE_TRUNC('month', Date)` | Monthly trends |
| "day-by-day" | `GROUP BY DATE_TRUNC('day', Date)` | Daily trends |

#### Period Comparisons

| Phrase | SQL Pattern |
|--------|-------------|
| "Q3 vs Q2" | `WHERE QUARTER(Date) IN (2, 3)` |
| "this year vs last year" | `WHERE YEAR(Date) IN (2024, 2023)` |
| "H1 vs H2" | Use months 1-6 vs 7-12 |

### Example: Last 2 Weeks vs Previous 2 Weeks

**Question:**
> "Compare campaign performance between the last 2 weeks vs. the previous 2 weeks"

**Generated SQL:**
```sql
WITH last_2_weeks AS (
    SELECT 
        SUM(Impressions) as impressions,
        SUM(Clicks) as clicks,
        SUM(Conversions) as conversions,
        SUM(Spend) as spend,
        SUM(Revenue) as revenue
    FROM campaigns
    WHERE CAST(Date AS DATE) >= CURRENT_DATE - INTERVAL 14 DAY
),
prev_2_weeks AS (
    SELECT 
        SUM(Impressions) as impressions,
        SUM(Clicks) as clicks,
        SUM(Conversions) as conversions,
        SUM(Spend) as spend,
        SUM(Revenue) as revenue
    FROM campaigns
    WHERE CAST(Date AS DATE) >= CURRENT_DATE - INTERVAL 28 DAY
      AND CAST(Date AS DATE) < CURRENT_DATE - INTERVAL 14 DAY
)
SELECT 
    'Last 2 Weeks' as Period,
    l.impressions,
    l.clicks,
    l.conversions,
    ROUND((l.clicks / NULLIF(l.impressions, 0)) * 100, 2) as CTR,
    ROUND(l.spend / NULLIF(l.conversions, 0), 2) as CPA,
    ROUND(l.revenue / NULLIF(l.spend, 0), 2) as ROAS,
    ROUND(((l.impressions - p.impressions) / NULLIF(p.impressions, 0)) * 100, 2) as Impressions_Change_Pct,
    ROUND(((l.clicks - p.clicks) / NULLIF(p.clicks, 0)) * 100, 2) as Clicks_Change_Pct,
    ROUND(((l.conversions - p.conversions) / NULLIF(p.conversions, 0)) * 100, 2) as Conversions_Change_Pct
FROM last_2_weeks l, prev_2_weeks p

UNION ALL

SELECT 
    'Previous 2 Weeks' as Period,
    p.impressions,
    p.clicks,
    p.conversions,
    ROUND((p.clicks / NULLIF(p.impressions, 0)) * 100, 2) as CTR,
    ROUND(p.spend / NULLIF(p.conversions, 0), 2) as CPA,
    ROUND(p.revenue / NULLIF(p.spend, 0), 2) as ROAS,
    NULL as Impressions_Change_Pct,
    NULL as Clicks_Change_Pct,
    NULL as Conversions_Change_Pct
FROM prev_2_weeks p
```

### Example: Week-over-Week Trend

**Question:**
> "Show me the trend for cost per click (CPC) over the last 2 months on a week-by-week basis"

**Generated SQL:**
```sql
SELECT 
    DATE_TRUNC('week', CAST(Date AS DATE)) as Week,
    SUM(Spend) as Total_Spend,
    SUM(Clicks) as Total_Clicks,
    ROUND(SUM(Spend) / NULLIF(SUM(Clicks), 0), 2) as CPC
FROM campaigns
WHERE CAST(Date AS DATE) >= CURRENT_DATE - INTERVAL 60 DAY
GROUP BY Week
ORDER BY Week
```

**Note:** CPC is calculated as `SUM(Spend) / SUM(Clicks)` for each week, NOT as `AVG(CPC)`.

## 📊 Multi-Dimensional Analysis

### Channel Analysis

**Question:**
> "Which marketing channel generated the highest ROI?"

**SQL Pattern:**
```sql
SELECT 
    Platform,
    SUM(Revenue) - SUM(Spend) as Profit,
    ROUND((SUM(Revenue) - SUM(Spend)) / NULLIF(SUM(Spend), 0) * 100, 2) as ROI_Pct,
    SUM(Conversions) as Total_Conversions,
    ROUND(SUM(Revenue) / NULLIF(SUM(Spend), 0), 2) as ROAS
FROM campaigns
GROUP BY Platform
ORDER BY ROI_Pct DESC
```

### Funnel Analysis

**Question:**
> "What was the conversion rate at each stage of the funnel?"

**SQL Pattern:**
```sql
SELECT 
    SUM(Impressions) as Awareness,
    SUM(Clicks) as Consideration,
    SUM(Conversions) as Conversion,
    ROUND((SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100, 2) as Awareness_to_Consideration_Rate,
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) as Consideration_to_Conversion_Rate,
    ROUND((SUM(Conversions) / NULLIF(SUM(Impressions), 0)) * 100, 2) as Overall_Conversion_Rate,
    ROUND((SUM(Impressions) - SUM(Clicks)) / NULLIF(SUM(Impressions), 0) * 100, 2) as Awareness_Dropoff_Pct,
    ROUND((SUM(Clicks) - SUM(Conversions)) / NULLIF(SUM(Clicks), 0) * 100, 2) as Consideration_Dropoff_Pct
FROM campaigns
```

### Audience Segmentation

**Question:**
> "How did different audience segments perform in terms of engagement rate and conversion rate?"

**SQL Pattern:**
```sql
SELECT 
    Audience_Segment,
    SUM(Impressions) as Total_Impressions,
    SUM(Clicks) as Total_Clicks,
    SUM(Conversions) as Total_Conversions,
    ROUND((SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100, 2) as Engagement_Rate,
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) as Conversion_Rate,
    ROUND(SUM(Spend) / NULLIF(SUM(Conversions), 0), 2) as CPA
FROM campaigns
GROUP BY Audience_Segment
ORDER BY Conversion_Rate DESC
```

## 🎯 Advanced Patterns

### Growth Rate Calculation

```sql
-- Month-over-month growth
SELECT 
    DATE_TRUNC('month', CAST(Date AS DATE)) as Month,
    SUM(Conversions) as Leads,
    LAG(SUM(Conversions)) OVER (ORDER BY Month) as Prev_Month_Leads,
    ROUND(
        ((SUM(Conversions) - LAG(SUM(Conversions)) OVER (ORDER BY Month)) 
         / NULLIF(LAG(SUM(Conversions)) OVER (ORDER BY Month), 0)) * 100, 
        2
    ) as Growth_Pct
FROM campaigns
WHERE CAST(Date AS DATE) >= CURRENT_DATE - INTERVAL 180 DAY
GROUP BY Month
ORDER BY Month
```

### Budget Variance Analysis

```sql
SELECT 
    Platform,
    SUM(Budgeted_Spend) as Budget,
    SUM(Actual_Spend) as Actual,
    SUM(Actual_Spend) - SUM(Budgeted_Spend) as Variance,
    ROUND(
        ((SUM(Actual_Spend) - SUM(Budgeted_Spend)) 
         / NULLIF(SUM(Budgeted_Spend), 0)) * 100, 
        2
    ) as Variance_Pct
FROM campaigns
GROUP BY Platform
ORDER BY Variance_Pct DESC
```

### Contribution Analysis

```sql
SELECT 
    Platform,
    SUM(Revenue) as Total_Revenue,
    ROUND(
        SUM(Revenue) * 100.0 / SUM(SUM(Revenue)) OVER (), 
        2
    ) as Revenue_Contribution_Pct,
    SUM(Conversions) as Total_Conversions,
    ROUND(
        SUM(Conversions) * 100.0 / SUM(SUM(Conversions)) OVER (), 
        2
    ) as Conversion_Contribution_Pct
FROM campaigns
GROUP BY Platform
ORDER BY Revenue_Contribution_Pct DESC
```

## 📝 Training Questions

The system has been trained on 20+ comprehensive questions covering:

1. **Temporal Comparisons** (5 questions)
   - Last 2 weeks vs previous 2 weeks
   - Week-over-week trends
   - Month-over-month comparisons
   - Quarterly comparisons
   - Year-over-year analysis

2. **Channel Analysis** (2 questions)
   - ROI by channel
   - CPA comparison across channels

3. **Funnel Analysis** (2 questions)
   - Conversion rates at each stage
   - Drop-off analysis

4. **Budget & ROI** (2 questions)
   - ROAS calculation and target achievement
   - Budget reallocation recommendations

5. **Audience Analysis** (2 questions)
   - Segment performance
   - New vs returning customers

6. **Creative Performance** (1 question)
   - Creative variant comparison

7. **Timing Optimization** (1 question)
   - Best performing days/times

8. **Comparative Analysis** (1 question)
   - Current vs historical campaigns

9. **Aggregation Rules Tests** (2 questions)
   - CTR calculation validation
   - CPM comparison validation

10. **Strategic Insights** (1 question)
    - Top channels by ROAS

## 🧪 Testing Your Questions

To test if the system handles your question correctly:

1. **Run the training script:**
   ```bash
   cd PCA_Agent
   python train_qa_system.py
   ```

2. **Check the generated SQL:**
   - Look for `SUM(numerator) / SUM(denominator)` patterns
   - Verify NO `AVG(rate_metric)` usage
   - Confirm proper date filtering with `CAST(Date AS DATE)`
   - Check for `NULLIF` to prevent division by zero

3. **Validate the results:**
   - Manually calculate expected values
   - Compare with SQL output
   - Verify temporal periods are correct

## 🔧 Interactive Testing

For manual testing:

```bash
python train_qa_system.py interactive
```

Then ask questions like:
- "Compare CTR for last month vs this month"
- "Show me ROAS by platform for Q3 2024"
- "What's the week-over-week trend for CPA?"

## 📚 Reference Files

- **Prompt Template:** `src/query_engine/nl_to_sql.py` (lines 70-136)
- **Query Templates:** `src/query_engine/nl_to_sql.py` (lines 319-395)
- **Training Questions:** `data/comprehensive_training_questions.json`
- **Streamlit UI:** `streamlit_app.py` (tab 2: Ask Questions)

## ✅ Best Practices Checklist

When writing questions or reviewing SQL:

- [ ] Rate metrics use `SUM(num) / SUM(denom)`, not `AVG()`
- [ ] Date columns are cast: `CAST(Date AS DATE)`
- [ ] Division uses `NULLIF` to prevent errors
- [ ] Temporal periods use proper DuckDB functions
- [ ] CTEs are used for complex comparisons
- [ ] Results are rounded appropriately
- [ ] Column aliases are descriptive
- [ ] Percentages are multiplied by 100

## 🎓 Training Summary

The PCA Agent Q&A system has been comprehensively trained to:

✅ **Understand** temporal phrases (last 2 weeks, Q3 vs Q2, YoY, etc.)  
✅ **Calculate** rate metrics correctly from aggregates  
✅ **Compare** periods with proper date logic  
✅ **Analyze** multi-dimensional data (channel, audience, creative)  
✅ **Generate** clean, efficient SQL with CTEs  
✅ **Prevent** common errors (division by zero, wrong aggregation)  

This ensures accurate, reliable analytics for campaign performance analysis.
