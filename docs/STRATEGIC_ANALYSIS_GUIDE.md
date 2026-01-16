# Strategic Analysis & Advanced Insights Guide

## Overview

This guide explains how the PCA Agent Q&A system handles advanced strategic analysis questions that go beyond basic reporting to provide actionable business insights.

## 🎯 Strategic Analysis Capabilities

### 1. Performance Anomaly & Pattern Detection

#### Anomaly Detection

**Question Types:**
- "Identify unusual spikes or drops in performance over the last 2 months"
- "What days/weeks had anomalies based on statistical outliers?"
- "Detect performance anomalies using 2 standard deviations from mean"

**SQL Pattern:**
```sql
WITH daily_metrics AS (
    SELECT 
        Date,
        SUM(Conversions) as conversions,
        SUM(Spend) as spend
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY
    GROUP BY Date
),
stats AS (
    SELECT 
        AVG(conversions) as avg_conv,
        STDDEV(conversions) as stddev_conv
    FROM daily_metrics
)
SELECT 
    d.Date,
    d.conversions,
    s.avg_conv,
    s.stddev_conv,
    CASE 
        WHEN d.conversions > s.avg_conv + 2*s.stddev_conv THEN 'Positive Spike'
        WHEN d.conversions < s.avg_conv - 2*s.stddev_conv THEN 'Negative Drop'
        ELSE 'Normal'
    END as Anomaly_Type,
    ROUND(((d.conversions - s.avg_conv) / NULLIF(s.stddev_conv, 0)), 2) as Z_Score
FROM daily_metrics d, stats s
WHERE d.conversions > s.avg_conv + 2*s.stddev_conv 
   OR d.conversions < s.avg_conv - 2*s.stddev_conv
ORDER BY ABS((d.conversions - s.avg_conv) / NULLIF(s.stddev_conv, 0)) DESC
```

**Business Context:**
- Identify days requiring investigation (campaign issues, external events)
- Understand what drove exceptional performance
- Detect problems early (traffic quality issues, technical problems)

#### Diminishing Returns Analysis

**Question:**
> "Analyze the correlation between ad spend increases and conversion rates. Is there a point of diminishing returns?"

**SQL Pattern:**
```sql
WITH weekly_data AS (
    SELECT 
        DATE_TRUNC('week', CAST(Date AS DATE)) as Week,
        SUM(Spend) as Total_Spend,
        SUM(Conversions) as Total_Conversions,
        ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) as Conversion_Rate
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 90 DAY
    GROUP BY Week
)
SELECT 
    Week,
    Total_Spend,
    Conversion_Rate,
    LAG(Total_Spend) OVER (ORDER BY Week) as Prev_Spend,
    LAG(Conversion_Rate) OVER (ORDER BY Week) as Prev_Conv_Rate,
    ROUND(((Total_Spend - LAG(Total_Spend) OVER (ORDER BY Week)) 
           / NULLIF(LAG(Total_Spend) OVER (ORDER BY Week), 0)) * 100, 2) as Spend_Change_Pct,
    ROUND(Conversion_Rate - LAG(Conversion_Rate) OVER (ORDER BY Week), 2) as Conv_Rate_Change,
    -- Efficiency ratio: Conv Rate change / Spend change
    ROUND((Conversion_Rate - LAG(Conversion_Rate) OVER (ORDER BY Week)) 
          / NULLIF(((Total_Spend - LAG(Total_Spend) OVER (ORDER BY Week)) 
                    / NULLIF(LAG(Total_Spend) OVER (ORDER BY Week), 0)) * 100, 0), 3) as Efficiency_Ratio
FROM weekly_data
ORDER BY Week
```

**Interpretation:**
- Efficiency Ratio < 0.5 → Diminishing returns detected
- Negative Conv_Rate_Change with positive Spend_Change_Pct → Over-spending

#### Seasonality Detection

**Question:**
> "Do certain days of the week consistently perform better?"

**SQL Pattern:**
```sql
SELECT 
    DAYNAME(CAST(Date AS DATE)) as Day_of_Week,
    DAYOFWEEK(CAST(Date AS DATE)) as Day_Number,
    COUNT(DISTINCT Date) as Days_Count,
    ROUND(AVG(daily_conversions), 2) as Avg_Conversions,
    ROUND(AVG(daily_ctr), 2) as Avg_CTR,
    ROUND(AVG(daily_cpa), 2) as Avg_CPA,
    ROUND(STDDEV(daily_conversions), 2) as StdDev_Conversions
FROM (
    SELECT 
        Date,
        SUM(Conversions) as daily_conversions,
        (SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100 as daily_ctr,
        SUM(Spend) / NULLIF(SUM(Conversions), 0) as daily_cpa
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 180 DAY
    GROUP BY Date
) daily_metrics
GROUP BY Day_of_Week, Day_Number
ORDER BY Day_Number
```

### 2. Cohort & Segmentation Analysis

#### Cohort Comparison

**Question:**
> "Compare customers acquired in last 3 months vs. 6 months ago. Are recent strategies bringing higher-value customers?"

**SQL Pattern:**
```sql
WITH recent_cohort AS (
    SELECT 
        'Last 3 Months' as Cohort,
        COUNT(DISTINCT Customer_ID) as Customer_Count,
        AVG(Order_Value) as AOV,
        SUM(Order_Value) as Total_Revenue,
        AVG(Lifetime_Value) as Avg_LTV
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 90 DAY
),
older_cohort AS (
    SELECT 
        'Previous 3 Months (4-6 months ago)' as Cohort,
        COUNT(DISTINCT Customer_ID) as Customer_Count,
        AVG(Order_Value) as AOV,
        SUM(Order_Value) as Total_Revenue,
        AVG(Lifetime_Value) as Avg_LTV
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 180 DAY 
      AND Date < CURRENT_DATE - INTERVAL 90 DAY
)
SELECT 
    Cohort,
    Customer_Count,
    ROUND(AOV, 2) as AOV,
    ROUND(Total_Revenue, 2) as Total_Revenue,
    ROUND(Avg_LTV, 2) as Avg_LTV,
    ROUND(Total_Revenue / NULLIF(Customer_Count, 0), 2) as Revenue_Per_Customer
FROM recent_cohort
UNION ALL
SELECT 
    Cohort,
    Customer_Count,
    ROUND(AOV, 2),
    ROUND(Total_Revenue, 2),
    ROUND(Avg_LTV, 2),
    ROUND(Total_Revenue / NULLIF(Customer_Count, 0), 2)
FROM older_cohort
```

#### Engagement Segmentation

**Question:**
> "Segment audience by engagement level (high, medium, low). Which segment has best conversion rate?"

**SQL Pattern:**
```sql
WITH campaign_engagement AS (
    SELECT 
        Campaign_Name,
        Platform,
        (SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100 as CTR,
        (SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100 as Conversion_Rate,
        SUM(Spend) / NULLIF(SUM(Conversions), 0) as CPA,
        SUM(Conversions) as Total_Conversions
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY
    GROUP BY Campaign_Name, Platform
),
engagement_segments AS (
    SELECT 
        *,
        CASE 
            WHEN CTR >= 3.0 THEN 'High Engagement'
            WHEN CTR >= 1.5 THEN 'Medium Engagement'
            ELSE 'Low Engagement'
        END as Engagement_Level
    FROM campaign_engagement
)
SELECT 
    Engagement_Level,
    COUNT(*) as Campaign_Count,
    ROUND(AVG(CTR), 2) as Avg_CTR,
    ROUND(AVG(Conversion_Rate), 2) as Avg_Conversion_Rate,
    ROUND(AVG(CPA), 2) as Avg_CPA,
    SUM(Total_Conversions) as Total_Conversions
FROM engagement_segments
GROUP BY Engagement_Level
ORDER BY Avg_Conversion_Rate DESC
```

### 3. Trend Forecasting & Predictive Analysis

#### Simple Trend Forecasting

**Question:**
> "Based on last 6 months, forecast expected CPA for next month"

**SQL Pattern:**
```sql
WITH monthly_cpa AS (
    SELECT 
        DATE_TRUNC('month', CAST(Date AS DATE)) as Month,
        SUM(Spend) / NULLIF(SUM(Conversions), 0) as CPA
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 180 DAY
    GROUP BY Month
    ORDER BY Month
),
trend_analysis AS (
    SELECT 
        AVG(CPA) as Avg_CPA,
        STDDEV(CPA) as StdDev_CPA,
        -- Simple linear trend
        (MAX(CPA) - MIN(CPA)) / 6 as Monthly_Change,
        -- Recent trend (last 3 months)
        AVG(CASE WHEN Month >= CURRENT_DATE - INTERVAL 90 DAY THEN CPA END) as Recent_Avg
    FROM monthly_cpa
)
SELECT 
    ROUND(Avg_CPA, 2) as Historical_Avg_CPA,
    ROUND(StdDev_CPA, 2) as Volatility,
    ROUND(Monthly_Change, 2) as Trend_Direction,
    ROUND(Recent_Avg, 2) as Recent_3Month_Avg,
    -- Forecasts
    ROUND(Recent_Avg + Monthly_Change, 2) as Trend_Based_Forecast,
    ROUND(Recent_Avg - StdDev_CPA, 2) as Conservative_Forecast,
    ROUND(Recent_Avg + StdDev_CPA, 2) as Optimistic_Forecast
FROM trend_analysis
```

#### Growth Trajectory Analysis

**Question:**
> "Analyze growth trajectory over last year. Are we on track for annual goals?"

**SQL Pattern:**
```sql
WITH monthly_conversions AS (
    SELECT 
        DATE_TRUNC('month', CAST(Date AS DATE)) as Month,
        SUM(Conversions) as Total_Conversions
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY Month
    ORDER BY Month
),
growth_rates AS (
    SELECT 
        Month,
        Total_Conversions,
        LAG(Total_Conversions) OVER (ORDER BY Month) as Prev_Month,
        ROUND(((Total_Conversions - LAG(Total_Conversions) OVER (ORDER BY Month)) 
               / NULLIF(LAG(Total_Conversions) OVER (ORDER BY Month), 0)) * 100, 2) as MoM_Growth_Pct
    FROM monthly_conversions
)
SELECT 
    Month,
    Total_Conversions,
    MoM_Growth_Pct,
    AVG(MoM_Growth_Pct) OVER (ORDER BY Month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as Rolling_3Month_Avg_Growth,
    SUM(Total_Conversions) OVER (ORDER BY Month) as Cumulative_Conversions,
    -- Projection to year-end
    SUM(Total_Conversions) OVER () as YTD_Total,
    ROUND(SUM(Total_Conversions) OVER () * (12.0 / COUNT(*) OVER ()), 0) as Projected_Annual_Total
FROM growth_rates
ORDER BY Month
```

### 4. Efficiency & Optimization Insights

#### Pareto (80/20) Analysis

**Question:**
> "Identify top 20% of campaigns driving 80% of results"

**SQL Pattern:**
```sql
WITH campaign_performance AS (
    SELECT 
        Campaign_Name,
        SUM(Conversions) as Total_Conversions,
        SUM(Spend) as Total_Spend,
        ROUND(SUM(Spend) / NULLIF(SUM(Conversions), 0), 2) as CPA
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY
    GROUP BY Campaign_Name
),
ranked_campaigns AS (
    SELECT 
        Campaign_Name,
        Total_Conversions,
        Total_Spend,
        CPA,
        SUM(Total_Conversions) OVER (ORDER BY Total_Conversions DESC) as Cumulative_Conversions,
        SUM(Total_Conversions) OVER () as Grand_Total,
        ROUND((SUM(Total_Conversions) OVER (ORDER BY Total_Conversions DESC) * 100.0 
               / SUM(Total_Conversions) OVER ()), 2) as Cumulative_Pct,
        ROW_NUMBER() OVER (ORDER BY Total_Conversions DESC) as Rank,
        COUNT(*) OVER () as Total_Campaigns
    FROM campaign_performance
)
SELECT 
    Campaign_Name,
    Total_Conversions,
    Total_Spend,
    CPA,
    Cumulative_Pct,
    ROUND((Rank * 100.0 / Total_Campaigns), 1) as Percentile,
    CASE 
        WHEN Cumulative_Pct <= 80 THEN 'Top 20% (Drives 80% of results)'
        ELSE 'Bottom 80%'
    END as Pareto_Category
FROM ranked_campaigns
ORDER BY Total_Conversions DESC
```

**Business Action:**
- Focus budget on top 20%
- Investigate why bottom 80% underperforms
- Consider pausing lowest performers

#### Marginal ROAS Analysis

**Question:**
> "Compare incremental ROAS of last $10K vs. first $10K spent. Diminishing returns?"

**SQL Pattern:**
```sql
WITH daily_spend AS (
    SELECT 
        Date,
        SUM(Spend) as Daily_Spend,
        SUM(Revenue) as Daily_Revenue,
        SUM(SUM(Spend)) OVER (ORDER BY Date) as Cumulative_Spend
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY Date
    ORDER BY Date
),
spend_buckets AS (
    SELECT 
        *,
        CASE 
            WHEN Cumulative_Spend <= 10000 THEN 'First $10K'
            WHEN Cumulative_Spend > (SELECT MAX(Cumulative_Spend) FROM daily_spend) - 10000 
                THEN 'Last $10K'
            ELSE 'Middle'
        END as Spend_Bucket
    FROM daily_spend
)
SELECT 
    Spend_Bucket,
    SUM(Daily_Spend) as Total_Spend,
    SUM(Daily_Revenue) as Total_Revenue,
    ROUND(SUM(Daily_Revenue) / NULLIF(SUM(Daily_Spend), 0), 2) as ROAS,
    COUNT(*) as Days
FROM spend_buckets
WHERE Spend_Bucket IN ('First $10K', 'Last $10K')
GROUP BY Spend_Bucket
```

**Interpretation:**
- If Last $10K ROAS < First $10K ROAS → Diminishing returns confirmed
- Consider capping daily budget at efficiency threshold

### 5. Risk & Volatility Analysis

#### Performance Volatility

**Question:**
> "Calculate performance volatility (CPA standard deviation) for each campaign. Which are most unpredictable?"

**SQL Pattern:**
```sql
WITH daily_cpa AS (
    SELECT 
        Campaign_Name,
        Date,
        SUM(Spend) / NULLIF(SUM(Conversions), 0) as Daily_CPA
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 60 DAY
    GROUP BY Campaign_Name, Date
),
campaign_volatility AS (
    SELECT 
        Campaign_Name,
        AVG(Daily_CPA) as Avg_CPA,
        STDDEV(Daily_CPA) as CPA_Volatility,
        MIN(Daily_CPA) as Min_CPA,
        MAX(Daily_CPA) as Max_CPA,
        COUNT(*) as Days_Active
    FROM daily_cpa
    GROUP BY Campaign_Name
)
SELECT 
    Campaign_Name,
    ROUND(Avg_CPA, 2) as Avg_CPA,
    ROUND(CPA_Volatility, 2) as CPA_StdDev,
    ROUND(Min_CPA, 2) as Min_CPA,
    ROUND(Max_CPA, 2) as Max_CPA,
    Days_Active,
    -- Coefficient of Variation (CV) = StdDev / Mean
    ROUND((CPA_Volatility / NULLIF(Avg_CPA, 0)) * 100, 2) as CV_Pct,
    CASE 
        WHEN (CPA_Volatility / NULLIF(Avg_CPA, 0)) > 0.3 THEN 'High Risk'
        WHEN (CPA_Volatility / NULLIF(Avg_CPA, 0)) > 0.15 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as Risk_Level
FROM campaign_volatility
ORDER BY CPA_Volatility DESC
```

**Risk Levels:**
- CV > 30% → High Risk (unpredictable, needs monitoring)
- CV 15-30% → Medium Risk (some variability)
- CV < 15% → Low Risk (stable, predictable)

### 6. Strategic Recommendations Framework

#### Budget Reallocation Scenario

**Question:**
> "If we increased budget by 25%, which channels should receive it based on ROAS and scalability?"

**SQL Pattern:**
```sql
WITH channel_performance AS (
    SELECT 
        Platform,
        SUM(Spend) as Current_Spend,
        SUM(Revenue) as Current_Revenue,
        ROUND(SUM(Revenue) / NULLIF(SUM(Spend), 0), 2) as ROAS,
        SUM(Conversions) as Total_Conversions,
        COUNT(DISTINCT Campaign_Name) as Campaign_Count
    FROM campaigns
    WHERE Date >= CURRENT_DATE - INTERVAL 90 DAY
    GROUP BY Platform
),
total_budget AS (
    SELECT 
        SUM(Current_Spend) as Total_Current_Spend,
        SUM(Current_Spend) * 1.25 as Total_New_Budget
    FROM channel_performance
),
allocation AS (
    SELECT 
        cp.Platform,
        cp.Current_Spend,
        cp.ROAS,
        cp.Total_Conversions,
        cp.Campaign_Count,
        tb.Total_New_Budget,
        -- ROAS-weighted allocation
        ROUND(cp.Current_Spend * (cp.ROAS / (SELECT SUM(ROAS) FROM channel_performance)), 2) as ROAS_Weight,
        -- Recommended new budget
        ROUND((cp.Current_Spend * (cp.ROAS / (SELECT SUM(ROAS) FROM channel_performance))) 
              * (tb.Total_New_Budget / tb.Total_Current_Spend), 2) as Recommended_New_Budget
    FROM channel_performance cp, total_budget tb
)
SELECT 
    Platform,
    Current_Spend,
    ROAS,
    Campaign_Count,
    Recommended_New_Budget,
    ROUND(Recommended_New_Budget - Current_Spend, 2) as Budget_Increase,
    ROUND(((Recommended_New_Budget - Current_Spend) / NULLIF(Current_Spend, 0)) * 100, 2) as Increase_Pct
FROM allocation
ORDER BY ROAS DESC
```

## 🎓 Agent Training Principles

### Always Provide Context

**Bad Response:**
> "CPA is $45.23"

**Good Response:**
> "CPA is $45.23, which is 15% higher than last month's $39.32 and 8% above the target of $42.00. This increase is primarily driven by the Google Ads channel, which saw a 25% CPA increase due to increased competition in the auction."

### Identify Causation vs. Correlation

**Example:**
> "While CTR and conversions both increased 20% this month, the correlation doesn't imply causation. The CTR increase was driven by creative refresh, while conversions increased due to a promotional offer. These are independent factors that happened to align."

### Quantify Business Impact

**Example:**
> "Reallocating 20% of budget from low-ROAS campaigns (ROAS < 2.0) to high-ROAS campaigns (ROAS > 4.0) would:
> - Free up $15,000 in budget
> - Potentially generate an additional $60,000 in revenue (at 4.0x ROAS)
> - Improve overall campaign ROAS from 2.8x to 3.4x
> - Estimated impact: +$45,000 net revenue"

### Acknowledge Uncertainty

**Example:**
> "Based on the last 6 months, forecasted CPA for next month is $42-48 (confidence interval). However, this assumes:
> - No major market changes
> - Similar campaign mix
> - Consistent budget levels
> 
> External factors (seasonality, competition, economic conditions) could impact this forecast."

### Think Holistically

**Example:**
> "While Facebook has the lowest CPA ($35 vs. $45 for Google), Google drives 60% of total conversions and has higher customer lifetime value ($250 vs. $180). Cutting Google budget would reduce CPA but hurt overall business outcomes."

## 📊 Testing Strategic Questions

```bash
# Run advanced training questions
python train_qa_system.py

# Test specific strategic question
python train_qa_system.py interactive
```

Then ask:
- "Identify performance anomalies in the last 2 months using statistical outliers"
- "Which campaigns show diminishing returns based on spend vs. conversion correlation?"
- "Forecast next month's CPA based on last 6 months trend"
- "Identify top 20% of campaigns driving 80% of results"
- "Calculate performance volatility for each campaign"

## 📚 Reference

- **Advanced Patterns:** `src/query_engine/nl_to_sql.py` lines 134-182
- **Strategic Questions:** `data/advanced_strategic_questions.json`
- **Training Guide:** This document

---

**The PCA Agent is now trained to be a strategic analyst, not just a data reporter!** 🎯
