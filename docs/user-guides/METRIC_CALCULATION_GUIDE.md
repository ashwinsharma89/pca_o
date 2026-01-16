# 📊 Metric Calculation Guide - Aggregated vs Averaged

## ⚠️ **Critical Concept: Never Average Calculated Metrics**

When aggregating data across multiple rows, **you must recalculate metrics from base values**, not average pre-calculated metrics.

---

## ❌ **Why Averaging is Wrong**

### **Example: CTR Calculation**

**Scenario**: You have 2 campaigns

| Campaign | Impressions | Clicks | CTR |
|----------|-------------|--------|-----|
| A | 1,000 | 20 | 2.0% |
| B | 100,000 | 1,000 | 1.0% |

**Wrong Approach** (Averaging CTR):
```sql
SELECT AVG(CTR) as avg_ctr FROM campaigns
-- Result: (2.0 + 1.0) / 2 = 1.5%
```

**Correct Approach** (Recalculating from totals):
```sql
SELECT (SUM(Clicks) * 100.0 / SUM(Impressions)) as avg_ctr FROM campaigns
-- Result: (1,020 / 101,000) * 100 = 1.01%
```

**Why the difference?**
- Campaign A has only 1,000 impressions but contributes equally to the average (wrong!)
- Campaign B has 100,000 impressions and should dominate the calculation (correct!)
- The correct CTR is 1.01%, not 1.5%

---

## ✅ **Correct Formulas for All Metrics**

### **1. CTR (Click-Through Rate)**
```sql
-- ✅ CORRECT
SELECT 
    Placement,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr
FROM campaigns
GROUP BY Placement

-- ❌ WRONG
SELECT Placement, AVG(CTR) as ctr FROM campaigns GROUP BY Placement
```

### **2. CPC (Cost Per Click)**
```sql
-- ✅ CORRECT
SELECT 
    Campaign_Name,
    (SUM(Spend) / SUM(Clicks)) as cpc
FROM campaigns
GROUP BY Campaign_Name

-- ❌ WRONG
SELECT Campaign_Name, AVG(CPC) as cpc FROM campaigns GROUP BY Campaign_Name
```

### **3. CPM (Cost Per Thousand Impressions)**
```sql
-- ✅ CORRECT
SELECT 
    Device,
    (SUM(Spend) / SUM(Impressions) * 1000) as cpm
FROM campaigns
GROUP BY Device

-- ❌ WRONG
SELECT Device, AVG(CPM) as cpm FROM campaigns GROUP BY Device
```

### **4. ROAS (Return on Ad Spend)**
```sql
-- ✅ CORRECT
SELECT 
    Campaign_Name,
    (SUM(Revenue) / SUM(Spend)) as roas
FROM campaigns
GROUP BY Campaign_Name

-- ❌ WRONG
SELECT Campaign_Name, AVG(ROAS) as roas FROM campaigns GROUP BY Campaign_Name
```

### **5. Conversion Rate**
```sql
-- ✅ CORRECT
SELECT 
    Age,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate
FROM campaigns
GROUP BY Age

-- ❌ WRONG
SELECT Age, AVG(Conv_Rate) as conv_rate FROM campaigns GROUP BY Age
```

### **6. CPA (Cost Per Acquisition/Conversion)**
```sql
-- ✅ CORRECT
SELECT 
    Objective,
    (SUM(Spend) / SUM(Conversions)) as cpa
FROM campaigns
GROUP BY Objective

-- ❌ WRONG
SELECT Objective, AVG(Cost_Per_Conv) as cpa FROM campaigns GROUP BY Objective
```

### **7. AOV (Average Order Value)**
```sql
-- ✅ CORRECT
SELECT 
    Gender,
    (SUM(Revenue) / SUM(Conversions)) as aov
FROM campaigns
GROUP BY Gender

-- ❌ WRONG
SELECT Gender, AVG(AOV) as aov FROM campaigns GROUP BY Gender
```

### **8. Engagement Rate**
```sql
-- ✅ CORRECT
SELECT 
    Placement,
    (SUM(Engagement) * 100.0 / SUM(Impressions)) as engagement_rate
FROM campaigns
GROUP BY Placement

-- ❌ WRONG
SELECT Placement, AVG(Engagement_Rate) as engagement_rate FROM campaigns GROUP BY Placement
```

### **9. Video View Rate**
```sql
-- ✅ CORRECT
SELECT 
    Device,
    (SUM(Video_Views) * 100.0 / SUM(Impressions)) as video_view_rate
FROM campaigns
WHERE Video_Views > 0
GROUP BY Device

-- ❌ WRONG
SELECT Device, AVG(Video_View_Rate) as video_view_rate FROM campaigns GROUP BY Device
```

### **10. Link Click Rate**
```sql
-- ✅ CORRECT
SELECT 
    Placement,
    (SUM(Link_Clicks) * 100.0 / SUM(Clicks)) as link_click_rate
FROM campaigns
GROUP BY Placement

-- ❌ WRONG
SELECT Placement, AVG(Link_Click_Rate) as link_click_rate FROM campaigns GROUP BY Placement
```

### **11. Frequency**
```sql
-- ✅ CORRECT
SELECT 
    Campaign_Name,
    (SUM(Impressions) * 1.0 / SUM(Reach)) as frequency
FROM campaigns
GROUP BY Campaign_Name

-- ❌ WRONG
SELECT Campaign_Name, AVG(Frequency) as frequency FROM campaigns GROUP BY Campaign_Name
```

### **12. Cost Per Lead (LinkedIn)**
```sql
-- ✅ CORRECT
SELECT 
    Job_Function,
    (SUM(Spend) / SUM(Leads)) as cost_per_lead
FROM campaigns
GROUP BY Job_Function

-- ❌ WRONG
SELECT Job_Function, AVG(Cost_Per_Lead) as cost_per_lead FROM campaigns GROUP BY Job_Function
```

### **13. Swipe Up Rate (Snapchat)**
```sql
-- ✅ CORRECT
SELECT 
    Placement,
    (SUM(Swipes) * 100.0 / SUM(Impressions)) as swipe_rate
FROM campaigns
GROUP BY Placement

-- ❌ WRONG
SELECT Placement, AVG(Swipe_Up_Rate) as swipe_rate FROM campaigns GROUP BY Placement
```

### **14. Quality Score (Google Ads)**
```sql
-- ⚠️ SPECIAL CASE: Quality Score is a discrete value (1-10), not a calculated metric
-- For Quality Score, you might want to use MODE or show distribution

-- Option 1: Weighted average by impressions
SELECT 
    Campaign_Name,
    SUM(Quality_Score * Impressions) / SUM(Impressions) as weighted_quality_score
FROM campaigns
GROUP BY Campaign_Name

-- Option 2: Simple average (acceptable for Quality Score)
SELECT Campaign_Name, AVG(Quality_Score) as avg_quality_score
FROM campaigns
GROUP BY Campaign_Name
```

---

## 🎯 **When Can You Use AVG()?**

### **✅ Safe to Average:**

1. **Quality Score** - Discrete rating, not a calculated ratio
2. **Frequency** - When already calculated at user level
3. **Viewability Rate** - When measured independently
4. **Watch Time** - Absolute time values

### **❌ Never Average:**

1. **CTR** - Always recalculate from clicks/impressions
2. **CPC** - Always recalculate from spend/clicks
3. **CPM** - Always recalculate from spend/impressions
4. **ROAS** - Always recalculate from revenue/spend
5. **Conversion Rate** - Always recalculate from conversions/clicks
6. **CPA** - Always recalculate from spend/conversions
7. **Any ratio or percentage derived from base metrics**

---

## 📐 **Mathematical Proof**

### **Why Averaging Ratios is Wrong**

Given two data points:
- Point 1: a₁/b₁
- Point 2: a₂/b₂

**Wrong** (Average of ratios):
```
(a₁/b₁ + a₂/b₂) / 2
```

**Correct** (Ratio of sums):
```
(a₁ + a₂) / (b₁ + b₂)
```

**These are NOT equal!**

### **Example:**
- Point 1: 10/100 = 0.10
- Point 2: 20/200 = 0.10

**Average of ratios**: (0.10 + 0.10) / 2 = 0.10 ✅ (happens to be correct)

But with different ratios:
- Point 1: 10/100 = 0.10
- Point 2: 50/200 = 0.25

**Average of ratios**: (0.10 + 0.25) / 2 = 0.175 ❌
**Ratio of sums**: (10 + 50) / (100 + 200) = 60/300 = 0.20 ✅

The correct answer is 0.20, not 0.175!

---

## 🔍 **Real-World Impact**

### **Scenario: Campaign Performance Analysis**

You're analyzing 3 campaigns:

| Campaign | Spend | Conversions | CPA (Pre-calc) |
|----------|-------|-------------|----------------|
| Brand | $100,000 | 1,000 | $100 |
| Generic | $10,000 | 200 | $50 |
| Competitor | $1,000 | 10 | $100 |

**Wrong Calculation** (Average CPA):
```sql
AVG(CPA) = ($100 + $50 + $100) / 3 = $83.33
```

**Correct Calculation** (Recalculated CPA):
```sql
Total Spend / Total Conversions = $111,000 / 1,210 = $91.74
```

**Impact**: You'd underestimate your true CPA by $8.41 per conversion!

With 1,210 conversions, that's a **$10,176 error** in your cost analysis!

---

## 📝 **Best Practices**

### **1. Always Use Base Metrics**
```sql
-- Store base metrics in your data
Impressions, Clicks, Spend, Conversions, Revenue

-- Calculate ratios in queries
(SUM(Clicks) / SUM(Impressions)) as ctr
```

### **2. Add Null Checks**
```sql
-- Prevent division by zero
CASE 
    WHEN SUM(Impressions) > 0 
    THEN (SUM(Clicks) * 100.0 / SUM(Impressions))
    ELSE 0 
END as ctr
```

### **3. Use Proper Data Types**
```sql
-- Use FLOAT or DECIMAL for ratios
(SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr  -- Note the 100.0, not 100
```

### **4. Document Your Calculations**
```sql
-- Good: Clear calculation
SELECT 
    Campaign_Name,
    SUM(Spend) as total_spend,
    SUM(Conversions) as total_conversions,
    (SUM(Spend) / SUM(Conversions)) as cpa
FROM campaigns
GROUP BY Campaign_Name
```

---

## 🎓 **Training Question Updates**

All training questions have been updated to use correct formulas:

### **Before (Wrong):**
```json
{
  "question": "Show average CTR by placement",
  "expected_sql": "SELECT Placement, AVG(CTR) as avg_ctr FROM campaigns GROUP BY Placement"
}
```

### **After (Correct):**
```json
{
  "question": "Show average CTR by placement",
  "expected_sql": "SELECT Placement, (SUM(Clicks) * 100.0 / SUM(Impressions)) as avg_ctr FROM campaigns GROUP BY Placement"
}
```

---

## ✅ **Summary**

| Metric | Formula | Never Use |
|--------|---------|-----------|
| **CTR** | `(SUM(Clicks) * 100.0 / SUM(Impressions))` | `AVG(CTR)` |
| **CPC** | `(SUM(Spend) / SUM(Clicks))` | `AVG(CPC)` |
| **CPM** | `(SUM(Spend) / SUM(Impressions) * 1000)` | `AVG(CPM)` |
| **ROAS** | `(SUM(Revenue) / SUM(Spend))` | `AVG(ROAS)` |
| **Conv Rate** | `(SUM(Conversions) * 100.0 / SUM(Clicks))` | `AVG(Conv_Rate)` |
| **CPA** | `(SUM(Spend) / SUM(Conversions))` | `AVG(CPA)` |
| **AOV** | `(SUM(Revenue) / SUM(Conversions))` | `AVG(AOV)` |

**Golden Rule**: If it's a ratio or percentage, **recalculate from base metrics**!

---

**This is a fundamental principle in data analytics and must be followed in all training questions and production queries.** 🎯
