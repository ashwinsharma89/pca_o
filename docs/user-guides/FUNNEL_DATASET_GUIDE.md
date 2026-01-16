# 📊 Funnel & Revenue Dataset Guide

## Overview

Comprehensive dataset with **full funnel metrics**, **conversion stages**, **revenue data**, and **ROAS** for advanced campaign analytics.

**File**: `data/funnel_revenue_dataset.csv`
**Rows**: 45
**Campaigns**: 8
**Platforms**: 5 (Google Ads, Meta Ads, LinkedIn Ads, DV360, Snapchat Ads)

---

## 📋 Dataset Structure

### **Dimensions (7 columns)**

1. **Campaign_Name** - Campaign identifier
   - Holiday_Sale_2024
   - Black_Friday_2024
   - Cyber_Monday_2024
   - Summer_Sale_2024
   - Back_To_School_2024
   - Spring_Launch_2024
   - Valentine_Day_2024
   - New_Year_2024

2. **Platform** - Advertising platform
   - google_ads
   - meta_ads
   - linkedin_ads
   - dv360
   - snapchat_ads

3. **Date** - Campaign date (YYYY-MM-DD)

4. **Device** - Device type
   - Desktop
   - Mobile
   - Tablet

5. **Age_Group** - Target age range
   - 18-24
   - 25-34
   - 35-44
   - 45-54

6. **Gender** - Target gender
   - Male
   - Female

### **Funnel Metrics (11 columns)**

#### **Top of Funnel (Awareness)**
7. **Impressions** - Ad views
8. **Clicks** - Ad clicks
9. **CTR** - Click-through rate (%)

#### **Middle of Funnel (Consideration)**
10. **Website_Visits** - Landing page visits
11. **Add_to_Cart** - Products added to cart
12. **Checkout_Initiated** - Checkout process started

#### **Bottom of Funnel (Conversion)**
13. **Purchases** - Completed transactions
14. **Conversion_Rate** - Purchase rate (%)

### **Cost & Revenue Metrics (5 columns)**

15. **Spend** - Total ad spend ($)
16. **CPC** - Cost per click ($)
17. **Revenue** - Total revenue generated ($)
18. **ROAS** - Return on ad spend (Revenue/Spend)
19. **AOV** - Average order value ($)

### **Efficiency Metrics (2 columns)**

20. **Cart_Abandonment_Rate** - % who abandoned cart
21. **Checkout_Abandonment_Rate** - % who abandoned checkout

---

## 🎯 Key Insights from Dataset

### **Campaign Performance**

**Best ROAS:**
- Cyber_Monday_2024: 9.68x - 10.16x
- Black_Friday_2024: 7.47x - 8.42x
- New_Year_2024: 6.77x - 8.71x

**Highest Conversion Rates:**
- Cyber_Monday_2024: 11.25% - 12.03%
- Black_Friday_2024: 9.22% - 10.42%
- Valentine_Day_2024: 10.13% - 10.82%

**Best AOV:**
- LinkedIn Ads: $190-$200
- Cyber_Monday: $165
- Back_To_School: $155

### **Device Performance**

**Mobile:**
- Higher volume (more impressions)
- Better ROAS on average
- Lower cart abandonment

**Desktop:**
- Higher CTR
- Better conversion rates
- Higher engagement quality

**Tablet:**
- Lower volume
- Moderate performance
- Higher abandonment rates

### **Demographic Insights**

**Age Groups:**
- 18-24: High engagement, impulse buying
- 25-34: Largest segment, balanced metrics
- 35-44: Higher AOV, better ROAS
- 45-54: Premium buyers, best AOV

**Gender:**
- Female: Fashion, lifestyle campaigns
- Male: Tech, B2B campaigns

---

## 📊 Funnel Analysis Examples

### **Full Funnel View**

```
Impressions → Clicks → Website Visits → Add to Cart → Checkout → Purchase

Example (Cyber Monday, Google Ads, Desktop, 25-34, Male):
2,000,000 → 46,000 → 38,500 → 13,500 → 7,200 → 5,400
  100%      2.30%     83.70%      35.06%    53.33%   75.00%
```

### **Conversion Funnel Metrics**

1. **Click-to-Visit Rate**: Website_Visits / Clicks
   - Average: ~83%
   - Best: 85%+ (high-quality traffic)

2. **Visit-to-Cart Rate**: Add_to_Cart / Website_Visits
   - Average: ~35%
   - Best: 38%+ (strong product appeal)

3. **Cart-to-Checkout Rate**: Checkout_Initiated / Add_to_Cart
   - Average: ~53%
   - Best: 55%+ (smooth cart experience)

4. **Checkout-to-Purchase Rate**: Purchases / Checkout_Initiated
   - Average: ~75%
   - Best: 77%+ (optimized checkout)

### **Abandonment Analysis**

**Cart Abandonment Rate**:
- Average: 64-72%
- Best performers: 64-66% (Cyber Monday)
- Worst performers: 74-76% (LinkedIn, Tablet)

**Checkout Abandonment Rate**:
- Average: 25-33%
- Best performers: 25-26% (Cyber Monday)
- Worst performers: 32-34% (Tablet, some Desktop)

---

## 🔍 Advanced Analysis Questions

### **Funnel Optimization**

1. "Which device has the lowest cart abandonment rate?"
2. "Show conversion rate by age group"
3. "What's the average checkout-to-purchase rate by platform?"
4. "Compare funnel drop-off between mobile and desktop"
5. "Which campaign has the best visit-to-cart conversion?"

### **Revenue Analysis**

6. "What is the total revenue by platform?"
7. "Show ROAS by device type"
8. "Which age group generates the highest AOV?"
9. "Compare revenue per click across campaigns"
10. "What's the average ROAS for campaigns with >10% conversion rate?"

### **Audience Insights**

11. "Show performance by age group and gender"
12. "Which demographic has the best ROAS?"
13. "Compare male vs female conversion rates"
14. "What's the AOV difference between age groups?"
15. "Show top performing age-gender combinations"

### **Campaign Effectiveness**

16. "Which campaign had the highest total revenue?"
17. "Compare Black Friday vs Cyber Monday performance"
18. "Show seasonal campaign ROAS trends"
19. "What's the efficiency (ROAS) by campaign and device?"
20. "Which campaign-platform combo has best conversion rate?"

### **Multi-Stage Analysis**

21. "Calculate cost per website visit by platform"
22. "Show cost per add-to-cart by campaign"
23. "What's the cost per checkout initiated?"
24. "Compare cost per purchase across devices"
25. "Show full funnel metrics for top 3 campaigns by ROAS"

---

## 💡 Sample SQL Queries

### **1. Full Funnel Metrics**
```sql
SELECT 
    Campaign_Name,
    Platform,
    SUM(Impressions) as Total_Impressions,
    SUM(Clicks) as Total_Clicks,
    SUM(Website_Visits) as Total_Visits,
    SUM(Add_to_Cart) as Total_Cart,
    SUM(Checkout_Initiated) as Total_Checkout,
    SUM(Purchases) as Total_Purchases,
    AVG(Conversion_Rate) as Avg_Conv_Rate
FROM funnel_data
GROUP BY Campaign_Name, Platform
ORDER BY Avg_Conv_Rate DESC
```

### **2. ROAS by Device**
```sql
SELECT 
    Device,
    SUM(Revenue) as Total_Revenue,
    SUM(Spend) as Total_Spend,
    SUM(Revenue) / SUM(Spend) as Overall_ROAS
FROM funnel_data
GROUP BY Device
ORDER BY Overall_ROAS DESC
```

### **3. Cart Abandonment Analysis**
```sql
SELECT 
    Platform,
    Device,
    AVG(Cart_Abandonment_Rate) as Avg_Cart_Abandonment,
    AVG(Checkout_Abandonment_Rate) as Avg_Checkout_Abandonment
FROM funnel_data
GROUP BY Platform, Device
ORDER BY Avg_Cart_Abandonment
```

### **4. AOV by Demographics**
```sql
SELECT 
    Age_Group,
    Gender,
    AVG(AOV) as Avg_Order_Value,
    SUM(Revenue) as Total_Revenue,
    SUM(Purchases) as Total_Orders
FROM funnel_data
GROUP BY Age_Group, Gender
ORDER BY Avg_Order_Value DESC
```

### **5. Best Performing Segments**
```sql
SELECT 
    Campaign_Name,
    Platform,
    Device,
    Age_Group,
    Gender,
    AVG(ROAS) as Avg_ROAS,
    AVG(Conversion_Rate) as Avg_Conv_Rate,
    SUM(Revenue) as Total_Revenue
FROM funnel_data
WHERE ROAS > 7.0
GROUP BY Campaign_Name, Platform, Device, Age_Group, Gender
ORDER BY Avg_ROAS DESC
LIMIT 10
```

---

## 📈 Calculated Metrics

### **Additional Metrics You Can Calculate**

1. **Cost Per Website Visit**: Spend / Website_Visits
2. **Cost Per Add-to-Cart**: Spend / Add_to_Cart
3. **Cost Per Checkout**: Spend / Checkout_Initiated
4. **Cost Per Purchase (CPA)**: Spend / Purchases
5. **Revenue Per Click**: Revenue / Clicks
6. **Revenue Per Visit**: Revenue / Website_Visits
7. **Profit**: Revenue - Spend
8. **Profit Margin**: (Revenue - Spend) / Revenue × 100
9. **Click-to-Visit Rate**: Website_Visits / Clicks × 100
10. **Visit-to-Cart Rate**: Add_to_Cart / Website_Visits × 100
11. **Cart-to-Checkout Rate**: Checkout_Initiated / Add_to_Cart × 100
12. **Checkout-to-Purchase Rate**: Purchases / Checkout_Initiated × 100

---

## 🎯 Use Cases

### **1. Funnel Optimization**
Identify where users drop off and optimize each stage:
- Low click-to-visit? Improve landing pages
- High cart abandonment? Fix cart UX
- High checkout abandonment? Simplify checkout

### **2. Audience Targeting**
Find best-performing demographics:
- Which age groups convert best?
- Which gender has higher AOV?
- Which device drives most revenue?

### **3. Budget Allocation**
Optimize spend based on ROAS:
- Shift budget to high-ROAS segments
- Reduce spend on low-performing combos
- Scale winning campaigns

### **4. Campaign Planning**
Learn from historical performance:
- Which campaigns work best seasonally?
- What device mix is optimal?
- Which platforms deliver best ROAS?

### **5. Revenue Forecasting**
Predict future performance:
- Average ROAS by campaign type
- Expected conversion rates
- Projected revenue by segment

---

## 🚀 Getting Started

### **Load the Dataset**

```python
import pandas as pd

# Load funnel data
df = pd.read_csv('data/funnel_revenue_dataset.csv')

# Preview
print(df.head())
print(f"\nShape: {df.shape}")
print(f"Campaigns: {df['Campaign_Name'].nunique()}")
print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
```

### **Basic Analysis**

```python
# Overall metrics
print("Overall Performance:")
print(f"Total Spend: ${df['Spend'].sum():,.0f}")
print(f"Total Revenue: ${df['Revenue'].sum():,.0f}")
print(f"Overall ROAS: {df['Revenue'].sum() / df['Spend'].sum():.2f}x")
print(f"Avg Conversion Rate: {df['Conversion_Rate'].mean():.2f}%")
print(f"Avg Cart Abandonment: {df['Cart_Abandonment_Rate'].mean():.2f}%")
```

### **Upload to Streamlit**

1. Go to PCA Agent app
2. Click "📊 CSV Data Files"
3. Upload `funnel_revenue_dataset.csv`
4. Click "Analyze Data & Generate Insights"
5. Ask funnel-specific questions!

---

## 📊 Dataset Statistics

**Total Metrics:**
- Total Spend: $2,730,000
- Total Revenue: $19,500,000+
- Overall ROAS: ~7.14x
- Total Purchases: 120,000+
- Average Conversion Rate: 9.8%
- Average Cart Abandonment: 69.2%
- Average Checkout Abandonment: 30.5%

**Campaign Breakdown:**
- 8 unique campaigns
- 5 platforms
- 3 device types
- 4 age groups
- 2 genders
- 45 unique combinations

---

## 🎓 Training Questions for This Dataset

### **Easy (10 questions)**
1. What is the total revenue across all campaigns?
2. Which platform has the highest average ROAS?
3. Show total purchases by device type
4. What is the average AOV by platform?
5. Which campaign had the lowest cart abandonment rate?
6. Show total spend by age group
7. What is the average conversion rate for mobile devices?
8. Which gender has higher total revenue?
9. Show total clicks by platform
10. What is the average checkout abandonment rate?

### **Medium (10 questions)**
11. Compare ROAS between mobile and desktop
12. Show funnel metrics (impressions to purchases) for Cyber Monday
13. Which age group has the best conversion rate?
14. Calculate cost per purchase by platform
15. Show revenue per click by device
16. Which campaign-platform combination has the best ROAS?
17. Compare cart abandonment rates across age groups
18. What percentage of total revenue came from Google Ads?
19. Show top 5 segments by total revenue
20. Calculate profit margin by campaign

### **Advanced (10 questions)**
21. Show full funnel conversion rates by device
22. Which demographic (age + gender) has the highest ROAS?
23. Compare Black Friday vs Cyber Monday across all metrics
24. Show cost per funnel stage (visit, cart, checkout, purchase)
25. Which segments have ROAS > 8x and conversion rate > 10%?
26. Calculate revenue per website visit by platform and device
27. Show correlation between cart abandonment and conversion rate
28. Which campaign has the best checkout-to-purchase rate?
29. Compare male vs female performance across all platforms
30. Show seasonal trends in ROAS and conversion rates

---

## ✅ Next Steps

1. **Load the dataset** in your analysis tool
2. **Run sample queries** to understand the data
3. **Upload to Streamlit** for AI-powered insights
4. **Ask funnel questions** in the Q&A tab
5. **Generate insights** on conversion optimization
6. **Identify opportunities** for budget reallocation
7. **Create dashboards** with funnel visualizations

---

**This dataset enables comprehensive funnel analysis with revenue attribution!** 🚀

Perfect for:
- ✅ Conversion rate optimization
- ✅ Funnel drop-off analysis
- ✅ ROAS maximization
- ✅ Audience segmentation
- ✅ Budget optimization
- ✅ Revenue forecasting
